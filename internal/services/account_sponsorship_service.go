package services

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/stellar/go/clients/horizonclient"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"
	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/signing"
)

var (
	ErrAccountAlreadyExists                = errors.New("account already exists")
	ErrSponsorshipLimitExceeded            = errors.New("sponsorship limit exceeded")
	ErrAccountNotEligibleForBeingSponsored = errors.New("account not eligible for being sponsored")
	ErrFeeExceedsMaximumBaseFee            = errors.New("fee exceeds maximum base fee to sponsor")
	ErrNoSignaturesProvided                = errors.New("should have at least one signature")
)

type ErrOperationNotAllowed struct {
	OperationType xdr.OperationType
}

func (e ErrOperationNotAllowed) Error() string {
	return fmt.Sprintf("operation %s not allowed", e.OperationType.String())
}

const (
	// Sufficient to cover three average ledger close time.
	CreateAccountTxnTimeBounds             = 18
	CreateAccountTxnTimeBoundsSafetyMargin = 12
)

type AccountSponsorshipService interface {
	SponsorAccountCreationTransaction(ctx context.Context, address string, signers []entities.Signer, supportedAssets []entities.Asset) (string, string, error)
	WrapTransaction(ctx context.Context, tx *txnbuild.Transaction, blockedOperationsTypes []xdr.OperationType) (string, string, error)
}

type accountSponsorshipService struct {
	DistributionAccountSignatureClient signing.SignatureClient
	ChannelAccountSignatureClient      signing.SignatureClient
	HorizonClient                      horizonclient.ClientInterface
	MaxSponsoredBaseReserves           int
	BaseFee                            int64
	Models                             *data.Models
}

var _ AccountSponsorshipService = (*accountSponsorshipService)(nil)

func (s *accountSponsorshipService) SponsorAccountCreationTransaction(ctx context.Context, accountToSponsor string, signers []entities.Signer, supportedAssets []entities.Asset) (string, string, error) {
	// Check the accountToSponsor does not exist on Stellar
	_, err := s.HorizonClient.AccountDetail(horizonclient.AccountRequest{AccountID: accountToSponsor})
	if err == nil {
		return "", "", ErrAccountAlreadyExists
	}
	if !horizonclient.IsNotFoundError(err) {
		return "", "", fmt.Errorf("getting details for account %s: %w", accountToSponsor, err)
	}

	fullSignerWeight, err := entities.ValidateSignersWeights(signers)
	if err != nil {
		return "", "", err
	}

	// Make sure the total number of entries does not exceed the numSponsoredThreshold
	numEntries := 2 + len(supportedAssets) + len(signers) // 2 entries for account creation + 1 entry per supported asset + 1 entry per signer
	if numEntries > s.MaxSponsoredBaseReserves {
		return "", "", ErrSponsorshipLimitExceeded
	}

	distributionAccountPublicKey, err := s.DistributionAccountSignatureClient.GetAccountPublicKey(ctx)
	if err != nil {
		return "", "", fmt.Errorf("getting distribution account public key: %w", err)
	}

	fullSignerThreshold := txnbuild.NewThreshold(txnbuild.Threshold(fullSignerWeight))
	ops := []txnbuild.Operation{
		&txnbuild.BeginSponsoringFutureReserves{
			SponsoredID:   accountToSponsor,
			SourceAccount: distributionAccountPublicKey,
		},
		&txnbuild.CreateAccount{
			Destination:   accountToSponsor,
			Amount:        "0",
			SourceAccount: distributionAccountPublicKey,
		},
	}
	for _, signer := range signers {
		ops = append(ops, &txnbuild.SetOptions{
			Signer:        &txnbuild.Signer{Address: signer.Address, Weight: txnbuild.Threshold(signer.Weight)},
			SourceAccount: accountToSponsor,
		})
	}
	for _, asset := range supportedAssets {
		ops = append(ops, &txnbuild.ChangeTrust{
			Line: txnbuild.CreditAsset{
				Code:   asset.Code,
				Issuer: asset.Issuer,
			}.MustToChangeTrustAsset(),
			SourceAccount: accountToSponsor,
		})
	}
	ops = append(ops,
		&txnbuild.EndSponsoringFutureReserves{
			SourceAccount: accountToSponsor,
		},
		&txnbuild.SetOptions{
			MasterWeight:    txnbuild.NewThreshold(0),
			LowThreshold:    fullSignerThreshold,
			MediumThreshold: fullSignerThreshold,
			HighThreshold:   fullSignerThreshold,
			SourceAccount:   accountToSponsor,
		},
	)

	channelAccountPublicKey, err := s.ChannelAccountSignatureClient.GetAccountPublicKey(ctx)
	if err != nil {
		return "", "", fmt.Errorf("getting channel account public key: %w", err)
	}

	channelAccount, err := s.HorizonClient.AccountDetail(horizonclient.AccountRequest{AccountID: channelAccountPublicKey})
	if err != nil {
		return "", "", fmt.Errorf("getting distribution account details: %w", err)
	}

	tx, err := txnbuild.NewTransaction(
		txnbuild.TransactionParams{
			SourceAccount:        &channelAccount,
			IncrementSequenceNum: true,
			Operations:           ops,
			BaseFee:              s.BaseFee,
			Preconditions:        txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(CreateAccountTxnTimeBounds + CreateAccountTxnTimeBoundsSafetyMargin)},
		},
	)
	if err != nil {
		return "", "", fmt.Errorf("building transaction: %w", err)
	}

	tx, err = s.ChannelAccountSignatureClient.SignStellarTransaction(ctx, tx, channelAccountPublicKey)
	if err != nil {
		return "", "", fmt.Errorf("signing transaction: %w", err)
	}

	tx, err = s.DistributionAccountSignatureClient.SignStellarTransaction(ctx, tx, distributionAccountPublicKey)
	if err != nil {
		return "", "", fmt.Errorf("signing transaction: %w", err)
	}

	txe, err := tx.Base64()
	if err != nil {
		return "", "", fmt.Errorf("getting transaction envelope: %w", err)
	}

	if err := s.Models.Account.Insert(ctx, accountToSponsor); err != nil {
		return "", "", fmt.Errorf("inserting the sponsored account: %w", err)
	}

	return txe, s.ChannelAccountSignatureClient.NetworkPassphrase(), nil
}

// WrapTransaction wraps a stellar transaction with a fee bump transaction with the configured distribution account as the fee account.
func (s *accountSponsorshipService) WrapTransaction(ctx context.Context, tx *txnbuild.Transaction, blockedOperationsTypes []xdr.OperationType) (string, string, error) {
	isFeeBumpEligible, err := s.Models.Account.IsAccountFeeBumpEligible(ctx, tx.SourceAccount().AccountID)
	if err != nil {
		return "", "", fmt.Errorf("checking if transaction source account is eligible for being fee-bumped: %w", err)
	}
	if !isFeeBumpEligible {
		return "", "", ErrAccountNotEligibleForBeingSponsored
	}

	for _, op := range tx.Operations() {
		operationXDR, err := op.BuildXDR()
		if err != nil {
			return "", "", fmt.Errorf("retrieving xdr for operation: %w", err)
		}

		if slices.Contains(blockedOperationsTypes, operationXDR.Body.Type) {
			log.Ctx(ctx).Warnf("blocked operation type: %s", operationXDR.Body.Type.String())
			return "", "", &ErrOperationNotAllowed{OperationType: operationXDR.Body.Type}
		}
	}

	if tx.BaseFee() > int64(s.BaseFee) {
		return "", "", ErrFeeExceedsMaximumBaseFee
	}

	sigs := tx.Signatures()
	if len(sigs) == 0 {
		return "", "", ErrNoSignaturesProvided
	}

	distributionAccountPublicKey, err := s.DistributionAccountSignatureClient.GetAccountPublicKey(ctx)
	if err != nil {
		return "", "", fmt.Errorf("getting distribution account public key: %w", err)
	}

	feeBumpTx, err := txnbuild.NewFeeBumpTransaction(
		txnbuild.FeeBumpTransactionParams{
			Inner:      tx,
			FeeAccount: distributionAccountPublicKey,
			BaseFee:    int64(s.BaseFee),
		},
	)
	if err != nil {
		return "", "", fmt.Errorf("creating fee-bump transaction: %w", err)
	}

	signedFeeBumpTx, err := s.DistributionAccountSignatureClient.SignStellarFeeBumpTransaction(ctx, feeBumpTx)
	if err != nil {
		return "", "", fmt.Errorf("signing fee bump transaction: %w", err)
	}

	feeBumpTxe, err := signedFeeBumpTx.Base64()
	if err != nil {
		return "", "", fmt.Errorf("getting transaction envelope: %w", err)
	}

	return feeBumpTxe, s.DistributionAccountSignatureClient.NetworkPassphrase(), nil
}

type AccountSponsorshipServiceOptions struct {
	DistributionAccountSignatureClient signing.SignatureClient
	ChannelAccountSignatureClient      signing.SignatureClient
	HorizonClient                      horizonclient.ClientInterface
	MaxSponsoredBaseReserves           int
	BaseFee                            int64
	Models                             *data.Models
}

func (o *AccountSponsorshipServiceOptions) Validate() error {
	if o.DistributionAccountSignatureClient == nil {
		return fmt.Errorf("distribution account signature client cannot be nil")
	}

	if o.ChannelAccountSignatureClient == nil {
		return fmt.Errorf("channel account signature client cannot be nil")
	}

	if o.HorizonClient == nil {
		return fmt.Errorf("horizon client cannot be nil")
	}

	if o.BaseFee < int64(txnbuild.MinBaseFee) {
		return fmt.Errorf("base fee is lower than the minimum network fee")
	}

	if o.Models == nil {
		return fmt.Errorf("models cannot be nil")
	}

	return nil
}

func NewAccountSponsorshipService(opts AccountSponsorshipServiceOptions) (*accountSponsorshipService, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	return &accountSponsorshipService{
		DistributionAccountSignatureClient: opts.DistributionAccountSignatureClient,
		ChannelAccountSignatureClient:      opts.ChannelAccountSignatureClient,
		HorizonClient:                      opts.HorizonClient,
		MaxSponsoredBaseReserves:           opts.MaxSponsoredBaseReserves,
		BaseFee:                            opts.BaseFee,
		Models:                             opts.Models,
	}, nil
}
