package services

import (
	"context"
	"errors"
	"fmt"
	"slices"

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
	ErrAccountNotFound                     = errors.New("account not found")
)

type OperationNotAllowedError struct {
	OperationType xdr.OperationType
}

func (e OperationNotAllowedError) Error() string {
	return fmt.Sprintf("operation %s not allowed", e.OperationType.String())
}

const (
	// Sufficient to cover three average ledger close time.
	CreateAccountTxnTimeBounds             = 18
	CreateAccountTxnTimeBoundsSafetyMargin = 12
)

type SponsorAccountCreationOptions struct {
	Address            string            `json:"address"                        validate:"required,public_key"`
	Assets             []entities.Asset  `json:"assets"                         validate:"dive"`
	Signers            []entities.Signer `json:"signers"                        validate:"required,gt=0,dive"`
	MasterSignerWeight *int              `json:"master_signer_weight,omitempty" validate:"omitempty,gt=0"`
}

type AccountSponsorshipService interface {
	SponsorAccountCreationTransaction(ctx context.Context, opts SponsorAccountCreationOptions) (string, error)
	WrapTransaction(ctx context.Context, tx *txnbuild.Transaction) (string, string, error)
}

type accountSponsorshipService struct {
	DistributionAccountSignatureClient signing.SignatureClient
	ChannelAccountSignatureClient      signing.SignatureClient
	RPCService                         RPCService
	MaxSponsoredBaseReserves           int
	BaseFee                            int64
	Models                             *data.Models
	BlockedOperationsTypes             []xdr.OperationType
}

var _ AccountSponsorshipService = (*accountSponsorshipService)(nil)

func (s *accountSponsorshipService) SponsorAccountCreationTransaction(ctx context.Context, opts SponsorAccountCreationOptions) (string, error) {
	// Validation: accountToSponsor does not exist on Stellar
	_, err := s.RPCService.GetAccountLedgerSequence(opts.Address)
	if err == nil {
		return "", ErrAccountAlreadyExists
	}
	if !errors.Is(err, ErrAccountNotFound) {
		return "", fmt.Errorf("getting details for account %s: %w", opts.Address, err)
	}

	// Validation: signers weights are valid
	fullSignerWeight, err := entities.ValidateSignersWeights(opts.Signers)
	if err != nil {
		return "", fmt.Errorf("validating signers weights: %w", err)
	}
	for _, signer := range opts.Signers {
		if signer.Address == opts.Address {
			return "", fmt.Errorf("master signer cannot be configured as a custom signer. Please configure the master signer weight instead.")
		}
	}

	// Validation: the total number of entries does not exceed the numSponsoredThreshold
	numEntries := 2 + len(opts.Assets) + len(opts.Signers) // 2 entries for account creation + 1 entry per asset and signer
	if numEntries > s.MaxSponsoredBaseReserves {
		return "", ErrSponsorshipLimitExceeded
	}

	distributionAccountPublicKey, err := s.DistributionAccountSignatureClient.GetAccountPublicKey(ctx)
	if err != nil {
		return "", fmt.Errorf("getting distribution account public key: %w", err)
	}

	ops := []txnbuild.Operation{
		&txnbuild.BeginSponsoringFutureReserves{
			SponsoredID:   opts.Address,
			SourceAccount: distributionAccountPublicKey,
		},
		&txnbuild.CreateAccount{
			Destination:   opts.Address,
			Amount:        "0",
			SourceAccount: distributionAccountPublicKey,
		},
	}

	// Add the signers to the transaction
	for _, signer := range opts.Signers {
		ops = append(ops, &txnbuild.SetOptions{
			Signer: &txnbuild.Signer{
				Address: signer.Address,
				Weight:  txnbuild.Threshold(signer.Weight),
			},
			SourceAccount: opts.Address,
		})
	}

	// Add the assets to the transaction
	for _, asset := range opts.Assets {
		ops = append(ops, &txnbuild.ChangeTrust{
			Line: txnbuild.CreditAsset{
				Code:   asset.Code,
				Issuer: asset.Issuer,
			}.MustToChangeTrustAsset(),
			SourceAccount: opts.Address,
		})
	}
	ops = append(ops,
		&txnbuild.EndSponsoringFutureReserves{
			SourceAccount: opts.Address,
		},
	)

	if len(opts.Signers) > 0 {
		fullSignerThreshold := txnbuild.NewThreshold(txnbuild.Threshold(fullSignerWeight))
		setOptionsOp := txnbuild.SetOptions{
			LowThreshold:    fullSignerThreshold,
			MediumThreshold: fullSignerThreshold,
			HighThreshold:   fullSignerThreshold,
			SourceAccount:   opts.Address,
		}
		if opts.MasterSignerWeight != nil {
			setOptionsOp.MasterWeight = txnbuild.NewThreshold(txnbuild.Threshold(*opts.MasterSignerWeight))
		}
		ops = append(ops, &setOptionsOp)
	}

	channelAccountPublicKey, err := s.ChannelAccountSignatureClient.GetAccountPublicKey(ctx)
	if err != nil {
		return "", fmt.Errorf("getting channel account public key: %w", err)
	}

	channelAccountSeq, err := s.RPCService.GetAccountLedgerSequence(channelAccountPublicKey)
	if err != nil {
		return "", fmt.Errorf("getting sequence number for channel account public key: %s: %w", channelAccountPublicKey, err)
	}

	tx, err := txnbuild.NewTransaction(
		txnbuild.TransactionParams{
			SourceAccount: &txnbuild.SimpleAccount{
				AccountID: channelAccountPublicKey,
				Sequence:  channelAccountSeq,
			},
			IncrementSequenceNum: true,
			Operations:           ops,
			BaseFee:              s.BaseFee,
			Preconditions:        txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(CreateAccountTxnTimeBounds + CreateAccountTxnTimeBoundsSafetyMargin)},
		},
	)
	if err != nil {
		return "", fmt.Errorf("building transaction: %w", err)
	}

	tx, err = s.ChannelAccountSignatureClient.SignStellarTransaction(ctx, tx, channelAccountPublicKey)
	if err != nil {
		return "", fmt.Errorf("signing transaction: %w", err)
	}

	tx, err = s.DistributionAccountSignatureClient.SignStellarTransaction(ctx, tx, distributionAccountPublicKey)
	if err != nil {
		return "", fmt.Errorf("signing transaction: %w", err)
	}

	txe, err := tx.Base64()
	if err != nil {
		return "", fmt.Errorf("getting transaction envelope: %w", err)
	}

	if err := s.Models.Account.Insert(ctx, opts.Address); err != nil {
		return "", fmt.Errorf("inserting the sponsored account: %w", err)
	}

	return txe, nil
}

// WrapTransaction wraps a stellar transaction with a fee bump transaction with the configured distribution account as the fee account.
func (s *accountSponsorshipService) WrapTransaction(ctx context.Context, tx *txnbuild.Transaction) (string, string, error) {
	isFeeBumpEligible, err := s.Models.Account.IsAccountFeeBumpEligible(ctx, tx.SourceAccount().AccountID)
	if err != nil {
		return "", "", fmt.Errorf("checking if transaction source account is eligible for being fee-bumped: %w", err)
	}
	if !isFeeBumpEligible {
		return "", "", ErrAccountNotEligibleForBeingSponsored
	}

	for _, op := range tx.Operations() {
		operationXDR, innerErr := op.BuildXDR()
		if innerErr != nil {
			return "", "", fmt.Errorf("retrieving xdr for operation: %w", innerErr)
		}

		if slices.Contains(s.BlockedOperationsTypes, operationXDR.Body.Type) {
			log.Ctx(ctx).Warnf("blocked operation type: %s", operationXDR.Body.Type.String())
			return "", "", &OperationNotAllowedError{OperationType: operationXDR.Body.Type}
		}
	}

	if tx.BaseFee() > s.BaseFee {
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
			BaseFee:    s.BaseFee,
		},
	)
	if err != nil {
		return "", "", fmt.Errorf("creating fee-bump transaction: %w", err)
	}

	signedFeeBumpTx, err := s.DistributionAccountSignatureClient.SignStellarFeeBumpTransaction(ctx, feeBumpTx)
	if err != nil {
		return "", "", fmt.Errorf("signing fee-bump transaction: %w", err)
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
	RPCService                         RPCService
	MaxSponsoredBaseReserves           int
	BaseFee                            int64
	Models                             *data.Models
	BlockedOperationsTypes             []xdr.OperationType
}

func (o *AccountSponsorshipServiceOptions) Validate() error {
	if o.DistributionAccountSignatureClient == nil {
		return fmt.Errorf("distribution account signature client cannot be nil")
	}

	if o.RPCService == nil {
		return fmt.Errorf("rpc client cannot be nil")
	}

	if o.ChannelAccountSignatureClient == nil {
		return fmt.Errorf("channel account signature client cannot be nil")
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
		return nil, fmt.Errorf("validating account sponsorship service options: %w", err)
	}

	return &accountSponsorshipService{
		DistributionAccountSignatureClient: opts.DistributionAccountSignatureClient,
		ChannelAccountSignatureClient:      opts.ChannelAccountSignatureClient,
		RPCService:                         opts.RPCService,
		MaxSponsoredBaseReserves:           opts.MaxSponsoredBaseReserves,
		BaseFee:                            opts.BaseFee,
		Models:                             opts.Models,
		BlockedOperationsTypes:             opts.BlockedOperationsTypes,
	}, nil
}
