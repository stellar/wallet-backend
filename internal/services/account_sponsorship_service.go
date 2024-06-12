package services

import (
	"context"
	"errors"
	"fmt"

	"github.com/stellar/go/clients/horizonclient"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/signing"
)

var (
	ErrAccountAlreadyExists     = errors.New("account already exists")
	ErrSponsorshipLimitExceeded = errors.New("sponsorship limit exceeded")
)

const (
	// Sufficient to cover three average ledger close time.
	CreateAccountTxnTimeBounds             = 18
	CreateAccountTxnTimeBoundsSafetyMargin = 12
)

type AccountSponsorshipService interface {
	SponsorAccountCreationTransaction(ctx context.Context, address string, signers []entities.Signer, supportedAssets []entities.Asset) (string, string, error)
}

type accountSponsorshipService struct {
	SignatureClient          signing.SignatureClient
	HorizonClient            horizonclient.ClientInterface
	MaxSponsoredBaseReserves int
	BaseFee                  int64
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

	fullSignerThreshold := txnbuild.NewThreshold(txnbuild.Threshold(fullSignerWeight))
	ops := []txnbuild.Operation{
		&txnbuild.BeginSponsoringFutureReserves{
			SponsoredID:   accountToSponsor,
			SourceAccount: s.SignatureClient.GetDistributionAccountPublicKey(),
		},
		&txnbuild.CreateAccount{
			Destination:   accountToSponsor,
			Amount:        "0",
			SourceAccount: s.SignatureClient.GetDistributionAccountPublicKey(),
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

	// TODO: use Channel Accounts instead of using the Distribution Account.
	distributionAccount, err := s.HorizonClient.AccountDetail(horizonclient.AccountRequest{AccountID: s.SignatureClient.GetDistributionAccountPublicKey()})
	if err != nil {
		return "", "", fmt.Errorf("getting distribution account details: %w", err)
	}

	tx, err := txnbuild.NewTransaction(
		txnbuild.TransactionParams{
			SourceAccount:        &distributionAccount,
			IncrementSequenceNum: true,
			Operations:           ops,
			BaseFee:              s.BaseFee,
			Preconditions:        txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(CreateAccountTxnTimeBounds + CreateAccountTxnTimeBoundsSafetyMargin)},
		},
	)
	if err != nil {
		return "", "", fmt.Errorf("building transaction: %w", err)
	}

	tx, err = s.SignatureClient.SignStellarTransaction(ctx, tx)
	if err != nil {
		return "", "", fmt.Errorf("signing transaction: %w", err)
	}

	txe, err := tx.Base64()
	if err != nil {
		return "", "", fmt.Errorf("getting transaction envelope: %w", err)
	}

	return txe, s.SignatureClient.NetworkPassphrase(), nil
}

func NewAccountSponsorshipService(signatureClient signing.SignatureClient, horizonClient horizonclient.ClientInterface, maxSponsoredBaseReserves int, baseFee int64) (*accountSponsorshipService, error) {
	if signatureClient == nil {
		return nil, fmt.Errorf("signature client cannot be nil")
	}

	if horizonClient == nil {
		return nil, fmt.Errorf("horizon client cannot be nil")
	}

	if baseFee < int64(txnbuild.MinBaseFee) {
		return nil, fmt.Errorf("base fee is lower than the minimum network fee")
	}

	return &accountSponsorshipService{
		SignatureClient:          signatureClient,
		HorizonClient:            horizonClient,
		MaxSponsoredBaseReserves: maxSponsoredBaseReserves,
		BaseFee:                  baseFee,
	}, nil
}
