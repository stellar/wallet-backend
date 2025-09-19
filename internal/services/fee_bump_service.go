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
	"github.com/stellar/wallet-backend/internal/signing"
)

var (
	ErrAccountNotFound                     = errors.New("account not found")
	ErrAccountNotEligibleForBeingSponsored = errors.New("account not eligible for being sponsored")
	ErrFeeExceedsMaximumBaseFee            = errors.New("fee exceeds maximum base fee to sponsor")
	ErrNoSignaturesProvided                = errors.New("should have at least one signature")
)

type OperationNotAllowedError struct {
	OperationType xdr.OperationType
}

func (e OperationNotAllowedError) Error() string {
	return fmt.Sprintf("operation %s not allowed", e.OperationType.String())
}

type FeeBumpService interface {
	WrapTransaction(ctx context.Context, tx *txnbuild.Transaction) (string, string, error)
	GetMaximumBaseFee() int64
}

type feeBumpService struct {
	DistributionAccountSignatureClient signing.SignatureClient
	BaseFee                            int64
	Models                             *data.Models
	BlockedOperationsTypes             []xdr.OperationType
}

var _ FeeBumpService = (*feeBumpService)(nil)

// WrapTransaction wraps a stellar transaction with a fee bump transaction with the configured distribution account as the fee account.
func (s *feeBumpService) WrapTransaction(ctx context.Context, tx *txnbuild.Transaction) (string, string, error) {
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

// GetMaximumBaseFee returns the maximum base fee allowed for fee bump transactions
func (s *feeBumpService) GetMaximumBaseFee() int64 {
	return s.BaseFee
}

type FeeBumpServiceOptions struct {
	DistributionAccountSignatureClient signing.SignatureClient
	BaseFee                            int64
	Models                             *data.Models
	BlockedOperationsTypes             []xdr.OperationType
}

func (o *FeeBumpServiceOptions) Validate() error {
	if o.DistributionAccountSignatureClient == nil {
		return fmt.Errorf("distribution account signature client cannot be nil")
	}

	if o.BaseFee < int64(txnbuild.MinBaseFee) {
		return fmt.Errorf("base fee is lower than the minimum network fee")
	}

	if o.Models == nil {
		return fmt.Errorf("models cannot be nil")
	}

	return nil
}

func NewFeeBumpService(opts FeeBumpServiceOptions) (*feeBumpService, error) {
	if err := opts.Validate(); err != nil {
		return nil, fmt.Errorf("validating fee bump service options: %w", err)
	}

	return &feeBumpService{
		DistributionAccountSignatureClient: opts.DistributionAccountSignatureClient,
		BaseFee:                            opts.BaseFee,
		Models:                             opts.Models,
		BlockedOperationsTypes:             opts.BlockedOperationsTypes,
	}, nil
}
