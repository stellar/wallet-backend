package services

import (
	"context"
	"errors"
	"fmt"

	"github.com/stellar/go-stellar-sdk/txnbuild"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/signing"
)

var (
	ErrAccountNotFound                     = errors.New("account not found")
	ErrAccountNotEligibleForBeingSponsored = errors.New("account not eligible for being sponsored")
	ErrFeeExceedsMaximumBaseFee            = errors.New("fee exceeds maximum base fee to sponsor")
	ErrNoSignaturesProvided                = errors.New("should have at least one signature")
)

type FeeBumpService interface {
	WrapTransaction(ctx context.Context, tx *txnbuild.Transaction) (string, string, error)
	GetMaximumBaseFee() int64
}

type feeBumpService struct {
	DistributionAccountSignatureClient signing.SignatureClient
	BaseFee                            int64
	Models                             *data.Models
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

	if tx.BaseFee() > s.BaseFee {
		return "", "", ErrFeeExceedsMaximumBaseFee
	}

	// For Soroban inner transactions the base fee alone does not reflect the total cost — the ResourceFee sits on the
	// transaction's Ext and is added on top. `tx.MaxFee()` on a parsed tx returns the envelope's `Fee` field which
	// for Soroban already equals `BaseFee*innerOps + ResourceFee`; we compare it directly to the per-op sponsor cap
	// (`s.BaseFee * innerOps`) to match the bound enforced by `validateSorobanResourceFee` in the build path.
	//
	// This catches envelopes that bypass the build path (i.e., client hand-crafted a Soroban envelope and submitted
	// it directly to the fee-bump endpoint); txs that went through the build path already had their ResourceFee
	// capped, so they land at exactly `s.BaseFee * innerOps` and pass the check.
	if _, ok := innerSorobanResourceFee(tx); ok {
		innerOps := int64(len(tx.Operations()))
		if innerOps == 0 {
			innerOps = 1
		}
		innerMaxFee := tx.MaxFee()
		sponsorCap := s.BaseFee * innerOps
		if innerMaxFee > sponsorCap {
			return "", "", fmt.Errorf("%w: soroban inner max fee %d exceeds sponsor cap %d",
				ErrFeeExceedsMaximumBaseFee, innerMaxFee, sponsorCap)
		}
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
	}, nil
}

// innerSorobanResourceFee returns the transaction-level Soroban ResourceFee declared in the inner transaction's Ext,
// if present. Classic (non-Soroban) transactions return (0, false).
func innerSorobanResourceFee(tx *txnbuild.Transaction) (int64, bool) {
	envelope := tx.ToXDR()
	v1, ok := envelope.GetV1()
	if !ok {
		return 0, false
	}
	if v1.Tx.Ext.V != 1 || v1.Tx.Ext.SorobanData == nil {
		return 0, false
	}
	if envelope.Type != xdr.EnvelopeTypeEnvelopeTypeTx {
		return 0, false
	}
	return int64(v1.Tx.Ext.SorobanData.ResourceFee), true
}
