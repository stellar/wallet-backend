package services

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/services"
	"github.com/stellar/wallet-backend/internal/signing"
	"github.com/stellar/wallet-backend/internal/signing/store"
	"github.com/stellar/wallet-backend/internal/utils"
	"github.com/stellar/wallet-backend/pkg/sorobanauth"
	pkgUtils "github.com/stellar/wallet-backend/pkg/utils"
)

const (
	MaxTimeoutInSeconds     = 300
	DefaultTimeoutInSeconds = 30
)

var ErrInvalidArguments = errors.New("invalid arguments")

type TransactionService interface {
	NetworkPassphrase() string
	BuildAndSignTransactionWithChannelAccount(ctx context.Context, operations []txnbuild.Operation, timeoutInSecs int64, simulationResult entities.RPCSimulateTransactionResult) (*txnbuild.Transaction, error)
	BuildFeeBumpTransaction(ctx context.Context, tx *txnbuild.Transaction) (*txnbuild.FeeBumpTransaction, error)
}

type transactionService struct {
	DB                                 db.ConnectionPool
	DistributionAccountSignatureClient signing.SignatureClient
	ChannelAccountSignatureClient      signing.SignatureClient
	ChannelAccountStore                store.ChannelAccountStore
	RPCService                         services.RPCService
	BaseFee                            int64
}

var _ TransactionService = (*transactionService)(nil)

type TransactionServiceOptions struct {
	DB                                 db.ConnectionPool
	DistributionAccountSignatureClient signing.SignatureClient
	ChannelAccountSignatureClient      signing.SignatureClient
	ChannelAccountStore                store.ChannelAccountStore
	RPCService                         services.RPCService
	BaseFee                            int64
}

func (o *TransactionServiceOptions) ValidateOptions() error {
	if o.DB == nil {
		return fmt.Errorf("DB cannot be nil")
	}
	if o.DistributionAccountSignatureClient == nil {
		return fmt.Errorf("distribution account signature client cannot be nil")
	}

	if o.RPCService == nil {
		return fmt.Errorf("rpc client cannot be nil")
	}

	if o.ChannelAccountSignatureClient == nil {
		return fmt.Errorf("channel account signature client cannot be nil")
	}

	if o.ChannelAccountStore == nil {
		return fmt.Errorf("channel account store cannot be nil")
	}

	if o.BaseFee < int64(txnbuild.MinBaseFee) {
		return fmt.Errorf("base fee is lower than the minimum network fee")
	}

	return nil
}

func NewTransactionService(opts TransactionServiceOptions) (*transactionService, error) {
	if err := opts.ValidateOptions(); err != nil {
		return nil, fmt.Errorf("validating transaction service options: %w", err)
	}
	return &transactionService{
		DB:                                 opts.DB,
		DistributionAccountSignatureClient: opts.DistributionAccountSignatureClient,
		ChannelAccountSignatureClient:      opts.ChannelAccountSignatureClient,
		ChannelAccountStore:                opts.ChannelAccountStore,
		RPCService:                         opts.RPCService,
		BaseFee:                            opts.BaseFee,
	}, nil
}

func (t *transactionService) NetworkPassphrase() string {
	return t.DistributionAccountSignatureClient.NetworkPassphrase()
}

func (t *transactionService) BuildAndSignTransactionWithChannelAccount(ctx context.Context, operations []txnbuild.Operation, timeoutInSecs int64, simulationResponse entities.RPCSimulateTransactionResult) (*txnbuild.Transaction, error) {
	if timeoutInSecs > MaxTimeoutInSeconds {
		return nil, fmt.Errorf("%w: timeout cannot be greater than %d seconds", ErrInvalidArguments, MaxTimeoutInSeconds)
	}
	if timeoutInSecs <= 0 {
		timeoutInSecs = DefaultTimeoutInSeconds
	}

	channelAccountPublicKey, err := t.ChannelAccountSignatureClient.GetAccountPublicKey(ctx, int(timeoutInSecs))
	if err != nil {
		return nil, fmt.Errorf("getting channel account public key: %w", err)
	}

	for _, op := range operations {
		// Prevent bad actors from using the channel account as a source account directly.
		if op.GetSourceAccount() == channelAccountPublicKey {
			return nil, fmt.Errorf("%w: operation source account cannot be the channel account public key", ErrInvalidArguments)
		}
		// Prevent bad actors from using the channel account as a source account (inherited from the parent transaction).
		if !pkgUtils.IsSorobanTxnbuildOp(op) && op.GetSourceAccount() == "" {
			return nil, fmt.Errorf("%w: operation source account cannot be empty", ErrInvalidArguments)
		}
	}

	channelAccountSeq, err := t.RPCService.GetAccountLedgerSequence(channelAccountPublicKey)
	if err != nil {
		return nil, fmt.Errorf("getting ledger sequence for channel account public key %q: %w", channelAccountPublicKey, err)
	}

	buildTxParams := txnbuild.TransactionParams{
		SourceAccount: &txnbuild.SimpleAccount{
			AccountID: channelAccountPublicKey,
			Sequence:  channelAccountSeq,
		},
		Operations: operations,
		BaseFee:    int64(t.BaseFee),
		Preconditions: txnbuild.Preconditions{
			TimeBounds: txnbuild.NewTimeout(timeoutInSecs),
		},
		IncrementSequenceNum: true,
	}
	// Adjust the transaction params with the soroban-related information (the `Ext` field):
	buildTxParams, err = t.adjustParamsForSoroban(ctx, channelAccountPublicKey, buildTxParams, simulationResponse)
	if err != nil {
		return nil, fmt.Errorf("handling soroban flows: %w", err)
	}

	tx, err := txnbuild.NewTransaction(buildTxParams)
	if err != nil {
		return nil, fmt.Errorf("building transaction: %w", err)
	}
	txHash, err := tx.HashHex(t.ChannelAccountSignatureClient.NetworkPassphrase())
	if err != nil {
		return nil, fmt.Errorf("unable to hashhex transaction: %w", err)
	}

	err = t.ChannelAccountStore.AssignTxToChannelAccount(ctx, channelAccountPublicKey, txHash)
	if err != nil {
		return nil, fmt.Errorf("assigning channel account to tx: %w", err)
	}

	tx, err = t.ChannelAccountSignatureClient.SignStellarTransaction(ctx, tx, channelAccountPublicKey)
	if err != nil {
		return nil, fmt.Errorf("signing transaction with channel account: %w", err)
	}

	return tx, nil
}

// adjustParamsForSoroban will use the `simulationResponse` to set the `Ext` field in the sorobanOp, in case the transaction
// is a soroban transaction. The resulting `buildTxParams` will be later used by `txnbuild.NewTransaction` to:
// - Calculate the total fee.
// - Include the `Ext` information to the transaction envelope.
func (t *transactionService) adjustParamsForSoroban(_ context.Context, channelAccountPublicKey string, buildTxParams txnbuild.TransactionParams, simulationResponse entities.RPCSimulateTransactionResult) (txnbuild.TransactionParams, error) {
	// Ensure this is a soroban transaction.
	operations := buildTxParams.Operations
	isSoroban := slices.ContainsFunc(operations, func(op txnbuild.Operation) bool {
		return pkgUtils.IsSorobanTxnbuildOp(op)
	})
	if !isSoroban {
		return buildTxParams, nil
	}

	// When soroban is used, only one operation is allowed.
	if len(operations) != 1 {
		return txnbuild.TransactionParams{}, fmt.Errorf("%w: Soroban transactions require exactly one operation but %d were provided", ErrInvalidArguments, len(operations))
	}

	if utils.IsEmpty(simulationResponse) {
		return txnbuild.TransactionParams{}, fmt.Errorf("%w: simulation response cannot be empty", ErrInvalidArguments)
	} else if simulationResponse.Error != "" {
		return txnbuild.TransactionParams{}, fmt.Errorf("%w: transaction simulation failed with error=%s", ErrInvalidArguments, simulationResponse.Error)
	}

	// Check if the channel account public key is used as a source account for any SourceAccount auth entry.
	err := sorobanauth.CheckForForbiddenSigners(simulationResponse.Results, operations[0].GetSourceAccount(), channelAccountPublicKey)
	if err != nil {
		return txnbuild.TransactionParams{}, fmt.Errorf("ensuring the channel account is not being misused: %w", err)
	}

	// 👋 This is the main goal of this method: setting the `Ext` field in the sorobanOp.
	transactionExt, err := xdr.NewTransactionExt(1, simulationResponse.TransactionData)
	if err != nil {
		return txnbuild.TransactionParams{}, fmt.Errorf("unable to create transaction ext: %w", err)
	}

	switch sorobanOp := operations[0].(type) {
	case *txnbuild.InvokeHostFunction:
		sorobanOp.Ext = transactionExt
	case *txnbuild.ExtendFootprintTtl:
		sorobanOp.Ext = transactionExt
	case *txnbuild.RestoreFootprint:
		sorobanOp.Ext = transactionExt
	default:
		return txnbuild.TransactionParams{}, fmt.Errorf("%w: operation type %T is not a supported soroban operation", ErrInvalidArguments, operations[0])
	}

	// Adjust the base fee to ensure the total fee computed by `txnbuild.NewTransaction` (baseFee+sorobanFee) will stay
	// within the limits configured by the host.
	sorobanFee := int64(transactionExt.SorobanData.ResourceFee)
	adjustedBaseFee := int64(buildTxParams.BaseFee) - sorobanFee
	buildTxParams.BaseFee = int64(math.Max(float64(adjustedBaseFee), float64(txnbuild.MinBaseFee)))

	return buildTxParams, nil
}

func (t *transactionService) BuildFeeBumpTransaction(ctx context.Context, tx *txnbuild.Transaction) (*txnbuild.FeeBumpTransaction, error) {
	distributionAccountPublicKey, err := t.DistributionAccountSignatureClient.GetAccountPublicKey(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting distribution account public key: %w", err)
	}
	feeBumpTx, err := txnbuild.NewFeeBumpTransaction(
		txnbuild.FeeBumpTransactionParams{
			Inner:      tx,
			FeeAccount: distributionAccountPublicKey,
			BaseFee:    int64(t.BaseFee),
		},
	)
	if err != nil {
		return nil, fmt.Errorf("building fee-bump transaction %w", err)
	}

	feeBumpTx, err = t.DistributionAccountSignatureClient.SignStellarFeeBumpTransaction(ctx, feeBumpTx)
	if err != nil {
		return nil, fmt.Errorf("signing the fee bump transaction with distribution account: %w", err)
	}
	return feeBumpTx, nil
}
