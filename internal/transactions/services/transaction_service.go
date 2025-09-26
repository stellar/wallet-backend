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

var (
	ErrInvalidTimeout                 = errors.New("invalid timeout: timeout cannot be greater than maximum allowed seconds")
	ErrInvalidOperationChannelAccount = errors.New("invalid operation: operation source account cannot be the channel account")
	ErrInvalidOperationMissingSource  = errors.New("invalid operation: operation source account cannot be empty for non-Soroban operations")
	ErrInvalidSorobanOperationCount   = errors.New("invalid Soroban transaction: must have exactly one operation")
	ErrInvalidSorobanSimulationEmpty  = errors.New("invalid Soroban transaction: simulation response cannot be empty")
	ErrInvalidSorobanSimulationFailed = errors.New("invalid Soroban transaction: simulation failed")
	ErrInvalidSorobanOperationType    = errors.New("invalid Soroban transaction: operation type not supported")
)

type TransactionService interface {
	NetworkPassphrase() string
	BuildAndSignTransactionWithChannelAccount(ctx context.Context, transaction *txnbuild.GenericTransaction, simulationResult *entities.RPCSimulateTransactionResult) (*txnbuild.Transaction, error)
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

func (t *transactionService) BuildAndSignTransactionWithChannelAccount(ctx context.Context, transaction *txnbuild.GenericTransaction, simulationResponse *entities.RPCSimulateTransactionResult) (*txnbuild.Transaction, error) {
	timeoutInSecs := DefaultTimeoutInSeconds
	channelAccountPublicKey, err := t.ChannelAccountSignatureClient.GetAccountPublicKey(ctx, timeoutInSecs)
	if err != nil {
		return nil, fmt.Errorf("getting channel account public key: %w", err)
	}

	// Extract the inner transaction (handle both regular and fee-bump transactions)
	var clientTx *txnbuild.Transaction
	if feeBumpTx, ok := transaction.FeeBump(); ok {
		clientTx = feeBumpTx.InnerTransaction()
	} else if tx, ok := transaction.Transaction(); ok {
		clientTx = tx
	} else {
		return nil, fmt.Errorf("invalid transaction type in XDR")
	}

	// Validate operations
	operations := clientTx.Operations()
	for _, op := range operations {
		// Prevent bad actors from using the channel account as a source account directly.
		if op.GetSourceAccount() == channelAccountPublicKey {
			return nil, fmt.Errorf("%w: %s", ErrInvalidOperationChannelAccount, channelAccountPublicKey)
		}
		// Prevent bad actors from using the channel account as a source account (inherited from the parent transaction).
		if !pkgUtils.IsSorobanTxnbuildOp(op) && op.GetSourceAccount() == "" {
			return nil, ErrInvalidOperationMissingSource
		}
	}

	channelAccountSeq, err := t.RPCService.GetAccountLedgerSequence(channelAccountPublicKey)
	if err != nil {
		return nil, fmt.Errorf("getting ledger sequence for channel account public key %q: %w", channelAccountPublicKey, err)
	}

	walletBackendTimeBounds := txnbuild.NewTimeout(int64(MaxTimeoutInSeconds))
	preconditions := txnbuild.Preconditions{}
	err = preconditions.FromXDR(clientTx.ToXDR().Preconditions())
	if err != nil {
		return nil, fmt.Errorf("parsing preconditions: %w", err)
	}

	// We need to restrict the max time to allow freeing channel accounts sooner and not
	// blocking them until user specified max time.
	if preconditions.TimeBounds.MaxTime != 0 {
		if preconditions.TimeBounds.MaxTime > walletBackendTimeBounds.MaxTime {
			preconditions.TimeBounds.MaxTime = walletBackendTimeBounds.MaxTime
		}
	}

	buildTxParams := txnbuild.TransactionParams{
		SourceAccount: &txnbuild.SimpleAccount{
			AccountID: channelAccountPublicKey,
			Sequence:  channelAccountSeq,
		},
		Operations:           operations,
		BaseFee:              min(t.BaseFee, clientTx.BaseFee()),
		Memo:                 clientTx.Memo(),
		Preconditions:        preconditions,
		IncrementSequenceNum: true,
	}

	// Adjust the transaction params with the soroban-related information (the `Ext` field):
	if simulationResponse != nil {
		buildTxParams, err = t.adjustParamsForSoroban(ctx, channelAccountPublicKey, buildTxParams, *simulationResponse)
		if err != nil {
			return nil, fmt.Errorf("handling soroban flows: %w", err)
		}
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
		return txnbuild.TransactionParams{}, fmt.Errorf("%w (%d provided)", ErrInvalidSorobanOperationCount, len(operations))
	}

	if utils.IsEmpty(simulationResponse) {
		return txnbuild.TransactionParams{}, ErrInvalidSorobanSimulationEmpty
	} else if simulationResponse.Error != "" {
		return txnbuild.TransactionParams{}, fmt.Errorf("%w: %s", ErrInvalidSorobanSimulationFailed, simulationResponse.Error)
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
		return txnbuild.TransactionParams{}, fmt.Errorf("%w: %T", ErrInvalidSorobanOperationType, operations[0])
	}

	// Adjust the base fee to ensure the total fee computed by `txnbuild.NewTransaction` (baseFee+sorobanFee) will stay
	// within the limits configured by the host.
	sorobanFee := int64(transactionExt.SorobanData.ResourceFee)
	adjustedBaseFee := buildTxParams.BaseFee - sorobanFee
	buildTxParams.BaseFee = int64(math.Max(float64(adjustedBaseFee), float64(txnbuild.MinBaseFee)))

	return buildTxParams, nil
}
