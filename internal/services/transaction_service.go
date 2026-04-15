package services

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/txnbuild"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/signing"
	"github.com/stellar/wallet-backend/internal/signing/store"
	"github.com/stellar/wallet-backend/internal/utils"
	"github.com/stellar/wallet-backend/pkg/sorobanauth"
	pkgUtils "github.com/stellar/wallet-backend/pkg/utils"
)

const (
	MaxTimeoutInSeconds     = 300
	DefaultTimeoutInSeconds = 30

	// sorobanResourceFeeSafetyFactor multiplies the server-authoritative MinResourceFee to produce the upper bound
	// we accept from the client. This absorbs normal simulation drift while preventing inflated ResourceFee values
	// from draining the sponsor account. The value is also capped by the operator-configured BaseFee.
	sorobanResourceFeeSafetyFactor = 2
)

var (
	ErrInvalidTimeout                 = errors.New("invalid timeout: timeout cannot be greater than maximum allowed seconds")
	ErrInvalidOperationChannelAccount = errors.New("invalid operation: operation source account cannot be the channel account")
	ErrInvalidOperationMissingSource  = errors.New("invalid operation: operation source account cannot be empty for non-Soroban operations")
	ErrInvalidSorobanOperationCount   = errors.New("invalid Soroban transaction: must have exactly one operation")
	ErrInvalidSorobanSimulationEmpty  = errors.New("invalid Soroban transaction: simulation response cannot be empty")
	ErrInvalidSorobanSimulationFailed = errors.New("invalid Soroban transaction: simulation failed")
	ErrInvalidSorobanOperationType    = errors.New("invalid Soroban transaction: operation type not supported")
	ErrSorobanResourceFeeExceedsBound = errors.New("invalid Soroban transaction: client-declared resource fee exceeds the allowed bound")
)

type TransactionService interface {
	NetworkPassphrase() string
	BuildAndSignTransactionWithChannelAccount(ctx context.Context, transaction *txnbuild.GenericTransaction, simulationResult *entities.RPCSimulateTransactionResult) (*txnbuild.Transaction, error)
}

type transactionService struct {
	DB                                 *pgxpool.Pool
	DistributionAccountSignatureClient signing.SignatureClient
	ChannelAccountSignatureClient      signing.SignatureClient
	ChannelAccountStore                store.ChannelAccountStore
	RPCService                         RPCService
	BaseFee                            int64
}

var _ TransactionService = (*transactionService)(nil)

type TransactionServiceOptions struct {
	DB                                 *pgxpool.Pool
	DistributionAccountSignatureClient signing.SignatureClient
	ChannelAccountSignatureClient      signing.SignatureClient
	ChannelAccountStore                store.ChannelAccountStore
	RPCService                         RPCService
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

func (t *transactionService) BuildAndSignTransactionWithChannelAccount(ctx context.Context, transaction *txnbuild.GenericTransaction, simulationResponse *entities.RPCSimulateTransactionResult) (signedTx *txnbuild.Transaction, retErr error) {
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
		// Resolve the operation source to a G-address to handle muxed accounts (M-addresses)
		// that wrap the same underlying Ed25519 key as the channel account.
		opSourceGAddress, resolveErr := pkgUtils.ResolveToGAddress(op.GetSourceAccount())
		if resolveErr != nil {
			return nil, fmt.Errorf("resolving operation source account: %w", resolveErr)
		}
		if opSourceGAddress == channelAccountPublicKey {
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
		BaseFee:              t.BaseFee,
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

	// After AssignTxToChannelAccount succeeds, the channel row is locked to this tx hash until the ingest path
	// confirms the tx on ledger (see internal/services/ingest_live.go:unlockChannelAccounts) or the locked_until
	// TTL expires. If anything below fails, the channel would otherwise be held hostage for the full TTL. Release on error.
	//
	// Use a cancel-detached context so the cleanup still runs when the caller's request context is cancelled
	// (client disconnect, request timeout); otherwise the DB op inside releaseChannelAccountLock would inherit
	// the cancellation and leave the channel locked until the locked_until TTL expires.
	unlockCtx := context.WithoutCancel(ctx)
	defer func() {
		if retErr != nil {
			t.releaseChannelAccountLock(unlockCtx, txHash)
		}
	}()

	tx, err = t.ChannelAccountSignatureClient.SignStellarTransaction(ctx, tx, channelAccountPublicKey)
	if err != nil {
		return nil, fmt.Errorf("signing transaction with channel account: %w", err)
	}

	return tx, nil
}

// releaseChannelAccountLock is a best-effort unlock of the channel account previously assigned to txHash. Failures
// are logged but not surfaced: the caller is already unwinding with a more meaningful error, and the lock will
// eventually expire via the channel's locked_until TTL even if this release fails.
func (t *transactionService) releaseChannelAccountLock(ctx context.Context, txHash string) {
	releaseErr := db.RunInTransaction(ctx, t.DB, func(pgxTx pgx.Tx) error {
		if _, err := t.ChannelAccountStore.UnassignTxAndUnlockChannelAccounts(ctx, pgxTx, txHash); err != nil {
			return fmt.Errorf("unassigning channel account for tx %s: %w", txHash, err)
		}
		return nil
	})
	if releaseErr != nil {
		log.Ctx(ctx).Errorf("failed to release channel account lock for tx %s: %v", txHash, releaseErr)
	}
}

// adjustParamsForSoroban will use the `simulationResponse` to set the `Ext` field in the sorobanOp, in case the transaction
// is a soroban transaction. The resulting `buildTxParams` will be later used by `txnbuild.NewTransaction` to:
// - Calculate the total fee.
// - Include the `Ext` information to the transaction envelope.
//
// Before the fee is accepted, this method re-simulates the transaction server-side and rejects any client-declared
// `ResourceFee` that exceeds `min(serverMinResourceFee * sorobanResourceFeeSafetyFactor, t.BaseFee)`.
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

	// Bound the client-declared ResourceFee against a server-side re-simulation.
	clientResourceFee := int64(transactionExt.SorobanData.ResourceFee)
	if err := t.validateSorobanResourceFee(buildTxParams, clientResourceFee); err != nil {
		return txnbuild.TransactionParams{}, err
	}

	// Adjust the base fee so the total computed by `txnbuild.NewTransaction` (BaseFee*ops + ResourceFee) stays within
	// the sponsor's per-op cap. The floor at MinBaseFee is protocol-required; validateSorobanResourceFee has already
	// ensured clientResourceFee leaves enough headroom that adjustedBaseFee >= MinBaseFee, so the flooring is a no-op
	// in practice and total ≤ t.BaseFee is provable rather than asymptotic.
	adjustedBaseFee := buildTxParams.BaseFee - clientResourceFee
	if adjustedBaseFee < int64(txnbuild.MinBaseFee) {
		adjustedBaseFee = int64(txnbuild.MinBaseFee)
	}
	buildTxParams.BaseFee = adjustedBaseFee

	return buildTxParams, nil
}

// validateSorobanResourceFee re-simulates the Soroban transaction against the wallet-backend's own Stellar RPC and
// rejects the request if the client-declared resource fee exceeds the bound
// `min(serverMinResourceFee * sorobanResourceFeeSafetyFactor, t.BaseFee - MinBaseFee*ops)`.
//
// The `t.BaseFee - MinBaseFee*ops` term (rather than plain t.BaseFee) reserves room for the protocol-required base-fee
// floor after adjustParamsForSoroban subtracts the resource fee, guaranteeing the total envelope fee stays within the
// sponsor's configured per-op cap.
func (t *transactionService) validateSorobanResourceFee(buildTxParams txnbuild.TransactionParams, clientResourceFee int64) error {
	// Build a throw-away transaction for simulation. Copy the source account so the real build path's sequence is
	// untouched. IncrementSequenceNum matches the real build path so the simulated tx has the exact sequence the
	// submitted tx will have.
	var simSourceAccount txnbuild.SimpleAccount
	if src, ok := buildTxParams.SourceAccount.(*txnbuild.SimpleAccount); ok && src != nil {
		simSourceAccount = *src
	} else {
		return fmt.Errorf("unexpected source account type for soroban re-simulation: %T", buildTxParams.SourceAccount)
	}

	simTx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount:        &simSourceAccount,
		Operations:           buildTxParams.Operations,
		BaseFee:              txnbuild.MinBaseFee,
		Preconditions:        buildTxParams.Preconditions,
		Memo:                 buildTxParams.Memo,
		IncrementSequenceNum: true,
	})
	if err != nil {
		return fmt.Errorf("building soroban re-simulation transaction: %w", err)
	}

	simTxXDR, err := simTx.Base64()
	if err != nil {
		return fmt.Errorf("serializing soroban re-simulation transaction: %w", err)
	}

	serverSim, err := t.RPCService.SimulateTransaction(simTxXDR, entities.RPCResourceConfig{})
	if err != nil {
		return fmt.Errorf("server-side re-simulation of soroban transaction: %w", err)
	}
	if serverSim.Error != "" {
		return fmt.Errorf("server-side re-simulation failed: %s", serverSim.Error)
	}

	serverMinResourceFee, err := strconv.ParseInt(serverSim.MinResourceFee, 10, 64)
	if err != nil {
		return fmt.Errorf("parsing server-side re-simulation MinResourceFee %q: %w", serverSim.MinResourceFee, err)
	}

	// Reserve MinBaseFee per op for the base fee portion so that after adjustParamsForSoroban subtracts resource
	// fee and floors at MinBaseFee, the total `BaseFee*ops + ResourceFee` provably stays ≤ t.BaseFee.
	opsCount := int64(len(buildTxParams.Operations))
	if opsCount < 1 {
		opsCount = 1
	}
	baseFeeCap := t.BaseFee - int64(txnbuild.MinBaseFee)*opsCount
	if baseFeeCap < 0 {
		baseFeeCap = 0
	}

	maxAllowedResourceFee := serverMinResourceFee * sorobanResourceFeeSafetyFactor
	if baseFeeCap < maxAllowedResourceFee {
		maxAllowedResourceFee = baseFeeCap
	}

	if clientResourceFee > maxAllowedResourceFee {
		return fmt.Errorf("%w: client resource fee %d exceeds allowed maximum %d (server MinResourceFee %d, base fee cap %d)",
			ErrSorobanResourceFeeExceedsBound, clientResourceFee, maxAllowedResourceFee, serverMinResourceFee, t.BaseFee)
	}

	return nil
}
