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
	ErrMissingSorobanTransactionData  = errors.New("invalid Soroban transaction: missing SorobanTransactionData in operation Ext field")
	ErrInvalidSorobanOperationType    = errors.New("invalid Soroban transaction: operation type not supported")
	ErrSorobanResourceFeeExceedsBound = errors.New("invalid Soroban transaction: client-declared resource fee exceeds the allowed bound")
)

type TransactionService interface {
	NetworkPassphrase() string
	BuildAndSignTransactionWithChannelAccount(ctx context.Context, transaction *txnbuild.GenericTransaction) (*txnbuild.Transaction, error)
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

func (t *transactionService) BuildAndSignTransactionWithChannelAccount(ctx context.Context, transaction *txnbuild.GenericTransaction) (signedTx *txnbuild.Transaction, retErr error) {
	timeoutInSecs := DefaultTimeoutInSeconds
	channelAccountPublicKey, err := t.ChannelAccountSignatureClient.GetAccountPublicKey(ctx, timeoutInSecs)
	if err != nil {
		return nil, fmt.Errorf("getting channel account public key: %w", err)
	}

	// GetAccountPublicKey already set locked_until in the DB. If anything below fails before the tx is submitted,
	// the row would otherwise be held hostage for the full locked_until TTL — an authenticated caller who flooded
	// buildTransaction with requests that fail during Soroban validation (or any later build step) could drain the
	// pool. Register release-on-error here so every failure path unlocks the account. The unlock matches by
	// public_key (not locked_tx_hash) because that column may still be NULL — AssignTxToChannelAccount runs later.
	//
	// Use a cancel-detached context so cleanup still runs when the caller's request context is cancelled
	// (client disconnect, request timeout).
	unlockCtx := context.WithoutCancel(ctx)
	defer func() {
		if retErr != nil {
			t.releaseChannelAccountLockByPublicKey(unlockCtx, channelAccountPublicKey)
		}
	}()

	// Extract the inner transaction (handle both regular and fee-bump transactions)
	var clientTx *txnbuild.Transaction
	if feeBumpTx, ok := transaction.FeeBump(); ok {
		clientTx = feeBumpTx.InnerTransaction()
	} else if tx, ok := transaction.Transaction(); ok {
		clientTx = tx
	} else {
		return nil, fmt.Errorf("invalid transaction type in XDR")
	}

	// Extract operations and propagate the transaction-level Ext (SorobanTransactionData) to
	// Soroban operations. The SDK's TransactionFromXDR only copies Ext to InvokeHostFunction;
	// ExtendFootprintTtl and RestoreFootprint need it too but their FromXDR doesn't set it.
	operations := clientTx.Operations()
	if txEnv := clientTx.ToXDR(); txEnv.V1 != nil && txEnv.V1.Tx.Ext.V != 0 {
		txExt := txEnv.V1.Tx.Ext
		for _, op := range operations {
			switch sorobanOp := op.(type) {
			case *txnbuild.ExtendFootprintTtl:
				if sorobanOp.Ext.V == 0 {
					sorobanOp.Ext = txExt
				}
			case *txnbuild.RestoreFootprint:
				if sorobanOp.Ext.V == 0 {
					sorobanOp.Ext = txExt
				}
			}
		}
	}

	// Validate operations
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

	// Adjust the transaction params for Soroban operations (validates auth entries, adjusts fees).
	// This is a no-op for non-Soroban transactions.
	buildTxParams, err = t.adjustParamsForSoroban(ctx, channelAccountPublicKey, buildTxParams)
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

// releaseChannelAccountLockByPublicKey is a best-effort unlock of the channel account by public key. Failures
// are logged but not surfaced: the caller is already unwinding with a more meaningful error, and the lock will
// eventually expire via the channel's locked_until TTL even if this release fails.
//
// Matches by public_key rather than locked_tx_hash because the defer that calls this fires for every failure
// path after GetAndLockIdleChannelAccount — including those before AssignTxToChannelAccount runs, when
// locked_tx_hash is still NULL.
func (t *transactionService) releaseChannelAccountLockByPublicKey(ctx context.Context, publicKey string) {
	releaseErr := db.RunInTransaction(ctx, t.DB, func(pgxTx pgx.Tx) error {
		if _, err := t.ChannelAccountStore.UnlockChannelAccountByPublicKey(ctx, pgxTx, publicKey); err != nil {
			return fmt.Errorf("unlocking channel account %s: %w", publicKey, err)
		}
		return nil
	})
	if releaseErr != nil {
		log.Ctx(ctx).Errorf("failed to release channel account lock for %s: %v", publicKey, releaseErr)
	}
}

// adjustParamsForSoroban extracts SorobanTransactionData and auth entries from the Soroban operation
// (already set by the client after simulation) and adjusts the transaction params accordingly.
// The resulting `buildTxParams` will be later used by `txnbuild.NewTransaction` to:
// - Calculate the total fee.
// - Include the `Ext` information to the transaction envelope.
//
// Before the fee is accepted, this method re-simulates the transaction server-side and rejects any client-declared
// `ResourceFee` that exceeds `min(serverMinResourceFee * sorobanResourceFeeSafetyFactor, t.BaseFee)`.
func (t *transactionService) adjustParamsForSoroban(_ context.Context, channelAccountPublicKey string, buildTxParams txnbuild.TransactionParams) (txnbuild.TransactionParams, error) {
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

	// Extract SorobanTransactionData and auth entries from the operation itself.
	// The client is expected to have already incorporated the simulation response into the transaction.
	var sorobanData xdr.SorobanTransactionData
	var authEntries []xdr.SorobanAuthorizationEntry

	switch sorobanOp := operations[0].(type) {
	case *txnbuild.InvokeHostFunction:
		sd, ok := sorobanOp.Ext.GetSorobanData()
		if !ok {
			return txnbuild.TransactionParams{}, ErrMissingSorobanTransactionData
		}
		sorobanData = sd
		authEntries = sorobanOp.Auth
	case *txnbuild.ExtendFootprintTtl:
		sd, ok := sorobanOp.Ext.GetSorobanData()
		if !ok {
			return txnbuild.TransactionParams{}, ErrMissingSorobanTransactionData
		}
		sorobanData = sd
	case *txnbuild.RestoreFootprint:
		sd, ok := sorobanOp.Ext.GetSorobanData()
		if !ok {
			return txnbuild.TransactionParams{}, ErrMissingSorobanTransactionData
		}
		sorobanData = sd
	default:
		return txnbuild.TransactionParams{}, fmt.Errorf("%w: %T", ErrInvalidSorobanOperationType, operations[0])
	}

	// Check if the channel account public key is used as a signer in any auth entry.
	if err := sorobanauth.CheckForForbiddenSigners(authEntries, operations[0].GetSourceAccount(), channelAccountPublicKey); err != nil {
		return txnbuild.TransactionParams{}, fmt.Errorf("ensuring the channel account is not being misused: %w", err)
	}

	// Bound the client-declared ResourceFee against a server-side re-simulation. Without this the caller can inflate
	// `sorobanData.ResourceFee` arbitrarily — the floor below would silently absorb it, but the fee-bump wrapper
	// would still commit distribution-account funds up to the fee-bump ceiling.
	clientResourceFee := int64(sorobanData.ResourceFee)
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
