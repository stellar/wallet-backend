// Liquidity pool processors extract pool-share balances and pool reserves from ledger changes.
//
// Pool-share balances live on pool_share trustlines (per-account shares); pool reserves live on
// LiquidityPoolEntry ledger entries (constituent assets + amounts). The two are ingested separately
// and joined at query time. Both processors read ledger changes directly (like the trustline and
// account processors) to capture ALL modifications, including those from deposits and withdrawals.
package processors

import (
	"context"
	"fmt"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
)

// LiquidityPoolSharesProcessor extracts an account's pool-share balances from pool_share
// trustline changes.
type LiquidityPoolSharesProcessor struct {
	metricsService *metrics.IngestionMetrics
}

// NewLiquidityPoolSharesProcessor creates a new liquidity-pool-shares processor.
func NewLiquidityPoolSharesProcessor(metricsService *metrics.IngestionMetrics) *LiquidityPoolSharesProcessor {
	return &LiquidityPoolSharesProcessor{
		metricsService: metricsService,
	}
}

// Name returns the processor name for logging and metrics.
func (p *LiquidityPoolSharesProcessor) Name() string {
	return "liquidity_pool_shares"
}

// ProcessOperation extracts pool-share balance changes from an operation's ledger changes.
func (p *LiquidityPoolSharesProcessor) ProcessOperation(ctx context.Context, opWrapper *TransactionOperationWrapper) ([]types.LiquidityPoolShareChange, error) {
	startTime := time.Now()
	defer func() {
		if p.metricsService != nil {
			duration := time.Since(startTime).Seconds()
			p.metricsService.StateChangeProcessingDuration.WithLabelValues("LiquidityPoolSharesProcessor").Observe(duration)
		}
	}()

	changes, err := opWrapper.Transaction.GetOperationChanges(opWrapper.Index)
	if err != nil {
		return nil, fmt.Errorf("getting operation changes: %w", err)
	}

	var shareChanges []types.LiquidityPoolShareChange
	for _, change := range changes {
		if change.Type != xdr.LedgerEntryTypeTrustline {
			continue
		}

		shareChange, skip := p.processShareChange(change, opWrapper)
		if skip {
			continue
		}
		shareChanges = append(shareChanges, shareChange)
	}

	return shareChanges, nil
}

// processShareChange converts a pool_share trustline change to a LiquidityPoolShareChange.
// Returns skip=true for non-pool-share trustlines (regular asset trustlines are handled by
// TrustlinesProcessor) and for invalid changes.
func (p *LiquidityPoolSharesProcessor) processShareChange(change ingest.Change, opWrapper *TransactionOperationWrapper) (types.LiquidityPoolShareChange, bool) {
	var shareChange types.LiquidityPoolShareChange

	var entry *xdr.LedgerEntry
	switch {
	case change.Pre == nil && change.Post != nil:
		shareChange.Operation = types.LiquidityPoolShareOpAdd
		entry = change.Post
	case change.Pre != nil && change.Post != nil:
		shareChange.Operation = types.LiquidityPoolShareOpUpdate
		entry = change.Post
	case change.Pre != nil && change.Post == nil:
		shareChange.Operation = types.LiquidityPoolShareOpRemove
		entry = change.Pre
	default:
		return shareChange, true // Invalid change, skip
	}

	trustLine := entry.Data.MustTrustLine()
	if trustLine.Asset.Type != xdr.AssetTypeAssetTypePoolShare {
		return shareChange, true // Regular asset trustline, not a pool share
	}

	shareChange.AccountID = trustLine.AccountId.Address()
	shareChange.PoolID = PoolIDToString(*trustLine.Asset.LiquidityPoolId)
	shareChange.OperationID = opWrapper.ID()
	shareChange.LedgerNumber = opWrapper.Transaction.Ledger.LedgerSequence()
	shareChange.Shares = int64(trustLine.Balance)

	return shareChange, false
}

// LiquidityPoolsProcessor extracts constant-product pool reserves from LiquidityPoolEntry changes.
type LiquidityPoolsProcessor struct {
	metricsService *metrics.IngestionMetrics
}

// NewLiquidityPoolsProcessor creates a new liquidity-pools processor.
func NewLiquidityPoolsProcessor(metricsService *metrics.IngestionMetrics) *LiquidityPoolsProcessor {
	return &LiquidityPoolsProcessor{
		metricsService: metricsService,
	}
}

// Name returns the processor name for logging and metrics.
func (p *LiquidityPoolsProcessor) Name() string {
	return "liquidity_pools"
}

// ProcessOperation extracts pool reserve changes from an operation's ledger changes.
func (p *LiquidityPoolsProcessor) ProcessOperation(ctx context.Context, opWrapper *TransactionOperationWrapper) ([]types.LiquidityPoolChange, error) {
	startTime := time.Now()
	defer func() {
		if p.metricsService != nil {
			duration := time.Since(startTime).Seconds()
			p.metricsService.StateChangeProcessingDuration.WithLabelValues("LiquidityPoolsProcessor").Observe(duration)
		}
	}()

	changes, err := opWrapper.Transaction.GetOperationChanges(opWrapper.Index)
	if err != nil {
		return nil, fmt.Errorf("getting operation changes: %w", err)
	}

	var poolChanges []types.LiquidityPoolChange
	for _, change := range changes {
		if change.Type != xdr.LedgerEntryTypeLiquidityPool {
			continue
		}

		poolChange, skip := p.processPoolChange(change, opWrapper)
		if skip {
			continue
		}
		poolChanges = append(poolChanges, poolChange)
	}

	return poolChanges, nil
}

// processPoolChange converts a LiquidityPoolEntry change to a LiquidityPoolChange.
// Returns skip=true for invalid changes and non-constant-product pools (the only pool type today).
func (p *LiquidityPoolsProcessor) processPoolChange(change ingest.Change, opWrapper *TransactionOperationWrapper) (types.LiquidityPoolChange, bool) {
	var poolChange types.LiquidityPoolChange

	var entry *xdr.LedgerEntry
	switch {
	case change.Pre == nil && change.Post != nil:
		poolChange.Operation = types.LiquidityPoolOpAdd
		entry = change.Post
	case change.Pre != nil && change.Post != nil:
		poolChange.Operation = types.LiquidityPoolOpUpdate
		entry = change.Post
	case change.Pre != nil && change.Post == nil:
		poolChange.Operation = types.LiquidityPoolOpRemove
		entry = change.Pre
	default:
		return poolChange, true // Invalid change, skip
	}

	pool := entry.Data.MustLiquidityPool()
	cp, ok := pool.Body.GetConstantProduct()
	if !ok {
		return poolChange, true // Only constant-product pools exist today
	}

	poolChange.PoolID = PoolIDToString(pool.LiquidityPoolId)
	poolChange.OperationID = opWrapper.ID()
	poolChange.LedgerNumber = opWrapper.Transaction.Ledger.LedgerSequence()
	poolChange.AssetA = cp.Params.AssetA.StringCanonical()
	poolChange.ReserveA = int64(cp.ReserveA)
	poolChange.AssetB = cp.Params.AssetB.StringCanonical()
	poolChange.ReserveB = int64(cp.ReserveB)

	return poolChange, false
}
