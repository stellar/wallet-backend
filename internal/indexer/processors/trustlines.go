// TrustlinesProcessor extracts trustline changes from ledger entries for balance tracking.
// It processes ledger changes directly (like Horizon) to capture ALL trustline modifications including payments.
package processors

import (
	"context"
	"fmt"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/wallet-backend/internal/indexer/types"
)

// TrustlinesProcessor processes ledger changes to extract trustline modifications.
// Unlike effects-based processing, this captures ALL trustline changes including balance
// updates from payments, path payments, and other operations that modify trustline balances.
type TrustlinesProcessor struct {
	metricsService MetricsServiceInterface
}

// NewTrustlinesProcessor creates a new trustlines processor.
func NewTrustlinesProcessor(metricsService MetricsServiceInterface) *TrustlinesProcessor {
	return &TrustlinesProcessor{
		metricsService: metricsService,
	}
}

// Name returns the processor name for logging and metrics.
func (p *TrustlinesProcessor) Name() string {
	return "trustlines"
}

// ProcessOperation extracts trustline changes from an operation's ledger changes.
// Returns TrustlineChange structs with full XDR data for database upsert.
func (p *TrustlinesProcessor) ProcessOperation(ctx context.Context, opWrapper *TransactionOperationWrapper) ([]types.TrustlineChange, error) {
	startTime := time.Now()
	defer func() {
		if p.metricsService != nil {
			duration := time.Since(startTime).Seconds()
			p.metricsService.ObserveStateChangeProcessingDuration("TrustlinesProcessor", duration)
		}
	}()

	changes, err := opWrapper.Transaction.GetOperationChanges(opWrapper.Index)
	if err != nil {
		return nil, fmt.Errorf("getting operation changes: %w", err)
	}

	var trustlineChanges []types.TrustlineChange

	for _, change := range changes {
		if change.Type != xdr.LedgerEntryTypeTrustline {
			continue
		}

		tlChange, skip, err := p.processTrustlineChange(change, opWrapper)
		if err != nil {
			return nil, err
		}
		if skip {
			continue
		}

		trustlineChanges = append(trustlineChanges, tlChange)
	}

	return trustlineChanges, nil
}

// processTrustlineChange converts a ledger change to a TrustlineChange.
// Returns (change, skip, error) where skip=true means the change should be ignored.
func (p *TrustlinesProcessor) processTrustlineChange(change ingest.Change, opWrapper *TransactionOperationWrapper) (types.TrustlineChange, bool, error) {
	var tlChange types.TrustlineChange

	// Determine operation type and get the relevant entry
	var entry *xdr.LedgerEntry
	switch {
	case change.Pre == nil && change.Post != nil:
		// Created
		tlChange.Operation = types.TrustlineOpAdd
		entry = change.Post
	case change.Pre != nil && change.Post != nil:
		// Updated
		tlChange.Operation = types.TrustlineOpUpdate
		entry = change.Post
	case change.Pre != nil && change.Post == nil:
		// Removed
		tlChange.Operation = types.TrustlineOpRemove
		entry = change.Pre
	default:
		return tlChange, true, nil // Invalid change, skip
	}

	trustLine := entry.Data.MustTrustLine()

	// Skip liquidity pool shares - we only track regular asset trustlines
	if trustLine.Asset.Type == xdr.AssetTypeAssetTypePoolShare {
		return tlChange, true, nil
	}

	// Extract asset code and issuer
	asset := trustLine.Asset.ToAsset()

	var assetCode, assetIssuer string
	assetType := asset.Type
	if err := asset.Extract(&assetType, &assetCode, &assetIssuer); err != nil {
		return tlChange, false, fmt.Errorf("extracting asset details: %w", err)
	}

	// Extract liabilities
	liabilities := trustLine.Liabilities()

	// Build the TrustlineChange
	tlChange.AccountID = trustLine.AccountId.Address()
	tlChange.Asset = fmt.Sprintf("%s:%s", assetCode, assetIssuer)
	tlChange.OperationID = opWrapper.ID()
	tlChange.LedgerNumber = opWrapper.Transaction.Ledger.LedgerSequence()
	tlChange.Balance = int64(trustLine.Balance)
	tlChange.Limit = int64(trustLine.Limit)
	tlChange.BuyingLiabilities = int64(liabilities.Buying)
	tlChange.SellingLiabilities = int64(liabilities.Selling)
	tlChange.Flags = uint32(trustLine.Flags)

	return tlChange, false, nil
}
