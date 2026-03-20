package services

import (
	"context"
	"fmt"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// getLedgerWithRetry fetches a ledger with exponential backoff retry logic.
// It respects context cancellation and limits retries to maxLedgerFetchRetries attempts.
func getLedgerWithRetry(ctx context.Context, backend ledgerbackend.LedgerBackend, ledgerSeq uint32) (xdr.LedgerCloseMeta, error) {
	var lastErr error
	for attempt := 0; attempt < maxLedgerFetchRetries; attempt++ {
		select {
		case <-ctx.Done():
			return xdr.LedgerCloseMeta{}, fmt.Errorf("context cancelled: %w", ctx.Err())
		default:
		}

		ledgerMeta, err := backend.GetLedger(ctx, ledgerSeq)
		if err == nil {
			return ledgerMeta, nil
		}
		lastErr = err

		backoff := time.Duration(1<<attempt) * time.Second
		if backoff > maxRetryBackoff {
			backoff = maxRetryBackoff
		}
		log.Ctx(ctx).Warnf("Error fetching ledger %d (attempt %d/%d): %v, retrying in %v...",
			ledgerSeq, attempt+1, maxLedgerFetchRetries, err, backoff)

		select {
		case <-ctx.Done():
			return xdr.LedgerCloseMeta{}, fmt.Errorf("context cancelled during backoff: %w", ctx.Err())
		case <-time.After(backoff):
		}
	}
	return xdr.LedgerCloseMeta{}, fmt.Errorf("failed after %d attempts: %w", maxLedgerFetchRetries, lastErr)
}

// buildProtocolProcessorMap converts a slice of ProtocolProcessors into a map keyed by protocol ID,
// validating that no entries are nil and no IDs are duplicated.
func buildProtocolProcessorMap(processors []ProtocolProcessor) (map[string]ProtocolProcessor, error) {
	ppMap := make(map[string]ProtocolProcessor, len(processors))
	for i, p := range processors {
		if p == nil {
			return nil, fmt.Errorf("protocol processor at index %d is nil", i)
		}
		id := p.ProtocolID()
		if _, exists := ppMap[id]; exists {
			return nil, fmt.Errorf("duplicate protocol processor ID %q", id)
		}
		ppMap[id] = p
	}
	return ppMap, nil
}

// protocolHistoryCursorName returns the ingest_store key for a protocol's history migration cursor.
func protocolHistoryCursorName(protocolID string) string {
	return fmt.Sprintf("protocol_%s_history_cursor", protocolID)
}

// protocolCurrentStateCursorName returns the ingest_store key for a protocol's current state cursor.
func protocolCurrentStateCursorName(protocolID string) string {
	return fmt.Sprintf("protocol_%s_current_state_cursor", protocolID)
}
