package processors

import (
	"context"
	"encoding/hex"
	"fmt"
	"sort"
	"time"

	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
)

// WasmClassifier matches WASM bytecode against a registered protocol's spec.
// It mirrors services.ProtocolValidator + services.WasmSpecExtractor combined
// into a single interface so this package doesn't have to import services
// (which would create a cycle: services already imports from indexer/types).
type WasmClassifier interface {
	// Classify returns the matched protocol id (empty string if no match)
	// for the given WASM bytecode.
	Classify(ctx context.Context, wasmCode []byte) (protocolID string, err error)
}

// ProtocolWasmProcessor extracts WASM hashes from ContractCode ledger entries
// and, when a classifier is provided, validates each new WASM against the
// registered protocol validators so that `protocol_wasms.protocol_id` is
// populated immediately on upload — matching the behavior the design doc
// specifies for live ingestion.
type ProtocolWasmProcessor struct {
	metricsService *metrics.IngestionMetrics
	classifier     WasmClassifier
}

// NewProtocolWasmProcessor creates a protocol WASM processor.
// Pass a non-nil classifier to enable inline classification; passing nil
// preserves the prior "record raw hash, leave protocol_id NULL" behavior.
func NewProtocolWasmProcessor(metricsService *metrics.IngestionMetrics, classifier WasmClassifier) *ProtocolWasmProcessor {
	return &ProtocolWasmProcessor{
		metricsService: metricsService,
		classifier:     classifier,
	}
}

// Name returns the processor name for logging and metrics.
func (p *ProtocolWasmProcessor) Name() string {
	return "protocol_wasms"
}

// ProcessOperation extracts WASM hashes from an operation's ledger changes.
// Only processes ContractCode entries that have a Post state (created or updated).
// When the processor has a classifier, each new WASM is validated against the
// registered protocols so the resulting record carries the matched protocol_id.
func (p *ProtocolWasmProcessor) ProcessOperation(ctx context.Context, opWrapper *TransactionOperationWrapper) ([]data.ProtocolWasms, error) {
	startTime := time.Now()
	defer func() {
		if p.metricsService != nil {
			duration := time.Since(startTime).Seconds()
			p.metricsService.StateChangeProcessingDuration.WithLabelValues("ProtocolWasmProcessor").Observe(duration)
		}
	}()

	changes, err := opWrapper.Transaction.GetOperationChanges(opWrapper.Index)
	if err != nil {
		return nil, fmt.Errorf("getting operation changes: %w", err)
	}

	var wasms []data.ProtocolWasms
	for _, change := range changes {
		if change.Type != xdr.LedgerEntryTypeContractCode || change.Post == nil {
			continue
		}

		entry := change.Post.Data.MustContractCode()
		record := data.ProtocolWasms{
			WasmHash: types.HashBytea(hex.EncodeToString(entry.Hash[:])),
		}

		if p.classifier != nil {
			protocolID, classifyErr := p.classifier.Classify(ctx, entry.Code)
			if classifyErr != nil {
				// Non-fatal: log and fall through with protocol_id=NULL so the
				// next protocol-setup re-run can re-classify (it scans
				// protocol_id IS NULL rows).
				log.Ctx(ctx).Debugf("protocol_wasms: classify failed for hash=%s: %v",
					hex.EncodeToString(entry.Hash[:]), classifyErr)
			} else if protocolID != "" {
				id := protocolID
				record.ProtocolID = &id
			}
		}

		wasms = append(wasms, record)
	}

	return wasms, nil
}

// SortProtocolIDs returns the validators sorted by ProtocolID for deterministic
// match order. Exported for callers that want a stable order; not used inside
// this processor (each WASM matches at most one protocol so order rarely
// matters).
func SortProtocolIDs(ids []string) []string {
	out := append([]string(nil), ids...)
	sort.Strings(out)
	return out
}
