package processors

import (
	"context"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
)

// ProtocolWasmObservation pairs a protocol_wasms record (with NULL ProtocolID
// at this stage) with the raw bytecode that was uploaded. Downstream
// classification fills in ProtocolID; the bytecode is what validators consume
// via the spec extractor.
type ProtocolWasmObservation struct {
	Record   data.ProtocolWasms
	Bytecode []byte
}

// ProtocolWasmProcessor extracts WASM hashes and bytecodes from ContractCode
// ledger entries. It does not classify on the fly — that is the job of the
// per-batch ProtocolValidator dispatcher run inside PersistLedgerData.
type ProtocolWasmProcessor struct {
	metricsService *metrics.IngestionMetrics
}

// NewProtocolWasmProcessor creates a protocol WASM processor.
func NewProtocolWasmProcessor(metricsService *metrics.IngestionMetrics) *ProtocolWasmProcessor {
	return &ProtocolWasmProcessor{
		metricsService: metricsService,
	}
}

// Name returns the processor name for logging and metrics.
func (p *ProtocolWasmProcessor) Name() string {
	return "protocol_wasms"
}

// ProcessOperation extracts WASM hashes and raw bytecode from an operation's
// ledger changes. Only processes ContractCode entries that have a Post state
// (created or updated).
func (p *ProtocolWasmProcessor) ProcessOperation(_ context.Context, opWrapper *TransactionOperationWrapper) ([]ProtocolWasmObservation, error) {
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

	var observations []ProtocolWasmObservation
	for _, change := range changes {
		if change.Type != xdr.LedgerEntryTypeContractCode || change.Post == nil {
			continue
		}

		entry := change.Post.Data.MustContractCode()
		bytecode := append([]byte(nil), entry.Code...)
		observations = append(observations, ProtocolWasmObservation{
			Record: data.ProtocolWasms{
				WasmHash: types.HashBytea(hex.EncodeToString(entry.Hash[:])),
			},
			Bytecode: bytecode,
		})
	}

	return observations, nil
}
