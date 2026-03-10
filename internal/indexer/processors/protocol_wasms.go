package processors

import (
	"context"
	"fmt"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/wallet-backend/internal/data"
)

// ProtocolWasmProcessor extracts WASM hashes from ContractCode ledger entries.
// It processes ledger changes to identify new WASM uploads for protocol tracking.
type ProtocolWasmProcessor struct{}

// NewProtocolWasmProcessor creates a new protocol WASM processor.
func NewProtocolWasmProcessor() *ProtocolWasmProcessor {
	return &ProtocolWasmProcessor{}
}

// Name returns the processor name for logging and metrics.
func (p *ProtocolWasmProcessor) Name() string {
	return "protocol_wasms"
}

// ProcessOperation extracts WASM hashes from an operation's ledger changes.
// Only processes ContractCode entries that have a Post state (created or updated).
func (p *ProtocolWasmProcessor) ProcessOperation(ctx context.Context, opWrapper *TransactionOperationWrapper) ([]data.ProtocolWasm, error) {
	changes, err := opWrapper.Transaction.GetOperationChanges(opWrapper.Index)
	if err != nil {
		return nil, fmt.Errorf("getting operation changes: %w", err)
	}

	var wasms []data.ProtocolWasm
	for _, change := range changes {
		if change.Type != xdr.LedgerEntryTypeContractCode || change.Post == nil {
			continue
		}

		hash := change.Post.Data.MustContractCode().Hash
		wasms = append(wasms, data.ProtocolWasm{
			WasmHash: hash[:],
		})
	}

	return wasms, nil
}
