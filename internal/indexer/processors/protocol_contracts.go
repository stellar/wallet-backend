package processors

import (
	"context"
	"encoding/hex"
	"fmt"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer/types"
)

// ProtocolContractProcessor extracts contract-to-WASM mappings from ContractData Instance entries.
// It processes ledger changes to identify contract deployments for protocol tracking.
type ProtocolContractProcessor struct{}

// NewProtocolContractProcessor creates a new protocol contract processor.
func NewProtocolContractProcessor() *ProtocolContractProcessor {
	return &ProtocolContractProcessor{}
}

// Name returns the processor name for logging and metrics.
func (p *ProtocolContractProcessor) Name() string {
	return "protocol_contracts"
}

// ProcessOperation extracts contract-to-WASM mappings from an operation's ledger changes.
// Only processes ContractData Instance entries with WASM executables.
func (p *ProtocolContractProcessor) ProcessOperation(ctx context.Context, opWrapper *TransactionOperationWrapper) ([]data.ProtocolContract, error) {
	changes, err := opWrapper.Transaction.GetOperationChanges(opWrapper.Index)
	if err != nil {
		return nil, fmt.Errorf("getting operation changes: %w", err)
	}

	var contracts []data.ProtocolContract
	for _, change := range changes {
		if change.Type != xdr.LedgerEntryTypeContractData || change.Post == nil {
			continue
		}

		contractData := change.Post.Data.MustContractData()
		if contractData.Key.Type != xdr.ScValTypeScvLedgerKeyContractInstance {
			continue
		}

		contractIDBytes, ok := contractData.Contract.GetContractId()
		if !ok {
			continue
		}

		instance := contractData.Val.MustInstance()
		if instance.Executable.Type != xdr.ContractExecutableTypeContractExecutableWasm {
			continue
		}
		if instance.Executable.WasmHash == nil {
			continue
		}

		hash := *instance.Executable.WasmHash

		contracts = append(contracts, data.ProtocolContract{
			ContractID: types.HashBytea(hex.EncodeToString(contractIDBytes[:])),
			WasmHash:   types.HashBytea(hex.EncodeToString(hash[:])),
		})
	}

	return contracts, nil
}
