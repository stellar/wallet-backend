package services

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/wallet-backend/internal/data"
)

// WasmIngestionService tracks and persists WASM hashes and contract-to-WASM mappings during checkpoint population.
type WasmIngestionService interface {
	ProcessContractCode(ctx context.Context, wasmHash xdr.Hash) error
	ProcessContractData(ctx context.Context, change ingest.Change) error
	PersistProtocolWasms(ctx context.Context, dbTx pgx.Tx) error
	PersistProtocolContracts(ctx context.Context, dbTx pgx.Tx) error
}

var _ WasmIngestionService = (*wasmIngestionService)(nil)

type wasmIngestionService struct {
	protocolWasmModel     data.ProtocolWasmModelInterface
	protocolContractModel data.ProtocolContractModelInterface
	wasmHashes            map[xdr.Hash]struct{}
	contractIDsByWasmHash map[xdr.Hash][][]byte
}

// NewWasmIngestionService creates a WasmIngestionService.
func NewWasmIngestionService(
	protocolWasmModel data.ProtocolWasmModelInterface,
	protocolContractModel data.ProtocolContractModelInterface,
) *wasmIngestionService {
	return &wasmIngestionService{
		protocolWasmModel:     protocolWasmModel,
		protocolContractModel: protocolContractModel,
		wasmHashes:            make(map[xdr.Hash]struct{}),
		contractIDsByWasmHash: make(map[xdr.Hash][][]byte),
	}
}

// ProcessContractCode tracks the WASM hash for later persistence.
func (s *wasmIngestionService) ProcessContractCode(ctx context.Context, wasmHash xdr.Hash) error {
	s.wasmHashes[wasmHash] = struct{}{}
	return nil
}

// ProcessContractData extracts contract-to-WASM-hash mappings from ContractData Instance entries.
func (s *wasmIngestionService) ProcessContractData(ctx context.Context, change ingest.Change) error {
	contractDataEntry := change.Post.Data.MustContractData()

	// Only process Instance entries
	if contractDataEntry.Key.Type != xdr.ScValTypeScvLedgerKeyContractInstance {
		return nil
	}

	// Extract contract address
	contractAddress, ok := contractDataEntry.Contract.GetContractId()
	if !ok {
		return nil
	}

	// Extract WASM hash from contract instance executable
	contractInstance := contractDataEntry.Val.MustInstance()
	if contractInstance.Executable.Type != xdr.ContractExecutableTypeContractExecutableWasm {
		return nil
	}
	if contractInstance.Executable.WasmHash == nil {
		return nil
	}

	hash := *contractInstance.Executable.WasmHash
	s.contractIDsByWasmHash[hash] = append(s.contractIDsByWasmHash[hash], contractAddress[:])

	return nil
}

// PersistProtocolWasms writes all accumulated WASM hashes to the protocol_wasms table.
func (s *wasmIngestionService) PersistProtocolWasms(ctx context.Context, dbTx pgx.Tx) error {
	if len(s.wasmHashes) == 0 {
		return nil
	}

	wasms := make([]data.ProtocolWasm, 0, len(s.wasmHashes))
	for hash := range s.wasmHashes {
		wasms = append(wasms, data.ProtocolWasm{
			WasmHash:   hash[:],
			ProtocolID: nil,
		})
	}

	if err := s.protocolWasmModel.BatchInsert(ctx, dbTx, wasms); err != nil {
		return fmt.Errorf("persisting protocol wasms: %w", err)
	}

	log.Ctx(ctx).Infof("Persisted %d protocol WASM hashes", len(wasms))
	return nil
}

// PersistProtocolContracts writes all accumulated contract-to-WASM mappings to the protocol_contracts table.
// Contracts referencing WASM hashes not present in wasmHashes are skipped (e.g., expired/evicted WASMs).
func (s *wasmIngestionService) PersistProtocolContracts(ctx context.Context, dbTx pgx.Tx) error {
	if len(s.contractIDsByWasmHash) == 0 {
		return nil
	}

	var contracts []data.ProtocolContract
	var skipped int
	for hash, contractIDs := range s.contractIDsByWasmHash {
		if _, exists := s.wasmHashes[hash]; !exists {
			skipped += len(contractIDs)
			continue
		}
		for _, contractID := range contractIDs {
			contracts = append(contracts, data.ProtocolContract{
				ContractID: contractID,
				WasmHash:   hash[:],
			})
		}
	}
	if skipped > 0 {
		log.Ctx(ctx).Infof("Skipped %d protocol contracts referencing missing WASM hashes (expired/evicted)", skipped)
	}

	if err := s.protocolContractModel.BatchInsert(ctx, dbTx, contracts); err != nil {
		return fmt.Errorf("persisting protocol contracts: %w", err)
	}

	log.Ctx(ctx).Infof("Persisted %d protocol contracts", len(contracts))
	return nil
}
