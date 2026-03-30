package services

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/wallet-backend/internal/data"
)

// WasmIngestionService tracks and persists WASM hashes during checkpoint population.
type WasmIngestionService interface {
	ProcessContractCode(ctx context.Context, wasmHash xdr.Hash, wasmCode []byte) error
	PersistKnownWasms(ctx context.Context, dbTx pgx.Tx) error
}

var _ WasmIngestionService = (*wasmIngestionService)(nil)

type wasmIngestionService struct {
	specExtractor  WasmSpecExtractor
	validators     []ProtocolValidator
	knownWasmModel data.KnownWasmModelInterface
	wasmHashes     map[xdr.Hash]struct{}
}

// NewWasmIngestionService creates a WasmIngestionService.
func NewWasmIngestionService(
	knownWasmModel data.KnownWasmModelInterface,
	specExtractor WasmSpecExtractor,
	validators ...ProtocolValidator,
) WasmIngestionService {
	return &wasmIngestionService{
		specExtractor:  specExtractor,
		validators:     validators,
		knownWasmModel: knownWasmModel,
		wasmHashes:     make(map[xdr.Hash]struct{}),
	}
}

// ProcessContractCode runs protocol validators against the WASM and tracks the hash.
func (s *wasmIngestionService) ProcessContractCode(ctx context.Context, wasmHash xdr.Hash, wasmCode []byte) error {
	// Extract spec entries from the WASM bytecode
	specEntries, err := s.specExtractor.ExtractSpec(ctx, wasmCode)
	if err != nil {
		log.Ctx(ctx).Warnf("failed to extract spec from WASM %s: %v", wasmHash.HexString(), err)
	}

	// Run all registered validators if we got spec entries
	if len(specEntries) > 0 {
		for _, v := range s.validators {
			if v.Validate(specEntries) {
				log.Ctx(ctx).Infof("WASM %s matched protocol %s", wasmHash.HexString(), v.ProtocolID())
			}
		}
	}

	// Track hash for later persistence
	s.wasmHashes[wasmHash] = struct{}{}
	return nil
}

// PersistKnownWasms writes all accumulated WASM hashes to the known_wasms table.
func (s *wasmIngestionService) PersistKnownWasms(ctx context.Context, dbTx pgx.Tx) error {
	if len(s.wasmHashes) == 0 {
		return nil
	}

	wasms := make([]data.KnownWasm, 0, len(s.wasmHashes))
	for hash := range s.wasmHashes {
		wasms = append(wasms, data.KnownWasm{
			WasmHash:   hash.HexString(),
			ProtocolID: nil, // No validators matched for now
		})
	}

	if err := s.knownWasmModel.BatchInsert(ctx, dbTx, wasms); err != nil {
		return fmt.Errorf("persisting known wasms: %w", err)
	}

	log.Ctx(ctx).Infof("Persisted %d known WASM hashes", len(wasms))
	return nil
}
