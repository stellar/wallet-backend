package services

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/wallet-backend/internal/data"
)

// ProtocolValidator validates WASM bytecode against a specific protocol.
type ProtocolValidator interface {
	ProtocolID() string
	Validate(ctx context.Context, wasmCode []byte) (bool, error)
}

// WasmIngestionService tracks and persists WASM hashes during checkpoint population.
type WasmIngestionService interface {
	ProcessContractCode(ctx context.Context, wasmHash xdr.Hash, wasmCode []byte) error
	PersistKnownWasms(ctx context.Context, dbTx pgx.Tx) error
}

var _ WasmIngestionService = (*wasmIngestionService)(nil)

type wasmIngestionService struct {
	validators     []ProtocolValidator
	knownWasmModel data.KnownWasmModelInterface
	wasmHashes     map[xdr.Hash]struct{}
}

// NewWasmIngestionService creates a WasmIngestionService.
func NewWasmIngestionService(
	knownWasmModel data.KnownWasmModelInterface,
	validators ...ProtocolValidator,
) WasmIngestionService {
	return &wasmIngestionService{
		validators:     validators,
		knownWasmModel: knownWasmModel,
		wasmHashes:     make(map[xdr.Hash]struct{}),
	}
}

// ProcessContractCode runs protocol validators against the WASM and tracks the hash.
func (s *wasmIngestionService) ProcessContractCode(ctx context.Context, wasmHash xdr.Hash, wasmCode []byte) error {
	// Run all registered validators
	for _, v := range s.validators {
		matched, err := v.Validate(ctx, wasmCode)
		if err != nil {
			log.Ctx(ctx).Warnf("protocol validator %s error for hash %s: %v", v.ProtocolID(), wasmHash.HexString(), err)
			continue
		}
		if matched {
			log.Ctx(ctx).Infof("WASM %s matched protocol %s", wasmHash.HexString(), v.ProtocolID())
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
