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
	PersistProtocolWasms(ctx context.Context, dbTx pgx.Tx) error
}

var _ WasmIngestionService = (*wasmIngestionService)(nil)

type wasmIngestionService struct {
	protocolWasmModel data.ProtocolWasmModelInterface
	wasmHashes        map[xdr.Hash]struct{}
}

// NewWasmIngestionService creates a WasmIngestionService.
func NewWasmIngestionService(
	protocolWasmModel data.ProtocolWasmModelInterface,
) WasmIngestionService {
	return &wasmIngestionService{
		protocolWasmModel: protocolWasmModel,
		wasmHashes:        make(map[xdr.Hash]struct{}),
	}
}

// ProcessContractCode tracks the WASM hash for later persistence.
func (s *wasmIngestionService) ProcessContractCode(ctx context.Context, wasmHash xdr.Hash, wasmCode []byte) error {
	s.wasmHashes[wasmHash] = struct{}{}
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
			WasmHash:   hash.HexString(),
			ProtocolID: nil,
		})
	}

	if err := s.protocolWasmModel.BatchInsert(ctx, dbTx, wasms); err != nil {
		return fmt.Errorf("persisting protocol wasms: %w", err)
	}

	log.Ctx(ctx).Infof("Persisted %d protocol WASM hashes", len(wasms))
	return nil
}
