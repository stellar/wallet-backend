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
	PersistProtocolWasms(ctx context.Context, dbTx pgx.Tx) error
}

var _ WasmIngestionService = (*wasmIngestionService)(nil)

type wasmIngestionService struct {
	validators           []ProtocolValidator
	protocolWasmModel    data.ProtocolWasmModelInterface
	wasmHashToProtocolID map[xdr.Hash]*string
}

// NewWasmIngestionService creates a WasmIngestionService.
func NewWasmIngestionService(
	protocolWasmModel data.ProtocolWasmModelInterface,
	validators ...ProtocolValidator,
) WasmIngestionService {
	return &wasmIngestionService{
		validators:           validators,
		protocolWasmModel:    protocolWasmModel,
		wasmHashToProtocolID: make(map[xdr.Hash]*string),
	}
}

// ProcessContractCode runs protocol validators against the WASM and tracks the hash.
func (s *wasmIngestionService) ProcessContractCode(ctx context.Context, wasmHash xdr.Hash, wasmCode []byte) error {
	// Run all registered validators; if multiple match, the last one's ID is used
	// since the data model supports a single protocol_id per wasm hash.
	var matchedProtocolID *string
	for _, v := range s.validators {
		matched, err := v.Validate(ctx, wasmCode)
		if err != nil {
			log.Ctx(ctx).Warnf("protocol validator %s error for hash %s: %v", v.ProtocolID(), wasmHash.HexString(), err)
			continue
		}
		if matched {
			pid := v.ProtocolID()
			matchedProtocolID = &pid
			log.Ctx(ctx).Infof("WASM %s matched protocol %s", wasmHash.HexString(), pid)
		}
	}

	// Track hash with its matched protocol ID (nil if no validator matched)
	s.wasmHashToProtocolID[wasmHash] = matchedProtocolID
	return nil
}

// PersistProtocolWasms writes all accumulated WASM hashes to the protocol_wasms table.
func (s *wasmIngestionService) PersistProtocolWasms(ctx context.Context, dbTx pgx.Tx) error {
	if len(s.wasmHashToProtocolID) == 0 {
		return nil
	}

	wasms := make([]data.ProtocolWasm, 0, len(s.wasmHashToProtocolID))
	for hash, protocolID := range s.wasmHashToProtocolID {
		wasms = append(wasms, data.ProtocolWasm{
			WasmHash:   hash.HexString(),
			ProtocolID: protocolID,
		})
	}

	if err := s.protocolWasmModel.BatchInsert(ctx, dbTx, wasms); err != nil {
		return fmt.Errorf("persisting protocol wasms: %w", err)
	}

	log.Ctx(ctx).Infof("Persisted %d protocol WASM hashes", len(wasms))
	return nil
}
