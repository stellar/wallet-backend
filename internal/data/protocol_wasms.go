package data

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/utils"
)

// ProtocolWasm represents a WASM hash tracked during checkpoint population.
type ProtocolWasm struct {
	WasmHash   string    `db:"wasm_hash"`
	ProtocolID *string   `db:"protocol_id"`
	CreatedAt  time.Time `db:"created_at"`
}

// ProtocolWasmModelInterface defines the interface for protocol_wasms operations.
type ProtocolWasmModelInterface interface {
	BatchInsert(ctx context.Context, dbTx pgx.Tx, wasms []ProtocolWasm) error
}

// ProtocolWasmModel implements ProtocolWasmModelInterface.
type ProtocolWasmModel struct {
	DB             db.ConnectionPool
	MetricsService metrics.MetricsService
}

var _ ProtocolWasmModelInterface = (*ProtocolWasmModel)(nil)

// BatchInsert inserts multiple protocol WASMs using UNNEST for efficient batch insertion.
// Uses ON CONFLICT (wasm_hash) DO NOTHING for idempotent operations.
func (m *ProtocolWasmModel) BatchInsert(ctx context.Context, dbTx pgx.Tx, wasms []ProtocolWasm) error {
	if len(wasms) == 0 {
		return nil
	}

	wasmHashes := make([]string, len(wasms))
	protocolIDs := make([]*string, len(wasms))

	for i, w := range wasms {
		wasmHashes[i] = w.WasmHash
		protocolIDs[i] = w.ProtocolID
	}

	const query = `
		INSERT INTO protocol_wasms (wasm_hash, protocol_id)
		SELECT * FROM UNNEST($1::text[], $2::text[])
		ON CONFLICT (wasm_hash) DO NOTHING
	`

	start := time.Now()
	_, err := dbTx.Exec(ctx, query, wasmHashes, protocolIDs)
	if err != nil {
		m.MetricsService.IncDBQueryError("BatchInsert", "protocol_wasms", utils.GetDBErrorType(err))
		return fmt.Errorf("batch inserting protocol wasms: %w", err)
	}

	m.MetricsService.ObserveDBQueryDuration("BatchInsert", "protocol_wasms", time.Since(start).Seconds())
	m.MetricsService.ObserveDBBatchSize("BatchInsert", "protocol_wasms", len(wasms))
	m.MetricsService.IncDBQuery("BatchInsert", "protocol_wasms")
	return nil
}
