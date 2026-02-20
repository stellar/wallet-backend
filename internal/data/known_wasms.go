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

// KnownWasm represents a WASM hash tracked during checkpoint population.
type KnownWasm struct {
	WasmHash   string    `db:"wasm_hash"`
	ProtocolID *string   `db:"protocol_id"`
	CreatedAt  time.Time `db:"created_at"`
}

// KnownWasmModelInterface defines the interface for known_wasms operations.
type KnownWasmModelInterface interface {
	BatchInsert(ctx context.Context, dbTx pgx.Tx, wasms []KnownWasm) error
}

// KnownWasmModel implements KnownWasmModelInterface.
type KnownWasmModel struct {
	DB             db.ConnectionPool
	MetricsService metrics.MetricsService
}

var _ KnownWasmModelInterface = (*KnownWasmModel)(nil)

// BatchInsert inserts multiple known WASMs using UNNEST for efficient batch insertion.
// Uses ON CONFLICT (wasm_hash) DO NOTHING for idempotent operations.
func (m *KnownWasmModel) BatchInsert(ctx context.Context, dbTx pgx.Tx, wasms []KnownWasm) error {
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
		INSERT INTO known_wasms (wasm_hash, protocol_id)
		SELECT * FROM UNNEST($1::text[], $2::text[])
		ON CONFLICT (wasm_hash) DO NOTHING
	`

	start := time.Now()
	_, err := dbTx.Exec(ctx, query, wasmHashes, protocolIDs)
	if err != nil {
		m.MetricsService.IncDBQueryError("BatchInsert", "known_wasms", utils.GetDBErrorType(err))
		return fmt.Errorf("batch inserting known wasms: %w", err)
	}

	m.MetricsService.ObserveDBQueryDuration("BatchInsert", "known_wasms", time.Since(start).Seconds())
	m.MetricsService.ObserveDBBatchSize("BatchInsert", "known_wasms", len(wasms))
	m.MetricsService.IncDBQuery("BatchInsert", "known_wasms")
	return nil
}
