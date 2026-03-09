package data

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/utils"
)

// ProtocolWasm represents a WASM hash tracked during checkpoint population.
type ProtocolWasm struct {
	WasmHash   types.HashBytea `db:"wasm_hash"`
	ProtocolID *string         `db:"protocol_id"`
	CreatedAt  time.Time       `db:"created_at"`
}

// ProtocolWasmModelInterface defines the interface for protocol_wasms operations.
type ProtocolWasmModelInterface interface {
	BatchInsert(ctx context.Context, dbTx pgx.Tx, wasms []ProtocolWasm) error
	GetUnclassified(ctx context.Context) ([]ProtocolWasm, error)
	BatchUpdateProtocolID(ctx context.Context, dbTx pgx.Tx, wasmHashes []string, protocolID string) error
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

	wasmHashes := make([][]byte, len(wasms))
	protocolIDs := make([]*string, len(wasms))

	for i, w := range wasms {
		val, err := w.WasmHash.Value()
		if err != nil {
			return fmt.Errorf("converting wasm hash to bytes: %w", err)
		}
		wasmHashes[i] = val.([]byte)
		protocolIDs[i] = w.ProtocolID
	}

	const query = `
		INSERT INTO protocol_wasms (wasm_hash, protocol_id)
		SELECT * FROM UNNEST($1::bytea[], $2::text[])
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

// GetUnclassified returns all protocol WASMs where protocol_id IS NULL.
func (m *ProtocolWasmModel) GetUnclassified(ctx context.Context) ([]ProtocolWasm, error) {
	const query = `SELECT wasm_hash, protocol_id, created_at FROM protocol_wasms WHERE protocol_id IS NULL`

	start := time.Now()
	rows, err := m.DB.PgxPool().Query(ctx, query)
	if err != nil {
		m.MetricsService.IncDBQueryError("GetUnclassified", "protocol_wasms", utils.GetDBErrorType(err))
		return nil, fmt.Errorf("querying unclassified protocol wasms: %w", err)
	}
	defer rows.Close()

	var wasms []ProtocolWasm
	for rows.Next() {
		var w ProtocolWasm
		if err := rows.Scan(&w.WasmHash, &w.ProtocolID, &w.CreatedAt); err != nil {
			return nil, fmt.Errorf("scanning protocol wasm row: %w", err)
		}
		wasms = append(wasms, w)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating protocol wasm rows: %w", err)
	}

	m.MetricsService.ObserveDBQueryDuration("GetUnclassified", "protocol_wasms", time.Since(start).Seconds())
	m.MetricsService.IncDBQuery("GetUnclassified", "protocol_wasms")
	return wasms, nil
}

// BatchUpdateProtocolID updates protocol_id for the given WASM hashes.
func (m *ProtocolWasmModel) BatchUpdateProtocolID(ctx context.Context, dbTx pgx.Tx, wasmHashes []string, protocolID string) error {
	if len(wasmHashes) == 0 {
		return nil
	}

	const query = `UPDATE protocol_wasms SET protocol_id = $1 WHERE wasm_hash = ANY($2)`

	start := time.Now()
	_, err := dbTx.Exec(ctx, query, protocolID, wasmHashes)
	if err != nil {
		m.MetricsService.IncDBQueryError("BatchUpdateProtocolID", "protocol_wasms", utils.GetDBErrorType(err))
		return fmt.Errorf("batch updating protocol_id for protocol wasms: %w", err)
	}

	m.MetricsService.ObserveDBQueryDuration("BatchUpdateProtocolID", "protocol_wasms", time.Since(start).Seconds())
	m.MetricsService.ObserveDBBatchSize("BatchUpdateProtocolID", "protocol_wasms", len(wasmHashes))
	m.MetricsService.IncDBQuery("BatchUpdateProtocolID", "protocol_wasms")
	return nil
}
