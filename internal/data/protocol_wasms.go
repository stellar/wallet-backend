package data

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

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
}

// ProtocolWasmModel implements ProtocolWasmModelInterface.
type ProtocolWasmModel struct {
	DB      *pgxpool.Pool
	Metrics *metrics.DBMetrics
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
	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("BatchInsert", "protocol_wasms").Observe(duration)
	m.Metrics.BatchSize.WithLabelValues("BatchInsert", "protocol_wasms").Observe(float64(len(wasms)))
	m.Metrics.QueriesTotal.WithLabelValues("BatchInsert", "protocol_wasms").Inc()
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("BatchInsert", "protocol_wasms", utils.GetDBErrorType(err)).Inc()
		return fmt.Errorf("batch inserting protocol wasms: %w", err)
	}
	return nil
}
