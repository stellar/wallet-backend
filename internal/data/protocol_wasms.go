package data

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/utils"
)

// ProtocolWasms represents a WASM hash tracked during checkpoint population.
type ProtocolWasms struct {
	WasmHash   types.HashBytea `db:"wasm_hash"`
	ProtocolID *string         `db:"protocol_id"`
	CreatedAt  time.Time       `db:"created_at"`
}

// ProtocolWasmsModelInterface defines the interface for protocol_wasms operations.
type ProtocolWasmsModelInterface interface {
	BatchInsert(ctx context.Context, dbTx pgx.Tx, wasms []ProtocolWasms) error
	GetUnclassified(ctx context.Context) ([]ProtocolWasms, error)
	BatchUpdateProtocolID(ctx context.Context, dbTx pgx.Tx, wasmHashes []types.HashBytea, protocolID string) error
}

// ProtocolWasmsModel implements ProtocolWasmsModelInterface.
type ProtocolWasmsModel struct {
	DB      *pgxpool.Pool
	Metrics *metrics.DBMetrics
}

var _ ProtocolWasmsModelInterface = (*ProtocolWasmsModel)(nil)

// BatchInsert inserts multiple protocol WASMs using UNNEST for efficient batch insertion.
// Uses ON CONFLICT (wasm_hash) DO NOTHING for idempotent operations.
func (m *ProtocolWasmsModel) BatchInsert(ctx context.Context, dbTx pgx.Tx, wasms []ProtocolWasms) error {
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

// GetUnclassified returns all protocol WASMs where protocol_id IS NULL.
func (m *ProtocolWasmsModel) GetUnclassified(ctx context.Context) ([]ProtocolWasms, error) {
	const query = `SELECT wasm_hash, protocol_id, created_at FROM protocol_wasms WHERE protocol_id IS NULL`

	start := time.Now()
	wasms, err := db.QueryMany[ProtocolWasms](ctx, m.DB, query)
	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("GetUnclassified", "protocol_wasms").Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("GetUnclassified", "protocol_wasms").Inc()
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("GetUnclassified", "protocol_wasms", utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("querying unclassified protocol wasms: %w", err)
	}
	return wasms, nil
}

// BatchUpdateProtocolID updates protocol_id for the given WASM hashes.
func (m *ProtocolWasmsModel) BatchUpdateProtocolID(ctx context.Context, dbTx pgx.Tx, wasmHashes []types.HashBytea, protocolID string) error {
	if len(wasmHashes) == 0 {
		return nil
	}

	wasmHashBytes := make([][]byte, len(wasmHashes))
	for i, wasmHash := range wasmHashes {
		val, err := wasmHash.Value()
		if err != nil {
			return fmt.Errorf("converting wasm hash to bytes: %w", err)
		}
		wasmHashBytes[i] = val.([]byte)
	}

	const query = `UPDATE protocol_wasms SET protocol_id = $1 WHERE wasm_hash = ANY($2::bytea[])`

	start := time.Now()
	_, err := dbTx.Exec(ctx, query, protocolID, wasmHashBytes)
	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("BatchUpdateProtocolID", "protocol_wasms").Observe(duration)
	m.Metrics.BatchSize.WithLabelValues("BatchUpdateProtocolID", "protocol_wasms").Observe(float64(len(wasmHashes)))
	m.Metrics.QueriesTotal.WithLabelValues("BatchUpdateProtocolID", "protocol_wasms").Inc()
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("BatchUpdateProtocolID", "protocol_wasms", utils.GetDBErrorType(err)).Inc()
		return fmt.Errorf("batch updating protocol_id for protocol wasms: %w", err)
	}
	return nil
}
