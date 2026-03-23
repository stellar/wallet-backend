package data

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
)

func TestProtocolWasmBatchInsert(t *testing.T) {
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	cleanUpDB := func() {
		_, err = dbConnectionPool.Exec(ctx, `DELETE FROM protocol_wasms`)
		require.NoError(t, err)
	}

	dbMetrics := metrics.NewMetrics(prometheus.NewRegistry()).DB

	t.Run("empty input returns no error", func(t *testing.T) {
		cleanUpDB()

		model := &ProtocolWasmModel{DB: dbConnectionPool, Metrics: dbMetrics}
		err := db.RunInTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return model.BatchInsert(ctx, dbTx, []ProtocolWasm{})
		})
		assert.NoError(t, err)
	})

	t.Run("single insert", func(t *testing.T) {
		cleanUpDB()

		wasmHash := types.HashBytea("abc123def4560000000000000000000000000000000000000000000000000000")
		model := &ProtocolWasmModel{DB: dbConnectionPool, Metrics: dbMetrics}
		err := db.RunInTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return model.BatchInsert(ctx, dbTx, []ProtocolWasm{
				{WasmHash: wasmHash, ProtocolID: nil},
			})
		})
		assert.NoError(t, err)

		var count int
		wasmHashBytes, err := wasmHash.Value()
		require.NoError(t, err)
		err = dbConnectionPool.QueryRow(ctx, `SELECT COUNT(*) FROM protocol_wasms WHERE wasm_hash = $1`, wasmHashBytes).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 1, count)
	})

	t.Run("multiple inserts", func(t *testing.T) {
		cleanUpDB()

		model := &ProtocolWasmModel{DB: dbConnectionPool, Metrics: dbMetrics}
		protocolID := "test-protocol"
		hash2 := types.HashBytea("0200000000000000000000000000000000000000000000000000000000000000")
		err := db.RunInTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return model.BatchInsert(ctx, dbTx, []ProtocolWasm{
				{WasmHash: types.HashBytea("0100000000000000000000000000000000000000000000000000000000000000"), ProtocolID: nil},
				{WasmHash: hash2, ProtocolID: &protocolID},
				{WasmHash: types.HashBytea("0300000000000000000000000000000000000000000000000000000000000000"), ProtocolID: nil},
			})
		})
		assert.NoError(t, err)

		var count int
		err = dbConnectionPool.QueryRow(ctx, `SELECT COUNT(*) FROM protocol_wasms`).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 3, count)

		var storedProtocolID *string
		hash2Bytes, err := hash2.Value()
		require.NoError(t, err)
		err = dbConnectionPool.QueryRow(ctx, `SELECT protocol_id FROM protocol_wasms WHERE wasm_hash = $1`, hash2Bytes).Scan(&storedProtocolID)
		require.NoError(t, err)
		require.NotNil(t, storedProtocolID)
		assert.Equal(t, "test-protocol", *storedProtocolID)
	})

	t.Run("duplicate inserts are idempotent", func(t *testing.T) {
		cleanUpDB()

		dupHash := types.HashBytea("ddee000000000000000000000000000000000000000000000000000000000000")
		model := &ProtocolWasmModel{DB: dbConnectionPool, Metrics: dbMetrics}

		err := db.RunInTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return model.BatchInsert(ctx, dbTx, []ProtocolWasm{
				{WasmHash: dupHash, ProtocolID: nil},
			})
		})
		assert.NoError(t, err)

		err = db.RunInTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return model.BatchInsert(ctx, dbTx, []ProtocolWasm{
				{WasmHash: dupHash, ProtocolID: nil},
			})
		})
		assert.NoError(t, err)

		var count int
		dupHashBytes, err := dupHash.Value()
		require.NoError(t, err)
		err = dbConnectionPool.QueryRow(ctx, `SELECT COUNT(*) FROM protocol_wasms WHERE wasm_hash = $1`, dupHashBytes).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 1, count)
	})
}
