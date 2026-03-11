package data

import (
	"context"
	"encoding/hex"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
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
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	cleanUpDB := func() {
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM protocol_wasms`)
		require.NoError(t, err)
	}

	t.Run("empty input returns no error", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		defer mockMetricsService.AssertExpectations(t)

		model := &ProtocolWasmModel{DB: dbConnectionPool, MetricsService: mockMetricsService}
		err := db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return model.BatchInsert(ctx, dbTx, []ProtocolWasm{})
		})
		assert.NoError(t, err)
	})

	t.Run("single insert", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", mock.Anything, mock.Anything, mock.Anything).Return()
		mockMetricsService.On("ObserveDBBatchSize", mock.Anything, mock.Anything, mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", mock.Anything, mock.Anything).Return()
		defer mockMetricsService.AssertExpectations(t)

		wasmHash := types.HashBytea("abc123def4560000000000000000000000000000000000000000000000000000")
		model := &ProtocolWasmModel{DB: dbConnectionPool, MetricsService: mockMetricsService}
		err := db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return model.BatchInsert(ctx, dbTx, []ProtocolWasm{
				{WasmHash: wasmHash, ProtocolID: nil},
			})
		})
		assert.NoError(t, err)

		// Verify the insert
		var count int
		wasmHashBytes, _ := hex.DecodeString(string(wasmHash))
		err = dbConnectionPool.GetContext(ctx, &count, `SELECT COUNT(*) FROM protocol_wasms WHERE wasm_hash = $1`, wasmHashBytes)
		require.NoError(t, err)
		assert.Equal(t, 1, count)
	})

	t.Run("multiple inserts", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", mock.Anything, mock.Anything, mock.Anything).Return()
		mockMetricsService.On("ObserveDBBatchSize", mock.Anything, mock.Anything, mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", mock.Anything, mock.Anything).Return()
		defer mockMetricsService.AssertExpectations(t)

		model := &ProtocolWasmModel{DB: dbConnectionPool, MetricsService: mockMetricsService}
		protocolID := "test-protocol"
		hash2 := types.HashBytea("0200000000000000000000000000000000000000000000000000000000000000")
		err := db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return model.BatchInsert(ctx, dbTx, []ProtocolWasm{
				{WasmHash: types.HashBytea("0100000000000000000000000000000000000000000000000000000000000000"), ProtocolID: nil},
				{WasmHash: hash2, ProtocolID: &protocolID},
				{WasmHash: types.HashBytea("0300000000000000000000000000000000000000000000000000000000000000"), ProtocolID: nil},
			})
		})
		assert.NoError(t, err)

		var count int
		err = dbConnectionPool.GetContext(ctx, &count, `SELECT COUNT(*) FROM protocol_wasms`)
		require.NoError(t, err)
		assert.Equal(t, 3, count)

		// Verify protocol_id was stored correctly
		var storedProtocolID *string
		hash2Bytes, _ := hex.DecodeString(string(hash2))
		err = dbConnectionPool.GetContext(ctx, &storedProtocolID, `SELECT protocol_id FROM protocol_wasms WHERE wasm_hash = $1`, hash2Bytes)
		require.NoError(t, err)
		require.NotNil(t, storedProtocolID)
		assert.Equal(t, "test-protocol", *storedProtocolID)
	})

	t.Run("duplicate inserts are idempotent", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", mock.Anything, mock.Anything, mock.Anything).Return()
		mockMetricsService.On("ObserveDBBatchSize", mock.Anything, mock.Anything, mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", mock.Anything, mock.Anything).Return()
		defer mockMetricsService.AssertExpectations(t)

		dupHash := types.HashBytea("ddee000000000000000000000000000000000000000000000000000000000000")
		model := &ProtocolWasmModel{DB: dbConnectionPool, MetricsService: mockMetricsService}

		// First insert
		err := db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return model.BatchInsert(ctx, dbTx, []ProtocolWasm{
				{WasmHash: dupHash, ProtocolID: nil},
			})
		})
		assert.NoError(t, err)

		// Second insert with same hash - should not error
		err = db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return model.BatchInsert(ctx, dbTx, []ProtocolWasm{
				{WasmHash: dupHash, ProtocolID: nil},
			})
		})
		assert.NoError(t, err)

		// Verify only one row
		var count int
		dupHashBytes, _ := hex.DecodeString(string(dupHash))
		err = dbConnectionPool.GetContext(ctx, &count, `SELECT COUNT(*) FROM protocol_wasms WHERE wasm_hash = $1`, dupHashBytes)
		require.NoError(t, err)
		assert.Equal(t, 1, count)
	})
}
