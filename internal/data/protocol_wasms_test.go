package data

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
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

		model := &ProtocolWasmModel{DB: dbConnectionPool, MetricsService: mockMetricsService}
		err := db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return model.BatchInsert(ctx, dbTx, []ProtocolWasm{
				{WasmHash: "abc123def456", ProtocolID: nil},
			})
		})
		assert.NoError(t, err)

		// Verify the insert
		var count int
		err = dbConnectionPool.GetContext(ctx, &count, `SELECT COUNT(*) FROM protocol_wasms WHERE wasm_hash = 'abc123def456'`)
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
		err := db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return model.BatchInsert(ctx, dbTx, []ProtocolWasm{
				{WasmHash: "hash1", ProtocolID: nil},
				{WasmHash: "hash2", ProtocolID: &protocolID},
				{WasmHash: "hash3", ProtocolID: nil},
			})
		})
		assert.NoError(t, err)

		var count int
		err = dbConnectionPool.GetContext(ctx, &count, `SELECT COUNT(*) FROM protocol_wasms`)
		require.NoError(t, err)
		assert.Equal(t, 3, count)

		// Verify protocol_id was stored correctly
		var storedProtocolID *string
		err = dbConnectionPool.GetContext(ctx, &storedProtocolID, `SELECT protocol_id FROM protocol_wasms WHERE wasm_hash = 'hash2'`)
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

		model := &ProtocolWasmModel{DB: dbConnectionPool, MetricsService: mockMetricsService}

		// First insert
		err := db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return model.BatchInsert(ctx, dbTx, []ProtocolWasm{
				{WasmHash: "duplicate_hash", ProtocolID: nil},
			})
		})
		assert.NoError(t, err)

		// Second insert with same hash - should not error
		err = db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return model.BatchInsert(ctx, dbTx, []ProtocolWasm{
				{WasmHash: "duplicate_hash", ProtocolID: nil},
			})
		})
		assert.NoError(t, err)

		// Verify only one row
		var count int
		err = dbConnectionPool.GetContext(ctx, &count, `SELECT COUNT(*) FROM protocol_wasms WHERE wasm_hash = 'duplicate_hash'`)
		require.NoError(t, err)
		assert.Equal(t, 1, count)
	})
}
