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
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
)

func TestProtocolContractsBatchInsert(t *testing.T) {
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	// Helper to insert prerequisite WASMs so FK constraint is satisfied
	insertWasms := func(t *testing.T, hashes ...types.HashBytea) {
		t.Helper()
		mockMetrics := metrics.NewMockMetricsService()
		mockMetrics.On("ObserveDBQueryDuration", mock.Anything, mock.Anything, mock.Anything).Return()
		mockMetrics.On("ObserveDBBatchSize", mock.Anything, mock.Anything, mock.Anything).Return()
		mockMetrics.On("IncDBQuery", mock.Anything, mock.Anything).Return()

		wasmModel := &ProtocolWasmsModel{DB: dbConnectionPool, MetricsService: mockMetrics}
		wasms := make([]ProtocolWasms, len(hashes))
		for i, h := range hashes {
			wasms[i] = ProtocolWasms{WasmHash: h}
		}
		err := db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return wasmModel.BatchInsert(ctx, dbTx, wasms)
		})
		require.NoError(t, err)
	}

	cleanUpDB := func() {
		// Delete contracts before wasms due to FK dependency
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM protocol_contracts`)
		require.NoError(t, err)
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM protocol_wasms`)
		require.NoError(t, err)
	}

	t.Run("empty input returns no error", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		defer mockMetricsService.AssertExpectations(t)

		model := &ProtocolContractsModel{DB: dbConnectionPool, MetricsService: mockMetricsService}
		err := db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return model.BatchInsert(ctx, dbTx, []ProtocolContracts{})
		})
		assert.NoError(t, err)
	})

	t.Run("single insert", func(t *testing.T) {
		cleanUpDB()

		wasmHash := types.HashBytea("abc123def4560000000000000000000000000000000000000000000000000000")
		insertWasms(t, wasmHash)

		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", mock.Anything, mock.Anything, mock.Anything).Return()
		mockMetricsService.On("ObserveDBBatchSize", mock.Anything, mock.Anything, mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", mock.Anything, mock.Anything).Return()
		defer mockMetricsService.AssertExpectations(t)

		contractID := types.HashBytea("0100000000000000000000000000000000000000000000000000000000000000")
		model := &ProtocolContractsModel{DB: dbConnectionPool, MetricsService: mockMetricsService}
		err := db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return model.BatchInsert(ctx, dbTx, []ProtocolContracts{
				{ContractID: contractID, WasmHash: wasmHash},
			})
		})
		assert.NoError(t, err)

		var count int
		cidBytes, err := contractID.Value()
		require.NoError(t, err)
		err = dbConnectionPool.GetContext(ctx, &count, `SELECT COUNT(*) FROM protocol_contracts WHERE contract_id = $1`, cidBytes)
		require.NoError(t, err)
		assert.Equal(t, 1, count)
	})

	t.Run("multiple inserts", func(t *testing.T) {
		cleanUpDB()

		wh1 := types.HashBytea("0100000000000000000000000000000000000000000000000000000000000000")
		wh2 := types.HashBytea("0200000000000000000000000000000000000000000000000000000000000000")
		insertWasms(t, wh1, wh2)

		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", mock.Anything, mock.Anything, mock.Anything).Return()
		mockMetricsService.On("ObserveDBBatchSize", mock.Anything, mock.Anything, mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", mock.Anything, mock.Anything).Return()
		defer mockMetricsService.AssertExpectations(t)

		model := &ProtocolContractsModel{DB: dbConnectionPool, MetricsService: mockMetricsService}
		err := db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return model.BatchInsert(ctx, dbTx, []ProtocolContracts{
				{ContractID: "aa00000000000000000000000000000000000000000000000000000000000000", WasmHash: wh1},
				{ContractID: "bb00000000000000000000000000000000000000000000000000000000000000", WasmHash: wh2},
				{ContractID: "cc00000000000000000000000000000000000000000000000000000000000000", WasmHash: wh1},
			})
		})
		assert.NoError(t, err)

		var count int
		err = dbConnectionPool.GetContext(ctx, &count, `SELECT COUNT(*) FROM protocol_contracts`)
		require.NoError(t, err)
		assert.Equal(t, 3, count)
	})

	t.Run("upsert updates wasm_hash", func(t *testing.T) {
		cleanUpDB()

		wh1 := types.HashBytea("0100000000000000000000000000000000000000000000000000000000000000")
		wh2 := types.HashBytea("0200000000000000000000000000000000000000000000000000000000000000")
		insertWasms(t, wh1, wh2)

		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", mock.Anything, mock.Anything, mock.Anything).Return()
		mockMetricsService.On("ObserveDBBatchSize", mock.Anything, mock.Anything, mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", mock.Anything, mock.Anything).Return()
		defer mockMetricsService.AssertExpectations(t)

		contractID := types.HashBytea("aa00000000000000000000000000000000000000000000000000000000000000")
		model := &ProtocolContractsModel{DB: dbConnectionPool, MetricsService: mockMetricsService}

		// First insert with wh1
		err := db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return model.BatchInsert(ctx, dbTx, []ProtocolContracts{
				{ContractID: contractID, WasmHash: wh1},
			})
		})
		require.NoError(t, err)

		// Upsert with wh2
		err = db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return model.BatchInsert(ctx, dbTx, []ProtocolContracts{
				{ContractID: contractID, WasmHash: wh2},
			})
		})
		require.NoError(t, err)

		// Verify wasm_hash was updated
		cidBytes, err := contractID.Value()
		require.NoError(t, err)
		wh2Bytes, err := wh2.Value()
		require.NoError(t, err)

		var storedWasmHash []byte
		err = dbConnectionPool.GetContext(ctx, &storedWasmHash, `SELECT wasm_hash FROM protocol_contracts WHERE contract_id = $1`, cidBytes)
		require.NoError(t, err)
		assert.Equal(t, wh2Bytes.([]byte), storedWasmHash)
	})

	t.Run("upsert preserves name when new value is NULL", func(t *testing.T) {
		cleanUpDB()

		wh1 := types.HashBytea("0100000000000000000000000000000000000000000000000000000000000000")
		wh2 := types.HashBytea("0200000000000000000000000000000000000000000000000000000000000000")
		insertWasms(t, wh1, wh2)

		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", mock.Anything, mock.Anything, mock.Anything).Return()
		mockMetricsService.On("ObserveDBBatchSize", mock.Anything, mock.Anything, mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", mock.Anything, mock.Anything).Return()
		defer mockMetricsService.AssertExpectations(t)

		contractID := types.HashBytea("aa00000000000000000000000000000000000000000000000000000000000000")
		contractName := "my-contract"
		model := &ProtocolContractsModel{DB: dbConnectionPool, MetricsService: mockMetricsService}

		// Insert with a name
		err := db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return model.BatchInsert(ctx, dbTx, []ProtocolContracts{
				{ContractID: contractID, WasmHash: wh1, Name: &contractName},
			})
		})
		require.NoError(t, err)

		// Upsert with NULL name and different wasm_hash
		err = db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return model.BatchInsert(ctx, dbTx, []ProtocolContracts{
				{ContractID: contractID, WasmHash: wh2, Name: nil},
			})
		})
		require.NoError(t, err)

		// Verify name was preserved and wasm_hash was updated
		cidBytes, err := contractID.Value()
		require.NoError(t, err)
		wh2Bytes, err := wh2.Value()
		require.NoError(t, err)

		var storedName *string
		var storedWasmHash []byte
		err = dbConnectionPool.GetContext(ctx, &storedName, `SELECT name FROM protocol_contracts WHERE contract_id = $1`, cidBytes)
		require.NoError(t, err)
		require.NotNil(t, storedName)
		assert.Equal(t, "my-contract", *storedName)

		err = dbConnectionPool.GetContext(ctx, &storedWasmHash, `SELECT wasm_hash FROM protocol_contracts WHERE contract_id = $1`, cidBytes)
		require.NoError(t, err)
		assert.Equal(t, wh2Bytes.([]byte), storedWasmHash)
	})

	t.Run("FK enforcement silently skips missing wasm", func(t *testing.T) {
		cleanUpDB()

		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", mock.Anything, mock.Anything, mock.Anything).Return()
		mockMetricsService.On("ObserveDBBatchSize", mock.Anything, mock.Anything, mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", mock.Anything, mock.Anything).Return()
		defer mockMetricsService.AssertExpectations(t)

		// Insert contract with wasm_hash that does NOT exist in protocol_wasms
		model := &ProtocolContractsModel{DB: dbConnectionPool, MetricsService: mockMetricsService}
		err := db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return model.BatchInsert(ctx, dbTx, []ProtocolContracts{
				{
					ContractID: "aa00000000000000000000000000000000000000000000000000000000000000",
					WasmHash:   "dead000000000000000000000000000000000000000000000000000000000000",
				},
			})
		})
		assert.NoError(t, err)

		// Verify nothing was inserted
		var count int
		err = dbConnectionPool.GetContext(ctx, &count, `SELECT COUNT(*) FROM protocol_contracts`)
		require.NoError(t, err)
		assert.Equal(t, 0, count)
	})
}
