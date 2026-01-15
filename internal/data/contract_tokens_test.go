package data

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/metrics"
)

// Compile-time check that pgx.Tx is still used in other tests
var _ pgx.Tx = (pgx.Tx)(nil)

func TestContractModel_GetByID(t *testing.T) {
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	cleanUpDB := func() {
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM contract_tokens`)
		require.NoError(t, err)
	}

	t.Run("returns error when contract not found", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "GetByID", "contract_tokens", mock.Anything).Return()
		mockMetricsService.On("IncDBQueryError", "GetByID", "contract_tokens", mock.Anything).Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &ContractModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		contract, err := m.GetByID(context.Background(), "nonexistent")
		require.Error(t, err)
		require.Nil(t, contract)
		require.Contains(t, err.Error(), "getting contract by ID nonexistent")

		cleanUpDB()
	})
}

func TestContractModel_GetAllIDs(t *testing.T) {
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	cleanUpDB := func() {
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM contract_tokens`)
		require.NoError(t, err)
	}

	t.Run("returns empty slice when table is empty", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "GetAllIDs", "contract_tokens", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "GetAllIDs", "contract_tokens").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &ContractModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		ids, err := m.GetAllIDs(ctx)
		require.NoError(t, err)
		require.Empty(t, ids)
	})

	t.Run("returns all contract IDs from DB", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("IncDBQueryError", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
		mockMetricsService.On("ObserveDBQueryDuration", "BatchInsert", "contract_tokens", mock.Anything).Return()
		mockMetricsService.On("ObserveDBBatchSize", "BatchInsert", "contract_tokens", 3).Return()
		mockMetricsService.On("IncDBQuery", "BatchInsert", "contract_tokens").Return()
		mockMetricsService.On("ObserveDBQueryDuration", "GetAllIDs", "contract_tokens", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "GetAllIDs", "contract_tokens").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &ContractModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		name := "Test"
		symbol := "TST"
		contracts := []*Contract{
			{ID: "contract1", Type: "sac", Name: &name, Symbol: &symbol, Decimals: 7},
			{ID: "contract2", Type: "sep41", Name: &name, Symbol: &symbol, Decimals: 18},
			{ID: "contract3", Type: "sac", Name: &name, Symbol: &symbol, Decimals: 6},
		}

		err := db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			insertedIDs, txErr := m.BatchInsert(ctx, dbTx, contracts)
			require.NoError(t, txErr)
			require.Len(t, insertedIDs, 3)
			return nil
		})
		require.NoError(t, err)

		ids, err := m.GetAllIDs(ctx)
		require.NoError(t, err)
		require.Len(t, ids, 3)
		require.ElementsMatch(t, []string{"contract1", "contract2", "contract3"}, ids)

		cleanUpDB()
	})
}

func TestContractModel_BatchGetByIDs(t *testing.T) {
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	cleanUpDB := func() {
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM contract_tokens`)
		require.NoError(t, err)
	}

	t.Run("returns empty slice for empty IDs slice", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "BatchGetByIDs", "contract_tokens", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "BatchGetByIDs", "contract_tokens").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &ContractModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		contracts, err := m.BatchGetByIDs(ctx, []string{})
		require.NoError(t, err)
		require.Empty(t, contracts)
	})

	t.Run("returns empty slice when no contracts found", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "BatchGetByIDs", "contract_tokens", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "BatchGetByIDs", "contract_tokens").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &ContractModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		contracts, err := m.BatchGetByIDs(ctx, []string{"nonexistent1", "nonexistent2"})
		require.NoError(t, err)
		require.Empty(t, contracts)
	})

	t.Run("returns existing contracts by IDs", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("IncDBQueryError", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
		mockMetricsService.On("ObserveDBQueryDuration", "BatchInsert", "contract_tokens", mock.Anything).Return()
		mockMetricsService.On("ObserveDBBatchSize", "BatchInsert", "contract_tokens", 2).Return()
		mockMetricsService.On("IncDBQuery", "BatchInsert", "contract_tokens").Return()
		mockMetricsService.On("ObserveDBQueryDuration", "BatchGetByIDs", "contract_tokens", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "BatchGetByIDs", "contract_tokens").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &ContractModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		code1 := "TEST1"
		issuer1 := "GTEST123"
		name1 := "Test Contract 1"
		symbol1 := "TST1"
		code2 := "TEST2"
		issuer2 := "GTEST456"
		name2 := "Test Contract 2"
		symbol2 := "TST2"
		contracts := []*Contract{
			{
				ID:       "contract1",
				Type:     "sac",
				Code:     &code1,
				Issuer:   &issuer1,
				Name:     &name1,
				Symbol:   &symbol1,
				Decimals: 7,
			},
			{
				ID:       "contract2",
				Type:     "sep41",
				Code:     &code2,
				Issuer:   &issuer2,
				Name:     &name2,
				Symbol:   &symbol2,
				Decimals: 18,
			},
		}

		// Insert contracts first
		err := db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			insertedIDs, txErr := m.BatchInsert(ctx, dbTx, contracts)
			require.NoError(t, txErr)
			require.Len(t, insertedIDs, 2)
			return nil
		})
		require.NoError(t, err)

		// Fetch contracts by IDs
		fetchedContracts, err := m.BatchGetByIDs(ctx, []string{"contract1", "contract2", "nonexistent"})
		require.NoError(t, err)
		require.Len(t, fetchedContracts, 2)

		// Verify fetched contracts
		contractMap := make(map[string]*Contract)
		for _, c := range fetchedContracts {
			contractMap[c.ID] = c
		}

		require.Equal(t, "sac", contractMap["contract1"].Type)
		require.Equal(t, "TEST1", *contractMap["contract1"].Code)
		require.Equal(t, "GTEST123", *contractMap["contract1"].Issuer)
		require.Equal(t, uint32(7), contractMap["contract1"].Decimals)

		require.Equal(t, "sep41", contractMap["contract2"].Type)
		require.Equal(t, "TEST2", *contractMap["contract2"].Code)
		require.Equal(t, uint32(18), contractMap["contract2"].Decimals)

		cleanUpDB()
	})
}

func TestContractModel_BatchInsert(t *testing.T) {
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	cleanUpDB := func() {
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM contract_tokens`)
		require.NoError(t, err)
	}

	t.Run("returns success for empty contracts slice", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		defer mockMetricsService.AssertExpectations(t)

		m := &ContractModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		err := db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			insertedIDs, txErr := m.BatchInsert(ctx, dbTx, []*Contract{})
			require.NoError(t, txErr)
			require.Nil(t, insertedIDs)
			return nil
		})
		require.NoError(t, err)
	})

	t.Run("returns success for multiple new contracts", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("IncDBQueryError", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
		mockMetricsService.On("ObserveDBQueryDuration", "BatchInsert", "contract_tokens", mock.Anything).Return()
		mockMetricsService.On("ObserveDBBatchSize", "BatchInsert", "contract_tokens", 3).Return()
		mockMetricsService.On("IncDBQuery", "BatchInsert", "contract_tokens").Return()
		mockMetricsService.On("ObserveDBQueryDuration", "GetByID", "contract_tokens", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "GetByID", "contract_tokens").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &ContractModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		code1 := "TEST1"
		issuer1 := "GTEST123"
		name1 := "Test Contract 1"
		symbol1 := "TST1"
		code2 := "TEST2"
		issuer2 := "GTEST456"
		name2 := "Test Contract 2"
		symbol2 := "TST2"
		name3 := "Test Contract 3"
		symbol3 := "TST3"
		contracts := []*Contract{
			{
				ID:       "contract1",
				Type:     "sac",
				Code:     &code1,
				Issuer:   &issuer1,
				Name:     &name1,
				Symbol:   &symbol1,
				Decimals: 7,
			},
			{
				ID:       "contract2",
				Type:     "sep41",
				Code:     &code2,
				Issuer:   &issuer2,
				Name:     &name2,
				Symbol:   &symbol2,
				Decimals: 18,
			},
			{
				ID:       "contract3",
				Type:     "unknown",
				Name:     &name3,
				Symbol:   &symbol3,
				Decimals: 6,
			},
		}

		err := db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			insertedIDs, txErr := m.BatchInsert(ctx, dbTx, contracts)
			require.NoError(t, txErr)
			require.Len(t, insertedIDs, 3)
			return nil
		})
		require.NoError(t, err)

		// Verify contracts were inserted
		contract1, err := m.GetByID(ctx, "contract1")
		require.NoError(t, err)
		require.Equal(t, "sac", contract1.Type)
		require.Equal(t, "TEST1", *contract1.Code)
		require.Equal(t, "GTEST123", *contract1.Issuer)
		require.Equal(t, "Test Contract 1", *contract1.Name)
		require.Equal(t, "TST1", *contract1.Symbol)
		require.Equal(t, uint32(7), contract1.Decimals)

		contract2, err := m.GetByID(ctx, "contract2")
		require.NoError(t, err)
		require.Equal(t, "sep41", contract2.Type)

		contract3, err := m.GetByID(ctx, "contract3")
		require.NoError(t, err)
		require.Nil(t, contract3.Code)
		require.Nil(t, contract3.Issuer)

		cleanUpDB()
	})

	t.Run("skips duplicate contracts with ON CONFLICT DO NOTHING", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("IncDBQueryError", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
		mockMetricsService.On("ObserveDBQueryDuration", "BatchInsert", "contract_tokens", mock.Anything).Return()
		mockMetricsService.On("ObserveDBBatchSize", "BatchInsert", "contract_tokens", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "BatchInsert", "contract_tokens").Return()
		mockMetricsService.On("ObserveDBQueryDuration", "GetByID", "contract_tokens", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "GetByID", "contract_tokens").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &ContractModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		// First insert
		origName := "Original Name"
		origSymbol := "ORIG"
		contracts := []*Contract{
			{
				ID:       "contract1",
				Type:     "sac",
				Name:     &origName,
				Symbol:   &origSymbol,
				Decimals: 7,
			},
		}

		err := db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			insertedIDs, txErr := m.BatchInsert(ctx, dbTx, contracts)
			require.NoError(t, txErr)
			require.Len(t, insertedIDs, 1)
			return nil
		})
		require.NoError(t, err)

		// Second insert with same ID and different data - should be skipped
		newName := "New Name"
		newSymbol := "NEW"
		contract2Name := "Contract 2"
		contract2Symbol := "C2"
		contracts = []*Contract{
			{
				ID:       "contract1",
				Type:     "sep41",
				Name:     &newName,
				Symbol:   &newSymbol,
				Decimals: 18,
			},
			{
				ID:       "contract2",
				Type:     "unknown",
				Name:     &contract2Name,
				Symbol:   &contract2Symbol,
				Decimals: 6,
			},
		}

		err = db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			insertedIDs, txErr := m.BatchInsert(ctx, dbTx, contracts)
			require.NoError(t, txErr)
			require.Len(t, insertedIDs, 1) // Only contract2 should be inserted
			require.Equal(t, "contract2", insertedIDs[0])
			return nil
		})
		require.NoError(t, err)

		// Verify original contract was not updated
		contract1, err := m.GetByID(ctx, "contract1")
		require.NoError(t, err)
		require.Equal(t, "sac", contract1.Type)
		require.Equal(t, "Original Name", *contract1.Name)
		require.Equal(t, "ORIG", *contract1.Symbol)
		require.Equal(t, uint32(7), contract1.Decimals)

		cleanUpDB()
	})
}
