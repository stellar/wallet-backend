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
		mockMetricsService.On("ObserveDBQueryDuration", "GetByContractID", "contract_tokens", mock.Anything).Return()
		mockMetricsService.On("IncDBQueryError", "GetByContractID", "contract_tokens", mock.Anything).Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &ContractModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		contract, err := m.GetByContractID(context.Background(), "nonexistent")
		require.Error(t, err)
		require.Nil(t, contract)
		require.Contains(t, err.Error(), "getting contract by contract_id nonexistent")

		cleanUpDB()
	})
}

func TestContractModel_GetAllContractIDs(t *testing.T) {
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
		mockMetricsService.On("ObserveDBQueryDuration", "GetAllContractIDs", "contract_tokens", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "GetAllContractIDs", "contract_tokens").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &ContractModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		ids, err := m.GetAllContractIDs(ctx)
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
		mockMetricsService.On("ObserveDBQueryDuration", "GetAllContractIDs", "contract_tokens", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "GetAllContractIDs", "contract_tokens").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &ContractModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		name := "Test"
		symbol := "TST"
		contracts := []*Contract{
			{ContractID: "contract1", Type: "sac", Name: &name, Symbol: &symbol, Decimals: 7},
			{ContractID: "contract2", Type: "sep41", Name: &name, Symbol: &symbol, Decimals: 18},
			{ContractID: "contract3", Type: "sac", Name: &name, Symbol: &symbol, Decimals: 6},
		}

		err := db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			insertedIDs, txErr := m.BatchInsert(ctx, dbTx, contracts)
			require.NoError(t, txErr)
			require.Len(t, insertedIDs, 3)
			return nil
		})
		require.NoError(t, err)

		ids, err := m.GetAllContractIDs(ctx)
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
		// No metrics expected - early return for empty input
		defer mockMetricsService.AssertExpectations(t)

		m := &ContractModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		contracts, err := m.BatchGetByIDs(ctx, []int64{})
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

		// Use non-existent numeric IDs
		contracts, err := m.BatchGetByIDs(ctx, []int64{999999, 999998})
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
				ContractID: "contract1",
				Type:       "sac",
				Code:       &code1,
				Issuer:     &issuer1,
				Name:       &name1,
				Symbol:     &symbol1,
				Decimals:   7,
			},
			{
				ContractID: "contract2",
				Type:       "sep41",
				Code:       &code2,
				Issuer:     &issuer2,
				Name:       &name2,
				Symbol:     &symbol2,
				Decimals:   18,
			},
		}

		// Insert contracts first and capture numeric IDs
		var insertedIDMap map[string]int64
		err := db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			var txErr error
			insertedIDMap, txErr = m.BatchInsert(ctx, dbTx, contracts)
			require.NoError(t, txErr)
			require.Len(t, insertedIDMap, 2)
			return nil
		})
		require.NoError(t, err)

		// Fetch contracts by numeric IDs (including a non-existent one)
		numericIDs := []int64{insertedIDMap["contract1"], insertedIDMap["contract2"], 999999}
		fetchedContracts, err := m.BatchGetByIDs(ctx, numericIDs)
		require.NoError(t, err)
		require.Len(t, fetchedContracts, 2)

		// Verify fetched contracts
		contractMap := make(map[string]*Contract)
		for _, c := range fetchedContracts {
			contractMap[c.ContractID] = c
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
			require.Empty(t, insertedIDs)
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
		mockMetricsService.On("ObserveDBQueryDuration", "GetByContractID", "contract_tokens", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "GetByContractID", "contract_tokens").Return()
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
				ContractID: "contract1",
				Type:       "sac",
				Code:       &code1,
				Issuer:     &issuer1,
				Name:       &name1,
				Symbol:     &symbol1,
				Decimals:   7,
			},
			{
				ContractID: "contract2",
				Type:       "sep41",
				Code:       &code2,
				Issuer:     &issuer2,
				Name:       &name2,
				Symbol:     &symbol2,
				Decimals:   18,
			},
			{
				ContractID: "contract3",
				Type:       "unknown",
				Name:       &name3,
				Symbol:     &symbol3,
				Decimals:   6,
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
		contract1, err := m.GetByContractID(ctx, "contract1")
		require.NoError(t, err)
		require.Equal(t, "sac", contract1.Type)
		require.Equal(t, "TEST1", *contract1.Code)
		require.Equal(t, "GTEST123", *contract1.Issuer)
		require.Equal(t, "Test Contract 1", *contract1.Name)
		require.Equal(t, "TST1", *contract1.Symbol)
		require.Equal(t, uint32(7), contract1.Decimals)

		contract2, err := m.GetByContractID(ctx, "contract2")
		require.NoError(t, err)
		require.Equal(t, "sep41", contract2.Type)

		contract3, err := m.GetByContractID(ctx, "contract3")
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
		mockMetricsService.On("ObserveDBQueryDuration", "GetByContractID", "contract_tokens", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "GetByContractID", "contract_tokens").Return()
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
				ContractID: "contract1",
				Type:       "sac",
				Name:       &origName,
				Symbol:     &origSymbol,
				Decimals:   7,
			},
		}

		err := db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			insertedIDs, txErr := m.BatchInsert(ctx, dbTx, contracts)
			require.NoError(t, txErr)
			require.Len(t, insertedIDs, 1)
			return nil
		})
		require.NoError(t, err)

		// Second insert with same ContractID and different data - should be skipped
		newName := "New Name"
		newSymbol := "NEW"
		contract2Name := "Contract 2"
		contract2Symbol := "C2"
		contracts = []*Contract{
			{
				ContractID: "contract1",
				Type:       "sep41",
				Name:       &newName,
				Symbol:     &newSymbol,
				Decimals:   18,
			},
			{
				ContractID: "contract2",
				Type:       "unknown",
				Name:       &contract2Name,
				Symbol:     &contract2Symbol,
				Decimals:   6,
			},
		}

		err = db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			insertedIDs, txErr := m.BatchInsert(ctx, dbTx, contracts)
			require.NoError(t, txErr)
			// BatchInsert returns all contract IDs (both existing and newly inserted)
			require.Len(t, insertedIDs, 2)
			_, hasContract1 := insertedIDs["contract1"]
			require.True(t, hasContract1)
			_, hasContract2 := insertedIDs["contract2"]
			require.True(t, hasContract2)
			return nil
		})
		require.NoError(t, err)

		// Verify original contract was not updated
		contract1, err := m.GetByContractID(ctx, "contract1")
		require.NoError(t, err)
		require.Equal(t, "sac", contract1.Type)
		require.Equal(t, "Original Name", *contract1.Name)
		require.Equal(t, "ORIG", *contract1.Symbol)
		require.Equal(t, uint32(7), contract1.Decimals)

		cleanUpDB()
	})
}
