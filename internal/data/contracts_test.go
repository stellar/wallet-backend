package data

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/metrics"
)

func TestContractModel_Insert(t *testing.T) {
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	cleanUpDB := func() {
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM token_contracts`)
		require.NoError(t, err)
	}

	t.Run("returns success for new contract", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("IncDBQueryError", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
		mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "token_contracts", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "INSERT", "token_contracts").Return()
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "token_contracts", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "SELECT", "token_contracts").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &ContractModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		contract := &Contract{
			ID:     "1",
			Name:   "Test Contract",
			Symbol: "TEST",
		}

		dbErr := db.RunInTransaction(context.Background(), dbConnectionPool, nil, func(tx db.Transaction) error {
			err = m.Insert(context.Background(), tx, contract)
			require.NoError(t, err)
			return nil
		})
		require.NoError(t, dbErr)

		contract, err = m.GetByID(context.Background(), "1")
		require.NoError(t, err)
		require.Equal(t, contract.ID, "1")
		require.Equal(t, contract.Name, "Test Contract")
		require.Equal(t, contract.Symbol, "TEST")

		cleanUpDB()
	})

	t.Run("returns error for duplicate contract", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("IncDBQueryError", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
		mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "token_contracts", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "INSERT", "token_contracts").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &ContractModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		contract := &Contract{
			ID:     "1",
			Name:   "Test Contract",
			Symbol: "TEST",
		}
		dbErr := db.RunInTransaction(context.Background(), dbConnectionPool, nil, func(tx db.Transaction) error {
			err = m.Insert(context.Background(), tx, contract)
			require.NoError(t, err)
			return nil
		})
		require.NoError(t, dbErr)

		dbErr = db.RunInTransaction(context.Background(), dbConnectionPool, nil, func(tx db.Transaction) error {
			err = m.Insert(context.Background(), tx, contract)
			require.ErrorContains(t, err, "duplicate key value violates unique constraint \"token_contracts_pkey\"")
			return nil
		})
		require.Error(t, dbErr)

		cleanUpDB()
	})
}

func TestContractModel_GetByID(t *testing.T) {
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	cleanUpDB := func() {
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM token_contracts`)
		require.NoError(t, err)
	}

	t.Run("returns error when contract not found", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "token_contracts", mock.Anything).Return()
		mockMetricsService.On("IncDBQueryError", "SELECT", "token_contracts", mock.Anything).Return()
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

func TestContractModel_Update(t *testing.T) {
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	cleanUpDB := func() {
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM token_contracts`)
		require.NoError(t, err)
	}

	t.Run("updates existing contract successfully", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("IncDBQueryError", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
		// Expectations for Insert
		mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "token_contracts", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "INSERT", "token_contracts").Return()
		// Expectations for Update
		mockMetricsService.On("ObserveDBQueryDuration", "UPDATE", "token_contracts", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "UPDATE", "token_contracts").Return()
		// Expectations for GetByID
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "token_contracts", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "SELECT", "token_contracts").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &ContractModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		// First insert a contract
		contract := &Contract{
			ID:     "1",
			Name:   "Test Contract",
			Symbol: "TEST",
		}

		dbErr := db.RunInTransaction(context.Background(), dbConnectionPool, nil, func(tx db.Transaction) error {
			err = m.Insert(context.Background(), tx, contract)
			require.NoError(t, err)
			return nil
		})
		require.NoError(t, dbErr)

		// Update the contract
		contract.Name = "Updated Contract"
		contract.Symbol = "UPDATED"

		dbErr = db.RunInTransaction(context.Background(), dbConnectionPool, nil, func(tx db.Transaction) error {
			err = m.Update(context.Background(), tx, contract)
			require.NoError(t, err)
			return nil
		})
		require.NoError(t, dbErr)

		// Verify the update
		var updatedContract *Contract
		updatedContract, err = m.GetByID(context.Background(), "1")
		require.NoError(t, err)
		require.Equal(t, "Updated Contract", updatedContract.Name)
		require.Equal(t, "UPDATED", updatedContract.Symbol)

		cleanUpDB()
	})

	t.Run("returns no error for non-existent contract", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("IncDBQueryError", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
		mockMetricsService.On("ObserveDBQueryDuration", "UPDATE", "token_contracts", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "UPDATE", "token_contracts").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &ContractModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		contract := &Contract{
			ID:     "nonexistent",
			Name:   "Test Contract",
			Symbol: "TEST",
		}

		dbErr := db.RunInTransaction(context.Background(), dbConnectionPool, nil, func(tx db.Transaction) error {
			err = m.Update(context.Background(), tx, contract)
			require.NoError(t, err)
			return nil
		})
		require.NoError(t, dbErr)

		cleanUpDB()
	})
}

func TestContractModel_GetAll(t *testing.T) {
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	cleanUpDB := func() {
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM token_contracts`)
		require.NoError(t, err)
	}

	t.Run("returns empty slice when no contracts exist", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("IncDBQueryError", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "token_contracts", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "SELECT", "token_contracts").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &ContractModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		var contracts []*Contract
		contracts, err = m.GetAll(context.Background())
		require.NoError(t, err)
		require.Empty(t, contracts)

		cleanUpDB()
	})

	t.Run("returns all contracts when multiple exist", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("IncDBQueryError", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
		// Expectations for Insert operations
		mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "token_contracts", mock.Anything).Return().Times(2)
		mockMetricsService.On("IncDBQuery", "INSERT", "token_contracts").Return().Times(2)
		// Expectations for GetAll
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "token_contracts", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "SELECT", "token_contracts").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &ContractModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		// Insert multiple contracts
		contract1 := &Contract{
			ID:     "1",
			Name:   "Contract One",
			Symbol: "ONE",
		}
		contract2 := &Contract{
			ID:     "2",
			Name:   "Contract Two",
			Symbol: "TWO",
		}

		dbErr := db.RunInTransaction(context.Background(), dbConnectionPool, nil, func(tx db.Transaction) error {
			err = m.Insert(context.Background(), tx, contract1)
			require.NoError(t, err)
			err = m.Insert(context.Background(), tx, contract2)
			require.NoError(t, err)
			return nil
		})
		require.NoError(t, dbErr)

		// Get all contracts
		contracts, err := m.GetAll(context.Background())
		require.NoError(t, err)
		require.Len(t, contracts, 2)

		// Verify both contracts are returned (order may vary)
		contractMap := make(map[string]*Contract)
		for _, c := range contracts {
			contractMap[c.ID] = c
		}

		require.Contains(t, contractMap, "1")
		require.Contains(t, contractMap, "2")
		require.Equal(t, "Contract One", contractMap["1"].Name)
		require.Equal(t, "ONE", contractMap["1"].Symbol)
		require.Equal(t, "Contract Two", contractMap["2"].Name)
		require.Equal(t, "TWO", contractMap["2"].Symbol)

		cleanUpDB()
	})
}
