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
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM contract_tokens`)
		require.NoError(t, err)
	}

	t.Run("returns success for new contract", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("IncDBQueryError", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
		mockMetricsService.On("ObserveDBQueryDuration", "Insert", "contract_tokens", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "Insert", "contract_tokens").Return()
		mockMetricsService.On("ObserveDBQueryDuration", "GetByID", "contract_tokens", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "GetByID", "contract_tokens").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &ContractModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		contract := &Contract{
			ID:       "1",
			Name:     "Test Contract",
			Symbol:   "TEST",
			Decimals: 7,
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
		require.Equal(t, int16(7), contract.Decimals)

		cleanUpDB()
	})

	t.Run("returns error for duplicate contract", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("IncDBQueryError", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
		mockMetricsService.On("ObserveDBQueryDuration", "Insert", "contract_tokens", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "Insert", "contract_tokens").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &ContractModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		contract := &Contract{
			ID:       "1",
			Name:     "Test Contract",
			Symbol:   "TEST",
			Decimals: 7,
		}
		dbErr := db.RunInTransaction(context.Background(), dbConnectionPool, nil, func(tx db.Transaction) error {
			err = m.Insert(context.Background(), tx, contract)
			require.NoError(t, err)
			return nil
		})
		require.NoError(t, dbErr)

		dbErr = db.RunInTransaction(context.Background(), dbConnectionPool, nil, func(tx db.Transaction) error {
			err = m.Insert(context.Background(), tx, contract)
			require.ErrorContains(t, err, "duplicate key value violates unique constraint \"contract_tokens_pkey\"")
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

func TestContractModel_Update(t *testing.T) {
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

	t.Run("updates existing contract successfully", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("IncDBQueryError", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
		// Expectations for Insert
		mockMetricsService.On("ObserveDBQueryDuration", "Insert", "contract_tokens", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "Insert", "contract_tokens").Return()
		// Expectations for Update
		mockMetricsService.On("ObserveDBQueryDuration", "Update", "contract_tokens", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "Update", "contract_tokens").Return()
		// Expectations for GetByID
		mockMetricsService.On("ObserveDBQueryDuration", "GetByID", "contract_tokens", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "GetByID", "contract_tokens").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &ContractModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		// First insert a contract
		contract := &Contract{
			ID:       "1",
			Name:     "Test Contract",
			Symbol:   "TEST",
			Decimals: 7,
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
		contract.Decimals = 18

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
		require.Equal(t, int16(18), updatedContract.Decimals)

		cleanUpDB()
	})

	t.Run("returns no error for non-existent contract", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("IncDBQueryError", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
		mockMetricsService.On("ObserveDBQueryDuration", "Update", "contract_tokens", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "Update", "contract_tokens").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &ContractModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		contract := &Contract{
			ID:       "nonexistent",
			Name:     "Test Contract",
			Symbol:   "TEST",
			Decimals: 7,
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
