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

		code := "TEST"
		issuer := "GTEST123"
		contract := &Contract{
			ID:       "1",
			Type:     "sep41",
			Code:     &code,
			Issuer:   &issuer,
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
		require.Equal(t, contract.Type, "sep41")
		require.Equal(t, "TEST", *contract.Code)
		require.Equal(t, "GTEST123", *contract.Issuer)
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
			Type:     "unknown",
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
		code := "TEST"
		issuer := "GTEST123"
		contract := &Contract{
			ID:       "1",
			Type:     "sac",
			Code:     &code,
			Issuer:   &issuer,
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
		newCode := "UPDATED"
		newIssuer := "GUPDATED123"
		contract.Type = "sep41"
		contract.Code = &newCode
		contract.Issuer = &newIssuer
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
		require.Equal(t, "sep41", updatedContract.Type)
		require.Equal(t, "UPDATED", *updatedContract.Code)
		require.Equal(t, "GUPDATED123", *updatedContract.Issuer)
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
			Type:     "unknown",
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

		insertedIDs, err := m.BatchInsert(ctx, nil, []*Contract{})
		require.NoError(t, err)
		require.Nil(t, insertedIDs)
	})

	t.Run("returns success for multiple new contracts", func(t *testing.T) {
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
		code2 := "TEST2"
		issuer2 := "GTEST456"
		contracts := []*Contract{
			{
				ID:       "contract1",
				Type:     "sac",
				Code:     &code1,
				Issuer:   &issuer1,
				Name:     "Test Contract 1",
				Symbol:   "TST1",
				Decimals: 7,
			},
			{
				ID:       "contract2",
				Type:     "sep41",
				Code:     &code2,
				Issuer:   &issuer2,
				Name:     "Test Contract 2",
				Symbol:   "TST2",
				Decimals: 18,
			},
			{
				ID:       "contract3",
				Type:     "unknown",
				Name:     "Test Contract 3",
				Symbol:   "TST3",
				Decimals: 6,
			},
		}

		insertedIDs, err := m.BatchInsert(ctx, nil, contracts)
		require.NoError(t, err)
		require.Len(t, insertedIDs, 3)

		// Verify contracts were inserted
		contract1, err := m.GetByID(ctx, "contract1")
		require.NoError(t, err)
		require.Equal(t, "sac", contract1.Type)
		require.Equal(t, "TEST1", *contract1.Code)
		require.Equal(t, "GTEST123", *contract1.Issuer)
		require.Equal(t, "Test Contract 1", contract1.Name)
		require.Equal(t, "TST1", contract1.Symbol)
		require.Equal(t, int16(7), contract1.Decimals)

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
		contracts := []*Contract{
			{
				ID:       "contract1",
				Type:     "sac",
				Name:     "Original Name",
				Symbol:   "ORIG",
				Decimals: 7,
			},
		}

		insertedIDs, err := m.BatchInsert(ctx, nil, contracts)
		require.NoError(t, err)
		require.Len(t, insertedIDs, 1)

		// Second insert with same ID and different data - should be skipped
		contracts = []*Contract{
			{
				ID:       "contract1",
				Type:     "sep41",
				Name:     "New Name",
				Symbol:   "NEW",
				Decimals: 18,
			},
			{
				ID:       "contract2",
				Type:     "unknown",
				Name:     "Contract 2",
				Symbol:   "C2",
				Decimals: 6,
			},
		}

		insertedIDs, err = m.BatchInsert(ctx, nil, contracts)
		require.NoError(t, err)
		require.Len(t, insertedIDs, 1) // Only contract2 should be inserted
		require.Equal(t, "contract2", insertedIDs[0])

		// Verify original contract was not updated
		contract1, err := m.GetByID(ctx, "contract1")
		require.NoError(t, err)
		require.Equal(t, "sac", contract1.Type)
		require.Equal(t, "Original Name", contract1.Name)
		require.Equal(t, "ORIG", contract1.Symbol)
		require.Equal(t, int16(7), contract1.Decimals)

		cleanUpDB()
	})
}
