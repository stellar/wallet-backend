package data

import (
	"context"
	"testing"

	"github.com/google/uuid"
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

func TestContractModel_GetExisting(t *testing.T) {
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

	t.Run("returns nil for empty input", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		defer mockMetricsService.AssertExpectations(t)

		m := &ContractModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		err := db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			ids, txErr := m.GetExisting(ctx, dbTx, []string{})
			require.NoError(t, txErr)
			require.Nil(t, ids)
			return nil
		})
		require.NoError(t, err)
	})

	t.Run("returns empty slice when no matches", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "GetExisting", "contract_tokens", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "GetExisting", "contract_tokens").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &ContractModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		err := db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			ids, txErr := m.GetExisting(ctx, dbTx, []string{"nonexistent1", "nonexistent2"})
			require.NoError(t, txErr)
			require.Empty(t, ids)
			return nil
		})
		require.NoError(t, err)
	})

	t.Run("returns partial matches", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("IncDBQueryError", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
		mockMetricsService.On("ObserveDBQueryDuration", "BatchInsert", "contract_tokens", mock.Anything).Return()
		mockMetricsService.On("ObserveDBBatchSize", "BatchInsert", "contract_tokens", 2).Return()
		mockMetricsService.On("IncDBQuery", "BatchInsert", "contract_tokens").Return()
		mockMetricsService.On("ObserveDBQueryDuration", "GetExisting", "contract_tokens", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "GetExisting", "contract_tokens").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &ContractModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		name := "Test"
		symbol := "TST"
		contracts := []*Contract{
			{ID: DeterministicContractID("contract1"), ContractID: "contract1", Type: "sac", Name: &name, Symbol: &symbol, Decimals: 7},
			{ID: DeterministicContractID("contract2"), ContractID: "contract2", Type: "sep41", Name: &name, Symbol: &symbol, Decimals: 18},
		}

		err := db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			txErr := m.BatchInsert(ctx, dbTx, contracts)
			require.NoError(t, txErr)
			return nil
		})
		require.NoError(t, err)

		err = db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			ids, txErr := m.GetExisting(ctx, dbTx, []string{"contract1", "contract3", "contract4"})
			require.NoError(t, txErr)
			require.Len(t, ids, 1)
			require.Contains(t, ids, "contract1")
			return nil
		})
		require.NoError(t, err)

		cleanUpDB()
	})

	t.Run("returns all matches when all exist", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("IncDBQueryError", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
		mockMetricsService.On("ObserveDBQueryDuration", "BatchInsert", "contract_tokens", mock.Anything).Return()
		mockMetricsService.On("ObserveDBBatchSize", "BatchInsert", "contract_tokens", 3).Return()
		mockMetricsService.On("IncDBQuery", "BatchInsert", "contract_tokens").Return()
		mockMetricsService.On("ObserveDBQueryDuration", "GetExisting", "contract_tokens", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "GetExisting", "contract_tokens").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &ContractModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		name := "Test"
		symbol := "TST"
		contracts := []*Contract{
			{ID: DeterministicContractID("contract1"), ContractID: "contract1", Type: "sac", Name: &name, Symbol: &symbol, Decimals: 7},
			{ID: DeterministicContractID("contract2"), ContractID: "contract2", Type: "sep41", Name: &name, Symbol: &symbol, Decimals: 18},
			{ID: DeterministicContractID("contract3"), ContractID: "contract3", Type: "sac", Name: &name, Symbol: &symbol, Decimals: 6},
		}

		err := db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			txErr := m.BatchInsert(ctx, dbTx, contracts)
			require.NoError(t, txErr)
			return nil
		})
		require.NoError(t, err)

		err = db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			ids, txErr := m.GetExisting(ctx, dbTx, []string{"contract1", "contract2"})
			require.NoError(t, txErr)
			require.Len(t, ids, 2)
			require.ElementsMatch(t, []string{"contract1", "contract2"}, ids)
			return nil
		})
		require.NoError(t, err)

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

		contracts, err := m.BatchGetByIDs(ctx, []uuid.UUID{})
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

		// Use random UUIDs that don't exist in the database
		contracts, err := m.BatchGetByIDs(ctx, []uuid.UUID{uuid.New(), uuid.New()})
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
				ID:         DeterministicContractID("contract1"),
				ContractID: "contract1",
				Type:       "sac",
				Code:       &code1,
				Issuer:     &issuer1,
				Name:       &name1,
				Symbol:     &symbol1,
				Decimals:   7,
			},
			{
				ID:         DeterministicContractID("contract2"),
				ContractID: "contract2",
				Type:       "sep41",
				Code:       &code2,
				Issuer:     &issuer2,
				Name:       &name2,
				Symbol:     &symbol2,
				Decimals:   18,
			},
		}

		// Insert contracts first
		err := db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			txErr := m.BatchInsert(ctx, dbTx, contracts)
			require.NoError(t, txErr)
			return nil
		})
		require.NoError(t, err)

		// Fetch contracts by deterministic UUIDs (including a non-existent one)
		uuids := []uuid.UUID{DeterministicContractID("contract1"), DeterministicContractID("contract2"), uuid.New()}
		fetchedContracts, err := m.BatchGetByIDs(ctx, uuids)
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
			txErr := m.BatchInsert(ctx, dbTx, []*Contract{})
			require.NoError(t, txErr)
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
				ID:         DeterministicContractID("contract1"),
				ContractID: "contract1",
				Type:       "sac",
				Code:       &code1,
				Issuer:     &issuer1,
				Name:       &name1,
				Symbol:     &symbol1,
				Decimals:   7,
			},
			{
				ID:         DeterministicContractID("contract2"),
				ContractID: "contract2",
				Type:       "sep41",
				Code:       &code2,
				Issuer:     &issuer2,
				Name:       &name2,
				Symbol:     &symbol2,
				Decimals:   18,
			},
			{
				ID:         DeterministicContractID("contract3"),
				ContractID: "contract3",
				Type:       "unknown",
				Name:       &name3,
				Symbol:     &symbol3,
				Decimals:   6,
			},
		}

		err := db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			txErr := m.BatchInsert(ctx, dbTx, contracts)
			require.NoError(t, txErr)
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
				ID:         DeterministicContractID("contract1"),
				ContractID: "contract1",
				Type:       "sac",
				Name:       &origName,
				Symbol:     &origSymbol,
				Decimals:   7,
			},
		}

		err := db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			txErr := m.BatchInsert(ctx, dbTx, contracts)
			require.NoError(t, txErr)
			return nil
		})
		require.NoError(t, err)

		// Second insert with same ContractID and different data - should be skipped (ON CONFLICT DO NOTHING)
		newName := "New Name"
		newSymbol := "NEW"
		contract2Name := "Contract 2"
		contract2Symbol := "C2"
		contracts = []*Contract{
			{
				ID:         DeterministicContractID("contract1"),
				ContractID: "contract1",
				Type:       "sep41",
				Name:       &newName,
				Symbol:     &newSymbol,
				Decimals:   18,
			},
			{
				ID:         DeterministicContractID("contract2"),
				ContractID: "contract2",
				Type:       "unknown",
				Name:       &contract2Name,
				Symbol:     &contract2Symbol,
				Decimals:   6,
			},
		}

		err = db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			txErr := m.BatchInsert(ctx, dbTx, contracts)
			require.NoError(t, txErr)
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
