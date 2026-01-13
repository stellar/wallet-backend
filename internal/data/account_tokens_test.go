// Unit tests for AccountTokensModel.
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

// setupTrustlineAssets inserts test trustline assets and returns their IDs.
func setupTrustlineAssets(t *testing.T, ctx context.Context, dbPool db.ConnectionPool, count int) []int64 {
	ids := make([]int64, count)
	for i := 0; i < count; i++ {
		code := "TEST" + string(rune('A'+i))
		issuer := "GISSUER" + string(rune('A'+i)) + "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
		id := DeterministicAssetID(code, issuer)
		_, err := dbPool.PgxPool().Exec(ctx,
			`INSERT INTO trustline_assets (id, code, issuer) VALUES ($1, $2, $3)`,
			id, code, issuer,
		)
		require.NoError(t, err)
		ids[i] = id
	}
	return ids
}

// setupContractTokens inserts test contract tokens and returns their IDs.
func setupContractTokens(t *testing.T, ctx context.Context, dbPool db.ConnectionPool, count int) []int64 {
	ids := make([]int64, count)
	for i := 0; i < count; i++ {
		contractID := "CCONTRACT" + string(rune('A'+i)) + "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
		id := DeterministicContractID(contractID)
		_, err := dbPool.PgxPool().Exec(ctx,
			`INSERT INTO contract_tokens (id, contract_id, type, decimals) VALUES ($1, $2, $3, $4)`,
			id, contractID, "SAC", 7,
		)
		require.NoError(t, err)
		ids[i] = id
	}
	return ids
}

func TestAccountTokensModel_GetTrustlineAssetIDs(t *testing.T) {
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	cleanUpDB := func() {
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM account_trustlines`)
		require.NoError(t, err)
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM trustline_assets`)
		require.NoError(t, err)
	}

	t.Run("returns error for empty account address", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		defer mockMetricsService.AssertExpectations(t)

		m := &AccountTokensModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		ids, err := m.GetTrustlineAssetIDs(ctx, "")
		require.Error(t, err)
		require.Contains(t, err.Error(), "empty account address")
		require.Nil(t, ids)
	})

	t.Run("returns empty for non-existent account", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "GetTrustlineAssetIDs", "account_trustlines", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "GetTrustlineAssetIDs", "account_trustlines").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &AccountTokensModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		ids, err := m.GetTrustlineAssetIDs(ctx, "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5")
		require.NoError(t, err)
		require.Empty(t, ids)
	})

	t.Run("returns asset IDs for existing account", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "BulkInsertTrustlines", "account_trustlines", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "BulkInsertTrustlines", "account_trustlines").Return()
		mockMetricsService.On("ObserveDBQueryDuration", "GetTrustlineAssetIDs", "account_trustlines", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "GetTrustlineAssetIDs", "account_trustlines").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &AccountTokensModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		// Setup test trustline assets first
		assetIDs := setupTrustlineAssets(t, ctx, dbConnectionPool, 3)

		// Insert test data
		accountAddress := "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5"
		err = db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return m.BulkInsertTrustlines(ctx, dbTx, map[string][]int64{accountAddress: assetIDs})
		})
		require.NoError(t, err)

		// Retrieve and verify
		ids, err := m.GetTrustlineAssetIDs(ctx, accountAddress)
		require.NoError(t, err)
		require.ElementsMatch(t, assetIDs, ids)

		cleanUpDB()
	})
}

func TestAccountTokensModel_GetContractIDs(t *testing.T) {
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	cleanUpDB := func() {
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM account_contracts`)
		require.NoError(t, err)
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM contract_tokens`)
		require.NoError(t, err)
	}

	t.Run("returns error for empty account address", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		defer mockMetricsService.AssertExpectations(t)

		m := &AccountTokensModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		ids, err := m.GetContractIDs(ctx, "")
		require.Error(t, err)
		require.Contains(t, err.Error(), "empty account address")
		require.Nil(t, ids)
	})

	t.Run("returns empty for non-existent account", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "GetContractIDs", "account_contracts", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "GetContractIDs", "account_contracts").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &AccountTokensModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		ids, err := m.GetContractIDs(ctx, "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5")
		require.NoError(t, err)
		require.Empty(t, ids)
	})

	t.Run("returns contract IDs for existing account", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "BulkInsertContracts", "account_contracts", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "BulkInsertContracts", "account_contracts").Return()
		mockMetricsService.On("ObserveDBQueryDuration", "GetContractIDs", "account_contracts", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "GetContractIDs", "account_contracts").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &AccountTokensModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		// Setup test contract tokens first
		contractIDs := setupContractTokens(t, ctx, dbConnectionPool, 3)

		// Insert test data
		accountAddress := "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5"
		err = db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return m.BulkInsertContracts(ctx, dbTx, map[string][]int64{accountAddress: contractIDs})
		})
		require.NoError(t, err)

		// Retrieve and verify
		ids, err := m.GetContractIDs(ctx, accountAddress)
		require.NoError(t, err)
		require.ElementsMatch(t, contractIDs, ids)

		cleanUpDB()
	})
}

func TestAccountTokensModel_BatchAddContracts(t *testing.T) {
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	cleanUpDB := func() {
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM account_contracts`)
		require.NoError(t, err)
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM contract_tokens`)
		require.NoError(t, err)
	}

	t.Run("returns nil for empty input", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		defer mockMetricsService.AssertExpectations(t)

		m := &AccountTokensModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		err := db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return m.BatchAddContracts(ctx, dbTx, map[string][]int64{})
		})
		require.NoError(t, err)
	})

	t.Run("adds contracts to new account", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "BatchAddContracts", "account_contracts", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "BatchAddContracts", "account_contracts").Return()
		mockMetricsService.On("ObserveDBQueryDuration", "GetContractIDs", "account_contracts", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "GetContractIDs", "account_contracts").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &AccountTokensModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		// Setup test contract tokens first
		contractIDs := setupContractTokens(t, ctx, dbConnectionPool, 2)

		accountAddress := "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5"

		err = db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return m.BatchAddContracts(ctx, dbTx, map[string][]int64{accountAddress: contractIDs})
		})
		require.NoError(t, err)

		// Verify
		result, err := m.GetContractIDs(ctx, accountAddress)
		require.NoError(t, err)
		require.ElementsMatch(t, contractIDs, result)

		cleanUpDB()
	})

	t.Run("appends contracts to existing account without duplicates", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "BatchAddContracts", "account_contracts", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "BatchAddContracts", "account_contracts").Return()
		mockMetricsService.On("ObserveDBQueryDuration", "GetContractIDs", "account_contracts", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "GetContractIDs", "account_contracts").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &AccountTokensModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		// Setup test contract tokens first
		contractIDs := setupContractTokens(t, ctx, dbConnectionPool, 3)

		accountAddress := "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5"

		// First add
		err = db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return m.BatchAddContracts(ctx, dbTx, map[string][]int64{accountAddress: contractIDs[:2]})
		})
		require.NoError(t, err)

		// Second add with overlap (contractIDs[1] is duplicated)
		err = db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return m.BatchAddContracts(ctx, dbTx, map[string][]int64{accountAddress: contractIDs[1:]})
		})
		require.NoError(t, err)

		// Verify - should have all 3 without duplicates
		result, err := m.GetContractIDs(ctx, accountAddress)
		require.NoError(t, err)
		require.Len(t, result, 3)
		require.ElementsMatch(t, contractIDs, result)

		cleanUpDB()
	})
}

func TestAccountTokensModel_BatchUpsertTrustlines(t *testing.T) {
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	cleanUpDB := func() {
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM account_trustlines`)
		require.NoError(t, err)
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM trustline_assets`)
		require.NoError(t, err)
	}

	t.Run("returns nil for empty input", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		defer mockMetricsService.AssertExpectations(t)

		m := &AccountTokensModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		err := db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return m.BatchUpsertTrustlines(ctx, dbTx, map[string]*TrustlineChanges{})
		})
		require.NoError(t, err)
	})

	t.Run("adds and removes trustlines", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "BatchUpsertTrustlines", "account_trustlines", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "BatchUpsertTrustlines", "account_trustlines").Return()
		mockMetricsService.On("ObserveDBQueryDuration", "GetTrustlineAssetIDs", "account_trustlines", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "GetTrustlineAssetIDs", "account_trustlines").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &AccountTokensModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		// Setup test trustline assets
		assetIDs := setupTrustlineAssets(t, ctx, dbConnectionPool, 5)
		accountAddress := "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5"

		// First add some trustlines
		err = db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return m.BatchUpsertTrustlines(ctx, dbTx, map[string]*TrustlineChanges{
				accountAddress: {AddIDs: assetIDs[:3]}, // Add first 3
			})
		})
		require.NoError(t, err)

		// Verify first add
		result, err := m.GetTrustlineAssetIDs(ctx, accountAddress)
		require.NoError(t, err)
		require.ElementsMatch(t, assetIDs[:3], result)

		// Now add 2 more and remove 1
		err = db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return m.BatchUpsertTrustlines(ctx, dbTx, map[string]*TrustlineChanges{
				accountAddress: {
					AddIDs:    assetIDs[3:5], // Add last 2
					RemoveIDs: assetIDs[0:1], // Remove first 1
				},
			})
		})
		require.NoError(t, err)

		// Verify - should have IDs[1], IDs[2], IDs[3], IDs[4]
		result, err = m.GetTrustlineAssetIDs(ctx, accountAddress)
		require.NoError(t, err)
		require.Len(t, result, 4)
		require.ElementsMatch(t, assetIDs[1:5], result)

		cleanUpDB()
	})
}

func TestAccountTokensModel_BulkInsertTrustlines(t *testing.T) {
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	cleanUpDB := func() {
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM account_trustlines`)
		require.NoError(t, err)
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM trustline_assets`)
		require.NoError(t, err)
	}

	t.Run("returns nil for empty input", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		defer mockMetricsService.AssertExpectations(t)

		m := &AccountTokensModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		err := db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return m.BulkInsertTrustlines(ctx, dbTx, map[string][]int64{})
		})
		require.NoError(t, err)
	})

	t.Run("inserts trustlines for multiple accounts", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "BulkInsertTrustlines", "account_trustlines", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "BulkInsertTrustlines", "account_trustlines").Return()
		mockMetricsService.On("ObserveDBQueryDuration", "GetTrustlineAssetIDs", "account_trustlines", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "GetTrustlineAssetIDs", "account_trustlines").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &AccountTokensModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		// Setup test trustline assets
		assetIDs := setupTrustlineAssets(t, ctx, dbConnectionPool, 5)

		account1 := "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5"
		account2 := "GCQYG3MNNPFNFUBWXF5IDNNC7V3ZDLWLKSQVHFZEBWNPPQ4XVRCVHWQJ"
		ids1 := assetIDs[:3]
		ids2 := assetIDs[3:]

		err = db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return m.BulkInsertTrustlines(ctx, dbTx, map[string][]int64{
				account1: ids1,
				account2: ids2,
			})
		})
		require.NoError(t, err)

		// Verify account1
		result1, err := m.GetTrustlineAssetIDs(ctx, account1)
		require.NoError(t, err)
		require.ElementsMatch(t, ids1, result1)

		// Verify account2
		result2, err := m.GetTrustlineAssetIDs(ctx, account2)
		require.NoError(t, err)
		require.ElementsMatch(t, ids2, result2)

		cleanUpDB()
	})

	t.Run("fails on duplicate keys", func(t *testing.T) {
		// BulkInsertTrustlines uses COPY protocol which doesn't support ON CONFLICT.
		// This is by design - it's for initial population only (empty table, no duplicates).
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "BulkInsertTrustlines", "account_trustlines", mock.Anything).Return().Maybe()
		mockMetricsService.On("IncDBQuery", "BulkInsertTrustlines", "account_trustlines").Return().Maybe()
		defer mockMetricsService.AssertExpectations(t)

		m := &AccountTokensModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		// Setup test trustline assets
		assetIDs := setupTrustlineAssets(t, ctx, dbConnectionPool, 5)

		accountAddress := "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5"

		// First insert succeeds
		err = db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return m.BulkInsertTrustlines(ctx, dbTx, map[string][]int64{accountAddress: assetIDs[:2]})
		})
		require.NoError(t, err)

		// Second insert with overlap fails (COPY doesn't support ON CONFLICT)
		err = db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return m.BulkInsertTrustlines(ctx, dbTx, map[string][]int64{accountAddress: assetIDs[1:4]})
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "duplicate key")

		cleanUpDB()
	})
}

func TestAccountTokensModel_BulkInsertContracts(t *testing.T) {
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	cleanUpDB := func() {
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM account_contracts`)
		require.NoError(t, err)
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM contract_tokens`)
		require.NoError(t, err)
	}

	t.Run("returns nil for empty input", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		defer mockMetricsService.AssertExpectations(t)

		m := &AccountTokensModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		err := db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return m.BulkInsertContracts(ctx, dbTx, map[string][]int64{})
		})
		require.NoError(t, err)
	})

	t.Run("inserts contracts for multiple accounts", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "BulkInsertContracts", "account_contracts", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "BulkInsertContracts", "account_contracts").Return()
		mockMetricsService.On("ObserveDBQueryDuration", "GetContractIDs", "account_contracts", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "GetContractIDs", "account_contracts").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &AccountTokensModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		// Setup test contract tokens
		contractIDs := setupContractTokens(t, ctx, dbConnectionPool, 3)

		account1 := "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5"
		account2 := "GCQYG3MNNPFNFUBWXF5IDNNC7V3ZDLWLKSQVHFZEBWNPPQ4XVRCVHWQJ"
		contracts1 := contractIDs[:2]
		contracts2 := contractIDs[2:]

		err = db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return m.BulkInsertContracts(ctx, dbTx, map[string][]int64{
				account1: contracts1,
				account2: contracts2,
			})
		})
		require.NoError(t, err)

		// Verify account1
		result1, err := m.GetContractIDs(ctx, account1)
		require.NoError(t, err)
		require.ElementsMatch(t, contracts1, result1)

		// Verify account2
		result2, err := m.GetContractIDs(ctx, account2)
		require.NoError(t, err)
		require.ElementsMatch(t, contracts2, result2)

		cleanUpDB()
	})
}
