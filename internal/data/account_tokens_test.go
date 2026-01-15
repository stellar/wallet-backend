// Unit tests for AccountTokensModel.
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

// setupTrustlineAssets inserts test trustline assets and returns their IDs.
func setupTrustlineAssets(t *testing.T, ctx context.Context, dbPool db.ConnectionPool, count int) []uuid.UUID {
	ids := make([]uuid.UUID, count)
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
func setupContractTokens(t *testing.T, ctx context.Context, dbPool db.ConnectionPool, count int) []uuid.UUID {
	ids := make([]uuid.UUID, count)
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

func TestAccountTokensModel_GetTrustlines(t *testing.T) {
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

		trustlines, err := m.GetTrustlines(ctx, "")
		require.Error(t, err)
		require.Contains(t, err.Error(), "empty account address")
		require.Nil(t, trustlines)
	})

	t.Run("returns empty for non-existent account", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "GetTrustlines", "account_trustlines", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "GetTrustlines", "account_trustlines").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &AccountTokensModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		trustlines, err := m.GetTrustlines(ctx, "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5")
		require.NoError(t, err)
		require.Empty(t, trustlines)
	})

	t.Run("returns trustlines with full data for existing account", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "BatchInsertTrustlines", "account_trustlines", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "BatchInsertTrustlines", "account_trustlines").Return()
		mockMetricsService.On("ObserveDBQueryDuration", "GetTrustlines", "account_trustlines", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "GetTrustlines", "account_trustlines").Return()
		defer mockMetricsService.AssertExpectations(t)

		m := &AccountTokensModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		// Setup test trustline assets first
		assetIDs := setupTrustlineAssets(t, ctx, dbConnectionPool, 3)

		// Insert test data
		accountAddress := "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5"
		trustlineData := make([]Trustline, len(assetIDs))
		for i, id := range assetIDs {
			trustlineData[i] = Trustline{AccountAddress: accountAddress, AssetID: id, Balance: int64(i * 100), Limit: 1000000, LedgerNumber: 100}
		}
		err = db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return m.BatchInsertTrustlines(ctx, dbTx, trustlineData)
		})
		require.NoError(t, err)

		// Retrieve and verify
		trustlines, err := m.GetTrustlines(ctx, accountAddress)
		require.NoError(t, err)
		require.Len(t, trustlines, 3)

		// Verify each trustline has code and issuer from JOIN
		for _, tl := range trustlines {
			require.NotEmpty(t, tl.Code)
			require.NotEmpty(t, tl.Issuer)
		}

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
		mockMetricsService.On("ObserveDBQueryDuration", "BatchInsertContractTokens", "account_contracts", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "BatchInsertContractTokens", "account_contracts").Return()
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
			return m.BatchInsertContractTokens(ctx, dbTx, map[string][]uuid.UUID{accountAddress: contractIDs})
		})
		require.NoError(t, err)

		// Retrieve and verify
		ids, err := m.GetContractIDs(ctx, accountAddress)
		require.NoError(t, err)
		require.ElementsMatch(t, contractIDs, ids)

		cleanUpDB()
	})
}

func TestAccountTokensModel_BatchInsertContractTokens(t *testing.T) {
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
			return m.BatchInsertContractTokens(ctx, dbTx, map[string][]uuid.UUID{})
		})
		require.NoError(t, err)
	})

	t.Run("inserts contracts to new account", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "BatchInsertContractTokens", "account_contracts", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "BatchInsertContractTokens", "account_contracts").Return()
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
			return m.BatchInsertContractTokens(ctx, dbTx, map[string][]uuid.UUID{accountAddress: contractIDs})
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
		mockMetricsService.On("ObserveDBQueryDuration", "BatchInsertContractTokens", "account_contracts", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "BatchInsertContractTokens", "account_contracts").Return()
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

		// First insert
		err = db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return m.BatchInsertContractTokens(ctx, dbTx, map[string][]uuid.UUID{accountAddress: contractIDs[:2]})
		})
		require.NoError(t, err)

		// Second insert with overlap (contractIDs[1] is duplicated)
		err = db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return m.BatchInsertContractTokens(ctx, dbTx, map[string][]uuid.UUID{accountAddress: contractIDs[1:]})
		})
		require.NoError(t, err)

		// Verify - should have all 3 without duplicates
		result, err := m.GetContractIDs(ctx, accountAddress)
		require.NoError(t, err)
		require.Len(t, result, 3)
		require.ElementsMatch(t, contractIDs, result)

		cleanUpDB()
	})

	t.Run("inserts contracts for multiple accounts", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "BatchInsertContractTokens", "account_contracts", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "BatchInsertContractTokens", "account_contracts").Return()
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
			return m.BatchInsertContractTokens(ctx, dbTx, map[string][]uuid.UUID{
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

func TestAccountTokensModel_BatchInsertTrustlines(t *testing.T) {
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
			return m.BatchInsertTrustlines(ctx, dbTx, []Trustline{})
		})
		require.NoError(t, err)
	})

	t.Run("inserts trustlines for multiple accounts", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "BatchInsertTrustlines", "account_trustlines", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "BatchInsertTrustlines", "account_trustlines").Return()
		mockMetricsService.On("ObserveDBQueryDuration", "GetTrustlines", "account_trustlines", mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", "GetTrustlines", "account_trustlines").Return()
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

		// Build trustlines slice
		var trustlines []Trustline
		for _, id := range ids1 {
			trustlines = append(trustlines, Trustline{AccountAddress: account1, AssetID: id, LedgerNumber: 100})
		}
		for _, id := range ids2 {
			trustlines = append(trustlines, Trustline{AccountAddress: account2, AssetID: id, LedgerNumber: 100})
		}

		err = db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return m.BatchInsertTrustlines(ctx, dbTx, trustlines)
		})
		require.NoError(t, err)

		// Verify account1
		trustlines1, err := m.GetTrustlines(ctx, account1)
		require.NoError(t, err)
		resultIDs1 := make([]uuid.UUID, len(trustlines1))
		for i, tl := range trustlines1 {
			resultIDs1[i] = tl.AssetID
		}
		require.ElementsMatch(t, ids1, resultIDs1)

		// Verify account2
		trustlines2, err := m.GetTrustlines(ctx, account2)
		require.NoError(t, err)
		resultIDs2 := make([]uuid.UUID, len(trustlines2))
		for i, tl := range trustlines2 {
			resultIDs2[i] = tl.AssetID
		}
		require.ElementsMatch(t, ids2, resultIDs2)

		cleanUpDB()
	})

	t.Run("fails on duplicate keys", func(t *testing.T) {
		// BatchInsertTrustlines uses COPY protocol which doesn't support ON CONFLICT.
		// This is by design - it's for initial population only (empty table, no duplicates).
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", "BatchInsertTrustlines", "account_trustlines", mock.Anything).Return().Maybe()
		mockMetricsService.On("IncDBQuery", "BatchInsertTrustlines", "account_trustlines").Return().Maybe()
		defer mockMetricsService.AssertExpectations(t)

		m := &AccountTokensModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}

		// Setup test trustline assets
		assetIDs := setupTrustlineAssets(t, ctx, dbConnectionPool, 5)

		accountAddress := "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5"

		// Build trustlines slices
		var tl1 []Trustline
		for _, id := range assetIDs[:2] {
			tl1 = append(tl1, Trustline{AccountAddress: accountAddress, AssetID: id, LedgerNumber: 100})
		}
		var tl2 []Trustline
		for _, id := range assetIDs[1:4] {
			tl2 = append(tl2, Trustline{AccountAddress: accountAddress, AssetID: id, LedgerNumber: 100})
		}

		// First insert succeeds
		err = db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return m.BatchInsertTrustlines(ctx, dbTx, tl1)
		})
		require.NoError(t, err)

		// Second insert with overlap fails (COPY doesn't support ON CONFLICT)
		err = db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return m.BatchInsertTrustlines(ctx, dbTx, tl2)
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "duplicate key")

		cleanUpDB()
	})
}
