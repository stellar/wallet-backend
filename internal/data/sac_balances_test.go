// Unit tests for SACBalanceModel.
package data

import (
	"context"
	"crypto/rand"
	"slices"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
)

// randomContractAddress generates a random valid C-address for testing.
// SAC balances store contract holder addresses which are C-addresses (not G-addresses).
func randomContractAddress(t *testing.T) string {
	t.Helper()
	var raw [32]byte
	_, err := rand.Read(raw[:])
	require.NoError(t, err)
	addr, err := strkey.Encode(strkey.VersionByteContract, raw[:])
	require.NoError(t, err)
	return addr
}

func TestSACBalanceModel_GetByAccount(t *testing.T) {
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	// Insert test contract tokens for foreign key references
	contractAddr1 := "CCONTRACT1AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	contractAddr2 := "CCONTRACT2AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	contractAddr3 := "CCONTRACT3AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	contractID1 := DeterministicContractID(contractAddr1)
	contractID2 := DeterministicContractID(contractAddr2)
	contractID3 := DeterministicContractID(contractAddr3)
	_, err = dbConnectionPool.Exec(ctx, `
		INSERT INTO contract_tokens (id, contract_id, type, code, issuer, decimals) VALUES
		($1, $2, 'SAC', 'USDC', 'GISSUER1AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', 7),
		($3, $4, 'SAC', 'EURC', 'GISSUER2AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', 7),
		($5, $6, 'SAC', 'BTC', 'GISSUER3AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', 7)
	`, contractID1, contractAddr1, contractID2, contractAddr2, contractID3, contractAddr3)
	require.NoError(t, err)

	cleanUpDB := func() {
		_, err = dbConnectionPool.Exec(ctx, `DELETE FROM sac_balances`)
		require.NoError(t, err)
	}

	reg := prometheus.NewRegistry()
	dbMetrics := metrics.NewMetrics(reg).DB

	t.Run("returns error for empty account address", func(t *testing.T) {
		m := &SACBalanceModel{
			DB:      dbConnectionPool,
			Metrics: dbMetrics,
		}

		balances, err := m.GetByAccount(ctx, "", nil, nil, ASC)
		require.Error(t, err)
		require.Nil(t, balances)
		require.Contains(t, err.Error(), "empty account address")
	})

	t.Run("returns empty slice for account with no SAC balances", func(t *testing.T) {
		cleanUpDB()

		m := &SACBalanceModel{
			DB:      dbConnectionPool,
			Metrics: dbMetrics,
		}

		balances, err := m.GetByAccount(ctx, randomContractAddress(t), nil, nil, ASC)
		require.NoError(t, err)
		require.Empty(t, balances)
	})

	t.Run("returns single SAC balance with correct data from JOIN", func(t *testing.T) {
		cleanUpDB()

		m := &SACBalanceModel{
			DB:      dbConnectionPool,
			Metrics: dbMetrics,
		}

		accountAddr := randomContractAddress(t)
		_, err := dbConnectionPool.Exec(ctx, `
			INSERT INTO sac_balances
			(account_id, contract_id, balance, is_authorized, is_clawback_enabled, last_modified_ledger)
			VALUES ($1, $2, '1000000000', true, false, 12345)
		`, types.AddressBytea(accountAddr), contractID1)
		require.NoError(t, err)

		balances, err := m.GetByAccount(ctx, accountAddr, nil, nil, ASC)
		require.NoError(t, err)
		require.Len(t, balances, 1)

		// Verify all fields including JOIN data
		require.Equal(t, types.AddressBytea(accountAddr), balances[0].AccountID)
		require.Equal(t, contractID1, balances[0].ContractID)
		require.Equal(t, "1000000000", balances[0].Balance)
		require.True(t, balances[0].IsAuthorized)
		require.False(t, balances[0].IsClawbackEnabled)
		require.Equal(t, uint32(12345), balances[0].LedgerNumber)
		// Verify JOIN data from contract_tokens
		require.Equal(t, contractAddr1, balances[0].TokenID)
		require.Equal(t, "USDC", balances[0].Code)
		require.Equal(t, "GISSUER1AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", balances[0].Issuer)
		require.Equal(t, uint32(7), balances[0].Decimals)
	})

	t.Run("returns multiple SAC balances for account", func(t *testing.T) {
		cleanUpDB()

		m := &SACBalanceModel{
			DB:      dbConnectionPool,
			Metrics: dbMetrics,
		}

		accountAddr := randomContractAddress(t)
		_, err := dbConnectionPool.Exec(ctx, `
			INSERT INTO sac_balances
			(account_id, contract_id, balance, is_authorized, is_clawback_enabled, last_modified_ledger)
			VALUES
			($1, $2, '1000', true, false, 100),
			($1, $3, '2000', true, true, 101)
		`, types.AddressBytea(accountAddr), contractID1, contractID2)
		require.NoError(t, err)

		balances, err := m.GetByAccount(ctx, accountAddr, nil, nil, ASC)
		require.NoError(t, err)
		require.Len(t, balances, 2)

		// Verify both balances belong to the correct account
		for _, b := range balances {
			require.Equal(t, types.AddressBytea(accountAddr), b.AccountID)
		}
	})

	t.Run("paginates with limit and cursor in ASC and DESC order", func(t *testing.T) {
		cleanUpDB()

		m := &SACBalanceModel{
			DB:      dbConnectionPool,
			Metrics: dbMetrics,
		}

		accountAddr := randomContractAddress(t)
		_, err := dbConnectionPool.Exec(ctx, `
			INSERT INTO sac_balances
			(account_id, contract_id, balance, is_authorized, is_clawback_enabled, last_modified_ledger)
			VALUES
			($1, $2, '1000', true, false, 100),
			($1, $3, '2000', true, false, 101),
			($1, $4, '3000', true, false, 102)
		`, types.AddressBytea(accountAddr), contractID1, contractID2, contractID3)
		require.NoError(t, err)

		expectedOrder := []string{contractID1.String(), contractID2.String(), contractID3.String()}
		slices.Sort(expectedOrder)

		limit := int32(2)
		page, err := m.GetByAccount(ctx, accountAddr, &limit, nil, ASC)
		require.NoError(t, err)
		require.Len(t, page, 2)
		require.Equal(t, expectedOrder[0], page[0].ContractID.String())
		require.Equal(t, expectedOrder[1], page[1].ContractID.String())

		cursor := page[1].ContractID
		nextPage, err := m.GetByAccount(ctx, accountAddr, &limit, &cursor, ASC)
		require.NoError(t, err)
		require.Len(t, nextPage, 1)
		require.Equal(t, expectedOrder[2], nextPage[0].ContractID.String())

		descPage, err := m.GetByAccount(ctx, accountAddr, &limit, nil, DESC)
		require.NoError(t, err)
		require.Len(t, descPage, 2)
		require.Equal(t, expectedOrder[2], descPage[0].ContractID.String())
		require.Equal(t, expectedOrder[1], descPage[1].ContractID.String())
	})
}

func TestSACBalanceModel_BatchUpsert(t *testing.T) {
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	// Insert test contract tokens for foreign key references
	contractAddr1 := "CCONTRACT1AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	contractAddr2 := "CCONTRACT2AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	contractID1 := DeterministicContractID(contractAddr1)
	contractID2 := DeterministicContractID(contractAddr2)
	_, err = dbConnectionPool.Exec(ctx, `
		INSERT INTO contract_tokens (id, contract_id, type, code, issuer, decimals) VALUES
		($1, $2, 'SAC', 'USDC', 'GISSUER1AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', 7),
		($3, $4, 'SAC', 'EURC', 'GISSUER2AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', 7)
	`, contractID1, contractAddr1, contractID2, contractAddr2)
	require.NoError(t, err)

	cleanUpDB := func() {
		_, err = dbConnectionPool.Exec(ctx, `DELETE FROM sac_balances`)
		require.NoError(t, err)
	}

	reg := prometheus.NewRegistry()
	dbMetrics := metrics.NewMetrics(reg).DB

	t.Run("returns nil for empty upserts and deletes", func(t *testing.T) {
		m := &SACBalanceModel{
			DB:      dbConnectionPool,
			Metrics: dbMetrics,
		}

		pgxTx, err := dbConnectionPool.Begin(ctx)
		require.NoError(t, err)
		defer pgxTx.Rollback(ctx) //nolint:errcheck

		err = m.BatchUpsert(ctx, pgxTx, nil, nil)
		require.NoError(t, err)
	})

	t.Run("inserts new SAC balance", func(t *testing.T) {
		cleanUpDB()

		m := &SACBalanceModel{
			DB:      dbConnectionPool,
			Metrics: dbMetrics,
		}

		accountAddr := randomContractAddress(t)
		pgxTx, err := dbConnectionPool.Begin(ctx)
		require.NoError(t, err)

		upserts := []SACBalance{
			{
				AccountID:         types.AddressBytea(accountAddr),
				ContractID:        contractID1,
				Balance:           "1000000000",
				IsAuthorized:      true,
				IsClawbackEnabled: false,
				LedgerNumber:      12345,
			},
		}

		err = m.BatchUpsert(ctx, pgxTx, upserts, nil)
		require.NoError(t, err)
		require.NoError(t, pgxTx.Commit(ctx))

		// Verify insert
		var count int
		err = dbConnectionPool.QueryRow(ctx,
			`SELECT COUNT(*) FROM sac_balances WHERE account_id = $1`,
			types.AddressBytea(accountAddr)).Scan(&count)
		require.NoError(t, err)
		require.Equal(t, 1, count)
	})

	t.Run("updates existing SAC balance on conflict", func(t *testing.T) {
		cleanUpDB()

		m := &SACBalanceModel{
			DB:      dbConnectionPool,
			Metrics: dbMetrics,
		}

		accountAddr := randomContractAddress(t)

		// First insert
		pgxTx1, err := dbConnectionPool.Begin(ctx)
		require.NoError(t, err)
		upserts := []SACBalance{
			{
				AccountID:         types.AddressBytea(accountAddr),
				ContractID:        contractID1,
				Balance:           "1000",
				IsAuthorized:      true,
				IsClawbackEnabled: false,
				LedgerNumber:      100,
			},
		}
		err = m.BatchUpsert(ctx, pgxTx1, upserts, nil)
		require.NoError(t, err)
		require.NoError(t, pgxTx1.Commit(ctx))

		// Update with new values
		pgxTx2, err := dbConnectionPool.Begin(ctx)
		require.NoError(t, err)
		upserts[0].Balance = "2000"
		upserts[0].IsAuthorized = false
		upserts[0].LedgerNumber = 200
		err = m.BatchUpsert(ctx, pgxTx2, upserts, nil)
		require.NoError(t, err)
		require.NoError(t, pgxTx2.Commit(ctx))

		// Verify update
		var balance string
		var isAuthorized bool
		var ledger uint32
		err = dbConnectionPool.QueryRow(ctx,
			`SELECT balance, is_authorized, last_modified_ledger FROM sac_balances WHERE account_id = $1 AND contract_id = $2`,
			types.AddressBytea(accountAddr), contractID1).Scan(&balance, &isAuthorized, &ledger)
		require.NoError(t, err)
		require.Equal(t, "2000", balance)
		require.False(t, isAuthorized)
		require.Equal(t, uint32(200), ledger)
	})

	t.Run("deletes SAC balance", func(t *testing.T) {
		cleanUpDB()

		m := &SACBalanceModel{
			DB:      dbConnectionPool,
			Metrics: dbMetrics,
		}

		accountAddr := randomContractAddress(t)

		// First insert
		pgxTx1, err := dbConnectionPool.Begin(ctx)
		require.NoError(t, err)
		upserts := []SACBalance{
			{
				AccountID:    types.AddressBytea(accountAddr),
				ContractID:   contractID1,
				Balance:      "1000",
				LedgerNumber: 100,
			},
		}
		err = m.BatchUpsert(ctx, pgxTx1, upserts, nil)
		require.NoError(t, err)
		require.NoError(t, pgxTx1.Commit(ctx))

		// Delete
		pgxTx2, err := dbConnectionPool.Begin(ctx)
		require.NoError(t, err)
		deletes := []SACBalance{
			{AccountID: types.AddressBytea(accountAddr), ContractID: contractID1},
		}
		err = m.BatchUpsert(ctx, pgxTx2, nil, deletes)
		require.NoError(t, err)
		require.NoError(t, pgxTx2.Commit(ctx))

		// Verify delete
		var count int
		err = dbConnectionPool.QueryRow(ctx,
			`SELECT COUNT(*) FROM sac_balances WHERE account_id = $1`,
			types.AddressBytea(accountAddr)).Scan(&count)
		require.NoError(t, err)
		require.Equal(t, 0, count)
	})

	t.Run("handles combined upserts and deletes", func(t *testing.T) {
		cleanUpDB()

		m := &SACBalanceModel{
			DB:      dbConnectionPool,
			Metrics: dbMetrics,
		}

		accountAddr1 := randomContractAddress(t)
		accountAddr2 := randomContractAddress(t)

		// Insert two balances
		pgxTx1, err := dbConnectionPool.Begin(ctx)
		require.NoError(t, err)
		upserts := []SACBalance{
			{AccountID: types.AddressBytea(accountAddr1), ContractID: contractID1, Balance: "1000", LedgerNumber: 100},
			{AccountID: types.AddressBytea(accountAddr1), ContractID: contractID2, Balance: "2000", LedgerNumber: 100},
		}
		err = m.BatchUpsert(ctx, pgxTx1, upserts, nil)
		require.NoError(t, err)
		require.NoError(t, pgxTx1.Commit(ctx))

		// Update one, delete one, add new one for different account
		pgxTx2, err := dbConnectionPool.Begin(ctx)
		require.NoError(t, err)
		newUpserts := []SACBalance{
			{AccountID: types.AddressBytea(accountAddr1), ContractID: contractID1, Balance: "1500", LedgerNumber: 200}, // update
			{AccountID: types.AddressBytea(accountAddr2), ContractID: contractID1, Balance: "3000", LedgerNumber: 200}, // new
		}
		deletes := []SACBalance{
			{AccountID: types.AddressBytea(accountAddr1), ContractID: contractID2}, // delete
		}
		err = m.BatchUpsert(ctx, pgxTx2, newUpserts, deletes)
		require.NoError(t, err)
		require.NoError(t, pgxTx2.Commit(ctx))

		// Verify results
		var count int
		err = dbConnectionPool.QueryRow(ctx, `SELECT COUNT(*) FROM sac_balances`).Scan(&count)
		require.NoError(t, err)
		require.Equal(t, 2, count)

		var balance string
		err = dbConnectionPool.QueryRow(ctx,
			`SELECT balance FROM sac_balances WHERE account_id = $1 AND contract_id = $2`,
			types.AddressBytea(accountAddr1), contractID1).Scan(&balance)
		require.NoError(t, err)
		require.Equal(t, "1500", balance)
	})
}

func TestSACBalanceModel_BatchCopy(t *testing.T) {
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	// Insert test contract tokens for foreign key references
	contractAddr1 := "CCONTRACT1AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	contractAddr2 := "CCONTRACT2AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	contractID1 := DeterministicContractID(contractAddr1)
	contractID2 := DeterministicContractID(contractAddr2)
	_, err = dbConnectionPool.Exec(ctx, `
		INSERT INTO contract_tokens (id, contract_id, type, code, issuer, decimals) VALUES
		($1, $2, 'SAC', 'USDC', 'GISSUER1AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', 7),
		($3, $4, 'SAC', 'EURC', 'GISSUER2AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', 7)
	`, contractID1, contractAddr1, contractID2, contractAddr2)
	require.NoError(t, err)

	cleanUpDB := func() {
		_, err = dbConnectionPool.Exec(ctx, `DELETE FROM sac_balances`)
		require.NoError(t, err)
	}

	reg := prometheus.NewRegistry()
	dbMetrics := metrics.NewMetrics(reg).DB

	t.Run("returns nil for empty input", func(t *testing.T) {
		m := &SACBalanceModel{
			DB:      dbConnectionPool,
			Metrics: dbMetrics,
		}

		pgxTx, err := dbConnectionPool.Begin(ctx)
		require.NoError(t, err)
		defer pgxTx.Rollback(ctx) //nolint:errcheck

		err = m.BatchCopy(ctx, pgxTx, nil)
		require.NoError(t, err)
	})

	t.Run("inserts single balance via COPY", func(t *testing.T) {
		cleanUpDB()

		m := &SACBalanceModel{
			DB:      dbConnectionPool,
			Metrics: dbMetrics,
		}

		accountAddr := randomContractAddress(t)
		pgxTx, err := dbConnectionPool.Begin(ctx)
		require.NoError(t, err)

		balances := []SACBalance{
			{
				AccountID:         types.AddressBytea(accountAddr),
				ContractID:        contractID1,
				Balance:           "1000000000",
				IsAuthorized:      true,
				IsClawbackEnabled: false,
				LedgerNumber:      12345,
			},
		}

		err = m.BatchCopy(ctx, pgxTx, balances)
		require.NoError(t, err)
		require.NoError(t, pgxTx.Commit(ctx))

		// Verify all fields
		var b SACBalance
		err = dbConnectionPool.QueryRow(ctx, `
			SELECT account_id, contract_id, balance, is_authorized, is_clawback_enabled, last_modified_ledger
			FROM sac_balances WHERE account_id = $1
		`, types.AddressBytea(accountAddr)).Scan(
			&b.AccountID, &b.ContractID, &b.Balance, &b.IsAuthorized, &b.IsClawbackEnabled, &b.LedgerNumber)
		require.NoError(t, err)
		require.Equal(t, balances[0].AccountID, b.AccountID)
		require.Equal(t, balances[0].ContractID, b.ContractID)
		require.Equal(t, balances[0].Balance, b.Balance)
		require.Equal(t, balances[0].IsAuthorized, b.IsAuthorized)
		require.Equal(t, balances[0].IsClawbackEnabled, b.IsClawbackEnabled)
		require.Equal(t, balances[0].LedgerNumber, b.LedgerNumber)
	})

	t.Run("inserts multiple balances via COPY", func(t *testing.T) {
		cleanUpDB()

		m := &SACBalanceModel{
			DB:      dbConnectionPool,
			Metrics: dbMetrics,
		}

		accountAddr1 := randomContractAddress(t)
		accountAddr2 := randomContractAddress(t)
		pgxTx, err := dbConnectionPool.Begin(ctx)
		require.NoError(t, err)

		balances := []SACBalance{
			{AccountID: types.AddressBytea(accountAddr1), ContractID: contractID1, Balance: "1000", LedgerNumber: 100},
			{AccountID: types.AddressBytea(accountAddr1), ContractID: contractID2, Balance: "2000", LedgerNumber: 100},
			{AccountID: types.AddressBytea(accountAddr2), ContractID: contractID1, Balance: "3000", LedgerNumber: 100},
		}

		err = m.BatchCopy(ctx, pgxTx, balances)
		require.NoError(t, err)
		require.NoError(t, pgxTx.Commit(ctx))

		// Verify count
		var count int
		err = dbConnectionPool.QueryRow(ctx, `SELECT COUNT(*) FROM sac_balances`).Scan(&count)
		require.NoError(t, err)
		require.Equal(t, 3, count)
	})
}
