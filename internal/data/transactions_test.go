package data

import (
	"context"
	"testing"
	"time"

	set "github.com/deckarep/golang-set/v2"
	"github.com/stellar/go/keypair"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
)

func Test_TransactionModel_BatchInsert(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()
	now := time.Now()

	// Create test data
	kp1 := keypair.MustRandom()
	kp2 := keypair.MustRandom()
	const q = "INSERT INTO accounts (stellar_address) SELECT UNNEST(ARRAY[$1, $2])"
	_, err = dbConnectionPool.ExecContext(ctx, q, kp1.Address(), kp2.Address())
	require.NoError(t, err)
	nonExistingAccount := keypair.MustRandom()

	tx1 := types.Transaction{
		Hash:            "tx1",
		ToID:            1,
		EnvelopeXDR:     "envelope1",
		ResultXDR:       "result1",
		MetaXDR:         "meta1",
		LedgerNumber:    1,
		LedgerCreatedAt: now,
	}
	tx2 := types.Transaction{
		Hash:            "tx2",
		ToID:            2,
		EnvelopeXDR:     "envelope2",
		ResultXDR:       "result2",
		MetaXDR:         "meta2",
		LedgerNumber:    2,
		LedgerCreatedAt: now,
	}

	testCases := []struct {
		name                   string
		useDBTx                bool
		txs                    []types.Transaction
		stellarAddressesByHash map[string]set.Set[string]
		wantAccountLinks       map[string][]string
		wantErrContains        string
		wantHashes             []string
	}{
		{
			name:                   "🟢successful_insert_without_dbTx",
			useDBTx:                false,
			txs:                    []types.Transaction{tx1, tx2},
			stellarAddressesByHash: map[string]set.Set[string]{tx1.Hash: set.NewSet(kp1.Address()), tx2.Hash: set.NewSet(kp2.Address())},
			wantAccountLinks:       map[string][]string{tx1.Hash: {kp1.Address()}, tx2.Hash: {kp2.Address()}},
			wantErrContains:        "",
			wantHashes:             []string{tx1.Hash, tx2.Hash},
		},
		{
			name:                   "🟢successful_insert_with_dbTx",
			useDBTx:                true,
			txs:                    []types.Transaction{tx1},
			stellarAddressesByHash: map[string]set.Set[string]{tx1.Hash: set.NewSet(kp1.Address())},
			wantAccountLinks:       map[string][]string{tx1.Hash: {kp1.Address()}},
			wantErrContains:        "",
			wantHashes:             []string{tx1.Hash},
		},
		{
			name:                   "🟢empty_input",
			useDBTx:                false,
			txs:                    []types.Transaction{},
			stellarAddressesByHash: map[string]set.Set[string]{},
			wantAccountLinks:       map[string][]string{},
			wantErrContains:        "",
			wantHashes:             nil,
		},
		{
			name:                   "🟡duplicate_transaction",
			useDBTx:                false,
			txs:                    []types.Transaction{tx1, tx1},
			stellarAddressesByHash: map[string]set.Set[string]{tx1.Hash: set.NewSet(kp1.Address())},
			wantAccountLinks:       map[string][]string{tx1.Hash: {kp1.Address()}},
			wantErrContains:        "",
			wantHashes:             []string{tx1.Hash},
		},
		{
			name:                   "🟡tx_with_all_non_existing_accounts_is_ignored",
			useDBTx:                false,
			txs:                    []types.Transaction{tx1},
			stellarAddressesByHash: map[string]set.Set[string]{tx1.Hash: set.NewSet(nonExistingAccount.Address())},
			wantAccountLinks:       map[string][]string{},
			wantErrContains:        "",
			wantHashes:             nil,
		},
		{
			name:                   "🟡non_existing_account_is_ignored_but_tx_and_other_accounts_links_are_inserted",
			useDBTx:                false,
			txs:                    []types.Transaction{tx1},
			stellarAddressesByHash: map[string]set.Set[string]{tx1.Hash: set.NewSet(kp1.Address(), kp2.Address(), nonExistingAccount.Address())},
			wantAccountLinks:       map[string][]string{tx1.Hash: {kp1.Address(), kp2.Address()}},
			wantErrContains:        "",
			wantHashes:             []string{tx1.Hash},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Clear the database before each test
			_, err = dbConnectionPool.ExecContext(ctx, "TRUNCATE transactions, transactions_accounts CASCADE")
			require.NoError(t, err)

			// Create fresh mock for each test case
			mockMetricsService := metrics.NewMockMetricsService()
			mockMetricsService.
				On("ObserveDBQueryDuration", "INSERT", "transactions", mock.Anything).Return().Once().
				On("ObserveDBQueryDuration", "INSERT", "transactions_accounts", mock.Anything).Return().Once().
				On("IncDBQuery", "INSERT", "transactions").Return().Once().
				On("IncDBQuery", "INSERT", "transactions_accounts").Return().Once()
			defer mockMetricsService.AssertExpectations(t)

			m := &TransactionModel{
				DB:             dbConnectionPool,
				MetricsService: mockMetricsService,
			}

			var sqlExecuter db.SQLExecuter = dbConnectionPool
			if tc.useDBTx {
				tx, err := dbConnectionPool.BeginTxx(ctx, nil)
				require.NoError(t, err)
				defer tx.Rollback()
				sqlExecuter = tx
			}

			gotInsertedHashes, err := m.BatchInsert(ctx, sqlExecuter, tc.txs, tc.stellarAddressesByHash)

			if tc.wantErrContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrContains)
				return
			}

			// Verify the results
			require.NoError(t, err)
			var dbInsertedHashes []string
			err = sqlExecuter.SelectContext(ctx, &dbInsertedHashes, "SELECT hash FROM transactions")
			require.NoError(t, err)
			assert.ElementsMatch(t, tc.wantHashes, dbInsertedHashes)
			assert.ElementsMatch(t, tc.wantHashes, gotInsertedHashes)

			// Verify the account links
			if len(tc.wantAccountLinks) > 0 {
				var accountLinks []struct {
					TxHash    string `db:"tx_hash"`
					AccountID string `db:"account_id"`
				}
				err = sqlExecuter.SelectContext(ctx, &accountLinks, "SELECT tx_hash, account_id FROM transactions_accounts ORDER BY tx_hash, account_id")
				require.NoError(t, err)

				// Create a map of tx_hash -> set of account_ids for O(1) lookups
				accountLinksMap := make(map[string][]string)
				for _, link := range accountLinks {
					accountLinksMap[link.TxHash] = append(accountLinksMap[link.TxHash], link.AccountID)
				}

				// Verify each transaction has its expected account links
				require.Equal(t, tc.wantAccountLinks, accountLinksMap)
			}
		})
	}
}
