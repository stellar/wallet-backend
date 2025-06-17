package data

import (
	"context"
	"testing"
	"time"

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

	tx1 := types.Transaction{
		Hash:            "tx1",
		EnvelopeXDR:     "envelope1",
		ResultXDR:       "result1",
		MetaXDR:         "meta1",
		LedgerNumber:    1,
		LedgerCreatedAt: now,
	}
	tx2 := types.Transaction{
		Hash:            "tx2",
		EnvelopeXDR:     "envelope2",
		ResultXDR:       "result2",
		MetaXDR:         "meta2",
		LedgerNumber:    2,
		LedgerCreatedAt: now,
	}

	testCases := []struct {
		name                   string
		useDbTx                bool
		txs                    []types.Transaction
		stellarAddressesByHash map[string][]string
		wantErrContains        string
		wantResults            []string
	}{
		{
			name:                   "🟢successful_insert_without_transaction",
			useDbTx:                false,
			txs:                    []types.Transaction{tx1, tx2},
			stellarAddressesByHash: map[string][]string{tx1.Hash: {kp1.Address()}, tx2.Hash: {kp2.Address()}},
			wantErrContains:        "",
			wantResults:            []string{tx1.Hash, tx2.Hash},
		},
		{
			name:                   "🟢successful_insert_with_transaction",
			useDbTx:                true,
			txs:                    []types.Transaction{tx1},
			stellarAddressesByHash: map[string][]string{tx1.Hash: {kp1.Address()}},
			wantErrContains:        "",
			wantResults:            []string{tx1.Hash},
		},
		{
			name:                   "🟢empty_input",
			useDbTx:                false,
			txs:                    []types.Transaction{},
			stellarAddressesByHash: map[string][]string{},
			wantErrContains:        "",
			wantResults:            nil,
		},
		{
			name:                   "🟡duplicate_transaction",
			useDbTx:                false,
			txs:                    []types.Transaction{tx1, tx1},
			stellarAddressesByHash: map[string][]string{tx1.Hash: {kp1.Address()}},
			wantErrContains:        "",
			wantResults:            []string{tx1.Hash},
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
				On("ObserveDBQueryDuration", "INSERT", "transactions", mock.Anything).Return().
				On("IncDBQuery", "INSERT", "transactions").Return().
				On("ObserveDBQueryDuration", "INSERT", "transactions_accounts", mock.Anything).Return().
				On("IncDBQuery", "INSERT", "transactions_accounts").Return()
			defer mockMetricsService.AssertExpectations(t)

			m := &TransactionModel{
				DB:             dbConnectionPool,
				MetricsService: mockMetricsService,
			}

			var sqlExecuter db.SQLExecuter = dbConnectionPool
			if tc.useDbTx {
				tx, err := dbConnectionPool.BeginTxx(ctx, nil)
				require.NoError(t, err)
				defer tx.Rollback()
				sqlExecuter = tx
			}

			err := m.BatchInsert(ctx, sqlExecuter, tc.txs, tc.stellarAddressesByHash)

			if tc.wantErrContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrContains)
				return
			}

			// Verify the results
			require.NoError(t, err)
			var insertedHashes []string
			err = sqlExecuter.SelectContext(ctx, &insertedHashes, "SELECT hash FROM transactions ORDER BY hash")
			require.NoError(t, err)
			assert.Equal(t, tc.wantResults, insertedHashes)

			// Verify the account links
			if len(tc.stellarAddressesByHash) > 0 {
				var accountLinks []struct {
					TxHash    string `db:"tx_hash"`
					AccountID string `db:"account_id"`
				}
				err = sqlExecuter.SelectContext(ctx, &accountLinks, "SELECT tx_hash, account_id FROM transactions_accounts ORDER BY tx_hash, account_id")
				require.NoError(t, err)

				// Create a map of tx_hash -> set of account_ids for O(1) lookups
				accountLinksMap := make(map[string]map[string]struct{})
				for _, link := range accountLinks {
					if _, exists := accountLinksMap[link.TxHash]; !exists {
						accountLinksMap[link.TxHash] = make(map[string]struct{})
					}
					accountLinksMap[link.TxHash][link.AccountID] = struct{}{}
				}

				// Verify each transaction has its expected account links
				for txHash, expectedAccounts := range tc.stellarAddressesByHash {
					accounts, exists := accountLinksMap[txHash]
					require.True(t, exists, "Transaction %s not found in account links", txHash)
					for _, expectedAccount := range expectedAccounts {
						_, found := accounts[expectedAccount]
						assert.True(t, found, "Expected account link not found: tx=%s, account=%s", txHash, expectedAccount)
					}
				}
			}
		})
	}
}
