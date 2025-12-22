package data

import (
	"context"
	"fmt"
	"testing"
	"time"

	set "github.com/deckarep/golang-set/v2"
	"github.com/jackc/pgx/v5"
	"github.com/stellar/go/keypair"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
)

// generateTestTransactions creates n test transactions for benchmarking.
func generateTestTransactions(n int, startToID int64) ([]*types.Transaction, map[string]set.Set[string]) {
	txs := make([]*types.Transaction, n)
	addressesByHash := make(map[string]set.Set[string])
	now := time.Now()

	for i := 0; i < n; i++ {
		hash := fmt.Sprintf("e76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48760-%d", startToID+int64(i))
		envelope := "AAAAAgAAAAB/NpQ+s+cP+ztX7ryuKgXrxowZPHd4qAxhseOye/JeUgAehIAC2NL/AAflugAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAAwAAAAFQQUxMAAAAAKHc4IKbcW8HPPgy3zOhuqv851y72nfLGa0HVXxIRNzHAAAAAAAAAAAAQ3FwMxshxQAfwV8AAAAAYTGQ3QAAAAAAAAAMAAAAAAAAAAFQQUxMAAAAAKHc4IKbcW8HPPgy3zOhuqv851y72nfLGa0HVXxIRNzHAAAAAAAGXwFksiHwAEXz8QAAAABhoaQjAAAAAAAAAAF78l5SAAAAQD7LgvZA8Pdvfh5L2b9B9RC7DlacGBJuOchuZDHQdVD1P0bn6nGQJXxDDI4oN76J49JxB7bIgDVim39MU43MOgE="
		meta := "AAAAAwAAAAAAAAAEAAAAAwM6nhwAAAAAAAAAAJjy0MY1CPlZ/co80nzufVmo4gd7NqWMb+RiGiPhiviJAAAAC4SozKUDMWgAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAQM6nhwAAAAAAAAAAJjy0MY1CPlZ/co80nzufVmo4gd7NqWMb+RiGiPhiviJAAAAC4SozKUDMWgAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAwM6LTkAAAAAAAAAAKl6DQcpepRdTbO/Vw4hYBENfE/95GevM7SNA0ftK0gtAAAAA8Kuf0AC+zZCAAAATAAAAAMAAAABAAAAAMRxxkNwYslQaok0LlOKGtpATS9Bzx06JV9DIffG4OF1AAAAAAAAAAlsb2JzdHIuY28AAAABAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAgAAAAAAAAAAAAAAAAAAAAMAAAAAAyZ54QAAAABmrTXCAAAAAAAAAAEDOp4cAAAAAAAAAACpeg0HKXqUXU2zv1cOIWARDXxP/eRnrzO0jQNH7StILQAAAAPCrn9AAvs2QgAAAE0AAAADAAAAAQAAAADEccZDcGLJUGqJNC5TihraQE0vQc8dOiVfQyH3xuDhdQAAAAAAAAAJbG9ic3RyLmNvAAAAAQAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAAAAAAAAAAAADAAAAAAM6nhwAAAAAZyGdHwAAAAAAAAABAAAABAAAAAMDOp4cAAAAAAAAAACpeg0HKXqUXU2zv1cOIWARDXxP/eRnrzO0jQNH7StILQAAAAPCrn9AAvs2QgAAAE0AAAADAAAAAQAAAADEccZDcGLJUGqJNC5TihraQE0vQc8dOiVfQyH3xuDhdQAAAAAAAAAJbG9ic3RyLmNvAAAAAQAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAAAAAAAAAAAADAAAAAAM6nhwAAAAAZyGdHwAAAAAAAAABAzqeHAAAAAAAAAAAqXoNByl6lF1Ns79XDiFgEQ18T/3kZ68ztI0DR+0rSC0AAAACmKiNQAL7NkIAAABNAAAAAwAAAAEAAAAAxHHGQ3BiyVBqiTQuU4oa2kBNL0HPHTolX0Mh98bg4XUAAAAAAAAACWxvYnN0ci5jbwAAAAEAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAACAAAAAAAAAAAAAAAAAAAAAwAAAAADOp4cAAAAAGchnR8AAAAAAAAAAwM6nZoAAAAAAAAAALKxMozkOH3rgpz3/u3+93wsR4p6z4K82HmJ5NTuaZbYAAACZaqAwoIBqycyAABVlQAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAgAAAAAAAAAAAAAAAAAAAAMAAAAAAzqSNgAAAABnIVdaAAAAAAAAAAEDOp4cAAAAAAAAAACysTKM5Dh964Kc9/7t/vd8LEeKes+CvNh5ieTU7mmW2AAAAmbUhrSCAasnMgAAVZUAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAAAAAAAAAAAADAAAAAAM6kjYAAAAAZyFXWgAAAAAAAAAAAAAAAA=="
		result := "AAAAAAAAAMgAAAAAAAAAAgAAAAAAAAADAAAAAAAAAAAAAAABAAAAAH82lD6z5w/7O1fuvK4qBevGjBk8d3ioDGGx47J78l5SAAAAAGExkN0AAAABUEFMTAAAAACh3OCCm3FvBzz4Mt8zobqr/Odcu9p3yxmtB1V8SETcxwAAAAAAAAAAAENxcDMbIcUAH8FfAAAAAAAAAAAAAAAAAAAADAAAAAAAAAAAAAAAAQAAAAB/NpQ+s+cP+ztX7ryuKgXrxowZPHd4qAxhseOye/JeUgAAAABhoaQjAAAAAAAAAAFQQUxMAAAAAKHc4IKbcW8HPPgy3zOhuqv851y72nfLGa0HVXxIRNzHAAAAAAkrzGAARfPxZLIh8AAAAAAAAAAAAAAAAA=="
		address := keypair.MustRandom().Address()

		txs[i] = &types.Transaction{
			Hash:            hash,
			ToID:            startToID + int64(i),
			EnvelopeXDR:     &envelope,
			ResultXDR:       result,
			MetaXDR:         &meta,
			LedgerCreatedAt: now,
		}
		addressesByHash[hash] = set.NewSet(address)
	}

	return txs, addressesByHash
}

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

	meta1, meta2 := "meta1", "meta2"
	envelope1, envelope2 := "envelope1", "envelope2"
	tx1 := types.Transaction{
		Hash:            "tx1",
		ToID:            1,
		EnvelopeXDR:     &envelope1,
		ResultXDR:       "result1",
		MetaXDR:         &meta1,
		LedgerCreatedAt: now,
	}
	tx2 := types.Transaction{
		Hash:            "tx2",
		ToID:            2,
		EnvelopeXDR:     &envelope2,
		ResultXDR:       "result2",
		MetaXDR:         &meta2,
		LedgerCreatedAt: now,
	}

	testCases := []struct {
		name                   string
		useDBTx                bool
		txs                    []*types.Transaction
		stellarAddressesByHash map[string]set.Set[string]
		wantAccountLinks       map[string][]string
		wantErrContains        string
		wantHashes             []string
	}{
		{
			name:                   "🟢successful_insert_without_dbTx",
			useDBTx:                false,
			txs:                    []*types.Transaction{&tx1, &tx2},
			stellarAddressesByHash: map[string]set.Set[string]{tx1.Hash: set.NewSet(kp1.Address()), tx2.Hash: set.NewSet(kp2.Address())},
			wantAccountLinks:       map[string][]string{tx1.Hash: {kp1.Address()}, tx2.Hash: {kp2.Address()}},
			wantErrContains:        "",
			wantHashes:             []string{tx1.Hash, tx2.Hash},
		},
		{
			name:                   "🟢successful_insert_with_dbTx",
			useDBTx:                true,
			txs:                    []*types.Transaction{&tx1},
			stellarAddressesByHash: map[string]set.Set[string]{tx1.Hash: set.NewSet(kp1.Address())},
			wantAccountLinks:       map[string][]string{tx1.Hash: {kp1.Address()}},
			wantErrContains:        "",
			wantHashes:             []string{tx1.Hash},
		},
		{
			name:                   "🟢empty_input",
			useDBTx:                false,
			txs:                    []*types.Transaction{},
			stellarAddressesByHash: map[string]set.Set[string]{},
			wantAccountLinks:       map[string][]string{},
			wantErrContains:        "",
			wantHashes:             nil,
		},
		{
			name:                   "🟡duplicate_transaction",
			useDBTx:                false,
			txs:                    []*types.Transaction{&tx1, &tx1},
			stellarAddressesByHash: map[string]set.Set[string]{tx1.Hash: set.NewSet(kp1.Address())},
			wantAccountLinks:       map[string][]string{tx1.Hash: {kp1.Address()}},
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
			// The implementation always loops through both tables and calls ObserveDBQueryDuration for each
			mockMetricsService.
				On("ObserveDBQueryDuration", "BatchInsert", "transactions", mock.Anything).Return().Once().
				On("ObserveDBQueryDuration", "BatchInsert", "transactions_accounts", mock.Anything).Return().Once()
			// ObserveDBBatchSize is only called for transactions table (not transactions_accounts)
			mockMetricsService.On("ObserveDBBatchSize", "BatchInsert", "transactions", mock.Anything).Return().Once()
			// IncDBQuery is called for both tables on success
			mockMetricsService.
				On("IncDBQuery", "BatchInsert", "transactions").Return().Once().
				On("IncDBQuery", "BatchInsert", "transactions_accounts").Return().Once()
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
				require.Equal(t, len(tc.wantAccountLinks), len(accountLinksMap), "number of elements in the maps don't match")
				for key, expectedSlice := range tc.wantAccountLinks {
					actualSlice, exists := accountLinksMap[key]
					require.True(t, exists, "key %s not found in actual map", key)
					assert.ElementsMatch(t, expectedSlice, actualSlice, "slices for key %s don't match", key)
				}
			}
		})
	}
}

func Test_TransactionModel_BatchCopy(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()
	now := time.Now()

	// Create test accounts
	kp1 := keypair.MustRandom()
	kp2 := keypair.MustRandom()
	const q = "INSERT INTO accounts (stellar_address) SELECT UNNEST(ARRAY[$1, $2])"
	_, err = dbConnectionPool.ExecContext(ctx, q, kp1.Address(), kp2.Address())
	require.NoError(t, err)

	meta1, meta2 := "meta1", "meta2"
	envelope1, envelope2 := "envelope1", "envelope2"
	tx1 := types.Transaction{
		Hash:            "tx1",
		ToID:            1,
		EnvelopeXDR:     &envelope1,
		ResultXDR:       "result1",
		MetaXDR:         &meta1,
		LedgerCreatedAt: now,
	}
	tx2 := types.Transaction{
		Hash:            "tx2",
		ToID:            2,
		EnvelopeXDR:     &envelope2,
		ResultXDR:       "result2",
		MetaXDR:         &meta2,
		LedgerCreatedAt: now,
	}
	// Transaction with nullable fields (nil envelope and meta)
	tx3 := types.Transaction{
		Hash:            "tx3",
		ToID:            3,
		EnvelopeXDR:     nil,
		ResultXDR:       "result3",
		MetaXDR:         nil,
		LedgerCreatedAt: now,
	}

	testCases := []struct {
		name                   string
		txs                    []*types.Transaction
		stellarAddressesByHash map[string]set.Set[string]
		wantCount              int
		wantErrContains        string
	}{
		{
			name:                   "🟢successful_insert_multiple",
			txs:                    []*types.Transaction{&tx1, &tx2},
			stellarAddressesByHash: map[string]set.Set[string]{tx1.Hash: set.NewSet(kp1.Address()), tx2.Hash: set.NewSet(kp2.Address())},
			wantCount:              2,
		},
		{
			name:                   "🟢empty_input",
			txs:                    []*types.Transaction{},
			stellarAddressesByHash: map[string]set.Set[string]{},
			wantCount:              0,
		},
		{
			name:                   "🟢nullable_fields",
			txs:                    []*types.Transaction{&tx3},
			stellarAddressesByHash: map[string]set.Set[string]{tx3.Hash: set.NewSet(kp1.Address())},
			wantCount:              1,
		},
		{
			name:                   "🟢no_participants",
			txs:                    []*types.Transaction{&tx1},
			stellarAddressesByHash: map[string]set.Set[string]{},
			wantCount:              1,
		},
	}

	// Create pgx connection for BatchCopy (requires pgx.Tx, not sqlx.Tx)
	conn, err := pgx.Connect(ctx, dbt.DSN)
	require.NoError(t, err)
	defer conn.Close(ctx)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Clear the database before each test
			_, err = dbConnectionPool.ExecContext(ctx, "TRUNCATE transactions, transactions_accounts CASCADE")
			require.NoError(t, err)

			// Create fresh mock for each test case
			mockMetricsService := metrics.NewMockMetricsService()
			// Only set up metric expectations if we have transactions to insert
			if len(tc.txs) > 0 {
				mockMetricsService.
					On("ObserveDBQueryDuration", "BatchCopy", "transactions", mock.Anything).Return().Once()
				mockMetricsService.
					On("ObserveDBBatchSize", "BatchCopy", "transactions", mock.Anything).Return().Once()
				mockMetricsService.
					On("IncDBQuery", "BatchCopy", "transactions").Return().Once()
				if len(tc.stellarAddressesByHash) > 0 {
					mockMetricsService.
						On("IncDBQuery", "BatchCopy", "transactions_accounts").Return().Once()
				}
			}
			defer mockMetricsService.AssertExpectations(t)

			m := &TransactionModel{
				DB:             dbConnectionPool,
				MetricsService: mockMetricsService,
			}

			// BatchCopy requires a pgx transaction
			pgxTx, err := conn.Begin(ctx)
			require.NoError(t, err)

			gotCount, err := m.BatchCopy(ctx, pgxTx, tc.txs, tc.stellarAddressesByHash)

			if tc.wantErrContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrContains)
				pgxTx.Rollback(ctx)
				return
			}

			require.NoError(t, err)
			require.NoError(t, pgxTx.Commit(ctx))
			assert.Equal(t, tc.wantCount, gotCount)

			// Verify from DB
			var dbInsertedHashes []string
			err = dbConnectionPool.SelectContext(ctx, &dbInsertedHashes, "SELECT hash FROM transactions ORDER BY hash")
			require.NoError(t, err)
			assert.Len(t, dbInsertedHashes, tc.wantCount)

			// Verify account links if expected
			if len(tc.stellarAddressesByHash) > 0 && tc.wantCount > 0 {
				var accountLinks []struct {
					TxHash    string `db:"tx_hash"`
					AccountID string `db:"account_id"`
				}
				err = dbConnectionPool.SelectContext(ctx, &accountLinks, "SELECT tx_hash, account_id FROM transactions_accounts ORDER BY tx_hash, account_id")
				require.NoError(t, err)

				// Create a map of tx_hash -> set of account_ids
				accountLinksMap := make(map[string][]string)
				for _, link := range accountLinks {
					accountLinksMap[link.TxHash] = append(accountLinksMap[link.TxHash], link.AccountID)
				}

				// Verify each expected transaction has its account links
				for txHash, expectedAddresses := range tc.stellarAddressesByHash {
					actualAddresses := accountLinksMap[txHash]
					assert.ElementsMatch(t, expectedAddresses.ToSlice(), actualAddresses, "account links for tx %s don't match", txHash)
				}
			}
		})
	}
}

func TestTransactionModel_GetByHash(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := metrics.NewMockMetricsService()
	mockMetricsService.On("ObserveDBQueryDuration", "GetByHash", "transactions", mock.Anything).Return()
	mockMetricsService.On("IncDBQuery", "GetByHash", "transactions").Return()
	defer mockMetricsService.AssertExpectations(t)

	m := &TransactionModel{
		DB:             dbConnectionPool,
		MetricsService: mockMetricsService,
	}

	ctx := context.Background()
	now := time.Now()

	// Create test transaction
	txHash := "test_tx_hash"
	_, err = dbConnectionPool.ExecContext(ctx, `
		INSERT INTO transactions (hash, to_id, envelope_xdr, result_xdr, meta_xdr, ledger_number, ledger_created_at)
		VALUES ($1, 1, 'envelope', 'result', 'meta', 1, $2)
	`, txHash, now)
	require.NoError(t, err)

	// Test GetByHash
	transaction, err := m.GetByHash(ctx, txHash, "")
	require.NoError(t, err)
	assert.Equal(t, txHash, transaction.Hash)
	assert.Equal(t, int64(1), transaction.ToID)
	require.NotNil(t, transaction.EnvelopeXDR)
	assert.Equal(t, "envelope", *transaction.EnvelopeXDR)
}

func TestTransactionModel_GetAll(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := metrics.NewMockMetricsService()
	mockMetricsService.On("ObserveDBQueryDuration", "GetAll", "transactions", mock.Anything).Return()
	mockMetricsService.On("IncDBQuery", "GetAll", "transactions").Return()
	defer mockMetricsService.AssertExpectations(t)

	m := &TransactionModel{
		DB:             dbConnectionPool,
		MetricsService: mockMetricsService,
	}

	ctx := context.Background()
	now := time.Now()

	// Create test transactions
	_, err = dbConnectionPool.ExecContext(ctx, `
		INSERT INTO transactions (hash, to_id, envelope_xdr, result_xdr, meta_xdr, ledger_number, ledger_created_at)
		VALUES
			('tx1', 1, 'envelope1', 'result1', 'meta1', 1, $1),
			('tx2', 2, 'envelope2', 'result2', 'meta2', 2, $1),
			('tx3', 3, 'envelope3', 'result3', 'meta3', 3, $1)
	`, now)
	require.NoError(t, err)

	// Test GetAll without specifying cursor and limit (gets all transactions)
	transactions, err := m.GetAll(ctx, "", nil, nil, ASC)
	require.NoError(t, err)
	assert.Len(t, transactions, 3)
	assert.Equal(t, int64(1), transactions[0].Cursor)
	assert.Equal(t, int64(2), transactions[1].Cursor)
	assert.Equal(t, int64(3), transactions[2].Cursor)

	// Test GetAll with smaller limit
	limit := int32(2)
	transactions, err = m.GetAll(ctx, "", &limit, nil, ASC)
	require.NoError(t, err)
	assert.Len(t, transactions, 2)
	assert.Equal(t, int64(1), transactions[0].Cursor)
	assert.Equal(t, int64(2), transactions[1].Cursor)
}

func TestTransactionModel_BatchGetByAccountAddress(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := metrics.NewMockMetricsService()
	mockMetricsService.On("ObserveDBQueryDuration", "BatchGetByAccountAddress", "transactions", mock.Anything).Return()
	mockMetricsService.On("IncDBQuery", "BatchGetByAccountAddress", "transactions").Return()
	defer mockMetricsService.AssertExpectations(t)

	m := &TransactionModel{
		DB:             dbConnectionPool,
		MetricsService: mockMetricsService,
	}

	ctx := context.Background()
	now := time.Now()

	// Create test accounts
	address1 := keypair.MustRandom().Address()
	address2 := keypair.MustRandom().Address()
	_, err = dbConnectionPool.ExecContext(ctx, "INSERT INTO accounts (stellar_address) VALUES ($1), ($2)", address1, address2)
	require.NoError(t, err)

	// Create test transactions
	_, err = dbConnectionPool.ExecContext(ctx, `
		INSERT INTO transactions (hash, to_id, envelope_xdr, result_xdr, meta_xdr, ledger_number, ledger_created_at)
		VALUES
			('tx1', 1, 'envelope1', 'result1', 'meta1', 1, $1),
			('tx2', 2, 'envelope2', 'result2', 'meta2', 2, $1),
			('tx3', 3, 'envelope3', 'result3', 'meta3', 3, $1)
	`, now)
	require.NoError(t, err)

	// Create test transactions_accounts links
	_, err = dbConnectionPool.ExecContext(ctx, `
		INSERT INTO transactions_accounts (tx_hash, account_id)
		VALUES
			('tx1', $1),
			('tx2', $1),
			('tx3', $2)
	`, address1, address2)
	require.NoError(t, err)

	// Test BatchGetByAccount
	transactions, err := m.BatchGetByAccountAddress(ctx, address1, "", nil, nil, "ASC")
	require.NoError(t, err)
	assert.Len(t, transactions, 2)

	assert.Equal(t, int64(1), transactions[0].Cursor)
	assert.Equal(t, int64(2), transactions[1].Cursor)
}

func TestTransactionModel_BatchGetByOperationIDs(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := metrics.NewMockMetricsService()
	mockMetricsService.On("ObserveDBQueryDuration", "BatchGetByOperationIDs", "transactions", mock.Anything).Return()
	mockMetricsService.On("ObserveDBBatchSize", "BatchGetByOperationIDs", "transactions", mock.Anything).Return()
	mockMetricsService.On("IncDBQuery", "BatchGetByOperationIDs", "transactions").Return()
	defer mockMetricsService.AssertExpectations(t)

	m := &TransactionModel{
		DB:             dbConnectionPool,
		MetricsService: mockMetricsService,
	}

	ctx := context.Background()
	now := time.Now()

	// Create test transactions
	_, err = dbConnectionPool.ExecContext(ctx, `
		INSERT INTO transactions (hash, to_id, envelope_xdr, result_xdr, meta_xdr, ledger_number, ledger_created_at)
		VALUES
			('tx1', 1, 'envelope1', 'result1', 'meta1', 1, $1),
			('tx2', 2, 'envelope2', 'result2', 'meta2', 2, $1),
			('tx3', 3, 'envelope3', 'result3', 'meta3', 3, $1)
	`, now)
	require.NoError(t, err)

	// Create test operations
	_, err = dbConnectionPool.ExecContext(ctx, `
		INSERT INTO operations (id, tx_hash, operation_type, operation_xdr, ledger_number, ledger_created_at)
		VALUES
			(1, 'tx1', 'payment', 'xdr1', 1, $1),
			(2, 'tx2', 'create_account', 'xdr2', 2, $1),
			(3, 'tx1', 'payment', 'xdr3', 3, $1)
	`, now)
	require.NoError(t, err)

	// Test BatchGetByOperationID
	transactions, err := m.BatchGetByOperationIDs(ctx, []int64{1, 2, 3}, "")
	require.NoError(t, err)
	assert.Len(t, transactions, 3)

	// Verify transactions are for correct operation IDs
	operationIDsFound := make(map[int64]string)
	for _, tx := range transactions {
		operationIDsFound[tx.OperationID] = tx.Hash
	}
	assert.Equal(t, "tx1", operationIDsFound[1])
	assert.Equal(t, "tx2", operationIDsFound[2])
	assert.Equal(t, "tx1", operationIDsFound[3])
}

func TestTransactionModel_BatchGetByStateChangeIDs(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := metrics.NewMockMetricsService()
	mockMetricsService.On("ObserveDBQueryDuration", "BatchGetByStateChangeIDs", "transactions", mock.Anything).Return()
	mockMetricsService.On("ObserveDBBatchSize", "BatchGetByStateChangeIDs", "transactions", mock.Anything).Return()
	mockMetricsService.On("IncDBQuery", "BatchGetByStateChangeIDs", "transactions").Return()
	defer mockMetricsService.AssertExpectations(t)

	m := &TransactionModel{
		DB:             dbConnectionPool,
		MetricsService: mockMetricsService,
	}

	ctx := context.Background()
	now := time.Now()

	// Create test account
	address := keypair.MustRandom().Address()
	_, err = dbConnectionPool.ExecContext(ctx, "INSERT INTO accounts (stellar_address) VALUES ($1)", address)
	require.NoError(t, err)

	// Create test transactions
	_, err = dbConnectionPool.ExecContext(ctx, `
		INSERT INTO transactions (hash, to_id, envelope_xdr, result_xdr, meta_xdr, ledger_number, ledger_created_at)
		VALUES
			('tx1', 1, 'envelope1', 'result1', 'meta1', 1, $1),
			('tx2', 2, 'envelope2', 'result2', 'meta2', 2, $1),
			('tx3', 3, 'envelope3', 'result3', 'meta3', 3, $1)
	`, now)
	require.NoError(t, err)

	// Create test state changes (state_change_category: 1=BALANCE)
	// to_id maps to transactions via (to_id & ~4095) = tx.to_id
	_, err = dbConnectionPool.ExecContext(ctx, `
		INSERT INTO state_changes (to_id, state_change_order, state_change_category, ledger_created_at, account_id)
		VALUES
			(1, 1, 1, $1, $2),
			(2, 1, 1, $1, $2),
			(3, 1, 1, $1, $2)
	`, now, address)
	require.NoError(t, err)

	// Test BatchGetByStateChangeID
	transactions, err := m.BatchGetByStateChangeIDs(ctx, []int64{1, 2, 3}, []int64{1, 1, 1}, "")
	require.NoError(t, err)
	assert.Len(t, transactions, 3)

	// Verify transactions are for correct state change IDs
	stateChangeIDsFound := make(map[string]string)
	for _, tx := range transactions {
		stateChangeIDsFound[tx.StateChangeID] = tx.Hash
	}
	assert.Equal(t, "tx1", stateChangeIDsFound["1-1"])
	assert.Equal(t, "tx2", stateChangeIDsFound["2-1"])
	assert.Equal(t, "tx1", stateChangeIDsFound["3-1"])
}

func BenchmarkTransactionModel_BatchInsert(b *testing.B) {
	dbt := dbtest.OpenB(b)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	if err != nil {
		b.Fatalf("failed to open db connection pool: %v", err)
	}
	defer dbConnectionPool.Close()

	ctx := context.Background()
	sqlxDB, err := dbConnectionPool.SqlxDB(ctx)
	if err != nil {
		b.Fatalf("failed to get sqlx db: %v", err)
	}
	metricsService := metrics.NewMetricsService(sqlxDB)

	m := &TransactionModel{
		DB:             dbConnectionPool,
		MetricsService: metricsService,
	}

	batchSizes := []int{1000, 5000, 10000, 50000, 100000}

	for _, size := range batchSizes {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				b.StopTimer()
				// Clean up before each iteration
				//nolint:errcheck // truncate is best-effort cleanup in benchmarks
				dbConnectionPool.ExecContext(ctx, "TRUNCATE transactions, transactions_accounts CASCADE")
				// Generate fresh test data for each iteration
				txs, addressesByHash := generateTestTransactions(size, int64(i*size))
				b.StartTimer()

				_, err := m.BatchInsert(ctx, nil, txs, addressesByHash)
				if err != nil {
					b.Fatalf("BatchInsert failed: %v", err)
				}
			}
		})
	}
}

// BenchmarkTransactionModel_BatchCopy benchmarks bulk insert using pgx's binary COPY protocol.
func BenchmarkTransactionModel_BatchCopy(b *testing.B) {
	dbt := dbtest.OpenB(b)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	if err != nil {
		b.Fatalf("failed to open db connection pool: %v", err)
	}
	defer dbConnectionPool.Close()

	ctx := context.Background()
	sqlxDB, err := dbConnectionPool.SqlxDB(ctx)
	if err != nil {
		b.Fatalf("failed to get sqlx db: %v", err)
	}
	metricsService := metrics.NewMetricsService(sqlxDB)

	m := &TransactionModel{
		DB:             dbConnectionPool,
		MetricsService: metricsService,
	}

	// Create pgx connection for BatchCopy
	conn, err := pgx.Connect(ctx, dbt.DSN)
	if err != nil {
		b.Fatalf("failed to connect with pgx: %v", err)
	}
	defer conn.Close(ctx)

	batchSizes := []int{1000, 5000, 10000, 50000, 100000}

	for _, size := range batchSizes {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				b.StopTimer()
				// Clean up before each iteration
				_, err = conn.Exec(ctx, "TRUNCATE transactions, transactions_accounts CASCADE")
				if err != nil {
					b.Fatalf("failed to truncate: %v", err)
				}

				// Generate fresh test data for each iteration
				txs, addressesByHash := generateTestTransactions(size, int64(i*size))

				// Start a pgx transaction
				pgxTx, err := conn.Begin(ctx)
				if err != nil {
					b.Fatalf("failed to begin transaction: %v", err)
				}
				b.StartTimer()

				_, err = m.BatchCopy(ctx, pgxTx, txs, addressesByHash)
				if err != nil {
					pgxTx.Rollback(ctx)
					b.Fatalf("BatchCopy failed: %v", err)
				}

				b.StopTimer()
				if err := pgxTx.Commit(ctx); err != nil {
					b.Fatalf("failed to commit transaction: %v", err)
				}
				b.StartTimer()
			}
		})
	}
}
