package data

import (
	"context"
	"fmt"
	"testing"
	"time"

	set "github.com/deckarep/golang-set/v2"
	"github.com/jackc/pgx/v5"
	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/toid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
)

// generateTestTransactions creates n test transactions for benchmarking.
// Uses toid.New to generate realistic ToIDs based on ledger sequence and transaction index.
func generateTestTransactions(n int, startLedger int32) ([]*types.Transaction, map[int64]set.Set[string]) {
	txs := make([]*types.Transaction, n)
	addressesByToID := make(map[int64]set.Set[string])
	now := time.Now()

	for i := 0; i < n; i++ {
		ledgerSeq := startLedger + int32(i)
		txIndex := int32(1) // First transaction in each ledger
		toID := toid.New(ledgerSeq, txIndex, 0).ToInt64()
		hash := fmt.Sprintf("e76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0%08x", i)
		envelope := "AAAAAgAAAAB/NpQ+s+cP+ztX7ryuKgXrxowZPHd4qAxhseOye/JeUgAehIAC2NL/AAflugAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAAwAAAAFQQUxMAAAAAKHc4IKbcW8HPPgy3zOhuqv851y72nfLGa0HVXxIRNzHAAAAAAAAAAAAQ3FwMxshxQAfwV8AAAAAYTGQ3QAAAAAAAAAMAAAAAAAAAAFQQUxMAAAAAKHc4IKbcW8HPPgy3zOhuqv851y72nfLGa0HVXxIRNzHAAAAAAAGXwFksiHwAEXz8QAAAABhoaQjAAAAAAAAAAF78l5SAAAAQD7LgvZA8Pdvfh5L2b9B9RC7DlacGBJuOchuZDHQdVD1P0bn6nGQJXxDDI4oN76J49JxB7bIgDVim39MU43MOgE="
		meta := "AAAAAwAAAAAAAAAEAAAAAwM6nhwAAAAAAAAAAJjy0MY1CPlZ/co80nzufVmo4gd7NqWMb+RiGiPhiviJAAAAC4SozKUDMWgAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAQM6nhwAAAAAAAAAAJjy0MY1CPlZ/co80nzufVmo4gd7NqWMb+RiGiPhiviJAAAAC4SozKUDMWgAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAwM6LTkAAAAAAAAAAKl6DQcpepRdTbO/Vw4hYBENfE/95GevM7SNA0ftK0gtAAAAA8Kuf0AC+zZCAAAATAAAAAMAAAABAAAAAMRxxkNwYslQaok0LlOKGtpATS9Bzx06JV9DIffG4OF1AAAAAAAAAAlsb2JzdHIuY28AAAABAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAgAAAAAAAAAAAAAAAAAAAAMAAAAAAyZ54QAAAABmrTXCAAAAAAAAAAEDOp4cAAAAAAAAAACpeg0HKXqUXU2zv1cOIWARDXxP/eRnrzO0jQNH7StILQAAAAPCrn9AAvs2QgAAAE0AAAADAAAAAQAAAADEccZDcGLJUGqJNC5TihraQE0vQc8dOiVfQyH3xuDhdQAAAAAAAAAJbG9ic3RyLmNvAAAAAQAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAAAAAAAAAAAADAAAAAAM6nhwAAAAAZyGdHwAAAAAAAAABAAAABAAAAAMDOp4cAAAAAAAAAACpeg0HKXqUXU2zv1cOIWARDXxP/eRnrzO0jQNH7StILQAAAAPCrn9AAvs2QgAAAE0AAAADAAAAAQAAAADEccZDcGLJUGqJNC5TihraQE0vQc8dOiVfQyH3xuDhdQAAAAAAAAAJbG9ic3RyLmNvAAAAAQAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAAAAAAAAAAAADAAAAAAM6nhwAAAAAZyGdHwAAAAAAAAABAzqeHAAAAAAAAAAAqXoNByl6lF1Ns79XDiFgEQ18T/3kZ68ztI0DR+0rSC0AAAACmKiNQAL7NkIAAABNAAAAAwAAAAEAAAAAxHHGQ3BiyVBqiTQuU4oa2kBNL0HPHTolX0Mh98bg4XUAAAAAAAAACWxvYnN0ci5jbwAAAAEAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAACAAAAAAAAAAAAAAAAAAAAAwAAAAADOp4cAAAAAGchnR8AAAAAAAAAAwM6nZoAAAAAAAAAALKxMozkOH3rgpz3/u3+93wsR4p6z4K82HmJ5NTuaZbYAAACZaqAwoIBqycyAABVlQAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAgAAAAAAAAAAAAAAAAAAAAMAAAAAAzqSNgAAAABnIVdaAAAAAAAAAAEDOp4cAAAAAAAAAACysTKM5Dh964Kc9/7t/vd8LEeKes+CvNh5ieTU7mmW2AAAAmbUhrSCAasnMgAAVZUAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAAAAAAAAAAAADAAAAAAM6kjYAAAAAZyFXWgAAAAAAAAAAAAAAAA=="
		address := keypair.MustRandom().Address()

		txs[i] = &types.Transaction{
			Hash:            types.HashBytea(hash),
			ToID:            toID,
			EnvelopeXDR:     &envelope,
			FeeCharged:      int64(100 * (i + 1)),
			ResultCode:      "TransactionResultCodeTxSuccess",
			MetaXDR:         &meta,
			LedgerNumber:    uint32(ledgerSeq),
			LedgerCreatedAt: now,
			IsFeeBump:       false,
		}
		addressesByToID[toID] = set.NewSet(address)
	}

	return txs, addressesByToID
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
	const q = "INSERT INTO accounts (stellar_address) VALUES ($1), ($2)"
	_, err = dbConnectionPool.ExecContext(ctx, q, types.AddressBytea(kp1.Address()), types.AddressBytea(kp2.Address()))
	require.NoError(t, err)

	meta1, meta2 := "meta1", "meta2"
	envelope1, envelope2 := "envelope1", "envelope2"
	tx1 := types.Transaction{
		Hash:            "e76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48760",
		ToID:            1,
		EnvelopeXDR:     &envelope1,
		FeeCharged:      100,
		ResultCode:      "TransactionResultCodeTxSuccess",
		MetaXDR:         &meta1,
		LedgerNumber:    1,
		LedgerCreatedAt: now,
		IsFeeBump:       false,
	}
	tx2 := types.Transaction{
		Hash:            "a76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48761",
		ToID:            2,
		EnvelopeXDR:     &envelope2,
		FeeCharged:      200,
		ResultCode:      "TransactionResultCodeTxSuccess",
		MetaXDR:         &meta2,
		LedgerNumber:    2,
		LedgerCreatedAt: now,
		IsFeeBump:       true,
	}

	testCases := []struct {
		name                   string
		useDBTx                bool
		txs                    []*types.Transaction
		stellarAddressesByToID map[int64]set.Set[string]
		wantAccountLinks       map[int64][]string
		wantErrContains        string
		wantHashes             []string
	}{
		{
			name:                   "🟢successful_insert_without_dbTx",
			useDBTx:                false,
			txs:                    []*types.Transaction{&tx1, &tx2},
			stellarAddressesByToID: map[int64]set.Set[string]{tx1.ToID: set.NewSet(kp1.Address()), tx2.ToID: set.NewSet(kp2.Address())},
			wantAccountLinks:       map[int64][]string{tx1.ToID: {kp1.Address()}, tx2.ToID: {kp2.Address()}},
			wantErrContains:        "",
			wantHashes:             []string{tx1.Hash.String(), tx2.Hash.String()},
		},
		{
			name:                   "🟢successful_insert_with_dbTx",
			useDBTx:                true,
			txs:                    []*types.Transaction{&tx1},
			stellarAddressesByToID: map[int64]set.Set[string]{tx1.ToID: set.NewSet(kp1.Address())},
			wantAccountLinks:       map[int64][]string{tx1.ToID: {kp1.Address()}},
			wantErrContains:        "",
			wantHashes:             []string{tx1.Hash.String()},
		},
		{
			name:                   "🟢empty_input",
			useDBTx:                false,
			txs:                    []*types.Transaction{},
			stellarAddressesByToID: map[int64]set.Set[string]{},
			wantAccountLinks:       map[int64][]string{},
			wantErrContains:        "",
			wantHashes:             nil,
		},
		{
			name:                   "🟡duplicate_transaction",
			useDBTx:                false,
			txs:                    []*types.Transaction{&tx1, &tx1},
			stellarAddressesByToID: map[int64]set.Set[string]{tx1.ToID: set.NewSet(kp1.Address())},
			wantAccountLinks:       map[int64][]string{tx1.ToID: {kp1.Address()}},
			wantErrContains:        "",
			wantHashes:             []string{tx1.Hash.String()},
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

			gotInsertedHashes, err := m.BatchInsert(ctx, sqlExecuter, tc.txs, tc.stellarAddressesByToID)

			if tc.wantErrContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrContains)
				return
			}

			// Verify the results
			require.NoError(t, err)
			var dbInsertedHashes []types.HashBytea
			err = sqlExecuter.SelectContext(ctx, &dbInsertedHashes, "SELECT hash FROM transactions")
			require.NoError(t, err)
			// Convert HashBytea to string for comparison
			dbHashStrings := make([]string, len(dbInsertedHashes))
			for i, h := range dbInsertedHashes {
				dbHashStrings[i] = h.String()
			}
			assert.ElementsMatch(t, tc.wantHashes, dbHashStrings)
			assert.ElementsMatch(t, tc.wantHashes, gotInsertedHashes)

			// Verify the account links
			if len(tc.wantAccountLinks) > 0 {
				var accountLinks []struct {
					TxToID    int64              `db:"tx_to_id"`
					AccountID types.AddressBytea `db:"account_id"`
				}
				err = sqlExecuter.SelectContext(ctx, &accountLinks, "SELECT tx_to_id, account_id FROM transactions_accounts ORDER BY tx_to_id, account_id")
				require.NoError(t, err)

				// Create a map of tx_to_id -> set of account_ids for O(1) lookups
				accountLinksMap := make(map[int64][]string)
				for _, link := range accountLinks {
					accountLinksMap[link.TxToID] = append(accountLinksMap[link.TxToID], string(link.AccountID))
				}

				// Verify each transaction has its expected account links
				require.Equal(t, len(tc.wantAccountLinks), len(accountLinksMap), "number of elements in the maps don't match")
				for key, expectedSlice := range tc.wantAccountLinks {
					actualSlice, exists := accountLinksMap[key]
					require.True(t, exists, "key %d not found in actual map", key)
					assert.ElementsMatch(t, expectedSlice, actualSlice, "slices for key %d don't match", key)
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
	const q = "INSERT INTO accounts (stellar_address) VALUES ($1), ($2)"
	_, err = dbConnectionPool.ExecContext(ctx, q, types.AddressBytea(kp1.Address()), types.AddressBytea(kp2.Address()))
	require.NoError(t, err)

	meta1, meta2 := "meta1", "meta2"
	envelope1, envelope2 := "envelope1", "envelope2"
	txCopy1 := types.Transaction{
		Hash:            "b76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48762",
		ToID:            1,
		EnvelopeXDR:     &envelope1,
		FeeCharged:      100,
		ResultCode:      "TransactionResultCodeTxSuccess",
		MetaXDR:         &meta1,
		LedgerNumber:    1,
		LedgerCreatedAt: now,
		IsFeeBump:       false,
	}
	txCopy2 := types.Transaction{
		Hash:            "c76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48763",
		ToID:            2,
		EnvelopeXDR:     &envelope2,
		FeeCharged:      200,
		ResultCode:      "TransactionResultCodeTxSuccess",
		MetaXDR:         &meta2,
		LedgerNumber:    2,
		LedgerCreatedAt: now,
		IsFeeBump:       true,
	}
	// Transaction with nullable fields (nil envelope and meta)
	txCopy3 := types.Transaction{
		Hash:            "d76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48764",
		ToID:            3,
		EnvelopeXDR:     nil,
		FeeCharged:      300,
		ResultCode:      "TransactionResultCodeTxSuccess",
		MetaXDR:         nil,
		LedgerNumber:    3,
		LedgerCreatedAt: now,
		IsFeeBump:       false,
	}

	testCases := []struct {
		name                   string
		txs                    []*types.Transaction
		stellarAddressesByToID map[int64]set.Set[string]
		wantCount              int
		wantErrContains        string
	}{
		{
			name:                   "🟢successful_insert_multiple",
			txs:                    []*types.Transaction{&txCopy1, &txCopy2},
			stellarAddressesByToID: map[int64]set.Set[string]{txCopy1.ToID: set.NewSet(kp1.Address()), txCopy2.ToID: set.NewSet(kp2.Address())},
			wantCount:              2,
		},
		{
			name:                   "🟢empty_input",
			txs:                    []*types.Transaction{},
			stellarAddressesByToID: map[int64]set.Set[string]{},
			wantCount:              0,
		},
		{
			name:                   "🟢nullable_fields",
			txs:                    []*types.Transaction{&txCopy3},
			stellarAddressesByToID: map[int64]set.Set[string]{txCopy3.ToID: set.NewSet(kp1.Address())},
			wantCount:              1,
		},
		{
			name:                   "🟢no_participants",
			txs:                    []*types.Transaction{&txCopy1},
			stellarAddressesByToID: map[int64]set.Set[string]{},
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
				if len(tc.stellarAddressesByToID) > 0 {
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

			gotCount, err := m.BatchCopy(ctx, pgxTx, tc.txs, tc.stellarAddressesByToID)

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
			var dbInsertedHashes []types.HashBytea
			err = dbConnectionPool.SelectContext(ctx, &dbInsertedHashes, "SELECT hash FROM transactions ORDER BY hash")
			require.NoError(t, err)
			assert.Len(t, dbInsertedHashes, tc.wantCount)

			// Verify account links if expected
			if len(tc.stellarAddressesByToID) > 0 && tc.wantCount > 0 {
				var accountLinks []struct {
					TxToID    int64              `db:"tx_to_id"`
					AccountID types.AddressBytea `db:"account_id"`
				}
				err = dbConnectionPool.SelectContext(ctx, &accountLinks, "SELECT tx_to_id, account_id FROM transactions_accounts ORDER BY tx_to_id, account_id")
				require.NoError(t, err)

				// Create a map of tx_to_id -> set of account_ids
				accountLinksMap := make(map[int64][]string)
				for _, link := range accountLinks {
					accountLinksMap[link.TxToID] = append(accountLinksMap[link.TxToID], string(link.AccountID))
				}

				// Verify each expected transaction has its account links
				for toID, expectedAddresses := range tc.stellarAddressesByToID {
					actualAddresses := accountLinksMap[toID]
					assert.ElementsMatch(t, expectedAddresses.ToSlice(), actualAddresses, "account links for tx %d don't match", toID)
				}
			}
		})
	}
}

func Test_TransactionModel_BatchCopy_DuplicateFails(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()
	now := time.Now()

	// Create test account
	kp1 := keypair.MustRandom()
	const q = "INSERT INTO accounts (stellar_address) VALUES ($1)"
	_, err = dbConnectionPool.ExecContext(ctx, q, types.AddressBytea(kp1.Address()))
	require.NoError(t, err)

	meta := "meta1"
	envelope := "envelope1"
	txDup := types.Transaction{
		Hash:            "f76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48766",
		ToID:            100,
		EnvelopeXDR:     &envelope,
		FeeCharged:      100,
		ResultCode:      "TransactionResultCodeTxSuccess",
		MetaXDR:         &meta,
		LedgerNumber:    1,
		LedgerCreatedAt: now,
		IsFeeBump:       false,
	}

	// Pre-insert the transaction using BatchInsert (which uses ON CONFLICT DO NOTHING)
	sqlxDB, err := dbConnectionPool.SqlxDB(ctx)
	require.NoError(t, err)
	txModel := &TransactionModel{DB: dbConnectionPool, MetricsService: metrics.NewMetricsService(sqlxDB)}
	_, err = txModel.BatchInsert(ctx, nil, []*types.Transaction{&txDup}, map[int64]set.Set[string]{
		txDup.ToID: set.NewSet(kp1.Address()),
	})
	require.NoError(t, err)

	// Verify the transaction was inserted
	var count int
	err = dbConnectionPool.GetContext(ctx, &count, "SELECT COUNT(*) FROM transactions WHERE hash = $1", txDup.Hash)
	require.NoError(t, err)
	require.Equal(t, 1, count)

	// Now try to insert the same transaction using BatchCopy - this should FAIL
	// because COPY does not support ON CONFLICT handling
	mockMetricsService := metrics.NewMockMetricsService()
	mockMetricsService.On("IncDBQueryError", "BatchCopy", "transactions", mock.Anything).Return().Once()
	defer mockMetricsService.AssertExpectations(t)

	m := &TransactionModel{
		DB:             dbConnectionPool,
		MetricsService: mockMetricsService,
	}

	conn, err := pgx.Connect(ctx, dbt.DSN)
	require.NoError(t, err)
	defer conn.Close(ctx)

	pgxTx, err := conn.Begin(ctx)
	require.NoError(t, err)

	_, err = m.BatchCopy(ctx, pgxTx, []*types.Transaction{&txDup}, map[int64]set.Set[string]{
		txDup.ToID: set.NewSet(kp1.Address()),
	})

	// BatchCopy should fail with a unique constraint violation
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate key value violates unique constraint")

	// Rollback the failed transaction
	require.NoError(t, pgxTx.Rollback(ctx))
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
	txHash := types.HashBytea("0076b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa4876")
	_, err = dbConnectionPool.ExecContext(ctx, `
		INSERT INTO transactions (hash, to_id, envelope_xdr, fee_charged, result_code, meta_xdr, ledger_number, ledger_created_at, is_fee_bump)
		VALUES ($1, 1, 'envelope', 100, 'TransactionResultCodeTxSuccess', 'meta', 1, $2, false)
	`, txHash, now)
	require.NoError(t, err)

	// Test GetByHash
	transaction, err := m.GetByHash(ctx, txHash.String(), "")
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
	testHash1 := types.HashBytea("1076b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa4876")
	testHash2 := types.HashBytea("2076b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa4876")
	testHash3 := types.HashBytea("3076b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa4876")
	_, err = dbConnectionPool.ExecContext(ctx, `
		INSERT INTO transactions (hash, to_id, envelope_xdr, fee_charged, result_code, meta_xdr, ledger_number, ledger_created_at, is_fee_bump)
		VALUES
			($1, 1, 'envelope1', 100, 'TransactionResultCodeTxSuccess', 'meta1', 1, $4, false),
			($2, 2, 'envelope2', 200, 'TransactionResultCodeTxSuccess', 'meta2', 2, $4, true),
			($3, 3, 'envelope3', 300, 'TransactionResultCodeTxSuccess', 'meta3', 3, $4, false)
	`, testHash1, testHash2, testHash3, now)
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
	_, err = dbConnectionPool.ExecContext(ctx, "INSERT INTO accounts (stellar_address) VALUES ($1), ($2)",
		types.AddressBytea(address1), types.AddressBytea(address2))
	require.NoError(t, err)

	// Create test transactions
	accTestHash1 := types.HashBytea("4076b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa4876")
	accTestHash2 := types.HashBytea("5076b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa4876")
	accTestHash3 := types.HashBytea("6076b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa4876")
	_, err = dbConnectionPool.ExecContext(ctx, `
		INSERT INTO transactions (hash, to_id, envelope_xdr, fee_charged, result_code, meta_xdr, ledger_number, ledger_created_at, is_fee_bump)
		VALUES
			($1, 1, 'envelope1', 100, 'TransactionResultCodeTxSuccess', 'meta1', 1, $4, false),
			($2, 2, 'envelope2', 200, 'TransactionResultCodeTxSuccess', 'meta2', 2, $4, true),
			($3, 3, 'envelope3', 300, 'TransactionResultCodeTxSuccess', 'meta3', 3, $4, false)
	`, accTestHash1, accTestHash2, accTestHash3, now)
	require.NoError(t, err)

	// Create test transactions_accounts links (account_id is BYTEA)
	_, err = dbConnectionPool.ExecContext(ctx, `
		INSERT INTO transactions_accounts (ledger_created_at, tx_to_id, account_id)
		VALUES
			($3, 1, $1),
			($3, 2, $1),
			($3, 3, $2)
	`, types.AddressBytea(address1), types.AddressBytea(address2), now)
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

	// Create test transactions with specific ToIDs
	// Operations IDs must be in TOID range for each transaction: (to_id, to_id + 4096)
	opTestHash1 := types.HashBytea("7076b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa4876")
	opTestHash2 := types.HashBytea("8076b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa4876")
	opTestHash3 := types.HashBytea("9076b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa4877")
	_, err = dbConnectionPool.ExecContext(ctx, `
		INSERT INTO transactions (hash, to_id, envelope_xdr, fee_charged, result_code, meta_xdr, ledger_number, ledger_created_at, is_fee_bump)
		VALUES
			($1, 4096, 'envelope1', 100, 'TransactionResultCodeTxSuccess', 'meta1', 1, $4, false),
			($2, 8192, 'envelope2', 200, 'TransactionResultCodeTxSuccess', 'meta2', 2, $4, true),
			($3, 12288, 'envelope3', 300, 'TransactionResultCodeTxSuccess', 'meta3', 3, $4, false)
	`, opTestHash1, opTestHash2, opTestHash3, now)
	require.NoError(t, err)

	// Create test operations (IDs must be in TOID range for each transaction)
	// opTestHash1 (to_id=4096): ops 4097, 4098
	// opTestHash2 (to_id=8192): op 8193
	xdr1 := types.XDRBytea([]byte("xdr1"))
	xdr2 := types.XDRBytea([]byte("xdr2"))
	xdr3 := types.XDRBytea([]byte("xdr3"))
	_, err = dbConnectionPool.ExecContext(ctx, `
		INSERT INTO operations (id, operation_type, operation_xdr, result_code, successful, ledger_number, ledger_created_at)
		VALUES
			(4097, 'PAYMENT', $2, 'op_success', true, 1, $1),
			(8193, 'CREATE_ACCOUNT', $3, 'op_success', true, 2, $1),
			(4098, 'PAYMENT', $4, 'op_success', true, 3, $1)
	`, now, xdr1, xdr2, xdr3)
	require.NoError(t, err)

	// Test BatchGetByOperationIDs
	transactions, err := m.BatchGetByOperationIDs(ctx, []int64{4097, 8193, 4098}, "")
	require.NoError(t, err)
	assert.Len(t, transactions, 3)

	// Verify transactions are for correct operation IDs
	operationIDsFound := make(map[int64]types.HashBytea)
	for _, tx := range transactions {
		operationIDsFound[tx.OperationID] = tx.Hash
	}
	assert.Equal(t, opTestHash1, operationIDsFound[4097])
	assert.Equal(t, opTestHash2, operationIDsFound[8193])
	assert.Equal(t, opTestHash1, operationIDsFound[4098])
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
	_, err = dbConnectionPool.ExecContext(ctx, "INSERT INTO accounts (stellar_address) VALUES ($1)", types.AddressBytea(address))
	require.NoError(t, err)

	// Create test transactions
	scTestHash1 := types.HashBytea("a176b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa4877")
	scTestHash2 := types.HashBytea("b176b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa4877")
	scTestHash3 := types.HashBytea("c176b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa4877")
	_, err = dbConnectionPool.ExecContext(ctx, `
		INSERT INTO transactions (hash, to_id, envelope_xdr, fee_charged, result_code, meta_xdr, ledger_number, ledger_created_at, is_fee_bump)
		VALUES
			($1, 1, 'envelope1', 100, 'TransactionResultCodeTxSuccess', 'meta1', 1, $4, false),
			($2, 2, 'envelope2', 200, 'TransactionResultCodeTxSuccess', 'meta2', 2, $4, true),
			($3, 3, 'envelope3', 300, 'TransactionResultCodeTxSuccess', 'meta3', 3, $4, false)
	`, scTestHash1, scTestHash2, scTestHash3, now)
	require.NoError(t, err)

	// Create test state changes
	_, err = dbConnectionPool.ExecContext(ctx, `
		INSERT INTO state_changes (to_id, state_change_order, state_change_category, ledger_created_at, ledger_number, account_id, operation_id)
		VALUES
			(1, 1, 'BALANCE', $1, 1, $2, 1),
			(2, 1, 'BALANCE', $1, 2, $2, 2),
			(3, 1, 'BALANCE', $1, 3, $2, 3)
	`, now, address)
	require.NoError(t, err)

	// Test BatchGetByStateChangeID
	transactions, err := m.BatchGetByStateChangeIDs(ctx, []int64{1, 2, 3}, []int64{1, 2, 3}, []int64{1, 1, 1}, "")
	require.NoError(t, err)
	assert.Len(t, transactions, 3)

	// Verify transactions are for correct state change IDs (format: to_id-operation_id-state_change_order)
	// State change (to_id, operation_id, order) should return transaction with matching to_id
	stateChangeIDsFound := make(map[string]types.HashBytea)
	for _, tx := range transactions {
		stateChangeIDsFound[tx.StateChangeID] = tx.Hash
	}
	assert.Equal(t, scTestHash1, stateChangeIDsFound["1-1-1"]) // to_id=1 -> scTestHash1 (to_id=1)
	assert.Equal(t, scTestHash2, stateChangeIDsFound["2-2-1"]) // to_id=2 -> scTestHash2 (to_id=2)
	assert.Equal(t, scTestHash3, stateChangeIDsFound["3-3-1"]) // to_id=3 -> scTestHash3 (to_id=3)
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
				txs, addressesByToID := generateTestTransactions(size, int32(i*size))
				b.StartTimer()

				_, err := m.BatchInsert(ctx, nil, txs, addressesByToID)
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
				txs, addressesByToID := generateTestTransactions(size, int32(i*size))

				// Start a pgx transaction
				pgxTx, err := conn.Begin(ctx)
				if err != nil {
					b.Fatalf("failed to begin transaction: %v", err)
				}
				b.StartTimer()

				_, err = m.BatchCopy(ctx, pgxTx, txs, addressesByToID)
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
