package data

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/toid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
)

// generateTestTransactions creates n test transactions for benchmarking.
// Uses toid.New to generate realistic ToIDs based on ledger sequence and transaction index.
func generateTestTransactions(n int, startLedger int32) ([]*types.Transaction, map[int64]types.ParticipantSet) {
	txs := make([]*types.Transaction, n)
	addressesByToID := make(map[int64]types.ParticipantSet)
	now := time.Now()

	for i := 0; i < n; i++ {
		ledgerSeq := startLedger + int32(i)
		txIndex := int32(1) // First transaction in each ledger
		toID := toid.New(ledgerSeq, txIndex, 0).ToInt64()
		hash := fmt.Sprintf("e76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0%08x", i)
		address := keypair.MustRandom().Address()

		txs[i] = &types.Transaction{
			Hash:            types.HashBytea(hash),
			ToID:            toID,
			FeeCharged:      int64(100 * (i + 1)),
			ResultCode:      "TransactionResultCodeTxSuccess",
			LedgerNumber:    uint32(ledgerSeq),
			LedgerCreatedAt: now,
			IsFeeBump:       false,
		}
		addressesByToID[toID] = types.NewParticipantSet(address)
	}

	return txs, addressesByToID
}

func Test_TransactionModel_BatchCopy(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	now := time.Now()

	kp1 := keypair.MustRandom()
	kp2 := keypair.MustRandom()

	txCopy1 := types.Transaction{
		Hash:            "b76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48762",
		ToID:            1,
		FeeCharged:      100,
		ResultCode:      "TransactionResultCodeTxSuccess",
		LedgerNumber:    1,
		LedgerCreatedAt: now,
		IsFeeBump:       false,
	}
	txCopy2 := types.Transaction{
		Hash:            "c76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48763",
		ToID:            2,
		FeeCharged:      200,
		ResultCode:      "TransactionResultCodeTxSuccess",
		LedgerNumber:    2,
		LedgerCreatedAt: now,
		IsFeeBump:       true,
	}
	txCopy3 := types.Transaction{
		Hash:            "d76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48764",
		ToID:            3,
		FeeCharged:      300,
		ResultCode:      "TransactionResultCodeTxSuccess",
		LedgerNumber:    3,
		LedgerCreatedAt: now,
		IsFeeBump:       false,
	}

	testCases := []struct {
		name                   string
		txs                    []*types.Transaction
		stellarAddressesByToID map[int64]types.ParticipantSet
		wantCount              int
		wantErrContains        string
	}{
		{
			name:                   "🟢successful_insert_multiple",
			txs:                    []*types.Transaction{&txCopy1, &txCopy2},
			stellarAddressesByToID: map[int64]types.ParticipantSet{txCopy1.ToID: types.NewParticipantSet(kp1.Address()), txCopy2.ToID: types.NewParticipantSet(kp2.Address())},
			wantCount:              2,
		},
		{
			name:                   "🟢empty_input",
			txs:                    []*types.Transaction{},
			stellarAddressesByToID: map[int64]types.ParticipantSet{},
			wantCount:              0,
		},
		{
			name:                   "🟢single_transaction",
			txs:                    []*types.Transaction{&txCopy3},
			stellarAddressesByToID: map[int64]types.ParticipantSet{txCopy3.ToID: types.NewParticipantSet(kp1.Address())},
			wantCount:              1,
		},
		{
			name:                   "🟢no_participants",
			txs:                    []*types.Transaction{&txCopy1},
			stellarAddressesByToID: map[int64]types.ParticipantSet{},
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
			_, err = dbConnectionPool.Exec(ctx, "TRUNCATE transactions, transactions_accounts CASCADE")
			require.NoError(t, err)

			reg := prometheus.NewRegistry()
			dbMetrics := metrics.NewMetrics(reg).DB

			m := &TransactionModel{
				DB:      dbConnectionPool,
				Metrics: dbMetrics,
			}

			// BatchCopy requires a pgx transaction
			pgxTx, err := conn.Begin(ctx)
			require.NoError(t, err)

			gotCount, err := m.BatchCopy(ctx, pgxTx, tc.txs)

			if tc.wantErrContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrContains)
				pgxTx.Rollback(ctx)
				return
			}

			require.NoError(t, err)
			require.NoError(t, m.BatchCopyAccounts(ctx, pgxTx, tc.txs, tc.stellarAddressesByToID))
			require.NoError(t, pgxTx.Commit(ctx))
			assert.Equal(t, tc.wantCount, gotCount)

			// Verify from DB
			dbInsertedHashes, err := db.QueryMany[types.HashBytea](ctx, dbConnectionPool, "SELECT hash FROM transactions ORDER BY hash")
			require.NoError(t, err)
			assert.Len(t, dbInsertedHashes, tc.wantCount)

			// Verify account links if expected
			if len(tc.stellarAddressesByToID) > 0 && tc.wantCount > 0 {
				type txAccountLink struct {
					TxToID    int64              `db:"tx_to_id"`
					AccountID types.AddressBytea `db:"account_id"`
				}
				accountLinks, err := db.QueryMany[txAccountLink](ctx, dbConnectionPool, "SELECT tx_to_id, account_id FROM transactions_accounts ORDER BY tx_to_id, account_id")
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

func TestTransactionModel_GetByHash(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	reg := prometheus.NewRegistry()
	dbMetrics := metrics.NewMetrics(reg).DB

	m := &TransactionModel{
		DB:      dbConnectionPool,
		Metrics: dbMetrics,
	}

	now := time.Now()

	// Create test transaction
	txHash := types.HashBytea("0076b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa4876")
	_, err = dbConnectionPool.Exec(ctx, `
		INSERT INTO transactions (hash, to_id, fee_charged, result_code, ledger_number, ledger_created_at, is_fee_bump)
		VALUES ($1, 1, 100, 'TransactionResultCodeTxSuccess', 1, $2, false)
	`, txHash, now)
	require.NoError(t, err)

	// Test GetByHash
	transaction, err := m.GetByHash(ctx, txHash.String(), "")
	require.NoError(t, err)
	assert.Equal(t, txHash, transaction.Hash)
	assert.Equal(t, int64(1), transaction.ToID)
}

func TestTransactionModel_GetAll(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	reg := prometheus.NewRegistry()
	dbMetrics := metrics.NewMetrics(reg).DB

	m := &TransactionModel{
		DB:      dbConnectionPool,
		Metrics: dbMetrics,
	}

	now := time.Now()

	// Create test transactions
	testHash1 := types.HashBytea("1076b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa4876")
	testHash2 := types.HashBytea("2076b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa4876")
	testHash3 := types.HashBytea("3076b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa4876")
	_, err = dbConnectionPool.Exec(ctx, `
		INSERT INTO transactions (hash, to_id, fee_charged, result_code, ledger_number, ledger_created_at, is_fee_bump)
		VALUES
			($1, 1, 100, 'TransactionResultCodeTxSuccess', 1, $4, false),
			($2, 2, 200, 'TransactionResultCodeTxSuccess', 2, $4, true),
			($3, 3, 300, 'TransactionResultCodeTxSuccess', 3, $4, false)
	`, testHash1, testHash2, testHash3, now)
	require.NoError(t, err)

	// Test GetAll without specifying cursor and limit (gets all transactions)
	transactions, err := m.GetAll(ctx, "", nil, nil, ASC)
	require.NoError(t, err)
	assert.Len(t, transactions, 3)
	assert.Equal(t, int64(1), transactions[0].CompositeCursor.ID)
	assert.Equal(t, int64(2), transactions[1].CompositeCursor.ID)
	assert.Equal(t, int64(3), transactions[2].CompositeCursor.ID)

	// Test GetAll with smaller limit
	limit := int32(2)
	transactions, err = m.GetAll(ctx, "", &limit, nil, ASC)
	require.NoError(t, err)
	assert.Len(t, transactions, 2)
	assert.Equal(t, int64(1), transactions[0].CompositeCursor.ID)
	assert.Equal(t, int64(2), transactions[1].CompositeCursor.ID)
}

func TestTransactionModel_BatchGetByAccountAddress(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	reg := prometheus.NewRegistry()
	dbMetrics := metrics.NewMetrics(reg).DB

	m := &TransactionModel{
		DB:      dbConnectionPool,
		Metrics: dbMetrics,
	}

	now := time.Now()

	address1 := keypair.MustRandom().Address()
	address2 := keypair.MustRandom().Address()

	// Create test transactions
	accTestHash1 := types.HashBytea("4076b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa4876")
	accTestHash2 := types.HashBytea("5076b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa4876")
	accTestHash3 := types.HashBytea("6076b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa4876")
	_, err = dbConnectionPool.Exec(ctx, `
		INSERT INTO transactions (hash, to_id, fee_charged, result_code, ledger_number, ledger_created_at, is_fee_bump)
		VALUES
			($1, 1, 100, 'TransactionResultCodeTxSuccess', 1, $4, false),
			($2, 2, 200, 'TransactionResultCodeTxSuccess', 2, $4, true),
			($3, 3, 300, 'TransactionResultCodeTxSuccess', 3, $4, false)
	`, accTestHash1, accTestHash2, accTestHash3, now)
	require.NoError(t, err)

	// Create test transactions_accounts links (account_id is BYTEA)
	_, err = dbConnectionPool.Exec(ctx, `
		INSERT INTO transactions_accounts (ledger_created_at, tx_to_id, account_id)
		VALUES
			($3, 1, $1),
			($3, 2, $1),
			($3, 3, $2)
	`, types.AddressBytea(address1), types.AddressBytea(address2), now)
	require.NoError(t, err)

	// Test BatchGetByAccount
	transactions, err := m.BatchGetByAccountAddress(ctx, address1, "", nil, nil, ASC, nil)
	require.NoError(t, err)
	assert.Len(t, transactions, 2)

	assert.Equal(t, int64(1), transactions[0].CompositeCursor.ID)
	assert.Equal(t, int64(2), transactions[1].CompositeCursor.ID)
}

func TestTransactionModel_BatchGetByOperationIDs(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	reg := prometheus.NewRegistry()
	dbMetrics := metrics.NewMetrics(reg).DB

	m := &TransactionModel{
		DB:      dbConnectionPool,
		Metrics: dbMetrics,
	}

	now := time.Now()

	// Create test transactions with specific ToIDs
	// Operations IDs must be in TOID range for each transaction: (to_id, to_id + 4096)
	opTestHash1 := types.HashBytea("7076b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa4876")
	opTestHash2 := types.HashBytea("8076b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa4876")
	opTestHash3 := types.HashBytea("9076b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa4877")
	_, err = dbConnectionPool.Exec(ctx, `
		INSERT INTO transactions (hash, to_id, fee_charged, result_code, ledger_number, ledger_created_at, is_fee_bump)
		VALUES
			($1, 4096, 100, 'TransactionResultCodeTxSuccess', 1, $4, false),
			($2, 8192, 200, 'TransactionResultCodeTxSuccess', 2, $4, true),
			($3, 12288, 300, 'TransactionResultCodeTxSuccess', 3, $4, false)
	`, opTestHash1, opTestHash2, opTestHash3, now)
	require.NoError(t, err)

	// Create test operations (IDs must be in TOID range for each transaction)
	// opTestHash1 (to_id=4096): ops 4097, 4098
	// opTestHash2 (to_id=8192): op 8193
	xdr1 := types.XDRBytea([]byte("xdr1"))
	xdr2 := types.XDRBytea([]byte("xdr2"))
	xdr3 := types.XDRBytea([]byte("xdr3"))
	_, err = dbConnectionPool.Exec(ctx, `
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
	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	reg := prometheus.NewRegistry()
	dbMetrics := metrics.NewMetrics(reg).DB

	m := &TransactionModel{
		DB:      dbConnectionPool,
		Metrics: dbMetrics,
	}

	now := time.Now()

	address := keypair.MustRandom().Address()

	// Create test transactions
	scTestHash1 := types.HashBytea("a176b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa4877")
	scTestHash2 := types.HashBytea("b176b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa4877")
	scTestHash3 := types.HashBytea("c176b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa4877")
	_, err = dbConnectionPool.Exec(ctx, `
		INSERT INTO transactions (hash, to_id, fee_charged, result_code, ledger_number, ledger_created_at, is_fee_bump)
		VALUES
			($1, 1, 100, 'TransactionResultCodeTxSuccess', 1, $4, false),
			($2, 2, 200, 'TransactionResultCodeTxSuccess', 2, $4, true),
			($3, 3, 300, 'TransactionResultCodeTxSuccess', 3, $4, false)
	`, scTestHash1, scTestHash2, scTestHash3, now)
	require.NoError(t, err)

	// Create test state changes
	_, err = dbConnectionPool.Exec(ctx, `
		INSERT INTO state_changes (to_id, state_change_id, state_change_category, state_change_reason, ledger_created_at, ledger_number, account_id, operation_id)
		VALUES
			(1, 1, 'BALANCE', 'CREDIT', $1, 1, $2, 1),
			(2, 1, 'BALANCE', 'CREDIT', $1, 2, $2, 2),
			(3, 1, 'BALANCE', 'CREDIT', $1, 3, $2, 3)
	`, now, address)
	require.NoError(t, err)

	// Test BatchGetByStateChangeID
	transactions, err := m.BatchGetByStateChangeIDs(ctx, []int64{1, 2, 3}, []int64{1, 2, 3}, []int64{1, 1, 1}, "")
	require.NoError(t, err)
	assert.Len(t, transactions, 3)

	// Verify transactions are for correct state change IDs (format: to_id-operation_id-state_change_id)
	// State change (to_id, operation_id, state_change_id) should return transaction with matching to_id
	stateChangeIDsFound := make(map[string]types.HashBytea)
	for _, tx := range transactions {
		stateChangeIDsFound[tx.StateChangeID] = tx.Hash
	}
	assert.Equal(t, scTestHash1, stateChangeIDsFound["1-1-1"]) // to_id=1 -> scTestHash1 (to_id=1)
	assert.Equal(t, scTestHash2, stateChangeIDsFound["2-2-1"]) // to_id=2 -> scTestHash2 (to_id=2)
	assert.Equal(t, scTestHash3, stateChangeIDsFound["3-3-1"]) // to_id=3 -> scTestHash3 (to_id=3)
}

// BenchmarkTransactionModel_BatchCopy benchmarks bulk insert using pgx's binary COPY protocol.
func BenchmarkTransactionModel_BatchCopy(b *testing.B) {
	dbt := dbtest.OpenB(b)
	defer dbt.Close()
	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	if err != nil {
		b.Fatalf("failed to open db connection pool: %v", err)
	}
	defer dbConnectionPool.Close()

	reg := prometheus.NewRegistry()
	dbMetrics := metrics.NewMetrics(reg).DB

	m := &TransactionModel{
		DB:      dbConnectionPool,
		Metrics: dbMetrics,
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

				_, err = m.BatchCopy(ctx, pgxTx, txs)
				if err != nil {
					pgxTx.Rollback(ctx)
					b.Fatalf("BatchCopy failed: %v", err)
				}
				if err = m.BatchCopyAccounts(ctx, pgxTx, txs, addressesByToID); err != nil {
					pgxTx.Rollback(ctx)
					b.Fatalf("BatchCopyAccounts failed: %v", err)
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
