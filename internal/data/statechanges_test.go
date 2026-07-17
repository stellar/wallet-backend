package data

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
)

// generateTestStateChanges creates n test state changes for benchmarking.
// Populates all fields to provide an upper-bound benchmark.
// The auxAddresses parameter provides pre-generated valid Stellar addresses for nullable account_id fields.
func generateTestStateChanges(n int, accountID string, startToID int64, auxAddresses []string) []types.StateChange {
	scs := make([]types.StateChange, n)
	now := time.Now()
	reason := types.StateChangeReasonCredit

	for i := 0; i < n; i++ {
		// Use modulo to cycle through auxiliary addresses for nullable account_id fields
		auxIdx := i % len(auxAddresses)
		scs[i] = types.StateChange{
			ToID:                startToID + int64(i),
			StateChangeID:       1,
			StateChangeCategory: types.StateChangeCategoryBalance,
			StateChangeReason:   reason,
			LedgerCreatedAt:     now,
			LedgerNumber:        uint32(i + 1),
			AccountID:           types.AddressBytea(accountID),
			OperationID:         int64(i + 1),
			// NullAddressBytea token field
			TokenID: types.NullAddressBytea{AddressBytea: types.AddressBytea(auxAddresses[(auxIdx+6)%len(auxAddresses)]), Valid: true},
			Amount:  sql.NullString{String: fmt.Sprintf("%d", (i+1)*100), Valid: true},
			// NullAddressBytea fields
			SignerAccountID:    types.NullAddressBytea{AddressBytea: types.AddressBytea(auxAddresses[auxIdx]), Valid: true},
			SpenderAccountID:   types.NullAddressBytea{AddressBytea: types.AddressBytea(auxAddresses[(auxIdx+1)%len(auxAddresses)]), Valid: true},
			SponsoredAccountID: types.NullAddressBytea{AddressBytea: types.AddressBytea(auxAddresses[(auxIdx+2)%len(auxAddresses)]), Valid: true},
			SponsorAccountID:   types.NullAddressBytea{AddressBytea: types.AddressBytea(auxAddresses[(auxIdx+3)%len(auxAddresses)]), Valid: true},
			DeployerAccountID:  types.NullAddressBytea{AddressBytea: types.AddressBytea(auxAddresses[(auxIdx+4)%len(auxAddresses)]), Valid: true},
			FunderAccountID:    types.NullAddressBytea{AddressBytea: types.AddressBytea(auxAddresses[(auxIdx+5)%len(auxAddresses)]), Valid: true},
			// Typed fields (previously JSONB)
			SignerWeightOld:   sql.NullInt16{Int16: int16(i % 256), Valid: true},
			SignerWeightNew:   sql.NullInt16{Int16: int16((i + 1) % 256), Valid: true},
			ThresholdOld:      sql.NullInt16{Int16: 1, Valid: true},
			ThresholdNew:      sql.NullInt16{Int16: 2, Valid: true},
			TrustlineLimitOld: sql.NullString{String: fmt.Sprintf("%d", i*1000), Valid: true},
			TrustlineLimitNew: sql.NullString{String: fmt.Sprintf("%d", (i+1)*1000), Valid: true},
			Flags:             sql.NullInt16{Int16: 6, Valid: true}, // Bitmask for auth_required (2) | auth_revocable (4)
			KeyValue:          types.NullableJSONB{"key": fmt.Sprintf("data_key_%d", i), "value": fmt.Sprintf("data_value_%d", i)},
		}
	}

	return scs
}

func TestStateChangeModel_BatchCopy(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	now := time.Now()

	kp1 := keypair.MustRandom()
	kp2 := keypair.MustRandom()

	// Create referenced transactions first
	tx1 := types.Transaction{
		Hash:            "f176b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa4877",
		ToID:            1,
		FeeCharged:      100,
		ResultCode:      "TransactionResultCodeTxSuccess",
		LedgerNumber:    1,
		LedgerCreatedAt: now,
		IsFeeBump:       false,
	}
	tx2 := types.Transaction{
		Hash:            "0276b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa4877",
		ToID:            2,
		FeeCharged:      200,
		ResultCode:      "TransactionResultCodeTxSuccess",
		LedgerNumber:    2,
		LedgerCreatedAt: now,
		IsFeeBump:       true,
	}
	// Insert transactions using direct SQL
	_, err = dbConnectionPool.Exec(ctx, `
		INSERT INTO transactions (hash, to_id, fee_charged, result_code, ledger_number, ledger_created_at, is_fee_bump)
		VALUES ($1, $2, $3, $4, $5, $6, $7), ($8, $9, $10, $11, $12, $13, $14)
	`, tx1.Hash, tx1.ToID, tx1.FeeCharged, tx1.ResultCode, tx1.LedgerNumber, tx1.LedgerCreatedAt, tx1.IsFeeBump,
		tx2.Hash, tx2.ToID, tx2.FeeCharged, tx2.ResultCode, tx2.LedgerNumber, tx2.LedgerCreatedAt, tx2.IsFeeBump)
	require.NoError(t, err)

	reason := types.StateChangeReasonAdd
	sc1 := types.StateChange{
		ToID:                1,
		StateChangeID:       1,
		StateChangeCategory: types.StateChangeCategoryBalance,
		StateChangeReason:   reason,
		LedgerCreatedAt:     now,
		LedgerNumber:        1,
		AccountID:           types.AddressBytea(kp1.Address()),
		OperationID:         123,
		TokenID:             types.NullAddressBytea{AddressBytea: types.AddressBytea(kp1.Address()), Valid: true},
		Amount:              sql.NullString{String: "100", Valid: true},
	}
	sc2 := types.StateChange{
		ToID:                2,
		StateChangeID:       1,
		StateChangeCategory: types.StateChangeCategoryBalance,
		StateChangeReason:   reason,
		LedgerCreatedAt:     now,
		LedgerNumber:        2,
		AccountID:           types.AddressBytea(kp2.Address()),
		OperationID:         456,
	}
	// State change with typed signer/threshold fields (uses to_id=1 to reference tx1)
	sc3 := types.StateChange{
		ToID:                1,
		StateChangeID:       2, // Different StateChangeID to avoid PK conflict with sc1
		StateChangeCategory: types.StateChangeCategorySigner,
		StateChangeReason:   types.StateChangeReasonAdd,
		LedgerCreatedAt:     now,
		LedgerNumber:        3,
		AccountID:           types.AddressBytea(kp1.Address()),
		OperationID:         789,
		SignerWeightOld:     sql.NullInt16{Int16: 0, Valid: true},
		SignerWeightNew:     sql.NullInt16{Int16: 10, Valid: true},
		ThresholdOld:        sql.NullInt16{Int16: 1, Valid: true},
		ThresholdNew:        sql.NullInt16{Int16: 3, Valid: true},
	}

	testCases := []struct {
		name            string
		stateChanges    []types.StateChange
		wantCount       int
		wantErrContains string
	}{
		{
			name:         "🟢successful_insert_multiple",
			stateChanges: []types.StateChange{sc1, sc2},
			wantCount:    2,
		},
		{
			name:         "🟢empty_input",
			stateChanges: []types.StateChange{},
			wantCount:    0,
		},
		{
			name:         "🟢nullable_fields",
			stateChanges: []types.StateChange{sc2},
			wantCount:    1,
		},
		{
			name:         "🟢jsonb_fields",
			stateChanges: []types.StateChange{sc3},
			wantCount:    1,
		},
	}

	// Create pgx connection for BatchCopy (requires pgx.Tx, not sqlx.Tx)
	conn, err := pgx.Connect(ctx, dbt.DSN)
	require.NoError(t, err)
	defer conn.Close(ctx)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Clear the database before each test
			_, err = dbConnectionPool.Exec(ctx, "TRUNCATE state_changes CASCADE")
			require.NoError(t, err)

			reg := prometheus.NewRegistry()
			dbMetrics := metrics.NewMetrics(reg).DB

			m := &StateChangeModel{
				DB:      dbConnectionPool,
				Metrics: dbMetrics,
			}

			// BatchCopy requires a pgx transaction
			pgxTx, err := conn.Begin(ctx)
			require.NoError(t, err)

			gotCount, err := m.BatchCopy(ctx, pgxTx, tc.stateChanges)

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
			dbInsertedIDs, err := db.QueryMany[string](ctx, dbConnectionPool, "SELECT CONCAT(to_id, '-', operation_id, '-', state_change_id) FROM state_changes ORDER BY to_id")
			require.NoError(t, err)
			assert.Len(t, dbInsertedIDs, tc.wantCount)
		})
	}
}

// TestStateChangeModel_BatchCopy_DuplicateFailsOnPK verifies the SQL-06
// idempotency backstop: since state_change_id is now a deterministic ordinal
// (assigned by types.AssignStateChangeOrdinals before BatchCopy is called,
// not randomly generated inside BatchCopy), re-copying an already-committed
// ledger's rows collides on the state_changes primary key and fails loudly
// instead of silently inserting duplicate rows.
func TestStateChangeModel_BatchCopy_DuplicateFailsOnPK(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	now := time.Now()
	kp := keypair.MustRandom()

	_, err = dbConnectionPool.Exec(ctx, `
		INSERT INTO transactions (hash, to_id, fee_charged, result_code, ledger_number, ledger_created_at, is_fee_bump)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`, "f176b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa4877", int64(1), 100, "TransactionResultCodeTxSuccess", uint32(1), now, false)
	require.NoError(t, err)

	reg := prometheus.NewRegistry()
	dbMetrics := metrics.NewMetrics(reg).DB
	m := &StateChangeModel{DB: dbConnectionPool, Metrics: dbMetrics}

	conn, err := pgx.Connect(ctx, dbt.DSN)
	require.NoError(t, err)
	defer conn.Close(ctx)

	// Same (to_id, operation_id, state_change_id, ledger_created_at) as a
	// deterministic emitter would recompute for the same ledger on a re-ingest.
	sc := types.StateChange{
		ToID:                1,
		StateChangeID:       1,
		StateChangeCategory: types.StateChangeCategoryBalance,
		StateChangeReason:   types.StateChangeReasonAdd,
		LedgerCreatedAt:     now,
		LedgerNumber:        1,
		AccountID:           types.AddressBytea(kp.Address()),
		OperationID:         123,
	}

	pgxTx, err := conn.Begin(ctx)
	require.NoError(t, err)
	gotCount, err := m.BatchCopy(ctx, pgxTx, []types.StateChange{sc})
	require.NoError(t, err)
	require.NoError(t, pgxTx.Commit(ctx))
	assert.Equal(t, 1, gotCount)

	// Re-copying the identical row (simulating a re-ingest of the same ledger)
	// must fail on the primary key rather than inserting a duplicate.
	pgxTx2, err := conn.Begin(ctx)
	require.NoError(t, err)
	_, err = m.BatchCopy(ctx, pgxTx2, []types.StateChange{sc})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate key value violates unique constraint")
	pgxTx2.Rollback(ctx)

	dbInsertedIDs, err := db.QueryMany[string](ctx, dbConnectionPool, "SELECT CONCAT(to_id, '-', operation_id, '-', state_change_id) FROM state_changes")
	require.NoError(t, err)
	assert.Len(t, dbInsertedIDs, 1, "the failed re-copy must not have left duplicate rows")
}

func TestStateChangeModel_BatchGetByAccountAddress(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	now := time.Now()

	address1 := keypair.MustRandom().Address()
	address2 := keypair.MustRandom().Address()

	// Create test transactions first (hash is BYTEA)
	testHash1 := types.HashBytea("0000000000000000000000000000000000000000000000000000000000000001")
	testHash2 := types.HashBytea("0000000000000000000000000000000000000000000000000000000000000002")
	testHash3 := types.HashBytea("0000000000000000000000000000000000000000000000000000000000000003")
	_, err = dbConnectionPool.Exec(ctx, `
		INSERT INTO transactions (hash, to_id, fee_charged, result_code, ledger_number, ledger_created_at, is_fee_bump)
		VALUES
			($2, 1, 100, 'TransactionResultCodeTxSuccess', 1, $1, false),
			($3, 2, 200, 'TransactionResultCodeTxSuccess', 2, $1, true),
			($4, 3, 300, 'TransactionResultCodeTxSuccess', 3, $1, false)
	`, now, testHash1, testHash2, testHash3)
	require.NoError(t, err)

	// Create test state changes
	_, err = dbConnectionPool.Exec(ctx, `
		INSERT INTO state_changes (to_id, state_change_id, state_change_category, state_change_reason, ledger_created_at, ledger_number, account_id, operation_id)
		VALUES
			(1, 1, 'BALANCE', 'CREDIT', $1, 1, $2, 123),
			(2, 1, 'BALANCE', 'CREDIT', $1, 2, $2, 456),
			(3, 1, 'BALANCE', 'CREDIT', $1, 3, $3, 789)
	`, now, types.AddressBytea(address1), types.AddressBytea(address2))
	require.NoError(t, err)

	reg := prometheus.NewRegistry()
	dbMetrics := metrics.NewMetrics(reg).DB

	m := &StateChangeModel{
		DB:      dbConnectionPool,
		Metrics: dbMetrics,
	}

	// Test BatchGetByAccount for address1
	stateChanges, err := m.BatchGetByAccountAddress(ctx, address1, nil, nil, nil, nil, "", nil, nil, ASC, nil)
	require.NoError(t, err)
	assert.Len(t, stateChanges, 2)
	for _, sc := range stateChanges {
		assert.Equal(t, address1, sc.AccountID.String())
	}

	// Test BatchGetByAccount for address2
	stateChanges, err = m.BatchGetByAccountAddress(ctx, address2, nil, nil, nil, nil, "", nil, nil, ASC, nil)
	require.NoError(t, err)
	assert.Len(t, stateChanges, 1)
	for _, sc := range stateChanges {
		assert.Equal(t, address2, sc.AccountID.String())
	}
}

func TestStateChangeModel_BatchGetByAccountAddress_WithFilters(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	now := time.Now()

	address := keypair.MustRandom().Address()

	// Create test transactions (hash is BYTEA)
	testHash1 := "0000000000000000000000000000000000000000000000000000000000000001"
	testHash2 := "0000000000000000000000000000000000000000000000000000000000000002"
	testHash3 := "0000000000000000000000000000000000000000000000000000000000000003"
	testHashNonExistent := "0000000000000000000000000000000000000000000000000000000000000004"
	_, err = dbConnectionPool.Exec(ctx, `
		INSERT INTO transactions (hash, to_id, fee_charged, result_code, ledger_number, ledger_created_at)
		VALUES
			($2, 1, 100, 'TransactionResultCodeTxSuccess', 1, $1),
			($3, 2, 200, 'TransactionResultCodeTxSuccess', 2, $1),
			($4, 3, 300, 'TransactionResultCodeTxSuccess', 3, $1)
	`, now, types.HashBytea(testHash1), types.HashBytea(testHash2), types.HashBytea(testHash3))
	require.NoError(t, err)

	// Create test state changes with different operation IDs, categories, and reasons
	// State changes must reference valid transaction to_ids (1, 2, or 3)
	_, err = dbConnectionPool.Exec(ctx, `
		INSERT INTO state_changes (to_id, state_change_id, state_change_category, state_change_reason, ledger_created_at, ledger_number, account_id, operation_id)
		VALUES
			(1, 1, 'BALANCE', 'CREDIT', $1, 1, $2, 123),
			(2, 1, 'BALANCE', 'DEBIT', $1, 2, $2, 456),
			(3, 1, 'SIGNER', 'ADD', $1, 3, $2, 789),
			(1, 2, 'BALANCE', 'DEBIT', $1, 4, $2, 124),
			(2, 2, 'SIGNER', 'ADD', $1, 5, $2, 999)
	`, now, types.AddressBytea(address))
	require.NoError(t, err)

	t.Run("filter by transaction hash only", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		dbMetrics := metrics.NewMetrics(reg).DB

		m := &StateChangeModel{
			DB:      dbConnectionPool,
			Metrics: dbMetrics,
		}

		txHash := testHash1
		stateChanges, err := m.BatchGetByAccountAddress(ctx, address, &txHash, nil, nil, nil, "", nil, nil, ASC, nil)
		require.NoError(t, err)
		// tx1 has to_id=1, so we get state changes where to_id=1 (2 state changes now)
		assert.Len(t, stateChanges, 2)
		for _, sc := range stateChanges {
			assert.Equal(t, int64(1), sc.StateChange.ToID)
			assert.Equal(t, address, sc.AccountID.String())
		}
	})

	t.Run("filter by operation ID only", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		dbMetrics := metrics.NewMetrics(reg).DB

		m := &StateChangeModel{
			DB:      dbConnectionPool,
			Metrics: dbMetrics,
		}

		operationID := int64(123)
		stateChanges, err := m.BatchGetByAccountAddress(ctx, address, nil, &operationID, nil, nil, "", nil, nil, ASC, nil)
		require.NoError(t, err)
		// Only 1 state change has operation_id=123 (the first one with to_id=1)
		assert.Len(t, stateChanges, 1)
		for _, sc := range stateChanges {
			assert.Equal(t, int64(123), sc.StateChange.OperationID)
			assert.Equal(t, address, sc.AccountID.String())
		}
	})

	t.Run("filter by both transaction hash and operation ID", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		dbMetrics := metrics.NewMetrics(reg).DB

		m := &StateChangeModel{
			DB:      dbConnectionPool,
			Metrics: dbMetrics,
		}

		txHash := testHash1
		operationID := int64(123)
		stateChanges, err := m.BatchGetByAccountAddress(ctx, address, &txHash, &operationID, nil, nil, "", nil, nil, ASC, nil)
		require.NoError(t, err)
		// Should get only state changes that match BOTH filters (to_id=1 from tx1 hash, operation_id=123)
		assert.Len(t, stateChanges, 1)
		for _, sc := range stateChanges {
			assert.Equal(t, int64(1), sc.StateChange.ToID)
			assert.Equal(t, int64(123), sc.StateChange.OperationID)
			assert.Equal(t, address, sc.AccountID.String())
		}
	})

	t.Run("filter by category only", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		dbMetrics := metrics.NewMetrics(reg).DB

		m := &StateChangeModel{
			DB:      dbConnectionPool,
			Metrics: dbMetrics,
		}

		category := "BALANCE"
		stateChanges, err := m.BatchGetByAccountAddress(ctx, address, nil, nil, &category, nil, "", nil, nil, ASC, nil)
		require.NoError(t, err)
		assert.Len(t, stateChanges, 3)
		for _, sc := range stateChanges {
			assert.Equal(t, types.StateChangeCategoryBalance, sc.StateChangeCategory)
			assert.Equal(t, address, sc.AccountID.String())
		}
	})

	t.Run("filter by reason only", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		dbMetrics := metrics.NewMetrics(reg).DB

		m := &StateChangeModel{
			DB:      dbConnectionPool,
			Metrics: dbMetrics,
		}

		reason := "ADD"
		stateChanges, err := m.BatchGetByAccountAddress(ctx, address, nil, nil, nil, &reason, "", nil, nil, ASC, nil)
		require.NoError(t, err)
		assert.Len(t, stateChanges, 2)
		for _, sc := range stateChanges {
			assert.Equal(t, types.StateChangeReasonAdd, sc.StateChangeReason)
			assert.Equal(t, address, sc.AccountID.String())
		}
	})

	t.Run("filter by both category and reason", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		dbMetrics := metrics.NewMetrics(reg).DB

		m := &StateChangeModel{
			DB:      dbConnectionPool,
			Metrics: dbMetrics,
		}

		category := "SIGNER"
		reason := "ADD"
		stateChanges, err := m.BatchGetByAccountAddress(ctx, address, nil, nil, &category, &reason, "", nil, nil, ASC, nil)
		require.NoError(t, err)
		assert.Len(t, stateChanges, 2)
		for _, sc := range stateChanges {
			assert.Equal(t, types.StateChangeCategorySigner, sc.StateChangeCategory)
			assert.Equal(t, types.StateChangeReasonAdd, sc.StateChangeReason)
			assert.Equal(t, address, sc.AccountID.String())
		}
	})

	t.Run("filter with all filters - txHash, operationID, category, reason", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		dbMetrics := metrics.NewMetrics(reg).DB

		m := &StateChangeModel{
			DB:      dbConnectionPool,
			Metrics: dbMetrics,
		}

		txHash := testHash1
		operationID := int64(123)
		category := "BALANCE"
		reason := "CREDIT"
		stateChanges, err := m.BatchGetByAccountAddress(ctx, address, &txHash, &operationID, &category, &reason, "", nil, nil, ASC, nil)
		require.NoError(t, err)
		assert.Len(t, stateChanges, 1)
		for _, sc := range stateChanges {
			assert.Equal(t, int64(1), sc.StateChange.ToID)
			assert.Equal(t, int64(123), sc.StateChange.OperationID)
			assert.Equal(t, types.StateChangeCategoryBalance, sc.StateChangeCategory)
			assert.Equal(t, types.StateChangeReasonCredit, sc.StateChangeReason)
			assert.Equal(t, address, sc.AccountID.String())
		}
	})

	t.Run("filter with no matching results", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		dbMetrics := metrics.NewMetrics(reg).DB

		m := &StateChangeModel{
			DB:      dbConnectionPool,
			Metrics: dbMetrics,
		}

		txHash := testHashNonExistent
		stateChanges, err := m.BatchGetByAccountAddress(ctx, address, &txHash, nil, nil, nil, "", nil, nil, ASC, nil)
		require.NoError(t, err)
		assert.Empty(t, stateChanges)
	})

	t.Run("filter with pagination", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		dbMetrics := metrics.NewMetrics(reg).DB

		m := &StateChangeModel{
			DB:      dbConnectionPool,
			Metrics: dbMetrics,
		}

		txHash := testHash1
		limit := int32(1)
		stateChanges, err := m.BatchGetByAccountAddress(ctx, address, &txHash, nil, nil, nil, "", &limit, nil, ASC, nil)
		require.NoError(t, err)
		assert.Len(t, stateChanges, 1)
		assert.Equal(t, int64(1), stateChanges[0].StateChange.ToID)
	})
}

// TestStateChangeModel_BatchGetByAccountAddress_DuplicateHashTolerated covers SQL-07: idx_transactions_hash
// is non-unique (TimescaleDB can't enforce unique(hash) on a hypertable), so a data bug could
// duplicate a hash across two to_ids. The txHash filter must tolerate this ("IN" rather than "=" /
// a scalar subquery) instead of erroring with "more than one row returned by a subquery".
func TestStateChangeModel_BatchGetByAccountAddress_DuplicateHashTolerated(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	now := time.Now()
	address := keypair.MustRandom().Address()
	dupHash := types.HashBytea("0000000000000000000000000000000000000000000000000000000000000009")

	// Two different transactions (different to_id, the hypertable's real uniqueness boundary)
	// sharing the same hash.
	_, err = dbConnectionPool.Exec(ctx, `
		INSERT INTO transactions (hash, to_id, fee_charged, result_code, ledger_number, ledger_created_at)
		VALUES
			($1, 1, 100, 'TransactionResultCodeTxSuccess', 1, $2),
			($1, 2, 100, 'TransactionResultCodeTxSuccess', 2, $2)
	`, dupHash, now)
	require.NoError(t, err)

	_, err = dbConnectionPool.Exec(ctx, `
		INSERT INTO state_changes (to_id, state_change_id, state_change_category, state_change_reason, ledger_created_at, ledger_number, account_id, operation_id)
		VALUES
			(1, 1, 'BALANCE', 'CREDIT', $1, 1, $2, 123),
			(2, 1, 'BALANCE', 'CREDIT', $1, 2, $2, 456)
	`, now, types.AddressBytea(address))
	require.NoError(t, err)

	reg := prometheus.NewRegistry()
	dbMetrics := metrics.NewMetrics(reg).DB
	m := &StateChangeModel{DB: dbConnectionPool, Metrics: dbMetrics}

	txHash := dupHash.String()
	stateChanges, err := m.BatchGetByAccountAddress(ctx, address, &txHash, nil, nil, nil, "", nil, nil, ASC, nil)
	require.NoError(t, err, "duplicate hash must not error as 'more than one row returned by a subquery'")
	assert.Len(t, stateChanges, 2, "state changes from both to_ids sharing the hash must be returned")

	toIDsFound := make(map[int64]bool)
	for _, sc := range stateChanges {
		toIDsFound[sc.StateChange.ToID] = true
	}
	assert.True(t, toIDsFound[1])
	assert.True(t, toIDsFound[2])
}

func TestStateChangeModel_BatchGetByToIDs(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	now := time.Now()

	address := keypair.MustRandom().Address()

	// Create test transactions first (hash is BYTEA)
	testHash1 := types.HashBytea("0000000000000000000000000000000000000000000000000000000000000001")
	testHash2 := types.HashBytea("0000000000000000000000000000000000000000000000000000000000000002")
	testHash3 := types.HashBytea("0000000000000000000000000000000000000000000000000000000000000003")
	_, err = dbConnectionPool.Exec(ctx, `
		INSERT INTO transactions (hash, to_id, fee_charged, result_code, ledger_number, ledger_created_at, is_fee_bump)
		VALUES
			($2, 1, 100, 'TransactionResultCodeTxSuccess', 1, $1, false),
			($3, 2, 200, 'TransactionResultCodeTxSuccess', 2, $1, true),
			($4, 3, 300, 'TransactionResultCodeTxSuccess', 3, $1, false)
	`, now, testHash1, testHash2, testHash3)
	require.NoError(t, err)

	// Create test state changes - multiple state changes per to_id to test ranking
	_, err = dbConnectionPool.Exec(ctx, `
		INSERT INTO state_changes (to_id, state_change_id, state_change_category, state_change_reason, ledger_created_at, ledger_number, account_id, operation_id)
		VALUES
			(1, 1, 'BALANCE', 'CREDIT', $1, 1, $2, 123),
			(1, 2, 'BALANCE', 'CREDIT', $1, 2, $2, 124),
			(1, 3, 'BALANCE', 'CREDIT', $1, 3, $2, 125),
			(2, 1, 'BALANCE', 'CREDIT', $1, 4, $2, 456),
			(2, 2, 'BALANCE', 'CREDIT', $1, 5, $2, 457),
			(3, 1, 'BALANCE', 'CREDIT', $1, 6, $2, 789)
	`, now, types.AddressBytea(address))
	require.NoError(t, err)

	testCases := []struct {
		name               string
		toIDs              []int64
		limit              *int32
		sortOrder          SortOrder
		expectedCount      int
		expectedToIDCounts map[int64]int
		expectMetricCalls  int
	}{
		{
			name:               "🟢 basic functionality with multiple to_ids",
			toIDs:              []int64{1, 2},
			limit:              nil,
			sortOrder:          ASC,
			expectedCount:      5, // 3 state changes for to_id=1 + 2 for to_id=2
			expectedToIDCounts: map[int64]int{1: 3, 2: 2},
			expectMetricCalls:  1,
		},
		{
			name:               "🟢 with limit parameter",
			toIDs:              []int64{1, 2},
			limit:              func() *int32 { v := int32(2); return &v }(),
			sortOrder:          ASC,
			expectedCount:      4, // 2 state changes per to_id (limited by ROW_NUMBER)
			expectedToIDCounts: map[int64]int{1: 2, 2: 2},
			expectMetricCalls:  1,
		},
		{
			name:               "🟢 DESC sort order",
			toIDs:              []int64{1},
			limit:              nil,
			sortOrder:          DESC,
			expectedCount:      3,
			expectedToIDCounts: map[int64]int{1: 3},
			expectMetricCalls:  1,
		},
		{
			name:               "🟢 single to_id",
			toIDs:              []int64{3},
			limit:              nil,
			sortOrder:          ASC,
			expectedCount:      1,
			expectedToIDCounts: map[int64]int{3: 1},
			expectMetricCalls:  1,
		},
		{
			name:               "🟡 empty to_ids array",
			toIDs:              []int64{},
			limit:              nil,
			sortOrder:          ASC,
			expectedCount:      0,
			expectedToIDCounts: map[int64]int{},
			expectMetricCalls:  1,
		},
		{
			name:               "🟡 non-existent to_id",
			toIDs:              []int64{999},
			limit:              nil,
			sortOrder:          ASC,
			expectedCount:      0,
			expectedToIDCounts: map[int64]int{},
			expectMetricCalls:  1,
		},
		{
			name:               "🟢 all to_ids",
			toIDs:              []int64{1, 2, 3},
			limit:              nil,
			sortOrder:          ASC,
			expectedCount:      6, // All state changes
			expectedToIDCounts: map[int64]int{1: 3, 2: 2, 3: 1},
			expectMetricCalls:  1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			dbMetrics := metrics.NewMetrics(reg).DB

			m := &StateChangeModel{
				DB:      dbConnectionPool,
				Metrics: dbMetrics,
			}

			ledgerCreatedAts := make([]time.Time, len(tc.toIDs))
			for i := range ledgerCreatedAts {
				ledgerCreatedAts[i] = now
			}
			stateChanges, err := m.BatchGetByToIDs(ctx, tc.toIDs, ledgerCreatedAts, "", tc.limit, tc.sortOrder)
			require.NoError(t, err)
			assert.Len(t, stateChanges, tc.expectedCount)

			// Verify state changes are for correct to_ids
			toIDsFound := make(map[int64]int)
			for _, sc := range stateChanges {
				toIDsFound[sc.StateChange.ToID]++
			}
			assert.Equal(t, tc.expectedToIDCounts, toIDsFound)

			// Verify cursor structure for returned state changes
			for _, sc := range stateChanges {
				assert.NotZero(t, sc.StateChangeCursor.ToID, "cursor ToID should be set")
				assert.NotZero(t, sc.StateChangeCursor.StateChangeID, "cursor StateChangeID should be set")
			}
		})
	}

	t.Run("wrong ledger_created_at for a key returns no state changes for that key (time pin enforced)", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		dbMetrics := metrics.NewMetrics(reg).DB
		m := &StateChangeModel{DB: dbConnectionPool, Metrics: dbMetrics}

		wrongTime := now.Add(-24 * time.Hour)
		stateChanges, err := m.BatchGetByToIDs(ctx, []int64{1}, []time.Time{wrongTime}, "", nil, ASC)
		require.NoError(t, err)
		assert.Empty(t, stateChanges)
	})

	t.Run("mismatched array lengths error", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		dbMetrics := metrics.NewMetrics(reg).DB
		m := &StateChangeModel{DB: dbConnectionPool, Metrics: dbMetrics}

		_, err := m.BatchGetByToIDs(ctx, []int64{1, 2}, []time.Time{now}, "", nil, ASC)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "parallel arrays of equal length")
	})

	// Regression: the cursor's ledger_created_at must not be read off the LATERAL's own
	// projection (which only carries the caller-requested columns) — a narrow column set that
	// excludes ledgerCreatedAt must not break the query with "column does not exist".
	t.Run("narrow column selection excluding ledger_created_at still resolves the cursor", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		dbMetrics := metrics.NewMetrics(reg).DB
		m := &StateChangeModel{DB: dbConnectionPool, Metrics: dbMetrics}

		stateChanges, err := m.BatchGetByToIDs(ctx, []int64{1}, []time.Time{now}, "state_change_category", nil, ASC)
		require.NoError(t, err)
		require.Len(t, stateChanges, 3)
		for _, sc := range stateChanges {
			assert.NotZero(t, sc.StateChangeCursor.LedgerCreatedAt)
		}
	})
}

func TestStateChangeModel_BatchGetByOperationIDs(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	reg := prometheus.NewRegistry()
	dbMetrics := metrics.NewMetrics(reg).DB

	m := &StateChangeModel{
		DB:      dbConnectionPool,
		Metrics: dbMetrics,
	}

	now := time.Now()

	address := keypair.MustRandom().Address()

	// Create test transactions first (hash is BYTEA)
	testHash1 := types.HashBytea("0000000000000000000000000000000000000000000000000000000000000001")
	testHash2 := types.HashBytea("0000000000000000000000000000000000000000000000000000000000000002")
	testHash3 := types.HashBytea("0000000000000000000000000000000000000000000000000000000000000003")
	_, err = dbConnectionPool.Exec(ctx, `
		INSERT INTO transactions (hash, to_id, fee_charged, result_code, ledger_number, ledger_created_at, is_fee_bump)
		VALUES
			($2, 1, 100, 'TransactionResultCodeTxSuccess', 1, $1, false),
			($3, 2, 200, 'TransactionResultCodeTxSuccess', 2, $1, true),
			($4, 3, 300, 'TransactionResultCodeTxSuccess', 3, $1, false)
	`, now, testHash1, testHash2, testHash3)
	require.NoError(t, err)

	// Create test state changes
	_, err = dbConnectionPool.Exec(ctx, `
		INSERT INTO state_changes (to_id, state_change_id, state_change_category, state_change_reason, ledger_created_at, ledger_number, account_id, operation_id)
		VALUES
			(1, 1, 'BALANCE', 'CREDIT', $1, 1, $2, 123),
			(2, 1, 'BALANCE', 'CREDIT', $1, 2, $2, 456),
			(3, 1, 'BALANCE', 'CREDIT', $1, 3, $2, 123)
	`, now, types.AddressBytea(address))
	require.NoError(t, err)

	// Test BatchGetByOperationID
	limit := int32(10)
	ledgerCreatedAts := []time.Time{now, now}
	stateChanges, err := m.BatchGetByOperationIDs(ctx, []int64{123, 456}, ledgerCreatedAts, "", &limit, ASC)
	require.NoError(t, err)
	assert.Len(t, stateChanges, 3)

	// Verify state changes are for correct operation IDs
	operationIDsFound := make(map[int64]int)
	for _, sc := range stateChanges {
		operationIDsFound[sc.StateChange.OperationID]++
	}
	assert.Equal(t, 2, operationIDsFound[123])
	assert.Equal(t, 1, operationIDsFound[456])

	t.Run("wrong ledger_created_at for a key excludes it (time pin enforced)", func(t *testing.T) {
		wrongTime := now.Add(-24 * time.Hour)
		stateChanges, err := m.BatchGetByOperationIDs(ctx, []int64{123}, []time.Time{wrongTime}, "", &limit, ASC)
		require.NoError(t, err)
		assert.Empty(t, stateChanges)
	})

	t.Run("mismatched array lengths error", func(t *testing.T) {
		_, err := m.BatchGetByOperationIDs(ctx, []int64{123, 456}, []time.Time{now}, "", &limit, ASC)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "parallel arrays of equal length")
	})

	// Regression: the cursor's ledger_created_at must not be read off the LATERAL's own
	// projection (which only carries the caller-requested columns).
	t.Run("narrow column selection excluding ledger_created_at still resolves the cursor", func(t *testing.T) {
		stateChanges, err := m.BatchGetByOperationIDs(ctx, []int64{123}, []time.Time{now}, "state_change_category", &limit, ASC)
		require.NoError(t, err)
		require.Len(t, stateChanges, 2)
		for _, sc := range stateChanges {
			assert.NotZero(t, sc.StateChangeCursor.LedgerCreatedAt)
		}
	})
}

func TestStateChangeModel_BatchGetByToID(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	reg := prometheus.NewRegistry()
	dbMetrics := metrics.NewMetrics(reg).DB

	m := &StateChangeModel{
		DB:      dbConnectionPool,
		Metrics: dbMetrics,
	}

	now := time.Now()

	address := keypair.MustRandom().Address()

	// Create test transactions first (hash is BYTEA)
	testHash1 := types.HashBytea("0000000000000000000000000000000000000000000000000000000000000001")
	testHash2 := types.HashBytea("0000000000000000000000000000000000000000000000000000000000000002")
	_, err = dbConnectionPool.Exec(ctx, `
		INSERT INTO transactions (hash, to_id, fee_charged, result_code, ledger_number, ledger_created_at, is_fee_bump)
		VALUES
			($2, 1, 100, 'TransactionResultCodeTxSuccess', 1, $1, false),
			($3, 2, 200, 'TransactionResultCodeTxSuccess', 2, $1, true)
	`, now, testHash1, testHash2)
	require.NoError(t, err)

	// Create test state changes for to_id=1 (multiple state_change_ids)
	_, err = dbConnectionPool.Exec(ctx, `
		INSERT INTO state_changes (to_id, state_change_id, state_change_category, state_change_reason, ledger_created_at, ledger_number, account_id, operation_id)
		VALUES
			(1, 1, 'BALANCE', 'CREDIT', $1, 1, $2, 123),
			(1, 2, 'BALANCE', 'CREDIT', $1, 2, $2, 124),
			(1, 3, 'BALANCE', 'CREDIT', $1, 3, $2, 125),
			(2, 1, 'BALANCE', 'CREDIT', $1, 4, $2, 456)
	`, now, types.AddressBytea(address))
	require.NoError(t, err)

	t.Run("get all state changes for single to_id", func(t *testing.T) {
		stateChanges, err := m.BatchGetByToID(ctx, 1, now, "", nil, nil, ASC)
		require.NoError(t, err)
		assert.Len(t, stateChanges, 3)

		// Verify all state changes are for to_id=1
		for _, sc := range stateChanges {
			assert.Equal(t, int64(1), sc.StateChange.ToID)
		}

		// Verify ordering (ASC by to_id, state_change_id)
		assert.Equal(t, int64(1), stateChanges[0].StateChange.StateChangeID)
		assert.Equal(t, int64(2), stateChanges[1].StateChange.StateChangeID)
		assert.Equal(t, int64(3), stateChanges[2].StateChange.StateChangeID)
	})

	t.Run("get state changes with pagination - first", func(t *testing.T) {
		limit := int32(2)
		stateChanges, err := m.BatchGetByToID(ctx, 1, now, "", &limit, nil, ASC)
		require.NoError(t, err)
		assert.Len(t, stateChanges, 2)

		assert.Equal(t, int64(1), stateChanges[0].StateChange.StateChangeID)
		assert.Equal(t, int64(2), stateChanges[1].StateChange.StateChangeID)
	})

	t.Run("get state changes with cursor pagination", func(t *testing.T) {
		limit := int32(2)
		cursor := &types.StateChangeCursor{LedgerCreatedAt: now, ToID: 1, OperationID: 123, StateChangeID: 1}
		stateChanges, err := m.BatchGetByToID(ctx, 1, now, "", &limit, cursor, ASC)
		require.NoError(t, err)
		assert.Len(t, stateChanges, 2)

		// Should get results after cursor (to_id=1, operation_id=123, state_change_id=1)
		assert.Equal(t, int64(2), stateChanges[0].StateChange.StateChangeID)
		assert.Equal(t, int64(3), stateChanges[1].StateChange.StateChangeID)
	})

	t.Run("get state changes with DESC ordering", func(t *testing.T) {
		stateChanges, err := m.BatchGetByToID(ctx, 1, now, "", nil, nil, DESC)
		require.NoError(t, err)
		assert.Len(t, stateChanges, 3)

		// Verify ordering (results should be in ASC order after DESC query transformation)
		assert.Equal(t, int64(1), stateChanges[0].StateChange.StateChangeID)
		assert.Equal(t, int64(2), stateChanges[1].StateChange.StateChangeID)
		assert.Equal(t, int64(3), stateChanges[2].StateChange.StateChangeID)
	})

	t.Run("no state changes for non-existent to_id", func(t *testing.T) {
		stateChanges, err := m.BatchGetByToID(ctx, 999, now, "", nil, nil, ASC)
		require.NoError(t, err)
		assert.Empty(t, stateChanges)
	})

	t.Run("wrong ledger_created_at returns no state changes (time pin enforced)", func(t *testing.T) {
		wrongTime := now.Add(-24 * time.Hour)
		stateChanges, err := m.BatchGetByToID(ctx, 1, wrongTime, "", nil, nil, ASC)
		require.NoError(t, err)
		assert.Empty(t, stateChanges)
	})
}

// BenchmarkStateChangeModel_BatchCopy benchmarks bulk insert using pgx's binary COPY protocol.
func BenchmarkStateChangeModel_BatchCopy(b *testing.B) {
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

	m := &StateChangeModel{
		DB:      dbConnectionPool,
		Metrics: dbMetrics,
	}

	// Create pgx connection for BatchCopy
	conn, err := pgx.Connect(ctx, dbt.DSN)
	if err != nil {
		b.Fatalf("failed to connect with pgx: %v", err)
	}
	defer conn.Close(ctx)

	// Create a parent transaction that state changes will reference
	const txHash = "benchmark_tx_hash"
	accountID := keypair.MustRandom().Address()
	now := time.Now()
	_, err = conn.Exec(ctx, `
		INSERT INTO transactions (hash, to_id, fee_charged, result_code, ledger_number, ledger_created_at, is_fee_bump)
		VALUES ($1, 1, 100, 'TransactionResultCodeTxSuccess', 1, $2, false)
	`, txHash, now)
	if err != nil {
		b.Fatalf("failed to create parent transaction: %v", err)
	}

	// Pre-generate auxiliary addresses for nullable account_id fields
	auxAddresses := make([]string, 10)
	for i := range auxAddresses {
		auxAddresses[i] = keypair.MustRandom().Address()
	}

	batchSizes := []int{1000, 5000, 10000, 50000, 100000}

	for _, size := range batchSizes {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				b.StopTimer()
				// Clean up state changes before each iteration (keep the parent transaction)
				_, err = conn.Exec(ctx, "TRUNCATE state_changes CASCADE")
				if err != nil {
					b.Fatalf("failed to truncate: %v", err)
				}

				// Generate fresh test data for each iteration
				scs := generateTestStateChanges(size, accountID, int64(i*size), auxAddresses)

				// Start a pgx transaction
				pgxTx, err := conn.Begin(ctx)
				if err != nil {
					b.Fatalf("failed to begin transaction: %v", err)
				}
				b.StartTimer()

				_, err = m.BatchCopy(ctx, pgxTx, scs)
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

func TestStateChangeModel_BatchGetAccountStateChangesByToIDs(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	reg := prometheus.NewRegistry()
	m := &StateChangeModel{DB: dbConnectionPool, Metrics: metrics.NewMetrics(reg).DB}

	now := time.Now().UTC().Truncate(time.Microsecond)
	acct := keypair.MustRandom().Address()
	other := keypair.MustRandom().Address()
	txHash := types.HashBytea("0000000000000000000000000000000000000000000000000000000000000001")

	_, err = dbConnectionPool.Exec(ctx, `
		INSERT INTO transactions (hash, to_id, fee_charged, result_code, ledger_number, ledger_created_at, is_fee_bump)
		VALUES ($2, 4096, 100, 'TransactionResultCodeTxSuccess', 1, $1, false)
	`, now, txHash)
	require.NoError(t, err)

	// to_id 4096: two state changes for acct, one for a different account.
	_, err = dbConnectionPool.Exec(ctx, `
		INSERT INTO state_changes (to_id, state_change_id, state_change_category, state_change_reason, ledger_created_at, ledger_number, account_id, operation_id)
		VALUES
			(4096, 1, 'BALANCE', 'CREDIT', $1, 1, $2, 4097),
			(4096, 2, 'BALANCE', 'CREDIT', $1, 1, $2, 4097),
			(4096, 3, 'BALANCE', 'CREDIT', $1, 1, $3, 4097)
	`, now, types.AddressBytea(acct), types.AddressBytea(other))
	require.NoError(t, err)

	t.Run("returns only the account's state changes for the transaction, full set", func(t *testing.T) {
		scs, err := m.BatchGetAccountStateChangesByToIDs(ctx, acct, []int64{4096}, []time.Time{now}, "")
		require.NoError(t, err)
		require.Len(t, scs, 2)
		for _, sc := range scs {
			assert.Equal(t, int64(4096), sc.ToID)
			assert.Equal(t, acct, sc.AccountID.String())
		}
		// DESC ordering: higher state_change_id first.
		assert.Equal(t, int64(2), scs[0].StateChangeID)
		assert.Equal(t, int64(1), scs[1].StateChangeID)
	})

	t.Run("minimal projection still hydrates the loader-key columns", func(t *testing.T) {
		// The operation/transaction relationship resolvers build their dataloader
		// key from (to_id, operation_id, state_change_id). A client selection that
		// maps to none of those scalars must still come back with them hydrated,
		// or the key collapses to "<to_id>-0-0": nested operation/transaction
		// resolve to null and the loader dedups every row into one.
		scs, err := m.BatchGetAccountStateChangesByToIDs(ctx, acct, []int64{4096}, []time.Time{now}, "state_change_category")
		require.NoError(t, err)
		require.Len(t, scs, 2)
		for _, sc := range scs {
			assert.Equal(t, int64(4096), sc.ToID, "to_id must be hydrated")
			assert.Equal(t, int64(4097), sc.OperationID, "operation_id must be hydrated for the loader key")
			assert.NotZero(t, sc.StateChangeID, "state_change_id must be hydrated for the loader key")
			assert.Equal(t, acct, sc.AccountID.String(), "account_id must be hydrated")
			assert.True(t, now.Equal(sc.LedgerCreatedAt), "ledger_created_at must be hydrated")
		}
	})

	t.Run("empty when account has no state changes in the transaction", func(t *testing.T) {
		none := keypair.MustRandom().Address()
		scs, err := m.BatchGetAccountStateChangesByToIDs(ctx, none, []int64{4096}, []time.Time{now}, "")
		require.NoError(t, err)
		assert.Empty(t, scs)
	})

	t.Run("returns the union across multiple to_ids in one batch", func(t *testing.T) {
		// Second transaction (to_id 8192) with one state change for acct.
		_, err := dbConnectionPool.Exec(ctx, `
			INSERT INTO transactions (hash, to_id, fee_charged, result_code, ledger_number, ledger_created_at, is_fee_bump)
			VALUES ($2, 8192, 100, 'TransactionResultCodeTxSuccess', 1, $1, false)
		`, now, types.HashBytea("0000000000000000000000000000000000000000000000000000000000000002"))
		require.NoError(t, err)
		_, err = dbConnectionPool.Exec(ctx, `
			INSERT INTO state_changes (to_id, state_change_id, state_change_category, state_change_reason, ledger_created_at, ledger_number, account_id, operation_id)
			VALUES (8192, 1, 'BALANCE', 'CREDIT', $1, 1, $2, 8193)
		`, now, types.AddressBytea(acct))
		require.NoError(t, err)

		scs, err := m.BatchGetAccountStateChangesByToIDs(ctx, acct, []int64{4096, 8192}, []time.Time{now, now}, "")
		require.NoError(t, err)
		// acct owns sc 1,2 (to_id 4096) and sc 1 (to_id 8192); the other account's sc stays excluded.
		require.Len(t, scs, 3)
		// ORDER BY ledger_created_at DESC, to_id DESC, operation_id DESC, state_change_id DESC.
		// Same ledger time -> to_id DESC: 8192 first, then 4096's two by state_change_id DESC (2, 1).
		assert.Equal(t, int64(8192), scs[0].ToID)
		assert.Equal(t, int64(4096), scs[1].ToID)
		assert.Equal(t, int64(2), scs[1].StateChangeID)
		assert.Equal(t, int64(4096), scs[2].ToID)
		assert.Equal(t, int64(1), scs[2].StateChangeID)
	})
}

// A client selecting no time fields must still get state changes whose LedgerCreatedAt is
// hydrated: the operation and transaction relationship resolvers pin their lookups on the
// parent state change's partition timestamp, so every projection forces ledger_created_at.
func TestStateChangeModel_MinimalProjectionHydratesLedgerCreatedAt(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	reg := prometheus.NewRegistry()
	m := &StateChangeModel{DB: dbConnectionPool, Metrics: metrics.NewMetrics(reg).DB}

	now := time.Now().UTC().Truncate(time.Microsecond)
	address := keypair.MustRandom().Address()
	_, err = dbConnectionPool.Exec(ctx, `
		INSERT INTO state_changes (to_id, state_change_id, state_change_category, state_change_reason, ledger_created_at, ledger_number, account_id, operation_id)
		VALUES (4096, 1, 'BALANCE', 'CREDIT', $1, 1, $2, 4098)
	`, now, types.AddressBytea(address))
	require.NoError(t, err)

	limit := int32(10)
	byToID, err := m.BatchGetByToID(ctx, 4096, now, "state_change_category", &limit, nil, ASC)
	require.NoError(t, err)
	require.Len(t, byToID, 1)
	assert.True(t, now.Equal(byToID[0].StateChange.LedgerCreatedAt), "BatchGetByToID with minimal projection must hydrate ledger_created_at")

	byOpID, err := m.BatchGetByOperationID(ctx, 4098, now, "state_change_category", &limit, nil, ASC)
	require.NoError(t, err)
	require.Len(t, byOpID, 1)
	assert.True(t, now.Equal(byOpID[0].StateChange.LedgerCreatedAt), "BatchGetByOperationID with minimal projection must hydrate ledger_created_at")
}
