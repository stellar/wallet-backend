package services

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/indexer"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
)

// Test_startLiveIngestion_ReleasesAdvisoryLockWhenContextCancelledMidStartup
// guards against ING-02: the advisory lock release used to run on the same
// (loop) context that PrepareRange/GetLedger use, so a shutdown signal
// arriving between lock acquisition and the ingest loop starting would cancel
// that context before the deferred release ran, and pgx refuses to execute a
// query on an already-cancelled context — silently leaking the lock.
//
// PrepareRange is used as the trigger point because it runs after the lock is
// acquired but before the ingest loop starts, standing in for a SIGTERM
// arriving during that narrow startup window.
func Test_startLiveIngestion_ReleasesAdvisoryLockWhenContextCancelledMidStartup(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer pool.Close()

	setupDBCursors(t, ctx, pool, 50, 40)

	m := metrics.NewMetrics(prometheus.NewRegistry())
	models, err := data.NewModels(pool, m.DB)
	require.NoError(t, err)

	mockBackend := &LedgerBackendMock{}
	mockBackend.On("PrepareRange", mock.Anything, mock.Anything).
		Run(func(mock.Arguments) { cancel() }).
		Return(nil)

	// The startup path runs the stale-SAC enrichment pass before preparing the
	// ledger range; production always wires a checkpoint service, so provide one.
	checkpointMock := NewCheckpointServiceMock(t)
	checkpointMock.On("EnrichStaleSACMetadata", mock.Anything).Return(nil)

	const testNetwork = "advisory-lock-release-test"
	svc, err := NewIngestService(IngestServiceConfig{
		IngestionMode:          IngestionModeLive,
		Models:                 models,
		OldestLedgerCursorName: "oldest_ledger_cursor",
		RPCService:             &RPCServiceMock{},
		LedgerBackend:          mockBackend,
		CheckpointService:      checkpointMock,
		Metrics:                m,
		Network:                testNetwork,
		NetworkPassphrase:      network.TestNetworkPassphrase,
		Archive:                &HistoryArchiveMock{},
	})
	require.NoError(t, err)

	err = svc.Run(ctx, 0, 0)
	require.Error(t, err, "run should surface the context cancellation")

	// Verify the lock was released using a second, independent pool: Postgres
	// advisory locks are reentrant within the same session, so checking with
	// the same connection that held the lock would pass even if the release
	// silently failed.
	verifyCtx := context.Background()
	pool2, err := db.OpenDBConnectionPool(verifyCtx, dbt.DSN)
	require.NoError(t, err)
	defer pool2.Close()

	acquired, err := db.AcquireAdvisoryLock(verifyCtx, pool2, generateAdvisoryLockID(testNetwork))
	require.NoError(t, err)
	assert.True(t, acquired, "advisory lock should have been released during shutdown despite the cancelled context")
}

// Test_isPermanentPersistError covers ING-06's classifier: SQLSTATE class 22/23/42 (and the
// ErrCursorGuardFailed / ErrCASCursorMissing cursor sentinels) must fail an ingestion attempt
// immediately instead of burning the full 5-attempt retry ladder; every other error (including
// no PgError at all) must still retry, same as before this classifier existed.
func Test_isPermanentPersistError(t *testing.T) {
	pgErrWithCode := func(code string) error {
		return &pgconn.PgError{Code: code, Message: "boom"}
	}

	testCases := []struct {
		name          string
		err           error
		wantPermanent bool
	}{
		{name: "nil_error", err: nil, wantPermanent: false},
		{name: "plain_error_no_pgerror", err: errors.New("db connection failed"), wantPermanent: false},
		{name: "data_exception_22001_string_data_right_truncation", err: pgErrWithCode("22001"), wantPermanent: true},
		{name: "integrity_constraint_23505_unique_violation", err: pgErrWithCode("23505"), wantPermanent: true},
		{name: "integrity_constraint_23503_foreign_key_violation", err: pgErrWithCode("23503"), wantPermanent: true},
		{name: "syntax_or_access_rule_42501_insufficient_privilege", err: pgErrWithCode("42501"), wantPermanent: true},
		{name: "syntax_or_access_rule_42P01_undefined_table", err: pgErrWithCode("42P01"), wantPermanent: true},
		{name: "serialization_failure_40001_is_transient", err: pgErrWithCode("40001"), wantPermanent: false},
		{name: "deadlock_detected_40P01_is_transient", err: pgErrWithCode("40P01"), wantPermanent: false},
		{name: "connection_exception_08006_is_transient", err: pgErrWithCode("08006"), wantPermanent: false},
		{name: "admin_shutdown_57P01_is_transient_cnpg_failover", err: pgErrWithCode("57P01"), wantPermanent: false},
		{name: "crash_shutdown_57P02_is_transient_cnpg_failover", err: pgErrWithCode("57P02"), wantPermanent: false},
		{name: "cannot_connect_now_57P03_is_transient_cnpg_failover", err: pgErrWithCode("57P03"), wantPermanent: false},
		{name: "unknown_sqlstate_defaults_to_transient", err: pgErrWithCode("99999"), wantPermanent: false},
		{
			name:          "wrapped_pgerror_still_classified_via_errors_As",
			err:           fmt.Errorf("persisting ledger data for ledger 100: running atomic function in RunInTransaction: %w", pgErrWithCode("23505")),
			wantPermanent: true,
		},
		{name: "cursor_guard_failed_is_permanent", err: data.ErrCursorGuardFailed, wantPermanent: true},
		{
			name:          "wrapped_cursor_guard_failed_is_permanent",
			err:           fmt.Errorf("updating cursor for ledger 100: %w", data.ErrCursorGuardFailed),
			wantPermanent: true,
		},
		{name: "cas_cursor_missing_is_permanent", err: data.ErrCASCursorMissing, wantPermanent: true},
		{
			name:          "wrapped_cas_cursor_missing_is_permanent",
			err:           fmt.Errorf("persisting ledger data for ledger 100: comparing and swapping protocol cursor blend: %w", data.ErrCASCursorMissing),
			wantPermanent: true,
		},
		{name: "partial_persist_is_permanent", err: ErrPartialPersist, wantPermanent: true},
		{
			name:          "wrapped_partial_persist_is_permanent",
			err:           fmt.Errorf("committing operations for ledger 100: %w: connection reset", ErrPartialPersist),
			wantPermanent: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.wantPermanent, isPermanentPersistError(tc.err))
		})
	}
}

// Test_mergedAcrossLedgers pins the cross-ledger merge semantics the balance
// siblings rely on: later ledgers win by POSITION in the batch, never by
// comparing order values — AccountChange.SortKey is a within-ledger rank, so
// an earlier ledger may legally carry a higher SortKey than a later one.
func Test_mergedAcrossLedgers(t *testing.T) {
	item := func(seq uint32, changes ...types.AccountChange) persistItem {
		buffer := indexer.NewIndexerBuffer()
		for _, c := range changes {
			buffer.PushAccountChange(c)
		}
		return persistItem{seq: seq, buffer: buffer}
	}
	get := (*indexer.IndexerBuffer).GetAccountChanges

	t.Run("later ledger overwrites earlier", func(t *testing.T) {
		merged := mergedAcrossLedgers([]persistItem{
			item(100, types.AccountChange{AccountID: testAddr1, SortKey: 1, LedgerNumber: 100, Operation: types.AccountOpUpdate, Balance: 10}),
			item(101, types.AccountChange{AccountID: testAddr1, SortKey: 1, LedgerNumber: 101, Operation: types.AccountOpUpdate, Balance: 20}),
		}, get)
		require.Len(t, merged, 1)
		assert.Equal(t, int64(20), merged[testAddr1].Balance)
	})

	t.Run("later ledger wins even against a higher earlier SortKey", func(t *testing.T) {
		// Legal: SortKeys rank within one ledger only. Ledger 100's final
		// change ranked high in ITS ledger; ledger 101's ranked low in its
		// own. Position in the batch must decide, not the SortKey.
		merged := mergedAcrossLedgers([]persistItem{
			item(100, types.AccountChange{AccountID: testAddr1, SortKey: 1 << 40, LedgerNumber: 100, Operation: types.AccountOpUpdate, Balance: 10}),
			item(101, types.AccountChange{AccountID: testAddr1, SortKey: 1, LedgerNumber: 101, Operation: types.AccountOpUpdate, Balance: 20}),
		}, get)
		require.Len(t, merged, 1)
		assert.Equal(t, int64(20), merged[testAddr1].Balance)
	})

	t.Run("remove then recreate nets to the recreate", func(t *testing.T) {
		merged := mergedAcrossLedgers([]persistItem{
			item(100, types.AccountChange{AccountID: testAddr1, SortKey: 1, LedgerNumber: 100, Operation: types.AccountOpRemove}),
			item(101, types.AccountChange{AccountID: testAddr1, SortKey: 1, LedgerNumber: 101, Operation: types.AccountOpCreate, Balance: 5}),
		}, get)
		require.Len(t, merged, 1)
		assert.Equal(t, types.AccountOpCreate, merged[testAddr1].Operation)
		assert.Equal(t, int64(5), merged[testAddr1].Balance)
	})

	t.Run("create then remove keeps the remove so its delete executes", func(t *testing.T) {
		merged := mergedAcrossLedgers([]persistItem{
			item(100, types.AccountChange{AccountID: testAddr1, SortKey: 1, LedgerNumber: 100, Operation: types.AccountOpCreate, Balance: 5}),
			item(101, types.AccountChange{AccountID: testAddr1, SortKey: 1, LedgerNumber: 101, Operation: types.AccountOpRemove}),
		}, get)
		require.Len(t, merged, 1)
		assert.Equal(t, types.AccountOpRemove, merged[testAddr1].Operation)
	})

	t.Run("key touched only by an earlier ledger survives", func(t *testing.T) {
		merged := mergedAcrossLedgers([]persistItem{
			item(100, types.AccountChange{AccountID: testAddr1, SortKey: 1, LedgerNumber: 100, Operation: types.AccountOpUpdate, Balance: 10}),
			item(101, types.AccountChange{AccountID: testAddr2, SortKey: 1, LedgerNumber: 101, Operation: types.AccountOpUpdate, Balance: 20}),
		}, get)
		require.Len(t, merged, 2)
		assert.Equal(t, int64(10), merged[testAddr1].Balance)
		assert.Equal(t, int64(20), merged[testAddr2].Balance)
	})

	t.Run("single item passes the live map through uncopied", func(t *testing.T) {
		it := item(100, types.AccountChange{AccountID: testAddr1, SortKey: 1, LedgerNumber: 100, Operation: types.AccountOpUpdate, Balance: 10})
		merged := mergedAcrossLedgers([]persistItem{it}, get)
		assert.Equal(t, reflect.ValueOf(it.buffer.GetAccountChanges()).Pointer(), reflect.ValueOf(merged).Pointer())
	})
}

func Test_mergedUniqueTrustlineAssets(t *testing.T) {
	item := func(seq uint32, assets ...string) persistItem {
		buffer := indexer.NewIndexerBuffer()
		for i, asset := range assets {
			buffer.PushTrustlineChange(types.TrustlineChange{
				AccountID:   testAddr1,
				Asset:       asset,
				OperationID: int64(seq)<<32 + int64(i),
				Operation:   types.TrustlineOpUpdate,
			})
		}
		return persistItem{seq: seq, buffer: buffer}
	}

	usdc := "USDC:" + testAddr1
	eurc := "EURC:" + testAddr2

	t.Run("same asset in two ledgers dedupes to one entry", func(t *testing.T) {
		assets := mergedUniqueTrustlineAssets([]persistItem{item(100, usdc), item(101, usdc)})
		require.Len(t, assets, 1)
		assert.Equal(t, "USDC", assets[0].Code)
	})

	t.Run("disjoint assets union", func(t *testing.T) {
		assets := mergedUniqueTrustlineAssets([]persistItem{item(100, usdc), item(101, eurc)})
		codes := make([]string, 0, len(assets))
		for _, a := range assets {
			codes = append(codes, a.Code)
		}
		assert.ElementsMatch(t, []string{"USDC", "EURC"}, codes)
	})

	t.Run("single item passes the buffer's slice through uncopied", func(t *testing.T) {
		it := item(100, usdc)
		got := mergedUniqueTrustlineAssets([]persistItem{it})
		require.Len(t, got, 1)
		fresh := it.buffer.GetUniqueTrustlineAssets()
		require.Len(t, fresh, 1)
		assert.Equal(t, fresh[0].ID, got[0].ID)
	})
}

// Test_persistLedgerData_CoalescesBalanceUpsertsAcrossBatch pins that the
// balance siblings upsert once per batch over the ledgers' merged final
// state: for a two-ledger batch touching the same account, the token service
// sees exactly one call carrying the later ledger's value.
func Test_persistLedgerData_CoalescesBalanceUpsertsAcrossBatch(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	pool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer pool.Close()

	m := metrics.NewMetrics(prometheus.NewRegistry())
	models, err := data.NewModels(pool, m.DB)
	require.NoError(t, err)

	var nativeCalls int
	var mergedArg map[string]types.AccountChange
	mockTokenIngestionService := NewTokenIngestionServiceMock(t)
	mockTokenIngestionService.On("ProcessNativeAndPoolChanges",
		mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
	).Run(func(args mock.Arguments) {
		nativeCalls++
		var ok bool
		mergedArg, ok = args.Get(2).(map[string]types.AccountChange)
		require.True(t, ok)
	}).Return(nil).Once()
	mockTokenIngestionService.On("ProcessTrustlineChanges",
		mock.Anything, mock.Anything, mock.Anything,
	).Return(nil).Once()
	mockTokenIngestionService.On("ProcessSACBalanceChanges",
		mock.Anything, mock.Anything, mock.Anything,
	).Return(nil).Maybe()

	svc, err := NewIngestService(IngestServiceConfig{
		IngestionMode:          IngestionModeLive,
		Models:                 models,
		OldestLedgerCursorName: "oldest_ledger_cursor",
		RPCService:             &RPCServiceMock{},
		LedgerBackend:          &LedgerBackendMock{},
		TokenIngestionService:  mockTokenIngestionService,
		Metrics:                m,
		Network:                network.TestNetworkPassphrase,
		NetworkPassphrase:      network.TestNetworkPassphrase,
		Archive:                &HistoryArchiveMock{},
	})
	require.NoError(t, err)

	setupDBCursors(t, ctx, pool, 99, 99)

	item := func(seq uint32, balance int64) persistItem {
		buffer := indexer.NewIndexerBuffer()
		buffer.PushAccountChange(types.AccountChange{
			AccountID:    testAddr1,
			SortKey:      1,
			LedgerNumber: seq,
			Operation:    types.AccountOpUpdate,
			Balance:      balance,
		})
		return persistItem{seq: seq, buffer: buffer}
	}

	err = svc.persistLedgerData(ctx, []persistItem{item(100, 10), item(101, 20)})
	require.NoError(t, err)

	assert.Equal(t, 1, nativeCalls, "one native/pool upsert per batch, not per ledger")
	require.Contains(t, mergedArg, testAddr1)
	assert.Equal(t, int64(20), mergedArg[testAddr1].Balance, "the merged map carries the later ledger's final state")
}
