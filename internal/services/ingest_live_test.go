package services

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/apptracker"
	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
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
		AppTracker:             &apptracker.MockAppTracker{},
		RPCService:             &RPCServiceMock{},
		LedgerBackend:          mockBackend,
		CheckpointService:      checkpointMock,
		Metrics:                m,
		GetLedgersLimit:        defaultGetLedgersLimit,
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
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.wantPermanent, isPermanentPersistError(tc.err))
		})
	}
}
