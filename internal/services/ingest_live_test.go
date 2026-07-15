package services

import (
	"context"
	"testing"

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

	const testNetwork = "advisory-lock-release-test"
	svc, err := NewIngestService(IngestServiceConfig{
		IngestionMode:          IngestionModeLive,
		Models:                 models,
		OldestLedgerCursorName: "oldest_ledger_cursor",
		AppTracker:             &apptracker.MockAppTracker{},
		RPCService:             &RPCServiceMock{},
		LedgerBackend:          mockBackend,
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
