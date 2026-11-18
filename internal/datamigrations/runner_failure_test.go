package datamigrations

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stellar/go-stellar-sdk/toid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
)

// chunkName returns the schema-qualified name of the transactions chunk holding closedAt.
func (f *runnerFixture) chunkName(t *testing.T, ctx context.Context, closedAt time.Time) string {
	t.Helper()
	var name string
	err := f.pool.QueryRow(ctx, `
		SELECT chunk_schema || '.' || chunk_name FROM timescaledb_information.chunks
		WHERE hypertable_name = 'transactions' AND range_start <= $1 AND range_end > $1`, closedAt).Scan(&name)
	require.NoError(t, err)
	return name
}

// compressDay2 compresses the day-2 chunk too, so the walk passes a compressed chunk before
// it reaches the oldest one and hands it to the recompressor mid-walk.
func (f *runnerFixture) compressDay2(t *testing.T, ctx context.Context) {
	t.Helper()
	_, err := f.pool.Exec(ctx, `SELECT compress_chunk($1::regclass)`, f.chunkName(t, ctx, f.day2))
	require.NoError(t, err)
	require.True(t, f.chunkCompressed(t, ctx, f.day2))
}

// holdLock opens a transaction that runs stmt and keeps it open until the returned
// function (also registered as cleanup) rolls it back.
func (f *runnerFixture) holdLock(t *testing.T, ctx context.Context, stmt string) (release func()) {
	t.Helper()
	tx, err := f.pool.Begin(ctx)
	require.NoError(t, err)
	_, err = tx.Exec(ctx, stmt)
	require.NoError(t, err)
	released := false
	release = func() {
		if !released {
			released = true
			assert.NoError(t, tx.Rollback(ctx))
		}
	}
	t.Cleanup(release)
	return release
}

// lockWaiter waits until a session of the fixture's database is blocked on a lock while
// running a query that matches pattern (a LIKE pattern) and returns its pid, or 0 when
// none shows up. compress_chunk retries its lock in a sleep loop (wait event
// Timeout/VacuumTruncate) rather than queueing on it. It is safe to call off the test
// goroutine.
func (f *runnerFixture) lockWaiter(t *testing.T, ctx context.Context, pattern string) int32 {
	var pid atomic.Int32
	assert.Eventually(t, func() bool {
		var found int32
		err := f.pool.QueryRow(ctx, `
			SELECT pid FROM pg_stat_activity
			WHERE datname = current_database() AND pid <> pg_backend_pid()
				AND wait_event_type IN ('Lock', 'Timeout') AND query LIKE $1`, pattern).Scan(&found)
		pid.Store(found)
		return err == nil
	}, 10*time.Second, 5*time.Millisecond, "no session waits on a lock running %q", pattern)
	return pid.Load()
}

// cancelLockWaiter cancels the query lockWaiter finds and reports whether it did. It is
// safe to call off the test goroutine.
func (f *runnerFixture) cancelLockWaiter(t *testing.T, ctx context.Context, pattern string) bool {
	pid := f.lockWaiter(t, ctx, pattern)
	if pid == 0 {
		return false
	}
	var cancelled bool
	if !assert.NoError(t, f.pool.QueryRow(ctx, `SELECT pg_cancel_backend($1)`, pid).Scan(&cancelled)) {
		return false
	}
	return assert.True(t, cancelled, "pg_cancel_backend(%d)", pid)
}

const (
	compressQuery = "%compress_chunk(c, if_not_compressed => true)%"
	reindexQuery  = "REINDEX TABLE %"
)

func TestRunner_UnservableOldestLedgerRecordsErrorAndDoesNotStart(t *testing.T) {
	ctx := context.Background()
	withRunnerSizes(t, 4, 4)
	f := newRunnerFixture(t, ctx)

	// The backend cannot serve the oldest ingested ledger.
	missing := f.ledgers[f.oldest]
	delete(f.ledgers, f.oldest)
	err := f.runner.Run(ctx, memoMigration)
	require.ErrorContains(t, err, "cannot serve the oldest ingested ledger 100")
	require.ErrorContains(t, err, "ledger 100 is not served")

	row := f.row(t, ctx)
	assert.Equal(t, data.DataMigrationPending, row.Status, "the run never starts")
	assert.Nil(t, row.StartedAt)
	assert.Equal(t, f.latest+1, row.CursorLedger)
	require.NotNil(t, row.LastError)
	assert.Equal(t, err.Error(), *row.LastError)
	assert.Equal(t, 1, f.factoryCalls, "only the preflight read")
	for toID, got := range f.memos(t, ctx) {
		assert.Equal(t, int16(-1), got.MemoType, "row %d is not rewritten", toID)
	}

	// Once the ledger is served, the next run finishes and clears the error.
	f.ledgers[f.oldest] = missing
	require.NoError(t, f.runner.Run(ctx, memoMigration))
	row = f.row(t, ctx)
	assert.Equal(t, data.DataMigrationDone, row.Status)
	assert.Nil(t, row.LastError)
	assert.Equal(t, f.want, f.memos(t, ctx))
}

func TestRunner_StopsAtAFloorRaisedMidWalk(t *testing.T) {
	ctx := context.Background()
	withRunnerSizes(t, 4, 4)
	f := newRunnerFixture(t, ctx)

	// Retention raises the oldest ingested ledger to 102 while the first slice [108, 111]
	// is read (factory call 2, after the preflight read of ledger 100).
	const raisedFloor uint32 = 102
	f.onFactory = func(calls int) {
		if calls == 2 {
			_, err := f.pool.Exec(ctx, `UPDATE ingest_store SET value = $2 WHERE key = $1`, data.OldestLedgerCursorName, "102")
			assert.NoError(t, err)
		}
	}
	require.NoError(t, f.runner.Run(ctx, memoMigration))

	row := f.row(t, ctx)
	assert.Equal(t, data.DataMigrationDone, row.Status)
	assert.Equal(t, raisedFloor, row.CursorLedger, "the walk ends at the raised floor")
	assert.Nil(t, row.LastError)
	assert.Equal(t, 4, f.factoryCalls, "one preflight read, then slices [108, 111], [104, 107] and [102, 103]")
	got := f.memos(t, ctx)
	for toID, want := range f.want {
		seq := uint32(toid.Parse(toID).LedgerSequence)
		if seq < raisedFloor {
			assert.Equal(t, int16(-1), got[toID].MemoType, "ledger %d below the floor is not touched", seq)
		} else {
			assert.Equal(t, want, got[toID], "ledger %d at or above the floor is filled", seq)
		}
	}
	assert.True(t, f.chunkCompressed(t, ctx, f.day1), "the rewritten compressed chunk is compressed again")
}

// A run cancelled while a chunk the walk has passed is being recompressed leaves that
// chunk partly decompressed; the next run starts with every chunk above its cursor and
// recompresses it.
func TestRunner_ResumeRecompressesChunksACancelledRunPassed(t *testing.T) {
	ctx := context.Background()
	withRunnerSizes(t, 4, 4)
	f := newRunnerFixture(t, ctx)
	f.compressDay2(t, ctx)

	// A ROW SHARE lock lets the walk update the day-2 chunk but holds its recompression.
	release := f.holdLock(t, ctx, `LOCK TABLE `+f.chunkName(t, ctx, f.day2)+` IN ROW SHARE MODE`)

	// The walk hands the day-2 chunk over after slice [104, 107]. Cancel once its
	// recompression waits on the lock, as slice [100, 103] starts (factory call 4).
	runCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	f.onFactory = func(calls int) {
		if calls == 4 {
			assert.NotZero(t, f.lockWaiter(t, ctx, compressQuery), "the day-2 chunk is being recompressed")
			cancel()
		}
	}
	err := f.runner.Run(runCtx, memoMigration)
	require.ErrorIs(t, err, context.Canceled)

	row := f.row(t, ctx)
	assert.Equal(t, data.DataMigrationInProgress, row.Status)
	assert.Equal(t, uint32(104), row.CursorLedger, "slices [108, 111] and [104, 107] committed")
	assert.False(t, f.chunkCompressed(t, ctx, f.day2), "the passed chunk is left partly decompressed")

	release()
	f.onFactory = nil
	require.NoError(t, f.runner.Run(ctx, memoMigration))
	assert.Equal(t, f.want, f.memos(t, ctx))
	row = f.row(t, ctx)
	assert.Equal(t, data.DataMigrationDone, row.Status)
	assert.Equal(t, f.oldest, row.CursorLedger)
	assert.True(t, f.chunkCompressed(t, ctx, f.day2), "the chunk the cancelled run passed is recompressed")
	assert.True(t, f.chunkCompressed(t, ctx, f.day1), "the oldest chunk is recompressed")
}

// A compressed chunk the walk passes without writing to it stays fully compressed, so it
// is neither recompressed nor reindexed.
func TestRunner_SkipsChunksTheWalkDidNotWrite(t *testing.T) {
	ctx := context.Background()
	testCases := []struct {
		name string
		// setup makes the walk write only one of the two compressed chunks.
		setup func(t *testing.T, f *runnerFixture)
		// written and untouched are the close times of the chunk the walk writes and the
		// chunk it does not.
		written, untouched func(f *runnerFixture) time.Time
	}{
		{
			name: "chunk_at_or_above_the_boundary",
			setup: func(t *testing.T, f *runnerFixture) {
				// The day-2 chunk holds ledgers [106, 111], all at or above the boundary.
				_, err := f.pool.Exec(ctx, `UPDATE data_migrations SET boundary_ledger = 106, cursor_ledger = 106 WHERE name = $1`, memoMigration)
				require.NoError(t, err)
			},
			written:   func(f *runnerFixture) time.Time { return f.day1 },
			untouched: func(f *runnerFixture) time.Time { return f.day2 },
		},
		{
			name: "chunk_below_the_floor",
			setup: func(t *testing.T, f *runnerFixture) {
				// The day-1 chunk holds ledgers [100, 105], all below the floor.
				_, err := f.pool.Exec(ctx, `UPDATE ingest_store SET value = '106' WHERE key = $1`, data.OldestLedgerCursorName)
				require.NoError(t, err)
			},
			written:   func(f *runnerFixture) time.Time { return f.day2 },
			untouched: func(f *runnerFixture) time.Time { return f.day1 },
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			withRunnerSizes(t, 4, 4)
			f := newRunnerFixture(t, ctx)
			f.compressDay2(t, ctx)
			tc.setup(t, f)
			writtenBefore := f.indexFiles(t, ctx, tc.written(f))
			untouchedBefore := f.indexFiles(t, ctx, tc.untouched(f))

			require.NoError(t, f.runner.Run(ctx, memoMigration))

			assert.Equal(t, data.DataMigrationDone, f.row(t, ctx).Status)
			assert.True(t, f.chunkCompressed(t, ctx, tc.written(f)), "the written chunk is recompressed")
			assert.NotEqual(t, writtenBefore, f.indexFiles(t, ctx, tc.written(f)), "the written chunk's indexes are rebuilt")
			assert.True(t, f.chunkCompressed(t, ctx, tc.untouched(f)))
			assert.Equal(t, untouchedBefore, f.indexFiles(t, ctx, tc.untouched(f)), "the untouched chunk's indexes are not rebuilt")
		})
	}
}

// A run that stops after it rebuilt a chunk's indexes leaves that chunk fully compressed,
// so the next run does not rebuild them again. The chunk whose rebuild was still waiting
// keeps its grown indexes.
func TestRunner_ResumeDoesNotRebuildIndexesAgain(t *testing.T) {
	ctx := context.Background()
	withRunnerSizes(t, 4, 4)
	reindexWait = time.Hour
	f := newRunnerFixture(t, ctx)
	f.compressDay2(t, ctx)
	day1, day2 := f.chunkName(t, ctx, f.day1), f.chunkName(t, ctx, f.day2)
	day2Before := f.indexFiles(t, ctx, f.day2)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- f.runner.Run(runCtx, memoMigration) }()

	// The day-2 chunk is recompressed mid-walk and its indexes are rebuilt once the day-1
	// chunk is recompressed. The day-1 rebuild then waits.
	require.Eventually(t, func() bool {
		files, err := f.queryIndexFiles(ctx, day2)
		if err != nil || assert.ObjectsAreEqual(day2Before, files) {
			return false
		}
		var recompressed bool
		err = f.pool.QueryRow(ctx, `SELECT (s & 1) <> 0 AND (s & 8) = 0 FROM _timescaledb_functions.chunk_status($1::regclass) s`, day1).Scan(&recompressed)
		return err == nil && recompressed
	}, 10*time.Second, 5*time.Millisecond, "the day-2 indexes are rebuilt and the day-1 chunk is recompressed")
	cancel()
	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(10 * time.Second):
		require.FailNow(t, "Run did not return after cancel")
	}
	require.Equal(t, data.DataMigrationInProgress, f.row(t, ctx).Status)
	day1Files, day2Files := f.indexFiles(t, ctx, f.day1), f.indexFiles(t, ctx, f.day2)

	reindexWait = 0
	require.NoError(t, f.runner.Run(ctx, memoMigration))
	assert.Equal(t, data.DataMigrationDone, f.row(t, ctx).Status)
	assert.Equal(t, f.want, f.memos(t, ctx))
	assert.Equal(t, day2Files, f.indexFiles(t, ctx, f.day2), "the rebuilt day-2 indexes are not rebuilt again")
	assert.Equal(t, day1Files, f.indexFiles(t, ctx, f.day1), "the day-1 chunk is not recompressed again, so its indexes are not rebuilt")
	assert.True(t, f.chunkCompressed(t, ctx, f.day1))
	assert.True(t, f.chunkCompressed(t, ctx, f.day2))
}

func TestRunner_RecompressionFailureStopsTheRun(t *testing.T) {
	ctx := context.Background()
	withRunnerSizes(t, 4, 4)
	f := newRunnerFixture(t, ctx)
	f.compressDay2(t, ctx)

	// The day-2 chunk's recompression waits on this lock until the test cancels it.
	release := f.holdLock(t, ctx, `LOCK TABLE `+f.chunkName(t, ctx, f.day2)+` IN ROW SHARE MODE`)
	runCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	cancelled := make(chan bool, 1)
	go func() { cancelled <- f.cancelLockWaiter(t, ctx, compressQuery) }()

	err := f.runner.Run(runCtx, memoMigration)
	require.True(t, <-cancelled)
	require.ErrorContains(t, err, "recompressing the remaining chunks")
	require.ErrorContains(t, err, "canceling statement due to user request")

	row := f.row(t, ctx)
	assert.Equal(t, data.DataMigrationInProgress, row.Status)
	assert.Equal(t, f.oldest, row.CursorLedger, "the walk finishes before the failure is seen")
	require.NotNil(t, row.LastError)
	assert.Equal(t, err.Error(), *row.LastError)
	assert.False(t, f.chunkCompressed(t, ctx, f.day2), "the chunk is left partly decompressed")

	release()
	require.NoError(t, f.runner.Run(ctx, memoMigration))
	assert.Equal(t, data.DataMigrationDone, f.row(t, ctx).Status)
	assert.True(t, f.chunkCompressed(t, ctx, f.day2))
	assert.True(t, f.chunkCompressed(t, ctx, f.day1))
}

// Every failure after the migration's row is loaded is recorded as the row's last error.
func TestRunner_RecordsWhyARunStopped(t *testing.T) {
	ctx := context.Background()
	testCases := []struct {
		name string
		// setup makes the run fail after it has started.
		setup   func(t *testing.T, f *runnerFixture)
		wantErr string
		// wantCursor is where the cursor stops.
		wantCursor func(f *runnerFixture) uint32
	}{
		{
			name: "oldest_ledger_cannot_be_read_again",
			setup: func(t *testing.T, f *runnerFixture) {
				// The oldest ledger becomes unreadable while the first slice [108, 111] is
				// read (factory call 2, after the preflight read).
				f.onFactory = func(calls int) {
					if calls == 2 {
						_, err := f.pool.Exec(ctx, `UPDATE ingest_store SET value = 'not a ledger' WHERE key = $1`, data.OldestLedgerCursorName)
						assert.NoError(t, err)
					}
				}
			},
			wantErr:    "reading oldest ingested ledger",
			wantCursor: func(*runnerFixture) uint32 { return 108 },
		},
		{
			name: "completion_cannot_be_recorded",
			setup: func(t *testing.T, f *runnerFixture) {
				_, err := f.pool.Exec(ctx, `
					CREATE FUNCTION refuse_done() RETURNS trigger LANGUAGE plpgsql AS $$
					BEGIN RAISE EXCEPTION 'refusing done'; END $$;
					CREATE TRIGGER refuse_done BEFORE UPDATE ON data_migrations
					FOR EACH ROW WHEN (NEW.status = 'done') EXECUTE FUNCTION refuse_done();`)
				require.NoError(t, err)
			},
			wantErr:    "recording completion",
			wantCursor: func(f *runnerFixture) uint32 { return f.oldest },
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			withRunnerSizes(t, 4, 4)
			f := newRunnerFixture(t, ctx)
			tc.setup(t, f)

			err := f.runner.Run(ctx, memoMigration)
			require.ErrorContains(t, err, tc.wantErr)

			row := f.row(t, ctx)
			assert.Equal(t, data.DataMigrationInProgress, row.Status)
			assert.Equal(t, tc.wantCursor(f), row.CursorLedger)
			require.NotNil(t, row.LastError)
			assert.Equal(t, err.Error(), *row.LastError)
		})
	}
}

func TestRunner_FailedIndexRebuildIsSkipped(t *testing.T) {
	ctx := context.Background()
	withRunnerSizes(t, 4, 4)
	f := newRunnerFixture(t, ctx)

	// A reader of the day-1 chunk holds its indexes, which recompression leaves alone and
	// REINDEX must lock exclusively. The test cancels the waiting rebuild instead of letting
	// it reach its lock timeout.
	f.holdLock(t, ctx, `SELECT count(*) FROM `+f.chunkName(t, ctx, f.day1))
	runCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	cancelled := make(chan bool, 1)
	go func() { cancelled <- f.cancelLockWaiter(t, ctx, reindexQuery) }()

	require.NoError(t, f.runner.Run(runCtx, memoMigration))
	require.True(t, <-cancelled, "the index rebuild ran and failed")

	row := f.row(t, ctx)
	assert.Equal(t, data.DataMigrationDone, row.Status)
	assert.Nil(t, row.LastError)
	assert.Equal(t, f.want, f.memos(t, ctx))
	assert.True(t, f.chunkCompressed(t, ctx, f.day1))
}

func TestRunner_CancelWhileWaitingToRebuildIndexesIsNotDone(t *testing.T) {
	ctx := context.Background()
	withRunnerSizes(t, 4, 4)
	reindexWait = time.Hour
	f := newRunnerFixture(t, ctx)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- f.runner.Run(runCtx, memoMigration) }()

	// The last chunk is recompressed after the walk; its index rebuild then waits.
	day1 := f.chunkName(t, ctx, f.day1)
	require.Eventually(t, func() bool {
		var recompressed bool
		err := f.pool.QueryRow(ctx, `
			SELECT m.cursor_ledger = $2 AND (s.status & 1) <> 0 AND (s.status & 8) = 0
			FROM data_migrations m, (SELECT _timescaledb_functions.chunk_status($1::regclass) AS status) s
			WHERE m.name = $3`, day1, int64(f.oldest), memoMigration).Scan(&recompressed)
		return err == nil && recompressed
	}, 10*time.Second, 5*time.Millisecond, "the walk finishes and the last chunk is recompressed")
	cancel()
	var err error
	select {
	case err = <-errCh:
	case <-time.After(10 * time.Second):
		require.FailNow(t, "Run did not return after cancel")
	}
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorContains(t, err, "recompressing the remaining chunks")

	row := f.row(t, ctx)
	assert.Equal(t, data.DataMigrationInProgress, row.Status)
	assert.Equal(t, f.oldest, row.CursorLedger)
	assert.Nil(t, row.LastError)
}
