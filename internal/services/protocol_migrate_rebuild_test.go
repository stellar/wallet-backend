package services

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stellar/go-stellar-sdk/toid"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/utils"
)

// testCursorObservingProcessor embeds testCursorAdvancingProcessor and records
// the migration cursor value visible when the fold's first ledger is
// processed — the value the wipe transaction committed.
type testCursorObservingProcessor struct {
	testCursorAdvancingProcessor
	cursorAtFirstFold uint32
	observed          bool
}

func (p *testCursorObservingProcessor) ProcessLedger(ctx context.Context, input ProtocolProcessorInput) error {
	if !p.observed {
		p.observed = true
		var s string
		if err := p.dbPool.QueryRow(ctx,
			`SELECT value FROM ingest_store WHERE key = $1`, p.cursorNameFunc(p.id)).Scan(&s); err != nil {
			return fmt.Errorf("reading cursor for test: %w", err)
		}
		v, err := strconv.ParseUint(s, 10, 32)
		if err != nil {
			return fmt.Errorf("parsing cursor for test: %w", err)
		}
		p.cursorAtFirstFold = uint32(v)
	}
	return p.testCursorAdvancingProcessor.ProcessLedger(ctx, input)
}

// testWipeFailingProcessor embeds testRecordingProcessor and fails the wipe, so
// the cursor reset sharing its transaction must roll back.
type testWipeFailingProcessor struct {
	testRecordingProcessor
}

func (p *testWipeFailingProcessor) WipeCurrentState(_ context.Context, _ pgx.Tx) error {
	p.wipeCalls++
	return fmt.Errorf("simulated wipe failure")
}

// seedStateChange inserts one state-change row in the given ordinal namespace at
// the given ledger. state_changes has no foreign key to transactions (hypertable
// FKs are unsupported), so a row stands alone.
func seedStateChange(t *testing.T, ctx context.Context, dbPool *pgxpool.Pool, base int64, ledger uint32) {
	t.Helper()
	_, err := dbPool.Exec(ctx, `
		INSERT INTO state_changes (
			to_id, operation_id, state_change_id, state_change_category, state_change_reason,
			ledger_number, account_id, ledger_created_at
		) VALUES ($1, $2, $3, 'BALANCE', 'CREDIT', $4, $5, $6)`,
		toid.New(int32(ledger), 1, 1).ToInt64(), int64(ledger), base+1, ledger,
		make([]byte, 32), time.Now())
	require.NoError(t, err)
}

// remainingStateChanges returns the (state_change_id, ledger_number) pairs still
// in state_changes, ordered so assertions read deterministically.
func remainingStateChanges(t *testing.T, ctx context.Context, dbPool *pgxpool.Pool) [][2]int64 {
	t.Helper()
	rows, err := dbPool.Query(ctx,
		`SELECT state_change_id, ledger_number FROM state_changes ORDER BY state_change_id, ledger_number`)
	require.NoError(t, err)
	defer rows.Close()

	var out [][2]int64
	for rows.Next() {
		var id, ledger int64
		require.NoError(t, rows.Scan(&id, &ledger))
		out = append(out, [2]int64{id, ledger})
	}
	require.NoError(t, rows.Err())
	return out
}

// TestCurrentStateRebuild pins the --rebuild path of the current-state engine:
// which protocols validate re-admits, that the wipe runs before any folding,
// and that the cursor reset and the row wipe share one transaction.
func TestCurrentStateRebuild(t *testing.T) {
	t.Run("re-admits a succeeded protocol, wipes, and resets the cursor to startLedger-1", func(t *testing.T) {
		ctx := context.Background()
		dbPool, ingestStore := setupTestDB(t)

		setIngestStoreValue(t, ctx, dbPool, "latest_ingest_ledger", 102)
		// A completed migration left the cursor far ahead; the rebuild must reset it.
		setIngestStoreValue(t, ctx, dbPool, utils.ProtocolCurrentStateCursorName("testproto"), 500)

		_, err := dbPool.Exec(ctx, `INSERT INTO protocols (id, classification_status) VALUES ('testproto', 'success') ON CONFLICT (id) DO UPDATE SET classification_status = 'success'`)
		require.NoError(t, err)

		protocolsModel := data.NewProtocolsModelMock(t)
		protocolContractsModel := data.NewProtocolContractsModelMock(t)
		processor := &testCursorObservingProcessor{
			testCursorAdvancingProcessor: testCursorAdvancingProcessor{
				testRecordingProcessor: testRecordingProcessor{id: "testproto", ingestStore: ingestStore},
				dbPool:                 dbPool,
				advanceAtSeq:           102,
				cursorNameFunc:         utils.ProtocolCurrentStateCursorName,
			},
		}

		// The rebuild's validate sees the completed migration; after the wipe
		// resets the status, the engine's validate sees not_started and admits.
		protocolsModel.On("GetByIDs", mock.Anything, []string{"testproto"}).Return([]data.Protocols{
			{ID: "testproto", ClassificationStatus: data.StatusSuccess, CurrentStateMigrationStatus: data.StatusSuccess},
		}, nil).Once()
		protocolsModel.On("GetByIDs", mock.Anything, []string{"testproto"}).Return([]data.Protocols{
			{ID: "testproto", ClassificationStatus: data.StatusSuccess, CurrentStateMigrationStatus: data.StatusNotStarted},
		}, nil)
		protocolsModel.On("UpdateCurrentStateMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusNotStarted).Return(nil)
		protocolsModel.On("UpdateCurrentStateMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusInProgress).Return(nil)
		protocolsModel.On("UpdateCurrentStateMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusSuccess).Return(nil)
		protocolContractsModel.On("GetByProtocolID", mock.Anything, mock.Anything, "testproto").Return([]data.ProtocolContracts{}, nil)

		backend := &multiLedgerBackend{ledgers: map[uint32]xdr.LedgerCloseMeta{
			100: dummyLedgerMeta(100), 101: dummyLedgerMeta(101), 102: dummyLedgerMeta(102),
		}}

		svc, err := NewProtocolCurrentStateRebuildService(ProtocolMigrateCurrentStateConfig{
			DB: dbPool, LedgerBackend: backend,
			ProtocolsModel: protocolsModel, ProtocolContractsModel: protocolContractsModel,
			IngestStore: ingestStore, NetworkPassphrase: "Test SDF Network ; September 2015",
			Processors:  []ProtocolProcessor{processor},
			StartLedger: 100,
		})
		require.NoError(t, err)

		require.NoError(t, svc.Run(ctx, []string{"testproto"}))

		// The wipe ran once, before any folding, and left the cursor at startLedger-1.
		assert.Equal(t, 1, processor.wipeCalls)
		assert.Equal(t, uint32(99), processor.cursorAtFirstFold)

		// The fold then re-derived from the start ledger rather than resuming at 500.
		require.Len(t, processor.processedInputs, 3)
		for i, seq := range []uint32{100, 101, 102} {
			assert.Equal(t, seq, processor.processedInputs[i].LedgerSequence)
		}
		// 102's CAS loses to the simulated live takeover, so its window is discarded.
		assert.Equal(t, []uint32{100, 101}, processor.persistedCurrentStateSeqs)
		assert.Equal(t, uint32(202), getIngestStoreValue(t, ctx, dbPool, utils.ProtocolCurrentStateCursorName("testproto")))
	})

	t.Run("refuses a protocol marked in_progress without wiping", func(t *testing.T) {
		ctx := context.Background()
		dbPool, ingestStore := setupTestDB(t)

		protocolsModel := data.NewProtocolsModelMock(t)
		processor := &testRecordingProcessor{id: "testproto", ingestStore: ingestStore}

		protocolsModel.On("GetByIDs", mock.Anything, []string{"testproto"}).Return([]data.Protocols{
			{ID: "testproto", ClassificationStatus: data.StatusSuccess, CurrentStateMigrationStatus: data.StatusInProgress},
		}, nil)

		svc, err := NewProtocolCurrentStateRebuildService(ProtocolMigrateCurrentStateConfig{
			DB: dbPool, LedgerBackend: &multiLedgerBackend{},
			ProtocolsModel: protocolsModel, ProtocolContractsModel: data.NewProtocolContractsModelMock(t),
			IngestStore: ingestStore, NetworkPassphrase: "Test SDF Network ; September 2015",
			Processors:  []ProtocolProcessor{processor},
			StartLedger: 100,
		})
		require.NoError(t, err)

		err = svc.Run(ctx, []string{"testproto"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "in_progress")
		assert.Zero(t, processor.wipeCalls)
		assert.Empty(t, processor.processedInputs)
	})

	t.Run("without rebuild a succeeded protocol is still skipped", func(t *testing.T) {
		ctx := context.Background()
		dbPool, ingestStore := setupTestDB(t)

		protocolsModel := data.NewProtocolsModelMock(t)
		processor := &testRecordingProcessor{id: "testproto", ingestStore: ingestStore}

		protocolsModel.On("GetByIDs", mock.Anything, []string{"testproto"}).Return([]data.Protocols{
			{ID: "testproto", ClassificationStatus: data.StatusSuccess, CurrentStateMigrationStatus: data.StatusSuccess},
		}, nil)

		svc, err := NewProtocolMigrateCurrentStateService(ProtocolMigrateCurrentStateConfig{
			DB: dbPool, LedgerBackend: &multiLedgerBackend{},
			ProtocolsModel: protocolsModel, ProtocolContractsModel: data.NewProtocolContractsModelMock(t),
			IngestStore: ingestStore, NetworkPassphrase: "Test SDF Network ; September 2015",
			Processors:  []ProtocolProcessor{processor},
			StartLedger: 100,
		})
		require.NoError(t, err)

		require.NoError(t, svc.Run(ctx, []string{"testproto"}))
		assert.Zero(t, processor.wipeCalls)
		assert.Empty(t, processor.processedInputs)
	})

	t.Run("a failed wipe rolls back the cursor reset in the same transaction", func(t *testing.T) {
		ctx := context.Background()
		dbPool, ingestStore := setupTestDB(t)

		setIngestStoreValue(t, ctx, dbPool, utils.ProtocolCurrentStateCursorName("testproto"), 500)

		protocolsModel := data.NewProtocolsModelMock(t)
		processor := &testWipeFailingProcessor{
			testRecordingProcessor: testRecordingProcessor{id: "testproto", ingestStore: ingestStore},
		}

		protocolsModel.On("GetByIDs", mock.Anything, []string{"testproto"}).Return([]data.Protocols{
			{ID: "testproto", ClassificationStatus: data.StatusSuccess, CurrentStateMigrationStatus: data.StatusSuccess},
		}, nil)
		protocolsModel.On("UpdateCurrentStateMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusNotStarted).Return(nil)

		svc, err := NewProtocolCurrentStateRebuildService(ProtocolMigrateCurrentStateConfig{
			DB: dbPool, LedgerBackend: &multiLedgerBackend{},
			ProtocolsModel: protocolsModel, ProtocolContractsModel: data.NewProtocolContractsModelMock(t),
			IngestStore: ingestStore, NetworkPassphrase: "Test SDF Network ; September 2015",
			Processors:  []ProtocolProcessor{processor},
			StartLedger: 100,
		})
		require.NoError(t, err)

		err = svc.Run(ctx, []string{"testproto"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "simulated wipe failure")
		assert.Equal(t, 1, processor.wipeCalls)

		// The reset to 99 committed only if the wipe did; it did not.
		assert.Equal(t, uint32(500), getIngestStoreValue(t, ctx, dbPool, utils.ProtocolCurrentStateCursorName("testproto")))
		assert.Empty(t, processor.processedInputs)
	})
}

// TestProtocolHistoryRebuildValidate pins the rebuild's preconditions: any
// settled migration status may be rebuilt, but in_progress is dead-run
// residue to investigate, not state to wipe under.
func TestProtocolHistoryRebuildValidate(t *testing.T) {
	testCases := []struct {
		name                   string
		classificationStatus   string
		historyMigrationStatus string
		wantErrContains        string
	}{
		{
			name:                   "history migration in progress is dead-run residue",
			classificationStatus:   data.StatusSuccess,
			historyMigrationStatus: data.StatusInProgress,
			wantErrContains:        "in_progress",
		},
		{
			name:                   "classification not complete",
			classificationStatus:   data.StatusInProgress,
			historyMigrationStatus: data.StatusSuccess,
			wantErrContains:        "classification not complete",
		},
		{
			name:                   "history migration succeeded",
			classificationStatus:   data.StatusSuccess,
			historyMigrationStatus: data.StatusSuccess,
		},
		{
			name:                   "history migration never ran",
			classificationStatus:   data.StatusSuccess,
			historyMigrationStatus: data.StatusNotStarted,
		},
		{
			name:                   "history migration previously failed",
			classificationStatus:   data.StatusSuccess,
			historyMigrationStatus: data.StatusFailed,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			dbPool, ingestStore := setupTestDB(t)

			protocolsModel := data.NewProtocolsModelMock(t)
			protocolsModel.On("GetByIDs", mock.Anything, []string{"testproto"}).Return([]data.Protocols{
				{
					ID:                     "testproto",
					ClassificationStatus:   tc.classificationStatus,
					HistoryMigrationStatus: tc.historyMigrationStatus,
				},
			}, nil)

			svc, err := NewProtocolHistoryRebuildService(ProtocolHistoryRebuildConfig{
				DB: dbPool, LedgerBackend: &multiLedgerBackend{},
				ProtocolsModel: protocolsModel, ProtocolContractsModel: data.NewProtocolContractsModelMock(t),
				IngestStore: ingestStore, StateChanges: &data.StateChangeModel{DB: dbPool, Metrics: metrics.NewMetrics(prometheus.NewRegistry()).DB},
				NetworkPassphrase: "Test SDF Network ; September 2015",
				Processors:        []ProtocolProcessor{&testRecordingProcessor{id: "testproto", ingestStore: ingestStore}},
			})
			require.NoError(t, err)

			err = svc.validate(ctx, []string{"testproto"})
			if tc.wantErrContains == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErrContains)
		})
	}
}

// TestProtocolHistoryRebuildOldestRetained pins the wipe's lower bound and the
// refusals that must happen before anything is reset: a run that cannot wipe a
// usable window must fail while the cursors are still untouched.
func TestProtocolHistoryRebuildOldestRetained(t *testing.T) {
	ctx := context.Background()
	dbPool, ingestStore := setupTestDB(t)

	testCases := []struct {
		name            string
		oldest          uint32
		latest          uint32
		wantErrContains string
	}{
		{name: "cursors present", oldest: 500, latest: 900},
		{name: "ingestion has not started", oldest: 0, latest: 900, wantErrContains: "oldest_ingest_ledger is 0"},
		{name: "no ledger committed yet", oldest: 500, latest: 0, wantErrContains: "latest_ingest_ledger is 0"},
		{name: "inverted window", oldest: 900, latest: 500, wantErrContains: "inverted window"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			setIngestStoreValue(t, ctx, dbPool, data.OldestLedgerCursorName, tc.oldest)
			setIngestStoreValue(t, ctx, dbPool, data.LatestLedgerCursorName, tc.latest)

			svc, err := NewProtocolHistoryRebuildService(ProtocolHistoryRebuildConfig{
				DB: dbPool, LedgerBackend: &multiLedgerBackend{},
				ProtocolsModel: data.NewProtocolsModelMock(t), ProtocolContractsModel: data.NewProtocolContractsModelMock(t),
				IngestStore: ingestStore, StateChanges: &data.StateChangeModel{DB: dbPool, Metrics: metrics.NewMetrics(prometheus.NewRegistry()).DB},
				NetworkPassphrase: "Test SDF Network ; September 2015",
				Processors:        []ProtocolProcessor{&testRecordingProcessor{id: "testproto", ingestStore: ingestStore}},
			})
			require.NoError(t, err)

			oldest, err := svc.oldestRetained(ctx)
			if tc.wantErrContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrContains)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.oldest, oldest)
		})
	}
}

// TestProtocolHistoryRebuildWipe pins the wipe: the cursor and status reset
// commit before the sliced deletes, the deletes cover exactly the retained
// window in slices, and neighbouring ledgers and other namespaces survive.
func TestProtocolHistoryRebuildWipe(t *testing.T) {
	ctx := context.Background()
	dbPool, ingestStore := setupTestDB(t)

	// A prior migration left the cursor at the frontier; the wipe must reset it.
	setIngestStoreValue(t, ctx, dbPool, utils.ProtocolHistoryCursorName("testproto"), 26_000)

	// [100, 25000] slices into [100,10099], [10100,20099], [20100,25000] — two
	// full slices and a partial tail. Seed both edges of every slice.
	const (
		oldest uint32 = 100
		latest uint32 = 25_000
	)
	// wipe reads the upper bound itself, after its cursor reset commits.
	setIngestStoreValue(t, ctx, dbPool, data.LatestLedgerCursorName, latest)
	inRange := []uint32{100, 10_099, 10_100, 20_099, 20_100, 25_000}
	outOfRange := []uint32{99, 25_001}
	for _, ledger := range append(append([]uint32{}, inRange...), outOfRange...) {
		seedStateChange(t, ctx, dbPool, types.StateChangeOrdinalBaseSEP41, ledger)
	}
	// Indexer-namespace rows inside the ledger range must survive.
	otherNamespace := []uint32{100, 20_100}
	for _, ledger := range otherNamespace {
		seedStateChange(t, ctx, dbPool, types.StateChangeOrdinalBaseIndexer, ledger)
	}
	require.Len(t, remainingStateChanges(t, ctx, dbPool), 10)

	protocolsModel := data.NewProtocolsModelMock(t)
	protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusNotStarted).Return(nil)

	svc, err := NewProtocolHistoryRebuildService(ProtocolHistoryRebuildConfig{
		DB: dbPool, LedgerBackend: &multiLedgerBackend{},
		ProtocolsModel: protocolsModel, ProtocolContractsModel: data.NewProtocolContractsModelMock(t),
		IngestStore: ingestStore, StateChanges: &data.StateChangeModel{DB: dbPool, Metrics: metrics.NewMetrics(prometheus.NewRegistry()).DB},
		NetworkPassphrase: "Test SDF Network ; September 2015",
		Processors:        []ProtocolProcessor{&testRecordingProcessor{id: "testproto", ingestStore: ingestStore}},
	})
	require.NoError(t, err)

	require.NoError(t, svc.wipe(ctx, "testproto", oldest))

	assert.Equal(t, [][2]int64{
		{types.StateChangeOrdinalBaseIndexer + 1, 100},
		{types.StateChangeOrdinalBaseIndexer + 1, 20_100},
		{types.StateChangeOrdinalBaseSEP41 + 1, 99},
		{types.StateChangeOrdinalBaseSEP41 + 1, 25_001},
	}, remainingStateChanges(t, ctx, dbPool))

	// The cursor sits at the retention floor so live's CAS fails until the
	// engine's folds catch up and hand off.
	assert.Equal(t, oldest-1, getIngestStoreValue(t, ctx, dbPool, utils.ProtocolHistoryCursorName("testproto")))
}

// TestProtocolHistoryRebuildRun exercises the whole service: validate, the
// wipe (cursor + status reset, rows deleted), and the re-migration through the
// engine to CAS handoff — mirroring the current-state rebuild.
func TestProtocolHistoryRebuildRun(t *testing.T) {
	ctx := context.Background()
	dbPool, ingestStore := setupTestDB(t)

	setIngestStoreValue(t, ctx, dbPool, data.OldestLedgerCursorName, 200)
	setIngestStoreValue(t, ctx, dbPool, data.LatestLedgerCursorName, 204)
	// A prior migration left the cursor at the frontier; the rebuild must
	// reset it so the fold restarts at the retention floor.
	setIngestStoreValue(t, ctx, dbPool, utils.ProtocolHistoryCursorName("testproto"), 204)

	// Rows the wipe must delete, one retention leftover below the floor that
	// must survive, and an indexer-namespace row inside the window that must
	// survive.
	for _, ledger := range []uint32{200, 202, 204} {
		seedStateChange(t, ctx, dbPool, types.StateChangeOrdinalBaseSEP41, ledger)
	}
	seedStateChange(t, ctx, dbPool, types.StateChangeOrdinalBaseSEP41, 199)
	seedStateChange(t, ctx, dbPool, types.StateChangeOrdinalBaseIndexer, 202)

	protocolsModel := data.NewProtocolsModelMock(t)
	// The rebuild's validate sees the completed migration; after the wipe
	// resets the status, the engine's validate sees not_started and admits.
	protocolsModel.On("GetByIDs", mock.Anything, []string{"testproto"}).Return([]data.Protocols{
		{ID: "testproto", ClassificationStatus: data.StatusSuccess, HistoryMigrationStatus: data.StatusSuccess},
	}, nil).Once()
	protocolsModel.On("GetByIDs", mock.Anything, []string{"testproto"}).Return([]data.Protocols{
		{ID: "testproto", ClassificationStatus: data.StatusSuccess, HistoryMigrationStatus: data.StatusNotStarted},
	}, nil)
	protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusNotStarted).Return(nil)
	protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusInProgress).Return(nil)
	protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusSuccess).Return(nil)
	protocolContractsModel := data.NewProtocolContractsModelMock(t)
	protocolContractsModel.On("GetByProtocolID", mock.Anything, mock.Anything, "testproto").Return([]data.ProtocolContracts{}, nil)

	processor := &testCursorObservingProcessor{
		testCursorAdvancingProcessor: testCursorAdvancingProcessor{
			testRecordingProcessor: testRecordingProcessor{id: "testproto", ingestStore: ingestStore},
			dbPool:                 dbPool,
			advanceAtSeq:           204,
			cursorNameFunc:         utils.ProtocolHistoryCursorName,
		},
	}

	backend := &multiLedgerBackend{ledgers: map[uint32]xdr.LedgerCloseMeta{
		200: dummyLedgerMeta(200), 201: dummyLedgerMeta(201), 202: dummyLedgerMeta(202),
		203: dummyLedgerMeta(203), 204: dummyLedgerMeta(204),
	}}

	svc, err := NewProtocolHistoryRebuildService(ProtocolHistoryRebuildConfig{
		DB: dbPool, LedgerBackend: backend,
		ProtocolsModel: protocolsModel, ProtocolContractsModel: protocolContractsModel,
		IngestStore: ingestStore, StateChanges: &data.StateChangeModel{DB: dbPool, Metrics: metrics.NewMetrics(prometheus.NewRegistry()).DB},
		NetworkPassphrase: "Test SDF Network ; September 2015",
		Processors:        []ProtocolProcessor{processor},
		WindowSize:        2,
	})
	require.NoError(t, err)

	require.NoError(t, svc.Run(ctx, []string{"testproto"}))

	// The wipe removed the protocol's retained-window rows; the retention
	// leftover below the floor and the indexer namespace survive.
	assert.Equal(t, [][2]int64{
		{types.StateChangeOrdinalBaseIndexer + 1, 202},
		{types.StateChangeOrdinalBaseSEP41 + 1, 199},
	}, remainingStateChanges(t, ctx, dbPool))

	// The fold restarted at the retention floor (cursor was 199 at first fold,
	// not the stale 204) and covered the whole window.
	assert.Equal(t, uint32(199), processor.cursorAtFirstFold)
	require.Len(t, processor.processedInputs, 5)
	for i, seq := range []uint32{200, 201, 202, 203, 204} {
		assert.Equal(t, seq, processor.processedInputs[i].LedgerSequence)
	}
	// Windows [200,201] and [202,203] persist; 204's CAS loses to the simulated
	// live takeover, so its window is discarded and the run hands off.
	assert.Equal(t, []uint32{201, 203}, processor.persistedHistorySeqs)
	assert.Equal(t, uint32(304), getIngestStoreValue(t, ctx, dbPool, utils.ProtocolHistoryCursorName("testproto")))
}

// TestProtocolHistoryRebuildRefusesWhileLockHeld pins that a history rebuild
// takes the per-protocol history advisory lock, so two history runs cannot
// write a protocol concurrently.
func TestProtocolHistoryRebuildRefusesWhileLockHeld(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	dbPool, ingestStore := setupTestDB(t)

	protocolsModel := data.NewProtocolsModelMock(t)
	protocolsModel.On("GetByIDs", mock.Anything, []string{"testproto"}).Return([]data.Protocols{
		{ID: "testproto", ClassificationStatus: data.StatusSuccess, HistoryMigrationStatus: data.StatusSuccess},
	}, nil)

	svc, err := NewProtocolHistoryRebuildService(ProtocolHistoryRebuildConfig{
		DB: dbPool, LedgerBackend: &multiLedgerBackend{},
		ProtocolsModel: protocolsModel, ProtocolContractsModel: data.NewProtocolContractsModelMock(t),
		IngestStore: ingestStore, StateChanges: &data.StateChangeModel{DB: dbPool, Metrics: metrics.NewMetrics(prometheus.NewRegistry()).DB},
		NetworkPassphrase: "Test SDF Network ; September 2015",
		Processors:        []ProtocolProcessor{&testRecordingProcessor{id: "testproto", ingestStore: ingestStore}},
	})
	require.NoError(t, err)

	conn, err := dbPool.Acquire(ctx)
	require.NoError(t, err)
	defer conn.Release()
	lockID := migrateAdvisoryLockID(lockScopeHistory, "testproto")
	acquired, err := db.AcquireAdvisoryLock(ctx, conn, lockID)
	require.NoError(t, err)
	require.True(t, acquired)
	defer func() {
		require.NoError(t, db.ReleaseAdvisoryLock(context.Background(), conn, lockID))
	}()

	err = svc.Run(ctx, []string{"testproto"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "is held")
}
