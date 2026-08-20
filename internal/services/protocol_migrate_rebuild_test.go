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

// TestProtocolHistoryRebuildValidate pins the precondition that makes the
// protocol's history cursor a safe range cap: the history migration must have
// completed.
func TestProtocolHistoryRebuildValidate(t *testing.T) {
	testCases := []struct {
		name                   string
		classificationStatus   string
		historyMigrationStatus string
		wantErrContains        string
	}{
		{
			name:                   "history migration not started",
			classificationStatus:   data.StatusSuccess,
			historyMigrationStatus: data.StatusNotStarted,
			wantErrContains:        "run protocol-migrate history first",
		},
		{
			name:                   "history migration in progress",
			classificationStatus:   data.StatusSuccess,
			historyMigrationStatus: data.StatusInProgress,
			wantErrContains:        "run protocol-migrate history first",
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

// TestProtocolHistoryRebuildResolveRange pins the clamping of a requested range
// to what exists: the retention floor below and the committed history frontier
// above.
func TestProtocolHistoryRebuildResolveRange(t *testing.T) {
	ctx := context.Background()
	dbPool, ingestStore := setupTestDB(t)

	testCases := []struct {
		name            string
		oldest          uint32
		frontier        uint32
		fromLedger      uint32
		toLedger        uint32
		wantFrom        uint32
		wantTo          uint32
		wantErrContains string
	}{
		{
			name:   "from below the retention floor clamps up",
			oldest: 500, frontier: 900,
			fromLedger: 100, toLedger: 800,
			wantFrom: 500, wantTo: 800,
		},
		{
			name:   "to above the frontier caps down",
			oldest: 500, frontier: 900,
			fromLedger: 600, toLedger: 5000,
			wantFrom: 600, wantTo: 900,
		},
		{
			name:   "to of zero defaults to the frontier",
			oldest: 500, frontier: 900,
			fromLedger: 600, toLedger: 0,
			wantFrom: 600, wantTo: 900,
		},
		{
			name:   "from of zero defaults to the retention floor",
			oldest: 500, frontier: 900,
			fromLedger: 0, toLedger: 0,
			wantFrom: 500, wantTo: 900,
		},
		{
			name:   "an in-range request is left alone",
			oldest: 500, frontier: 900,
			fromLedger: 600, toLedger: 700,
			wantFrom: 600, wantTo: 700,
		},
		{
			name:   "from above the frontier leaves an empty range",
			oldest: 500, frontier: 900,
			fromLedger: 1000, toLedger: 0,
			wantErrContains: "resolved rebuild range is empty",
		},
		{
			name:   "ingestion has not started",
			oldest: 0, frontier: 900,
			fromLedger: 600, toLedger: 700,
			wantErrContains: "ingestion has not started yet",
		},
		{
			name:   "no history cursor despite a completed migration",
			oldest: 500, frontier: 0,
			fromLedger: 600, toLedger: 700,
			wantErrContains: "no history cursor",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			setIngestStoreValue(t, ctx, dbPool, data.OldestLedgerCursorName, tc.oldest)
			setIngestStoreValue(t, ctx, dbPool, utils.ProtocolHistoryCursorName("testproto"), tc.frontier)

			svc, err := NewProtocolHistoryRebuildService(ProtocolHistoryRebuildConfig{
				DB: dbPool, LedgerBackend: &multiLedgerBackend{},
				ProtocolsModel: data.NewProtocolsModelMock(t), ProtocolContractsModel: data.NewProtocolContractsModelMock(t),
				IngestStore: ingestStore, StateChanges: &data.StateChangeModel{DB: dbPool, Metrics: metrics.NewMetrics(prometheus.NewRegistry()).DB},
				NetworkPassphrase: "Test SDF Network ; September 2015",
				Processors:        []ProtocolProcessor{&testRecordingProcessor{id: "testproto", ingestStore: ingestStore}},
				FromLedger:        tc.fromLedger,
				ToLedger:          tc.toLedger,
			})
			require.NoError(t, err)

			from, to, err := svc.resolveRange(ctx, []string{"testproto"})
			if tc.wantErrContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrContains)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantFrom, from)
			assert.Equal(t, tc.wantTo, to)
		})
	}
}

// TestProtocolHistoryRebuildResolveRangeMultiProtocol pins that the cap is the
// lowest frontier across the protocols — the highest ledger every one of them
// has committed.
func TestProtocolHistoryRebuildResolveRangeMultiProtocol(t *testing.T) {
	ctx := context.Background()
	dbPool, ingestStore := setupTestDB(t)

	setIngestStoreValue(t, ctx, dbPool, data.OldestLedgerCursorName, 100)
	setIngestStoreValue(t, ctx, dbPool, utils.ProtocolHistoryCursorName("proto1"), 900)
	setIngestStoreValue(t, ctx, dbPool, utils.ProtocolHistoryCursorName("proto2"), 700)

	svc, err := NewProtocolHistoryRebuildService(ProtocolHistoryRebuildConfig{
		DB: dbPool, LedgerBackend: &multiLedgerBackend{},
		ProtocolsModel: data.NewProtocolsModelMock(t), ProtocolContractsModel: data.NewProtocolContractsModelMock(t),
		IngestStore: ingestStore, StateChanges: &data.StateChangeModel{DB: dbPool, Metrics: metrics.NewMetrics(prometheus.NewRegistry()).DB},
		NetworkPassphrase: "Test SDF Network ; September 2015",
		Processors: []ProtocolProcessor{
			&testRecordingProcessor{id: "proto1", ingestStore: ingestStore},
			&testRecordingProcessor{id: "proto2", ingestStore: ingestStore},
		},
	})
	require.NoError(t, err)

	from, to, err := svc.resolveRange(ctx, []string{"proto1", "proto2"})
	require.NoError(t, err)
	assert.Equal(t, uint32(100), from)
	assert.Equal(t, uint32(700), to)
}

// TestProtocolHistoryRebuildDeleteHistoryRows pins the ledger slicing: a range
// spanning several full slices plus a final partial one deletes exactly the
// protocol's rows inside it, leaving neighbouring ledgers and other namespaces
// alone.
func TestProtocolHistoryRebuildDeleteHistoryRows(t *testing.T) {
	ctx := context.Background()
	dbPool, ingestStore := setupTestDB(t)

	// [100, 25000] slices into [100,10099], [10100,20099], [20100,25000] — two
	// full slices and a partial tail. Seed both edges of every slice.
	const (
		from uint32 = 100
		to   uint32 = 25_000
	)
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

	svc, err := NewProtocolHistoryRebuildService(ProtocolHistoryRebuildConfig{
		DB: dbPool, LedgerBackend: &multiLedgerBackend{},
		ProtocolsModel: data.NewProtocolsModelMock(t), ProtocolContractsModel: data.NewProtocolContractsModelMock(t),
		IngestStore: ingestStore, StateChanges: &data.StateChangeModel{DB: dbPool, Metrics: metrics.NewMetrics(prometheus.NewRegistry()).DB},
		NetworkPassphrase: "Test SDF Network ; September 2015",
		Processors:        []ProtocolProcessor{&testRecordingProcessor{id: "testproto", ingestStore: ingestStore}},
	})
	require.NoError(t, err)

	require.NoError(t, svc.deleteHistoryRows(ctx, "testproto", from, to))

	assert.Equal(t, [][2]int64{
		{types.StateChangeOrdinalBaseIndexer + 1, 100},
		{types.StateChangeOrdinalBaseIndexer + 1, 20_100},
		{types.StateChangeOrdinalBaseSEP41 + 1, 99},
		{types.StateChangeOrdinalBaseSEP41 + 1, 25_001},
	}, remainingStateChanges(t, ctx, dbPool))
}

// TestProtocolMigrateEngineBoundedFold pins the rebuild fold: it covers exactly
// the inclusive range, persists through the window machinery, and never reads or
// writes the protocol's cursor — which live ingestion owns.
func TestProtocolMigrateEngineBoundedFold(t *testing.T) {
	ctx := context.Background()
	dbPool, ingestStore := setupTestDB(t)

	// Live ingestion's committed frontier. The bounded fold must leave it alone.
	setIngestStoreValue(t, ctx, dbPool, utils.ProtocolHistoryCursorName("testproto"), 999)

	protocolContractsModel := data.NewProtocolContractsModelMock(t)
	protocolContractsModel.On("GetByProtocolID", mock.Anything, mock.Anything, "testproto").Return([]data.ProtocolContracts{}, nil)
	processor := &testRecordingProcessor{id: "testproto", ingestStore: ingestStore}

	backend := &multiLedgerBackend{ledgers: map[uint32]xdr.LedgerCloseMeta{
		199: dummyLedgerMeta(199), 200: dummyLedgerMeta(200), 201: dummyLedgerMeta(201),
		202: dummyLedgerMeta(202), 203: dummyLedgerMeta(203), 204: dummyLedgerMeta(204),
		205: dummyLedgerMeta(205),
	}}

	engine := protocolMigrateEngine{
		db:                     dbPool,
		ledgerBackend:          backend,
		protocolsModel:         data.NewProtocolsModelMock(t),
		protocolContractsModel: protocolContractsModel,
		ingestStore:            ingestStore,
		processors:             map[string]ProtocolProcessor{"testproto": processor},
		windowSize:             2,
		metrics:                metrics.NewMetrics(prometheus.NewRegistry()).Migration,
		bounded:                &boundedLedgerRange{from: 200, to: 204},
		strategy: migrationStrategy{
			Label: "history rebuild",
			Mode:  StagingModeHistory,
			Persist: func(ctx context.Context, dbTx pgx.Tx, proc ProtocolProcessor) error {
				return proc.PersistHistory(ctx, dbTx)
			},
			CursorName: utils.ProtocolHistoryCursorName,
			ResolveStartLedger: func(_ context.Context) (uint32, error) {
				return 200, nil
			},
		},
	}

	handedOff, err := engine.processAllProtocols(ctx, []string{"testproto"})
	require.NoError(t, err)
	assert.Empty(t, handedOff, "a bounded fold ends by exhausting its range, never by handoff")

	// Exactly [200, 204] — neither 199 below nor 205 above, both of which the
	// backend would have served.
	require.Len(t, processor.processedInputs, 5)
	for i, seq := range []uint32{200, 201, 202, 203, 204} {
		assert.Equal(t, seq, processor.processedInputs[i].LedgerSequence)
	}
	// Windows [200,201] and [202,203] commit at size 2; [204] is flushed when the
	// range runs out.
	assert.Equal(t, []uint32{201, 203, 204}, processor.persistedHistorySeqs)

	// The cursor is untouched: no CAS, no init, no advance.
	assert.Equal(t, uint32(999), getIngestStoreValue(t, ctx, dbPool, utils.ProtocolHistoryCursorName("testproto")))
}

// TestProtocolHistoryRebuildRun exercises the whole service: validate, range
// resolution, the delete, and the bounded re-derivation — with the protocol's
// history cursor and other namespaces intact at the end.
func TestProtocolHistoryRebuildRun(t *testing.T) {
	ctx := context.Background()
	dbPool, ingestStore := setupTestDB(t)

	setIngestStoreValue(t, ctx, dbPool, data.OldestLedgerCursorName, 100)
	setIngestStoreValue(t, ctx, dbPool, utils.ProtocolHistoryCursorName("testproto"), 210)

	// Rows the rebuild must delete, plus neighbours outside its range and a row
	// in another namespace inside it.
	for _, ledger := range []uint32{200, 202, 204} {
		seedStateChange(t, ctx, dbPool, types.StateChangeOrdinalBaseSEP41, ledger)
	}
	for _, ledger := range []uint32{199, 205} {
		seedStateChange(t, ctx, dbPool, types.StateChangeOrdinalBaseSEP41, ledger)
	}
	seedStateChange(t, ctx, dbPool, types.StateChangeOrdinalBaseIndexer, 202)

	protocolsModel := data.NewProtocolsModelMock(t)
	protocolsModel.On("GetByIDs", mock.Anything, []string{"testproto"}).Return([]data.Protocols{
		{ID: "testproto", ClassificationStatus: data.StatusSuccess, HistoryMigrationStatus: data.StatusSuccess},
	}, nil)
	protocolContractsModel := data.NewProtocolContractsModelMock(t)
	protocolContractsModel.On("GetByProtocolID", mock.Anything, mock.Anything, "testproto").Return([]data.ProtocolContracts{}, nil)
	processor := &testRecordingProcessor{id: "testproto", ingestStore: ingestStore}

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
		FromLedger:        200,
		ToLedger:          204,
		WindowSize:        2,
	})
	require.NoError(t, err)

	require.NoError(t, svc.Run(ctx, []string{"testproto"}))

	// The protocol's rows inside [200, 204] are gone; its neighbours and the
	// indexer namespace are untouched.
	assert.Equal(t, [][2]int64{
		{types.StateChangeOrdinalBaseIndexer + 1, 202},
		{types.StateChangeOrdinalBaseSEP41 + 1, 199},
		{types.StateChangeOrdinalBaseSEP41 + 1, 205},
	}, remainingStateChanges(t, ctx, dbPool))

	// Every ledger in the range was re-folded and persisted.
	require.Len(t, processor.processedInputs, 5)
	assert.Equal(t, []uint32{201, 203, 204}, processor.persistedHistorySeqs)

	// The rebuild never touches the cursor or the migration status.
	assert.Equal(t, uint32(210), getIngestStoreValue(t, ctx, dbPool, utils.ProtocolHistoryCursorName("testproto")))
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
