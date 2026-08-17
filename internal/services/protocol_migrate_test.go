package services

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/utils"
)

func TestMinCursor(t *testing.T) {
	trackers := []*protocolTracker{
		{cursorValue: 300},
		{cursorValue: 100},
		{cursorValue: 250},
	}
	assert.Equal(t, uint32(100), minCursor(trackers))

	single := []*protocolTracker{{cursorValue: 7}}
	assert.Equal(t, uint32(7), minCursor(single))

	firstIsMin := []*protocolTracker{
		{cursorValue: 5},
		{cursorValue: 9},
		{cursorValue: 12},
	}
	assert.Equal(t, uint32(5), minCursor(firstIsMin))
}

// multiLedgerBackend is a test double that serves ledger meta for a range of ledgers.
type multiLedgerBackend struct {
	ledgers map[uint32]xdr.LedgerCloseMeta
}

func (b *multiLedgerBackend) GetLatestLedgerSequence(_ context.Context) (uint32, error) {
	var max uint32
	for seq := range b.ledgers {
		if seq > max {
			max = seq
		}
	}
	return max, nil
}

func (b *multiLedgerBackend) GetLedger(ctx context.Context, sequence uint32) (xdr.LedgerCloseMeta, error) {
	if meta, ok := b.ledgers[sequence]; ok {
		return meta, nil
	}
	<-ctx.Done()
	return xdr.LedgerCloseMeta{}, ctx.Err()
}

func (b *multiLedgerBackend) PrepareRange(context.Context, ledgerbackend.Range) error {
	return nil
}

func (b *multiLedgerBackend) IsPrepared(context.Context, ledgerbackend.Range) (bool, error) {
	return true, nil
}

func (b *multiLedgerBackend) Close() error {
	return nil
}

// transientErrorBackend wraps multiLedgerBackend and injects transient errors
// on convergence-poll calls (unbounded PrepareRange, missing-ledger GetLedger)
// before delegating normally. This simulates RPC blips that should not be
// mistaken for convergence.
type transientErrorBackend struct {
	multiLedgerBackend
	// unboundedPrepareFailsLeft counts how many unbounded PrepareRange calls
	// (convergence polls) should return a transient error before succeeding.
	unboundedPrepareFailsLeft atomic.Int32
	// missingGetLedgerFailsLeft counts how many GetLedger calls for missing
	// ledgers should return a transient error instead of blocking.
	missingGetLedgerFailsLeft atomic.Int32
}

func (b *transientErrorBackend) PrepareRange(ctx context.Context, r ledgerbackend.Range) error {
	if !r.Bounded() && b.unboundedPrepareFailsLeft.Add(-1) >= 0 {
		return fmt.Errorf("transient RPC error: connection refused")
	}
	return b.multiLedgerBackend.PrepareRange(ctx, r)
}

func (b *transientErrorBackend) GetLedger(ctx context.Context, sequence uint32) (xdr.LedgerCloseMeta, error) {
	if _, ok := b.multiLedgerBackend.ledgers[sequence]; !ok {
		if b.missingGetLedgerFailsLeft.Add(-1) >= 0 {
			return xdr.LedgerCloseMeta{}, fmt.Errorf("transient RPC error: connection reset")
		}
	}
	return b.multiLedgerBackend.GetLedger(ctx, sequence)
}

func dummyLedgerMeta(seq uint32) xdr.LedgerCloseMeta {
	return xdr.LedgerCloseMeta{
		V: 0,
		V0: &xdr.LedgerCloseMetaV0{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Header: xdr.LedgerHeader{
					LedgerSeq: xdr.Uint32(seq),
				},
			},
		},
	}
}

func setupTestDB(t *testing.T) (*pgxpool.Pool, *data.IngestStoreModel) {
	t.Helper()
	dbt := dbtest.Open(t)
	t.Cleanup(func() { dbt.Close() })

	dbPool, err := db.OpenDBConnectionPool(context.Background(), dbt.DSN)
	require.NoError(t, err)
	t.Cleanup(func() { dbPool.Close() })

	dbMetrics := metrics.NewMetrics(prometheus.NewRegistry()).DB
	ingestStore := &data.IngestStoreModel{DB: dbPool, Metrics: dbMetrics}
	return dbPool, ingestStore
}

func setIngestStoreValue(t *testing.T, ctx context.Context, dbPool *pgxpool.Pool, key string, value uint32) {
	t.Helper()
	_, err := dbPool.Exec(ctx, `INSERT INTO ingest_store (key, value) VALUES ($1, $2)
		ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value`, key, strconv.FormatUint(uint64(value), 10))
	require.NoError(t, err)
}

func getIngestStoreValue(t *testing.T, ctx context.Context, dbPool *pgxpool.Pool, key string) uint32 {
	t.Helper()
	var s string
	err := dbPool.QueryRow(ctx, `SELECT value FROM ingest_store WHERE key = $1`, key).Scan(&s)
	require.NoError(t, err)
	val, err := strconv.ParseUint(s, 10, 32)
	require.NoError(t, err)
	return uint32(val)
}

// testRecordingProcessor is a unified test double that implements ProtocolProcessor.
// It records all ProcessLedger inputs and writes per-ledger sentinel keys to
// ingest_store during PersistHistory and PersistCurrentState, proving that
// Persist actually committed data inside the transaction.
type testRecordingProcessor struct {
	id                        string
	ingestStore               *data.IngestStoreModel
	processedInputs           []ProtocolProcessorInput
	persistedHistorySeqs      []uint32
	persistedCurrentStateSeqs []uint32
	lastProcessed             uint32
	resetCount                int
	requiresContractData      bool
}

func (p *testRecordingProcessor) ProtocolID() string { return p.id }

func (p *testRecordingProcessor) StateChangeOrdinalBase() int64 {
	return types.StateChangeOrdinalBaseSEP41
}

func (p *testRecordingProcessor) RequiresContractData() bool { return p.requiresContractData }

func (p *testRecordingProcessor) Reset() { p.resetCount++ }

func (p *testRecordingProcessor) ProcessLedger(_ context.Context, input ProtocolProcessorInput) error {
	p.processedInputs = append(p.processedInputs, input)
	p.lastProcessed = input.LedgerSequence
	return nil
}

func (p *testRecordingProcessor) PersistHistory(ctx context.Context, dbTx pgx.Tx) error {
	p.persistedHistorySeqs = append(p.persistedHistorySeqs, p.lastProcessed)
	return p.ingestStore.Update(ctx, dbTx, fmt.Sprintf("test_%s_history_%d", p.id, p.lastProcessed), p.lastProcessed)
}

func (p *testRecordingProcessor) PersistCurrentState(ctx context.Context, dbTx pgx.Tx) error {
	p.persistedCurrentStateSeqs = append(p.persistedCurrentStateSeqs, p.lastProcessed)
	return p.ingestStore.Update(ctx, dbTx, fmt.Sprintf("test_%s_current_state_%d", p.id, p.lastProcessed), p.lastProcessed)
}

// testCursorAdvancingProcessor embeds testRecordingProcessor and simulates
// live ingestion taking over by advancing its own cursor in the DB during
// ProcessLedger at a specific sequence, causing the subsequent CAS to fail.
type testCursorAdvancingProcessor struct {
	testRecordingProcessor
	dbPool         *pgxpool.Pool
	advanceAtSeq   uint32
	cursorNameFunc func(string) string
}

func (p *testCursorAdvancingProcessor) ProcessLedger(ctx context.Context, input ProtocolProcessorInput) error {
	if input.LedgerSequence == p.advanceAtSeq {
		if _, err := p.dbPool.Exec(ctx,
			`UPDATE ingest_store SET value = $1 WHERE key = $2`,
			strconv.FormatUint(uint64(p.advanceAtSeq+100), 10),
			p.cursorNameFunc(p.id)); err != nil {
			return fmt.Errorf("advancing cursor for test: %w", err)
		}
	}
	return p.testRecordingProcessor.ProcessLedger(ctx, input)
}

// testMidWindowClassificationProcessor embeds testRecordingProcessor and
// simulates live ingestion running concurrently with the engine's window: at
// advanceLiveAtSeq it commits live's cursor to liveCursorTarget (standing in
// for live's transaction that classifies a new contract and advances
// latest_ingest_ledger together), and at handoffAtSeq it moves the protocol
// cursor so the engine's next window CAS fails and the run terminates.
type testMidWindowClassificationProcessor struct {
	testRecordingProcessor
	dbPool           *pgxpool.Pool
	advanceLiveAtSeq uint32
	liveCursorTarget uint32
	handoffAtSeq     uint32
}

func (p *testMidWindowClassificationProcessor) ProcessLedger(ctx context.Context, input ProtocolProcessorInput) error {
	switch input.LedgerSequence {
	case p.advanceLiveAtSeq:
		if _, err := p.dbPool.Exec(ctx,
			`UPDATE ingest_store SET value = $1 WHERE key = 'latest_ingest_ledger'`,
			strconv.FormatUint(uint64(p.liveCursorTarget), 10)); err != nil {
			return fmt.Errorf("advancing live cursor for test: %w", err)
		}
	case p.handoffAtSeq:
		if _, err := p.dbPool.Exec(ctx,
			`UPDATE ingest_store SET value = $1 WHERE key = $2`,
			strconv.FormatUint(uint64(p.handoffAtSeq), 10),
			utils.ProtocolHistoryCursorName(p.id)); err != nil {
			return fmt.Errorf("advancing protocol cursor for test: %w", err)
		}
	}
	return p.testRecordingProcessor.ProcessLedger(ctx, input)
}

// testErrorAtSeqProcessor embeds testRecordingProcessor and returns an error
// when ProcessLedger is called for a specific ledger sequence.
type testErrorAtSeqProcessor struct {
	testRecordingProcessor
	errorAtSeq uint32
}

func (p *testErrorAtSeqProcessor) ProcessLedger(ctx context.Context, input ProtocolProcessorInput) error {
	if input.LedgerSequence == p.errorAtSeq {
		return fmt.Errorf("simulated error at ledger %d", p.errorAtSeq)
	}
	return p.testRecordingProcessor.ProcessLedger(ctx, input)
}

func getHistorySentinel(t *testing.T, ctx context.Context, dbPool *pgxpool.Pool, protocolID string, seq uint32) (uint32, bool) {
	t.Helper()
	var s string
	err := dbPool.QueryRow(ctx, `SELECT value FROM ingest_store WHERE key = $1`, fmt.Sprintf("test_%s_history_%d", protocolID, seq)).Scan(&s)
	if err != nil {
		return 0, false
	}
	val, err := strconv.ParseUint(s, 10, 32)
	if err != nil {
		return 0, false
	}
	return uint32(val), true
}

func getCurrentStateSentinel(t *testing.T, ctx context.Context, dbPool *pgxpool.Pool, protocolID string, seq uint32) (uint32, bool) {
	t.Helper()
	var s string
	err := dbPool.QueryRow(ctx, `SELECT value FROM ingest_store WHERE key = $1`, fmt.Sprintf("test_%s_current_state_%d", protocolID, seq)).Scan(&s)
	if err != nil {
		return 0, false
	}
	val, err := strconv.ParseUint(s, 10, 32)
	if err != nil {
		return 0, false
	}
	return uint32(val), true
}

// TestProtocolMigrateEngine exercises the shared protocolMigrateEngine logic
// using NewProtocolMigrateHistoryService as a proxy (since the engine logic is
// identical regardless of strategy).
func TestProtocolMigrateEngine(t *testing.T) {
	t.Run("happy path — single protocol, 3 ledgers, all CAS succeed", func(t *testing.T) {
		ctx := context.Background()
		dbPool, ingestStore := setupTestDB(t)

		// Set up ingest cursors
		setIngestStoreValue(t, ctx, dbPool, "oldest_ingest_ledger", 100)
		setIngestStoreValue(t, ctx, dbPool, "latest_ingest_ledger", 102)

		// Set up protocol in DB
		_, err := dbPool.Exec(ctx, `INSERT INTO protocols (id, classification_status) VALUES ('testproto', 'success') ON CONFLICT (id) DO UPDATE SET classification_status = 'success'`)
		require.NoError(t, err)

		protocolsModel := data.NewProtocolsModelMock(t)
		protocolContractsModel := data.NewProtocolContractsModelMock(t)
		// Use testCursorAdvancingProcessor to trigger CAS handoff on the last ledger,
		// allowing the unbounded loop to terminate.
		processor := &testCursorAdvancingProcessor{
			testRecordingProcessor: testRecordingProcessor{id: "testproto", ingestStore: ingestStore},
			dbPool:                 dbPool,
			advanceAtSeq:           102,
			cursorNameFunc:         utils.ProtocolHistoryCursorName,
		}

		protocolsModel.On("GetByIDs", mock.Anything, []string{"testproto"}).Return([]data.Protocols{
			{ID: "testproto", ClassificationStatus: data.StatusSuccess, HistoryMigrationStatus: data.StatusNotStarted},
		}, nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusInProgress).Return(nil)
		protocolContractsModel.On("GetByProtocolID", mock.Anything, mock.Anything, "testproto").Return([]data.ProtocolContracts{}, nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusSuccess).Return(nil)

		backend := &multiLedgerBackend{
			ledgers: map[uint32]xdr.LedgerCloseMeta{
				100: dummyLedgerMeta(100),
				101: dummyLedgerMeta(101),
				102: dummyLedgerMeta(102),
			},
		}

		svc, err := NewProtocolMigrateHistoryService(ProtocolMigrateHistoryConfig{
			DB: dbPool, LedgerBackend: backend,
			ProtocolsModel: protocolsModel, ProtocolContractsModel: protocolContractsModel,
			IngestStore: ingestStore, NetworkPassphrase: "Test SDF Network ; September 2015",
			Processors: []ProtocolProcessor{processor},
		})
		require.NoError(t, err)

		err = svc.Run(ctx, []string{"testproto"})
		require.NoError(t, err)

		// Cursor in DB is 202 (advanceAtSeq=102 + 100) because the processor
		// advanced it during ProcessLedger to simulate live ingestion takeover.
		// The CAS for ledger 102 failed, so the tracker's logical cursor stayed at 101.
		cursorVal := getIngestStoreValue(t, ctx, dbPool, "protocol_testproto_history_cursor")
		assert.Equal(t, uint32(202), cursorVal)

		// Verify PersistHistory committed sentinels for 100, 101 (not 102 — CAS failed)
		for _, seq := range []uint32{100, 101} {
			val, ok := getHistorySentinel(t, ctx, dbPool, "testproto", seq)
			require.True(t, ok, "sentinel for ledger %d should exist", seq)
			assert.Equal(t, seq, val, "sentinel value for ledger %d", seq)
		}
		_, ok := getHistorySentinel(t, ctx, dbPool, "testproto", 102)
		assert.False(t, ok, "sentinel for ledger 102 should NOT exist (CAS failed)")

		// Verify processor recorded all inputs
		require.Len(t, processor.processedInputs, 3)
		for i, seq := range []uint32{100, 101, 102} {
			assert.Equal(t, seq, processor.processedInputs[i].LedgerSequence)
		}
		assert.Equal(t, []uint32{100, 101}, processor.persistedHistorySeqs)
	})

	t.Run("CAS failure (handoff) — CAS fails at ledger N, status success", func(t *testing.T) {
		ctx := context.Background()
		dbPool, ingestStore := setupTestDB(t)

		setIngestStoreValue(t, ctx, dbPool, "oldest_ingest_ledger", 100)
		setIngestStoreValue(t, ctx, dbPool, "latest_ingest_ledger", 102)
		// Pre-set cursor to 100, so processing starts at 101
		setIngestStoreValue(t, ctx, dbPool, "protocol_testproto_history_cursor", 100)

		_, err := dbPool.Exec(ctx, `INSERT INTO protocols (id, classification_status) VALUES ('testproto', 'success') ON CONFLICT (id) DO UPDATE SET classification_status = 'success'`)
		require.NoError(t, err)

		protocolsModel := data.NewProtocolsModelMock(t)
		protocolContractsModel := data.NewProtocolContractsModelMock(t)
		// Use testCursorAdvancingProcessor to trigger CAS handoff at ledger 101.
		processor := &testCursorAdvancingProcessor{
			testRecordingProcessor: testRecordingProcessor{id: "testproto", ingestStore: ingestStore},
			dbPool:                 dbPool,
			advanceAtSeq:           101,
			cursorNameFunc:         utils.ProtocolHistoryCursorName,
		}

		protocolsModel.On("GetByIDs", mock.Anything, []string{"testproto"}).Return([]data.Protocols{
			{ID: "testproto", ClassificationStatus: data.StatusSuccess, HistoryMigrationStatus: data.StatusNotStarted},
		}, nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusInProgress).Return(nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusSuccess).Return(nil)

		protocolContractsModel.On("GetByProtocolID", mock.Anything, mock.Anything, "testproto").Return([]data.ProtocolContracts{}, nil).Maybe()

		backend := &multiLedgerBackend{
			ledgers: map[uint32]xdr.LedgerCloseMeta{
				101: dummyLedgerMeta(101),
				102: dummyLedgerMeta(102),
			},
		}

		svc, err := NewProtocolMigrateHistoryService(ProtocolMigrateHistoryConfig{
			DB: dbPool, LedgerBackend: backend,
			ProtocolsModel: protocolsModel, ProtocolContractsModel: protocolContractsModel,
			IngestStore: ingestStore, NetworkPassphrase: "Test SDF Network ; September 2015",
			Processors: []ProtocolProcessor{processor},
		})
		require.NoError(t, err)

		err = svc.Run(ctx, []string{"testproto"})
		require.NoError(t, err) // Handoff is success
	})

	t.Run("validation: classification not complete", func(t *testing.T) {
		ctx := context.Background()
		dbPool, ingestStore := setupTestDB(t)

		protocolsModel := data.NewProtocolsModelMock(t)
		protocolContractsModel := data.NewProtocolContractsModelMock(t)
		processorMock := NewProtocolProcessorMock(t)

		processorMock.On("ProtocolID").Return("testproto")
		protocolsModel.On("GetByIDs", ctx, []string{"testproto"}).Return([]data.Protocols{
			{ID: "testproto", ClassificationStatus: data.StatusInProgress, HistoryMigrationStatus: data.StatusNotStarted},
		}, nil)

		backend := &multiLedgerBackend{ledgers: map[uint32]xdr.LedgerCloseMeta{}}

		svc, err := NewProtocolMigrateHistoryService(ProtocolMigrateHistoryConfig{
			DB: dbPool, LedgerBackend: backend,
			ProtocolsModel: protocolsModel, ProtocolContractsModel: protocolContractsModel,
			IngestStore: ingestStore, NetworkPassphrase: "Test SDF Network ; September 2015",
			Processors: []ProtocolProcessor{processorMock},
		})
		require.NoError(t, err)

		err = svc.Run(ctx, []string{"testproto"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "classification not complete")
	})

	t.Run("validation: protocol not found in DB", func(t *testing.T) {
		ctx := context.Background()
		dbPool, ingestStore := setupTestDB(t)

		protocolsModel := data.NewProtocolsModelMock(t)
		protocolContractsModel := data.NewProtocolContractsModelMock(t)
		processorMock := NewProtocolProcessorMock(t)

		processorMock.On("ProtocolID").Return("testproto")
		protocolsModel.On("GetByIDs", ctx, []string{"testproto"}).Return([]data.Protocols{}, nil)

		backend := &multiLedgerBackend{ledgers: map[uint32]xdr.LedgerCloseMeta{}}

		svc, err := NewProtocolMigrateHistoryService(ProtocolMigrateHistoryConfig{
			DB: dbPool, LedgerBackend: backend,
			ProtocolsModel: protocolsModel, ProtocolContractsModel: protocolContractsModel,
			IngestStore: ingestStore, NetworkPassphrase: "Test SDF Network ; September 2015",
			Processors: []ProtocolProcessor{processorMock},
		})
		require.NoError(t, err)

		err = svc.Run(ctx, []string{"testproto"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found in DB")
	})

	t.Run("validation: no processor registered for protocol", func(t *testing.T) {
		ctx := context.Background()
		dbPool, ingestStore := setupTestDB(t)

		protocolsModel := data.NewProtocolsModelMock(t)
		protocolContractsModel := data.NewProtocolContractsModelMock(t)
		processorMock := NewProtocolProcessorMock(t)

		processorMock.On("ProtocolID").Return("otherproto")

		backend := &multiLedgerBackend{ledgers: map[uint32]xdr.LedgerCloseMeta{}}

		svc, err := NewProtocolMigrateHistoryService(ProtocolMigrateHistoryConfig{
			DB: dbPool, LedgerBackend: backend,
			ProtocolsModel: protocolsModel, ProtocolContractsModel: protocolContractsModel,
			IngestStore: ingestStore, NetworkPassphrase: "Test SDF Network ; September 2015",
			Processors: []ProtocolProcessor{processorMock},
		})
		require.NoError(t, err)

		err = svc.Run(ctx, []string{"testproto"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no processor registered")
	})

	t.Run("duplicate protocol IDs are deduplicated — each processed once", func(t *testing.T) {
		ctx := context.Background()
		dbPool, ingestStore := setupTestDB(t)

		setIngestStoreValue(t, ctx, dbPool, "oldest_ingest_ledger", 100)
		setIngestStoreValue(t, ctx, dbPool, "latest_ingest_ledger", 101)

		_, err := dbPool.Exec(ctx, `INSERT INTO protocols (id, classification_status) VALUES ('testproto', 'success') ON CONFLICT (id) DO UPDATE SET classification_status = 'success'`)
		require.NoError(t, err)

		protocolsModel := data.NewProtocolsModelMock(t)
		protocolContractsModel := data.NewProtocolContractsModelMock(t)
		// Use testCursorAdvancingProcessor to trigger CAS handoff on the last ledger.
		processor := &testCursorAdvancingProcessor{
			testRecordingProcessor: testRecordingProcessor{id: "testproto", ingestStore: ingestStore},
			dbPool:                 dbPool,
			advanceAtSeq:           101,
			cursorNameFunc:         utils.ProtocolHistoryCursorName,
		}

		// Mock expects the deduplicated slice (single element), not the duplicated input.
		protocolsModel.On("GetByIDs", mock.Anything, []string{"testproto"}).Return([]data.Protocols{
			{ID: "testproto", ClassificationStatus: data.StatusSuccess, HistoryMigrationStatus: data.StatusNotStarted},
		}, nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusInProgress).Return(nil)
		protocolContractsModel.On("GetByProtocolID", mock.Anything, mock.Anything, "testproto").Return([]data.ProtocolContracts{}, nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusSuccess).Return(nil)

		backend := &multiLedgerBackend{
			ledgers: map[uint32]xdr.LedgerCloseMeta{
				100: dummyLedgerMeta(100),
				101: dummyLedgerMeta(101),
			},
		}

		svc, err := NewProtocolMigrateHistoryService(ProtocolMigrateHistoryConfig{
			DB: dbPool, LedgerBackend: backend,
			ProtocolsModel: protocolsModel, ProtocolContractsModel: protocolContractsModel,
			IngestStore: ingestStore, NetworkPassphrase: "Test SDF Network ; September 2015",
			Processors: []ProtocolProcessor{processor},
		})
		require.NoError(t, err)

		// Pass duplicate IDs — should be deduplicated internally.
		err = svc.Run(ctx, []string{"testproto", "testproto", "testproto"})
		require.NoError(t, err)

		// DB cursor is 201 (advanceAtSeq=101 + 100) because the processor
		// advanced it to simulate live ingestion takeover; CAS failed on 101.
		cursorVal := getIngestStoreValue(t, ctx, dbPool, "protocol_testproto_history_cursor")
		assert.Equal(t, uint32(201), cursorVal)

		// Each ledger processed exactly once.
		require.Len(t, processor.processedInputs, 2)
		assert.Equal(t, uint32(100), processor.processedInputs[0].LedgerSequence)
		assert.Equal(t, uint32(101), processor.processedInputs[1].LedgerSequence)
		assert.Equal(t, []uint32{100}, processor.persistedHistorySeqs)
	})

	t.Run("resume from cursor — cursor already at N, process from N+1", func(t *testing.T) {
		ctx := context.Background()
		dbPool, ingestStore := setupTestDB(t)

		setIngestStoreValue(t, ctx, dbPool, "oldest_ingest_ledger", 100)
		setIngestStoreValue(t, ctx, dbPool, "latest_ingest_ledger", 103)
		// Cursor already at 101 (previous partial run)
		setIngestStoreValue(t, ctx, dbPool, "protocol_testproto_history_cursor", 101)

		_, err := dbPool.Exec(ctx, `INSERT INTO protocols (id, classification_status) VALUES ('testproto', 'success') ON CONFLICT (id) DO UPDATE SET classification_status = 'success'`)
		require.NoError(t, err)

		protocolsModel := data.NewProtocolsModelMock(t)
		protocolContractsModel := data.NewProtocolContractsModelMock(t)
		// Use testCursorAdvancingProcessor to trigger CAS handoff on the last ledger.
		processor := &testCursorAdvancingProcessor{
			testRecordingProcessor: testRecordingProcessor{id: "testproto", ingestStore: ingestStore},
			dbPool:                 dbPool,
			advanceAtSeq:           103,
			cursorNameFunc:         utils.ProtocolHistoryCursorName,
		}

		protocolsModel.On("GetByIDs", mock.Anything, []string{"testproto"}).Return([]data.Protocols{
			{ID: "testproto", ClassificationStatus: data.StatusSuccess, HistoryMigrationStatus: data.StatusNotStarted},
		}, nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusInProgress).Return(nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusSuccess).Return(nil)

		protocolContractsModel.On("GetByProtocolID", mock.Anything, mock.Anything, "testproto").Return([]data.ProtocolContracts{}, nil)

		backend := &multiLedgerBackend{
			ledgers: map[uint32]xdr.LedgerCloseMeta{
				102: dummyLedgerMeta(102),
				103: dummyLedgerMeta(103),
			},
		}

		svc, err := NewProtocolMigrateHistoryService(ProtocolMigrateHistoryConfig{
			DB: dbPool, LedgerBackend: backend,
			ProtocolsModel: protocolsModel, ProtocolContractsModel: protocolContractsModel,
			IngestStore: ingestStore, NetworkPassphrase: "Test SDF Network ; September 2015",
			Processors: []ProtocolProcessor{processor},
		})
		require.NoError(t, err)

		err = svc.Run(ctx, []string{"testproto"})
		require.NoError(t, err)

		// DB cursor is 203 (advanceAtSeq=103 + 100) because the processor
		// advanced it to simulate live ingestion takeover; CAS failed on 103.
		cursorVal := getIngestStoreValue(t, ctx, dbPool, "protocol_testproto_history_cursor")
		assert.Equal(t, uint32(203), cursorVal)

		// Verify sentinels exist only for 102 (not 100, 101, or 103)
		for _, seq := range []uint32{100, 101} {
			_, ok := getHistorySentinel(t, ctx, dbPool, "testproto", seq)
			assert.False(t, ok, "sentinel for ledger %d should NOT exist (already processed)", seq)
		}
		val, ok := getHistorySentinel(t, ctx, dbPool, "testproto", 102)
		require.True(t, ok, "sentinel for ledger 102 should exist")
		assert.Equal(t, uint32(102), val, "sentinel value for ledger 102")
		_, ok = getHistorySentinel(t, ctx, dbPool, "testproto", 103)
		assert.False(t, ok, "sentinel for ledger 103 should NOT exist (CAS failed)")

		assert.Equal(t, []uint32{102}, processor.persistedHistorySeqs)
	})

	t.Run("error during ProcessLedger — status failed", func(t *testing.T) {
		ctx := context.Background()
		dbPool, ingestStore := setupTestDB(t)

		setIngestStoreValue(t, ctx, dbPool, "oldest_ingest_ledger", 100)
		setIngestStoreValue(t, ctx, dbPool, "latest_ingest_ledger", 101)

		_, err := dbPool.Exec(ctx, `INSERT INTO protocols (id, classification_status) VALUES ('testproto', 'success') ON CONFLICT (id) DO UPDATE SET classification_status = 'success'`)
		require.NoError(t, err)

		protocolsModel := data.NewProtocolsModelMock(t)
		protocolContractsModel := data.NewProtocolContractsModelMock(t)
		processorMock := NewProtocolProcessorMock(t)

		protocolsModel.On("GetByIDs", ctx, []string{"testproto"}).Return([]data.Protocols{
			{ID: "testproto", ClassificationStatus: data.StatusSuccess, HistoryMigrationStatus: data.StatusNotStarted},
		}, nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusInProgress).Return(nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusFailed).Return(nil)

		protocolContractsModel.On("GetByProtocolID", mock.Anything, mock.Anything, "testproto").Return([]data.ProtocolContracts{}, nil)

		processorMock.On("ProtocolID").Return("testproto")
		processorMock.On("RequiresContractData").Return(false)
		processorMock.On("ProcessLedger", mock.Anything, mock.Anything).Return(fmt.Errorf("simulated ProcessLedger error"))

		backend := &multiLedgerBackend{
			ledgers: map[uint32]xdr.LedgerCloseMeta{
				100: dummyLedgerMeta(100),
			},
		}

		svc, err := NewProtocolMigrateHistoryService(ProtocolMigrateHistoryConfig{
			DB: dbPool, LedgerBackend: backend,
			ProtocolsModel: protocolsModel, ProtocolContractsModel: protocolContractsModel,
			IngestStore: ingestStore, NetworkPassphrase: "Test SDF Network ; September 2015",
			Processors: []ProtocolProcessor{processorMock},
		})
		require.NoError(t, err)

		err = svc.Run(ctx, []string{"testproto"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "simulated ProcessLedger error")
	})

	t.Run("error during PersistHistory — tx rolls back, status failed", func(t *testing.T) {
		ctx := context.Background()
		dbPool, ingestStore := setupTestDB(t)

		setIngestStoreValue(t, ctx, dbPool, "oldest_ingest_ledger", 100)
		setIngestStoreValue(t, ctx, dbPool, "latest_ingest_ledger", 100)

		_, err := dbPool.Exec(ctx, `INSERT INTO protocols (id, classification_status) VALUES ('testproto', 'success') ON CONFLICT (id) DO UPDATE SET classification_status = 'success'`)
		require.NoError(t, err)

		protocolsModel := data.NewProtocolsModelMock(t)
		protocolContractsModel := data.NewProtocolContractsModelMock(t)
		processorMock := NewProtocolProcessorMock(t)

		protocolsModel.On("GetByIDs", ctx, []string{"testproto"}).Return([]data.Protocols{
			{ID: "testproto", ClassificationStatus: data.StatusSuccess, HistoryMigrationStatus: data.StatusNotStarted},
		}, nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusInProgress).Return(nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusFailed).Return(nil)

		protocolContractsModel.On("GetByProtocolID", mock.Anything, mock.Anything, "testproto").Return([]data.ProtocolContracts{}, nil)

		processorMock.On("ProtocolID").Return("testproto")
		processorMock.On("RequiresContractData").Return(false)
		processorMock.On("ProcessLedger", mock.Anything, mock.Anything).Return(nil)
		processorMock.On("PersistHistory", mock.Anything, mock.Anything).Return(fmt.Errorf("simulated PersistHistory error"))

		backend := &multiLedgerBackend{
			ledgers: map[uint32]xdr.LedgerCloseMeta{
				100: dummyLedgerMeta(100),
			},
		}

		svc, err := NewProtocolMigrateHistoryService(ProtocolMigrateHistoryConfig{
			DB: dbPool, LedgerBackend: backend,
			ProtocolsModel: protocolsModel, ProtocolContractsModel: protocolContractsModel,
			IngestStore: ingestStore, NetworkPassphrase: "Test SDF Network ; September 2015",
			Processors: []ProtocolProcessor{processorMock},
		})
		require.NoError(t, err)

		err = svc.Run(ctx, []string{"testproto"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "simulated PersistHistory error")

		// Cursor should NOT have advanced because tx rolled back
		cursorVal := getIngestStoreValue(t, ctx, dbPool, "protocol_testproto_history_cursor")
		assert.Equal(t, uint32(99), cursorVal) // initialized to oldest-1
	})

	t.Run("already at tip — cursor equals latest, context timeout", func(t *testing.T) {
		dbPool, ingestStore := setupTestDB(t)
		setupCtx := context.Background()

		setIngestStoreValue(t, setupCtx, dbPool, "oldest_ingest_ledger", 100)
		setIngestStoreValue(t, setupCtx, dbPool, "latest_ingest_ledger", 105)
		setIngestStoreValue(t, setupCtx, dbPool, "protocol_testproto_history_cursor", 105)

		_, err := dbPool.Exec(setupCtx, `INSERT INTO protocols (id, classification_status) VALUES ('testproto', 'success') ON CONFLICT (id) DO UPDATE SET classification_status = 'success'`)
		require.NoError(t, err)

		protocolsModel := data.NewProtocolsModelMock(t)
		protocolContractsModel := data.NewProtocolContractsModelMock(t)
		processor := &testRecordingProcessor{id: "testproto", ingestStore: ingestStore}

		protocolsModel.On("GetByIDs", mock.Anything, []string{"testproto"}).Return([]data.Protocols{
			{ID: "testproto", ClassificationStatus: data.StatusSuccess, HistoryMigrationStatus: data.StatusNotStarted},
		}, nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusInProgress).Return(nil)
		// GetLedger(106) will block because no ledger 106 exists; context timeout
		// causes the run to fail, so the engine marks it as failed.
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusFailed).Return(nil)

		protocolContractsModel.On("GetByProtocolID", mock.Anything, mock.Anything, "testproto").Return([]data.ProtocolContracts{}, nil)

		backend := &multiLedgerBackend{ledgers: map[uint32]xdr.LedgerCloseMeta{}}

		svc, err := NewProtocolMigrateHistoryService(ProtocolMigrateHistoryConfig{
			DB: dbPool, LedgerBackend: backend,
			ProtocolsModel: protocolsModel, ProtocolContractsModel: protocolContractsModel,
			IngestStore: ingestStore, NetworkPassphrase: "Test SDF Network ; September 2015",
			Processors: []ProtocolProcessor{processor},
		})
		require.NoError(t, err)

		// Use a short timeout so the test doesn't hang — GetLedger(106) blocks until ctx done.
		runCtx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()

		err = svc.Run(runCtx, []string{"testproto"})
		require.Error(t, err)

		// No processing happened — no sentinels should exist
		verifyCtx := context.Background()
		_, ok := getHistorySentinel(t, verifyCtx, dbPool, "testproto", 105)
		assert.False(t, ok, "no sentinel should exist when already at tip")
		assert.Empty(t, processor.processedInputs)
	})

	t.Run("multiple protocols — both process each ledger via shared fetch", func(t *testing.T) {
		ctx := context.Background()
		dbPool, ingestStore := setupTestDB(t)

		setIngestStoreValue(t, ctx, dbPool, "oldest_ingest_ledger", 100)
		setIngestStoreValue(t, ctx, dbPool, "latest_ingest_ledger", 101)

		_, err := dbPool.Exec(ctx, `INSERT INTO protocols (id, classification_status) VALUES ('proto1', 'success') ON CONFLICT (id) DO UPDATE SET classification_status = 'success'`)
		require.NoError(t, err)
		_, err = dbPool.Exec(ctx, `INSERT INTO protocols (id, classification_status) VALUES ('proto2', 'success') ON CONFLICT (id) DO UPDATE SET classification_status = 'success'`)
		require.NoError(t, err)

		protocolsModel := data.NewProtocolsModelMock(t)
		protocolContractsModel := data.NewProtocolContractsModelMock(t)
		// Both use testCursorAdvancingProcessor to trigger CAS handoff on the last ledger.
		proc1 := &testCursorAdvancingProcessor{
			testRecordingProcessor: testRecordingProcessor{id: "proto1", ingestStore: ingestStore},
			dbPool:                 dbPool,
			advanceAtSeq:           101,
			cursorNameFunc:         utils.ProtocolHistoryCursorName,
		}
		proc2 := &testCursorAdvancingProcessor{
			testRecordingProcessor: testRecordingProcessor{id: "proto2", ingestStore: ingestStore},
			dbPool:                 dbPool,
			advanceAtSeq:           101,
			cursorNameFunc:         utils.ProtocolHistoryCursorName,
		}

		protocolsModel.On("GetByIDs", mock.Anything, []string{"proto1", "proto2"}).Return([]data.Protocols{
			{ID: "proto1", ClassificationStatus: data.StatusSuccess, HistoryMigrationStatus: data.StatusNotStarted},
			{ID: "proto2", ClassificationStatus: data.StatusSuccess, HistoryMigrationStatus: data.StatusNotStarted},
		}, nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"proto1", "proto2"}, data.StatusInProgress).Return(nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"proto1", "proto2"}, data.StatusSuccess).Return(nil)

		protocolContractsModel.On("GetByProtocolID", mock.Anything, mock.Anything, "proto1").Return([]data.ProtocolContracts{}, nil)
		protocolContractsModel.On("GetByProtocolID", mock.Anything, mock.Anything, "proto2").Return([]data.ProtocolContracts{}, nil)

		backend := &multiLedgerBackend{
			ledgers: map[uint32]xdr.LedgerCloseMeta{
				100: dummyLedgerMeta(100),
				101: dummyLedgerMeta(101),
			},
		}

		svc, err := NewProtocolMigrateHistoryService(ProtocolMigrateHistoryConfig{
			DB: dbPool, LedgerBackend: backend,
			ProtocolsModel: protocolsModel, ProtocolContractsModel: protocolContractsModel,
			IngestStore: ingestStore, NetworkPassphrase: "Test SDF Network ; September 2015",
			Processors: []ProtocolProcessor{proc1, proc2},
		})
		require.NoError(t, err)

		err = svc.Run(ctx, []string{"proto1", "proto2"})
		require.NoError(t, err)

		// DB cursors are 201 (advanceAtSeq=101 + 100) — CAS failed on 101
		cursor1 := getIngestStoreValue(t, ctx, dbPool, "protocol_proto1_history_cursor")
		cursor2 := getIngestStoreValue(t, ctx, dbPool, "protocol_proto2_history_cursor")
		assert.Equal(t, uint32(201), cursor1)
		assert.Equal(t, uint32(201), cursor2)

		// Verify each protocol has sentinels for 100 only (101 CAS failed)
		for _, id := range []string{"proto1", "proto2"} {
			val, ok := getHistorySentinel(t, ctx, dbPool, id, 100)
			require.True(t, ok, "sentinel for %s ledger 100 should exist", id)
			assert.Equal(t, uint32(100), val, "sentinel value for %s ledger 100", id)
			_, ok = getHistorySentinel(t, ctx, dbPool, id, 101)
			assert.False(t, ok, "sentinel for %s ledger 101 should NOT exist (CAS failed)", id)
		}
		assert.Equal(t, []uint32{100}, proc1.persistedHistorySeqs)
		assert.Equal(t, []uint32{100}, proc2.persistedHistorySeqs)
	})

	t.Run("protocols at different cursors — each starts from its own position", func(t *testing.T) {
		ctx := context.Background()
		dbPool, ingestStore := setupTestDB(t)

		setIngestStoreValue(t, ctx, dbPool, "oldest_ingest_ledger", 50)
		setIngestStoreValue(t, ctx, dbPool, "latest_ingest_ledger", 102)
		// proto1 cursor at 98, proto2 cursor at 100
		setIngestStoreValue(t, ctx, dbPool, "protocol_proto1_history_cursor", 98)
		setIngestStoreValue(t, ctx, dbPool, "protocol_proto2_history_cursor", 100)

		_, err := dbPool.Exec(ctx, `INSERT INTO protocols (id, classification_status) VALUES ('proto1', 'success') ON CONFLICT (id) DO UPDATE SET classification_status = 'success'`)
		require.NoError(t, err)
		_, err = dbPool.Exec(ctx, `INSERT INTO protocols (id, classification_status) VALUES ('proto2', 'success') ON CONFLICT (id) DO UPDATE SET classification_status = 'success'`)
		require.NoError(t, err)

		protocolsModel := data.NewProtocolsModelMock(t)
		protocolContractsModel := data.NewProtocolContractsModelMock(t)
		// Both use testCursorAdvancingProcessor to trigger CAS handoff on the last ledger.
		proc1 := &testCursorAdvancingProcessor{
			testRecordingProcessor: testRecordingProcessor{id: "proto1", ingestStore: ingestStore},
			dbPool:                 dbPool,
			advanceAtSeq:           102,
			cursorNameFunc:         utils.ProtocolHistoryCursorName,
		}
		proc2 := &testCursorAdvancingProcessor{
			testRecordingProcessor: testRecordingProcessor{id: "proto2", ingestStore: ingestStore},
			dbPool:                 dbPool,
			advanceAtSeq:           102,
			cursorNameFunc:         utils.ProtocolHistoryCursorName,
		}

		protocolsModel.On("GetByIDs", mock.Anything, []string{"proto1", "proto2"}).Return([]data.Protocols{
			{ID: "proto1", ClassificationStatus: data.StatusSuccess, HistoryMigrationStatus: data.StatusNotStarted},
			{ID: "proto2", ClassificationStatus: data.StatusSuccess, HistoryMigrationStatus: data.StatusNotStarted},
		}, nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"proto1", "proto2"}, data.StatusInProgress).Return(nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"proto1", "proto2"}, data.StatusSuccess).Return(nil)

		protocolContractsModel.On("GetByProtocolID", mock.Anything, mock.Anything, "proto1").Return([]data.ProtocolContracts{}, nil)
		protocolContractsModel.On("GetByProtocolID", mock.Anything, mock.Anything, "proto2").Return([]data.ProtocolContracts{}, nil)

		backend := &multiLedgerBackend{
			ledgers: map[uint32]xdr.LedgerCloseMeta{
				99:  dummyLedgerMeta(99),
				100: dummyLedgerMeta(100),
				101: dummyLedgerMeta(101),
				102: dummyLedgerMeta(102),
			},
		}

		svc, err := NewProtocolMigrateHistoryService(ProtocolMigrateHistoryConfig{
			DB: dbPool, LedgerBackend: backend,
			ProtocolsModel: protocolsModel, ProtocolContractsModel: protocolContractsModel,
			IngestStore: ingestStore, NetworkPassphrase: "Test SDF Network ; September 2015",
			Processors: []ProtocolProcessor{proc1, proc2},
		})
		require.NoError(t, err)

		err = svc.Run(ctx, []string{"proto1", "proto2"})
		require.NoError(t, err)

		// DB cursors are 202 (advanceAtSeq=102 + 100) — CAS failed on 102
		cursor1 := getIngestStoreValue(t, ctx, dbPool, "protocol_proto1_history_cursor")
		cursor2 := getIngestStoreValue(t, ctx, dbPool, "protocol_proto2_history_cursor")
		assert.Equal(t, uint32(202), cursor1)
		assert.Equal(t, uint32(202), cursor2)

		// proto1 should process 99-102, proto2 should process 101-102
		require.Len(t, proc1.processedInputs, 4)
		for i, seq := range []uint32{99, 100, 101, 102} {
			assert.Equal(t, seq, proc1.processedInputs[i].LedgerSequence)
		}
		require.Len(t, proc2.processedInputs, 2)
		for i, seq := range []uint32{101, 102} {
			assert.Equal(t, seq, proc2.processedInputs[i].LedgerSequence)
		}

		// proto1 persists 99-101 (not 102 — CAS failed), proto2 persists 101 (not 102)
		assert.Equal(t, []uint32{99, 100, 101}, proc1.persistedHistorySeqs)
		assert.Equal(t, []uint32{101}, proc2.persistedHistorySeqs)
	})

	t.Run("one protocol hands off, other continues", func(t *testing.T) {
		ctx := context.Background()
		dbPool, ingestStore := setupTestDB(t)

		setIngestStoreValue(t, ctx, dbPool, "oldest_ingest_ledger", 100)
		setIngestStoreValue(t, ctx, dbPool, "latest_ingest_ledger", 102)

		_, err := dbPool.Exec(ctx, `INSERT INTO protocols (id, classification_status) VALUES ('proto1', 'success') ON CONFLICT (id) DO UPDATE SET classification_status = 'success'`)
		require.NoError(t, err)
		_, err = dbPool.Exec(ctx, `INSERT INTO protocols (id, classification_status) VALUES ('proto2', 'success') ON CONFLICT (id) DO UPDATE SET classification_status = 'success'`)
		require.NoError(t, err)

		protocolsModel := data.NewProtocolsModelMock(t)
		protocolContractsModel := data.NewProtocolContractsModelMock(t)
		// proc1 advances its own cursor during ProcessLedger at seq 100, causing CAS failure
		proc1 := &testCursorAdvancingProcessor{
			testRecordingProcessor: testRecordingProcessor{id: "proto1", ingestStore: ingestStore},
			dbPool:                 dbPool,
			advanceAtSeq:           100,
			cursorNameFunc:         utils.ProtocolHistoryCursorName,
		}
		// proc2 also uses testCursorAdvancingProcessor to hand off at 102
		proc2 := &testCursorAdvancingProcessor{
			testRecordingProcessor: testRecordingProcessor{id: "proto2", ingestStore: ingestStore},
			dbPool:                 dbPool,
			advanceAtSeq:           102,
			cursorNameFunc:         utils.ProtocolHistoryCursorName,
		}

		protocolsModel.On("GetByIDs", mock.Anything, []string{"proto1", "proto2"}).Return([]data.Protocols{
			{ID: "proto1", ClassificationStatus: data.StatusSuccess, HistoryMigrationStatus: data.StatusNotStarted},
			{ID: "proto2", ClassificationStatus: data.StatusSuccess, HistoryMigrationStatus: data.StatusNotStarted},
		}, nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"proto1", "proto2"}, data.StatusInProgress).Return(nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"proto1", "proto2"}, data.StatusSuccess).Return(nil)

		protocolContractsModel.On("GetByProtocolID", mock.Anything, mock.Anything, "proto1").Return([]data.ProtocolContracts{}, nil)
		protocolContractsModel.On("GetByProtocolID", mock.Anything, mock.Anything, "proto2").Return([]data.ProtocolContracts{}, nil)

		backend := &multiLedgerBackend{
			ledgers: map[uint32]xdr.LedgerCloseMeta{
				100: dummyLedgerMeta(100),
				101: dummyLedgerMeta(101),
				102: dummyLedgerMeta(102),
			},
		}

		svc, err := NewProtocolMigrateHistoryService(ProtocolMigrateHistoryConfig{
			DB: dbPool, LedgerBackend: backend,
			ProtocolsModel: protocolsModel, ProtocolContractsModel: protocolContractsModel,
			IngestStore: ingestStore, NetworkPassphrase: "Test SDF Network ; September 2015",
			Processors: []ProtocolProcessor{proc1, proc2},
		})
		require.NoError(t, err)

		err = svc.Run(ctx, []string{"proto1", "proto2"})
		require.NoError(t, err)

		// proto2 DB cursor is 202 (advanceAtSeq=102 + 100) — CAS failed on 102
		cursor2 := getIngestStoreValue(t, ctx, dbPool, "protocol_proto2_history_cursor")
		assert.Equal(t, uint32(202), cursor2)
		assert.Equal(t, []uint32{100, 101}, proc2.persistedHistorySeqs)

		// proto1 should have processed only ledger 100 (then CAS failed, handed off)
		require.Len(t, proc1.processedInputs, 1)
		assert.Equal(t, uint32(100), proc1.processedInputs[0].LedgerSequence)
		// proto1 PersistHistory was NOT called because CAS failed
		assert.Empty(t, proc1.persistedHistorySeqs)

		// Verify proto2 sentinels exist for 100, 101 (not 102 — CAS failed)
		for _, seq := range []uint32{100, 101} {
			val, ok := getHistorySentinel(t, ctx, dbPool, "proto2", seq)
			require.True(t, ok, "sentinel for proto2 ledger %d should exist", seq)
			assert.Equal(t, seq, val)
		}
		_, ok := getHistorySentinel(t, ctx, dbPool, "proto2", 102)
		assert.False(t, ok, "sentinel for proto2 ledger 102 should NOT exist (CAS failed)")
	})

	t.Run("multi-protocol failure with handoff — handed-off gets success, other gets failed", func(t *testing.T) {
		ctx := context.Background()
		dbPool, ingestStore := setupTestDB(t)

		setIngestStoreValue(t, ctx, dbPool, "oldest_ingest_ledger", 100)
		setIngestStoreValue(t, ctx, dbPool, "latest_ingest_ledger", 102)

		_, err := dbPool.Exec(ctx, `INSERT INTO protocols (id, classification_status) VALUES ('proto1', 'success') ON CONFLICT (id) DO UPDATE SET classification_status = 'success'`)
		require.NoError(t, err)
		_, err = dbPool.Exec(ctx, `INSERT INTO protocols (id, classification_status) VALUES ('proto2', 'success') ON CONFLICT (id) DO UPDATE SET classification_status = 'success'`)
		require.NoError(t, err)

		protocolsModel := data.NewProtocolsModelMock(t)
		protocolContractsModel := data.NewProtocolContractsModelMock(t)

		// proto1: hands off via CAS failure at ledger 100
		proc1 := &testCursorAdvancingProcessor{
			testRecordingProcessor: testRecordingProcessor{id: "proto1", ingestStore: ingestStore},
			dbPool:                 dbPool,
			advanceAtSeq:           100,
			cursorNameFunc:         utils.ProtocolHistoryCursorName,
		}
		// proto2: errors at ledger 101
		proc2 := &testErrorAtSeqProcessor{
			testRecordingProcessor: testRecordingProcessor{id: "proto2", ingestStore: ingestStore},
			errorAtSeq:             101,
		}

		protocolsModel.On("GetByIDs", ctx, []string{"proto1", "proto2"}).Return([]data.Protocols{
			{ID: "proto1", ClassificationStatus: data.StatusSuccess, HistoryMigrationStatus: data.StatusNotStarted},
			{ID: "proto2", ClassificationStatus: data.StatusSuccess, HistoryMigrationStatus: data.StatusNotStarted},
		}, nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"proto1", "proto2"}, data.StatusInProgress).Return(nil)
		// proto1 should be marked success (handed off to live ingestion)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"proto1"}, data.StatusSuccess).Return(nil)
		// proto2 should be marked failed (ProcessLedger error)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"proto2"}, data.StatusFailed).Return(nil)

		protocolContractsModel.On("GetByProtocolID", mock.Anything, mock.Anything, "proto1").Return([]data.ProtocolContracts{}, nil)
		protocolContractsModel.On("GetByProtocolID", mock.Anything, mock.Anything, "proto2").Return([]data.ProtocolContracts{}, nil)

		backend := &multiLedgerBackend{
			ledgers: map[uint32]xdr.LedgerCloseMeta{
				100: dummyLedgerMeta(100),
				101: dummyLedgerMeta(101),
				102: dummyLedgerMeta(102),
			},
		}

		svc, err := NewProtocolMigrateHistoryService(ProtocolMigrateHistoryConfig{
			DB: dbPool, LedgerBackend: backend,
			ProtocolsModel: protocolsModel, ProtocolContractsModel: protocolContractsModel,
			IngestStore: ingestStore, NetworkPassphrase: "Test SDF Network ; September 2015",
			Processors: []ProtocolProcessor{proc1, proc2},
		})
		require.NoError(t, err)

		err = svc.Run(ctx, []string{"proto1", "proto2"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "simulated error at ledger 101")

		// Verify the mock expectations — proto1 got StatusSuccess, proto2 got StatusFailed
		protocolsModel.AssertExpectations(t)
	})

	t.Run("already success — skips without error", func(t *testing.T) {
		ctx := context.Background()
		dbPool, ingestStore := setupTestDB(t)

		protocolsModel := data.NewProtocolsModelMock(t)
		protocolContractsModel := data.NewProtocolContractsModelMock(t)
		processorMock := NewProtocolProcessorMock(t)

		processorMock.On("ProtocolID").Return("testproto")
		protocolsModel.On("GetByIDs", ctx, []string{"testproto"}).Return([]data.Protocols{
			{ID: "testproto", ClassificationStatus: data.StatusSuccess, HistoryMigrationStatus: data.StatusSuccess},
		}, nil)

		backend := &multiLedgerBackend{ledgers: map[uint32]xdr.LedgerCloseMeta{}}

		svc, err := NewProtocolMigrateHistoryService(ProtocolMigrateHistoryConfig{
			DB: dbPool, LedgerBackend: backend,
			ProtocolsModel: protocolsModel, ProtocolContractsModel: protocolContractsModel,
			IngestStore: ingestStore, NetworkPassphrase: "Test SDF Network ; September 2015",
			Processors: []ProtocolProcessor{processorMock},
		})
		require.NoError(t, err)

		err = svc.Run(ctx, []string{"testproto"})
		require.NoError(t, err) // No-op, nothing to do
	})

	t.Run("transient PrepareRange error retries then converges", func(t *testing.T) {
		ctx := context.Background()
		dbPool, ingestStore := setupTestDB(t)

		setIngestStoreValue(t, ctx, dbPool, "oldest_ingest_ledger", 100)
		setIngestStoreValue(t, ctx, dbPool, "latest_ingest_ledger", 101)

		_, err := dbPool.Exec(ctx, `INSERT INTO protocols (id, classification_status) VALUES ('testproto', 'success') ON CONFLICT (id) DO UPDATE SET classification_status = 'success'`)
		require.NoError(t, err)

		protocolsModel := data.NewProtocolsModelMock(t)
		protocolContractsModel := data.NewProtocolContractsModelMock(t)
		// Use testCursorAdvancingProcessor to trigger CAS handoff on the last ledger.
		processor := &testCursorAdvancingProcessor{
			testRecordingProcessor: testRecordingProcessor{id: "testproto", ingestStore: ingestStore},
			dbPool:                 dbPool,
			advanceAtSeq:           101,
			cursorNameFunc:         utils.ProtocolHistoryCursorName,
		}

		protocolsModel.On("GetByIDs", mock.Anything, []string{"testproto"}).Return([]data.Protocols{
			{ID: "testproto", ClassificationStatus: data.StatusSuccess, HistoryMigrationStatus: data.StatusNotStarted},
		}, nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusInProgress).Return(nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusSuccess).Return(nil)
		protocolContractsModel.On("GetByProtocolID", mock.Anything, mock.Anything, "testproto").Return([]data.ProtocolContracts{}, nil)

		backend := &transientErrorBackend{
			multiLedgerBackend: multiLedgerBackend{
				ledgers: map[uint32]xdr.LedgerCloseMeta{
					100: dummyLedgerMeta(100),
					101: dummyLedgerMeta(101),
				},
			},
		}
		// The initial PrepareRange(UnboundedRange) will fail once transiently.
		// RetryWithBackoff retries and the second call succeeds.
		backend.unboundedPrepareFailsLeft.Store(1)

		svc, err := NewProtocolMigrateHistoryService(ProtocolMigrateHistoryConfig{
			DB: dbPool, LedgerBackend: backend,
			ProtocolsModel: protocolsModel, ProtocolContractsModel: protocolContractsModel,
			IngestStore: ingestStore, NetworkPassphrase: "Test SDF Network ; September 2015",
			Processors: []ProtocolProcessor{processor},
		})
		require.NoError(t, err)

		err = svc.Run(ctx, []string{"testproto"})
		require.NoError(t, err)

		// Verify all ledgers were processed — the transient error did not prevent migration.
		// DB cursor is 201 (advanceAtSeq=101 + 100) — CAS failed on 101.
		cursorVal := getIngestStoreValue(t, ctx, dbPool, "protocol_testproto_history_cursor")
		assert.Equal(t, uint32(201), cursorVal)
		assert.Equal(t, []uint32{100}, processor.persistedHistorySeqs)
	})

	t.Run("ContractDataChanges populated when a selected processor requires it", func(t *testing.T) {
		ctx := context.Background()
		dbPool, ingestStore := setupTestDB(t)

		setIngestStoreValue(t, ctx, dbPool, "oldest_ingest_ledger", 100)
		setIngestStoreValue(t, ctx, dbPool, "latest_ingest_ledger", 101)

		_, err := dbPool.Exec(ctx, `INSERT INTO protocols (id, classification_status) VALUES ('cdproto', 'success') ON CONFLICT (id) DO UPDATE SET classification_status = 'success'`)
		require.NoError(t, err)
		_, err = dbPool.Exec(ctx, `INSERT INTO protocols (id, classification_status) VALUES ('noproto', 'success') ON CONFLICT (id) DO UPDATE SET classification_status = 'success'`)
		require.NoError(t, err)

		protocolsModel := data.NewProtocolsModelMock(t)
		protocolContractsModel := data.NewProtocolContractsModelMock(t)
		// cdProc requires ContractData; noProc does not. Both use testCursorAdvancingProcessor
		// to trigger CAS handoff on the last ledger, allowing the unbounded loop to terminate.
		cdProc := &testCursorAdvancingProcessor{
			testRecordingProcessor: testRecordingProcessor{id: "cdproto", ingestStore: ingestStore, requiresContractData: true},
			dbPool:                 dbPool,
			advanceAtSeq:           101,
			cursorNameFunc:         utils.ProtocolHistoryCursorName,
		}
		noProc := &testCursorAdvancingProcessor{
			testRecordingProcessor: testRecordingProcessor{id: "noproto", ingestStore: ingestStore, requiresContractData: false},
			dbPool:                 dbPool,
			advanceAtSeq:           101,
			cursorNameFunc:         utils.ProtocolHistoryCursorName,
		}

		protocolsModel.On("GetByIDs", mock.Anything, []string{"cdproto", "noproto"}).Return([]data.Protocols{
			{ID: "cdproto", ClassificationStatus: data.StatusSuccess, HistoryMigrationStatus: data.StatusNotStarted},
			{ID: "noproto", ClassificationStatus: data.StatusSuccess, HistoryMigrationStatus: data.StatusNotStarted},
		}, nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"cdproto", "noproto"}, data.StatusInProgress).Return(nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"cdproto", "noproto"}, data.StatusSuccess).Return(nil)

		protocolContractsModel.On("GetByProtocolID", mock.Anything, mock.Anything, "cdproto").Return([]data.ProtocolContracts{}, nil)
		protocolContractsModel.On("GetByProtocolID", mock.Anything, mock.Anything, "noproto").Return([]data.ProtocolContracts{}, nil)

		backend := &multiLedgerBackend{
			ledgers: map[uint32]xdr.LedgerCloseMeta{
				100: dummyLedgerMeta(100),
				101: dummyLedgerMeta(101),
			},
		}

		svc, err := NewProtocolMigrateHistoryService(ProtocolMigrateHistoryConfig{
			DB: dbPool, LedgerBackend: backend,
			ProtocolsModel: protocolsModel, ProtocolContractsModel: protocolContractsModel,
			IngestStore: ingestStore, NetworkPassphrase: "Test SDF Network ; September 2015",
			Processors: []ProtocolProcessor{cdProc, noProc},
		})
		require.NoError(t, err)

		err = svc.Run(ctx, []string{"cdproto", "noproto"})
		require.NoError(t, err)

		// The requiring processor sees a non-nil (extraction ran) map on every ledger it folded.
		require.Len(t, cdProc.processedInputs, 2)
		for _, input := range cdProc.processedInputs {
			assert.NotNil(t, input.ContractDataChanges, "ledger %d: requiring processor must see non-nil ContractDataChanges", input.LedgerSequence)
		}

		// The map is computed once per ledger and shared across trackers, so the
		// non-requiring processor also receives it (and is expected to ignore it).
		require.Len(t, noProc.processedInputs, 2)
		for _, input := range noProc.processedInputs {
			assert.NotNil(t, input.ContractDataChanges, "ledger %d: shared map must also reach the non-requiring processor", input.LedgerSequence)
		}
	})

	t.Run("ProtocolContracts membership refreshed at window start for requiring processors", func(t *testing.T) {
		ctx := context.Background()
		dbPool, ingestStore := setupTestDB(t)

		setIngestStoreValue(t, ctx, dbPool, "oldest_ingest_ledger", 100)
		setIngestStoreValue(t, ctx, dbPool, "latest_ingest_ledger", 102)

		_, err := dbPool.Exec(ctx, `INSERT INTO protocols (id, classification_status) VALUES ('cdproto', 'success') ON CONFLICT (id) DO UPDATE SET classification_status = 'success'`)
		require.NoError(t, err)
		_, err = dbPool.Exec(ctx, `INSERT INTO protocols (id, classification_status) VALUES ('noproto', 'success') ON CONFLICT (id) DO UPDATE SET classification_status = 'success'`)
		require.NoError(t, err)

		protocolsModel := data.NewProtocolsModelMock(t)
		protocolContractsModel := data.NewProtocolContractsModelMock(t)
		cdProc := &testCursorAdvancingProcessor{
			testRecordingProcessor: testRecordingProcessor{id: "cdproto", ingestStore: ingestStore, requiresContractData: true},
			dbPool:                 dbPool,
			advanceAtSeq:           102,
			cursorNameFunc:         utils.ProtocolHistoryCursorName,
		}
		noProc := &testCursorAdvancingProcessor{
			testRecordingProcessor: testRecordingProcessor{id: "noproto", ingestStore: ingestStore, requiresContractData: false},
			dbPool:                 dbPool,
			advanceAtSeq:           102,
			cursorNameFunc:         utils.ProtocolHistoryCursorName,
		}

		protocolsModel.On("GetByIDs", mock.Anything, []string{"cdproto", "noproto"}).Return([]data.Protocols{
			{ID: "cdproto", ClassificationStatus: data.StatusSuccess, HistoryMigrationStatus: data.StatusNotStarted},
			{ID: "noproto", ClassificationStatus: data.StatusSuccess, HistoryMigrationStatus: data.StatusNotStarted},
		}, nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"cdproto", "noproto"}, data.StatusInProgress).Return(nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"cdproto", "noproto"}, data.StatusSuccess).Return(nil)

		// Realistic 32-byte hex IDs (the BYTEA scan form): the footprint gate
		// hex-decodes tracked contract IDs and fails the run on malformed ones.
		contractA := data.ProtocolContracts{ContractID: types.HashBytea(strings.Repeat("aa", 32))}
		contractB := data.ProtocolContracts{ContractID: types.HashBytea(strings.Repeat("bb", 32))}
		// Every tracker's membership is re-read at the start of every window,
		// after the window's first ledger has been fetched (WindowSize defaults
		// to 1, so before every ledger's fold): the run-start snapshot returns
		// only A, every window-start refresh returns A+B — simulating live
		// ingestion classifying B between the snapshot and the first window.
		// The event-only tracker refreshes on the same cadence: its processor
		// filters events by membership, so a run-start-only snapshot would
		// silently drop B's events for the rest of the run.
		protocolContractsModel.On("GetByProtocolID", mock.Anything, mock.Anything, "cdproto").Return([]data.ProtocolContracts{contractA}, nil).Once()
		protocolContractsModel.On("GetByProtocolID", mock.Anything, mock.Anything, "cdproto").Return([]data.ProtocolContracts{contractA, contractB}, nil)
		protocolContractsModel.On("GetByProtocolID", mock.Anything, mock.Anything, "noproto").Return([]data.ProtocolContracts{contractA}, nil).Once()
		protocolContractsModel.On("GetByProtocolID", mock.Anything, mock.Anything, "noproto").Return([]data.ProtocolContracts{contractA, contractB}, nil)

		backend := &multiLedgerBackend{
			ledgers: map[uint32]xdr.LedgerCloseMeta{
				100: dummyLedgerMeta(100),
				101: dummyLedgerMeta(101),
				102: dummyLedgerMeta(102),
			},
		}

		svc, err := NewProtocolMigrateHistoryService(ProtocolMigrateHistoryConfig{
			DB: dbPool, LedgerBackend: backend,
			ProtocolsModel: protocolsModel, ProtocolContractsModel: protocolContractsModel,
			IngestStore: ingestStore, NetworkPassphrase: "Test SDF Network ; September 2015",
			Processors: []ProtocolProcessor{cdProc, noProc},
		})
		require.NoError(t, err)

		err = svc.Run(ctx, []string{"cdproto", "noproto"})
		require.NoError(t, err)

		// Requiring processor: every ledger — including the first — folds with
		// the refreshed membership, because the refresh runs at window start,
		// BEFORE the window's first fold, not after the previous window's
		// commit. A post-commit refresh would fold ledger 100 with the stale
		// run-start snapshot (one contract), so these assertions pin the
		// ordering that closes the frontier race with live's classification
		// commit.
		require.Len(t, cdProc.processedInputs, 3)
		for _, input := range cdProc.processedInputs {
			assert.Len(t, input.ProtocolContracts, 2, "ledger %d: window-start membership must include the newly classified contract", input.LedgerSequence)
		}

		// Event-only processor: same window-start refresh cadence — every fold
		// sees the newly classified contract too.
		require.Len(t, noProc.processedInputs, 3)
		for _, input := range noProc.processedInputs {
			assert.Len(t, input.ProtocolContracts, 2, "ledger %d: event-only membership must include the newly classified contract", input.LedgerSequence)
		}
	})

	t.Run("ContractDataChanges left nil when no selected processor requires it", func(t *testing.T) {
		ctx := context.Background()
		dbPool, ingestStore := setupTestDB(t)

		setIngestStoreValue(t, ctx, dbPool, "oldest_ingest_ledger", 100)
		setIngestStoreValue(t, ctx, dbPool, "latest_ingest_ledger", 101)

		_, err := dbPool.Exec(ctx, `INSERT INTO protocols (id, classification_status) VALUES ('noproto2', 'success') ON CONFLICT (id) DO UPDATE SET classification_status = 'success'`)
		require.NoError(t, err)

		protocolsModel := data.NewProtocolsModelMock(t)
		protocolContractsModel := data.NewProtocolContractsModelMock(t)
		noProc := &testCursorAdvancingProcessor{
			testRecordingProcessor: testRecordingProcessor{id: "noproto2", ingestStore: ingestStore, requiresContractData: false},
			dbPool:                 dbPool,
			advanceAtSeq:           101,
			cursorNameFunc:         utils.ProtocolHistoryCursorName,
		}

		protocolsModel.On("GetByIDs", mock.Anything, []string{"noproto2"}).Return([]data.Protocols{
			{ID: "noproto2", ClassificationStatus: data.StatusSuccess, HistoryMigrationStatus: data.StatusNotStarted},
		}, nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"noproto2"}, data.StatusInProgress).Return(nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"noproto2"}, data.StatusSuccess).Return(nil)

		protocolContractsModel.On("GetByProtocolID", mock.Anything, mock.Anything, "noproto2").Return([]data.ProtocolContracts{}, nil)

		backend := &multiLedgerBackend{
			ledgers: map[uint32]xdr.LedgerCloseMeta{
				100: dummyLedgerMeta(100),
				101: dummyLedgerMeta(101),
			},
		}

		svc, err := NewProtocolMigrateHistoryService(ProtocolMigrateHistoryConfig{
			DB: dbPool, LedgerBackend: backend,
			ProtocolsModel: protocolsModel, ProtocolContractsModel: protocolContractsModel,
			IngestStore: ingestStore, NetworkPassphrase: "Test SDF Network ; September 2015",
			Processors: []ProtocolProcessor{noProc},
		})
		require.NoError(t, err)

		err = svc.Run(ctx, []string{"noproto2"})
		require.NoError(t, err)

		// No selected processor requires ContractData, so extraction never ran — the
		// field must stay nil for every recorded input.
		require.Len(t, noProc.processedInputs, 2)
		for _, input := range noProc.processedInputs {
			assert.Nil(t, input.ContractDataChanges, "ledger %d: extraction must be skipped", input.LedgerSequence)
		}
	})
}

func TestProtocolMigrateEngine_WindowedCoalescing(t *testing.T) {
	t.Run("commits once per window and discards a window on handoff", func(t *testing.T) {
		ctx := context.Background()
		dbPool, ingestStore := setupTestDB(t)

		setIngestStoreValue(t, ctx, dbPool, "oldest_ingest_ledger", 100)
		setIngestStoreValue(t, ctx, dbPool, "latest_ingest_ledger", 105)

		_, err := dbPool.Exec(ctx, `INSERT INTO protocols (id, classification_status) VALUES ('testproto', 'success') ON CONFLICT (id) DO UPDATE SET classification_status = 'success'`)
		require.NoError(t, err)

		protocolsModel := data.NewProtocolsModelMock(t)
		protocolContractsModel := data.NewProtocolContractsModelMock(t)
		protocolsModel.On("GetByIDs", mock.Anything, []string{"testproto"}).Return([]data.Protocols{
			{ID: "testproto", ClassificationStatus: data.StatusSuccess, HistoryMigrationStatus: data.StatusNotStarted},
		}, nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusInProgress).Return(nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusSuccess).Return(nil)
		protocolContractsModel.On("GetByProtocolID", mock.Anything, mock.Anything, "testproto").Return([]data.ProtocolContracts{}, nil)

		// Advancing the cursor at 105 makes the second window's CAS fail -> handoff -> loop ends.
		processor := &testCursorAdvancingProcessor{
			testRecordingProcessor: testRecordingProcessor{id: "testproto", ingestStore: ingestStore},
			dbPool:                 dbPool,
			advanceAtSeq:           105,
			cursorNameFunc:         utils.ProtocolHistoryCursorName,
		}

		backend := &multiLedgerBackend{ledgers: map[uint32]xdr.LedgerCloseMeta{
			100: dummyLedgerMeta(100), 101: dummyLedgerMeta(101), 102: dummyLedgerMeta(102),
			103: dummyLedgerMeta(103), 104: dummyLedgerMeta(104), 105: dummyLedgerMeta(105),
		}}

		svc, err := NewProtocolMigrateHistoryService(ProtocolMigrateHistoryConfig{
			DB: dbPool, LedgerBackend: backend,
			ProtocolsModel: protocolsModel, ProtocolContractsModel: protocolContractsModel,
			IngestStore: ingestStore, NetworkPassphrase: "Test SDF Network ; September 2015",
			Processors: []ProtocolProcessor{processor}, WindowSize: 3,
		})
		require.NoError(t, err)
		require.NoError(t, svc.Run(ctx, []string{"testproto"}))

		// All six ledgers folded; only the first window committed (once, at winEnd=102).
		assert.Len(t, processor.processedInputs, 6)
		assert.Equal(t, []uint32{102}, processor.persistedHistorySeqs)
		// Engine owns Reset and calls it at each window boundary: commit at 102, handoff at 105.
		assert.Equal(t, 2, processor.resetCount)

		// Sentinel exists only at the window boundary 102 — not per-ledger.
		_, ok102 := getHistorySentinel(t, ctx, dbPool, "testproto", 102)
		assert.True(t, ok102, "window boundary 102 should be persisted")
		for _, seq := range []uint32{100, 101, 103, 104, 105} {
			_, ok := getHistorySentinel(t, ctx, dbPool, "testproto", seq)
			assert.False(t, ok, "no per-ledger sentinel expected for %d", seq)
		}

		// Cursor reflects the live-ingestion takeover (advanceAtSeq + 100).
		assert.Equal(t, uint32(205), getIngestStoreValue(t, ctx, dbPool, "protocol_testproto_history_cursor"))
	})

	t.Run("tip-shrink flushes a partial window at the live tip", func(t *testing.T) {
		ctx := context.Background()
		dbPool, ingestStore := setupTestDB(t)

		setIngestStoreValue(t, ctx, dbPool, "oldest_ingest_ledger", 100)
		setIngestStoreValue(t, ctx, dbPool, "latest_ingest_ledger", 101)

		_, err := dbPool.Exec(ctx, `INSERT INTO protocols (id, classification_status) VALUES ('testproto', 'success') ON CONFLICT (id) DO UPDATE SET classification_status = 'success'`)
		require.NoError(t, err)

		protocolsModel := data.NewProtocolsModelMock(t)
		protocolContractsModel := data.NewProtocolContractsModelMock(t)
		protocolsModel.On("GetByIDs", mock.Anything, []string{"testproto"}).Return([]data.Protocols{
			{ID: "testproto", ClassificationStatus: data.StatusSuccess, HistoryMigrationStatus: data.StatusNotStarted},
		}, nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusInProgress).Return(nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusSuccess).Return(nil)
		protocolContractsModel.On("GetByProtocolID", mock.Anything, mock.Anything, "testproto").Return([]data.ProtocolContracts{}, nil)

		processor := &testCursorAdvancingProcessor{
			testRecordingProcessor: testRecordingProcessor{id: "testproto", ingestStore: ingestStore},
			dbPool:                 dbPool,
			advanceAtSeq:           101,
			cursorNameFunc:         utils.ProtocolHistoryCursorName,
		}

		backend := &multiLedgerBackend{ledgers: map[uint32]xdr.LedgerCloseMeta{
			100: dummyLedgerMeta(100), 101: dummyLedgerMeta(101),
		}}

		svc, err := NewProtocolMigrateHistoryService(ProtocolMigrateHistoryConfig{
			DB: dbPool, LedgerBackend: backend,
			ProtocolsModel: protocolsModel, ProtocolContractsModel: protocolContractsModel,
			IngestStore: ingestStore, NetworkPassphrase: "Test SDF Network ; September 2015",
			Processors: []ProtocolProcessor{processor}, WindowSize: 10,
		})
		require.NoError(t, err)
		require.NoError(t, svc.Run(ctx, []string{"testproto"})) // returns => tip-shrink flushed; no hang

		assert.Empty(t, processor.persistedHistorySeqs) // partial window's CAS failed -> handoff
		assert.Equal(t, uint32(201), getIngestStoreValue(t, ctx, dbPool, "protocol_testproto_history_cursor"))
	})

	t.Run("tip-shrink flushes a partial window successfully, then blocks at tip", func(t *testing.T) {
		setupCtx := context.Background()
		dbPool, ingestStore := setupTestDB(t)

		setIngestStoreValue(t, setupCtx, dbPool, "oldest_ingest_ledger", 100)
		setIngestStoreValue(t, setupCtx, dbPool, "latest_ingest_ledger", 101)

		_, err := dbPool.Exec(setupCtx, `INSERT INTO protocols (id, classification_status) VALUES ('testproto', 'success') ON CONFLICT (id) DO UPDATE SET classification_status = 'success'`)
		require.NoError(t, err)

		protocolsModel := data.NewProtocolsModelMock(t)
		protocolContractsModel := data.NewProtocolContractsModelMock(t)
		protocolsModel.On("GetByIDs", mock.Anything, []string{"testproto"}).Return([]data.Protocols{
			{ID: "testproto", ClassificationStatus: data.StatusSuccess, HistoryMigrationStatus: data.StatusNotStarted},
		}, nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusInProgress).Return(nil)
		// GetLedger(102) blocks (102 never closes) -> ctx timeout -> run fails. No handoff
		// happened (the partial window's CAS succeeded), so the active protocol is marked failed.
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusFailed).Return(nil)
		protocolContractsModel.On("GetByProtocolID", mock.Anything, mock.Anything, "testproto").Return([]data.ProtocolContracts{}, nil)

		// Plain recording processor (no cursor advance): the partial window's CAS succeeds,
		// so we observe a committed partial flush rather than a handoff.
		processor := &testRecordingProcessor{id: "testproto", ingestStore: ingestStore}

		backend := &multiLedgerBackend{ledgers: map[uint32]xdr.LedgerCloseMeta{
			100: dummyLedgerMeta(100), 101: dummyLedgerMeta(101),
		}}

		svc, err := NewProtocolMigrateHistoryService(ProtocolMigrateHistoryConfig{
			DB: dbPool, LedgerBackend: backend,
			ProtocolsModel: protocolsModel, ProtocolContractsModel: protocolContractsModel,
			IngestStore: ingestStore, NetworkPassphrase: "Test SDF Network ; September 2015",
			Processors: []ProtocolProcessor{processor}, WindowSize: 10,
		})
		require.NoError(t, err)

		// The timeout bounds only the deliberate GetLedger(102) block at the tip — the partial
		// window for [100,101] is flushed and committed before it.
		runCtx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		require.Error(t, svc.Run(runCtx, []string{"testproto"}))

		// The partial window (size 2 < WindowSize 10) committed successfully at winEnd=101,
		// advancing the cursor to the frontier — this is what arms the live-ingestion handoff.
		val, ok := getHistorySentinel(t, setupCtx, dbPool, "testproto", 101)
		require.True(t, ok, "partial window boundary 101 should be persisted")
		assert.Equal(t, uint32(101), val)
		_, ok = getHistorySentinel(t, setupCtx, dbPool, "testproto", 100)
		assert.False(t, ok, "no per-ledger sentinel for 100 — it coalesced into the window")
		assert.Equal(t, []uint32{101}, processor.persistedHistorySeqs)
		assert.Equal(t, 1, processor.resetCount)
		assert.Equal(t, uint32(101), getIngestStoreValue(t, setupCtx, dbPool, "protocol_testproto_history_cursor"))
	})

	t.Run("multiple windows commit then final window hands off", func(t *testing.T) {
		ctx := context.Background()
		dbPool, ingestStore := setupTestDB(t)

		setIngestStoreValue(t, ctx, dbPool, "oldest_ingest_ledger", 100)
		setIngestStoreValue(t, ctx, dbPool, "latest_ingest_ledger", 111)

		_, err := dbPool.Exec(ctx, `INSERT INTO protocols (id, classification_status) VALUES ('testproto', 'success') ON CONFLICT (id) DO UPDATE SET classification_status = 'success'`)
		require.NoError(t, err)

		protocolsModel := data.NewProtocolsModelMock(t)
		protocolContractsModel := data.NewProtocolContractsModelMock(t)
		protocolsModel.On("GetByIDs", mock.Anything, []string{"testproto"}).Return([]data.Protocols{
			{ID: "testproto", ClassificationStatus: data.StatusSuccess, HistoryMigrationStatus: data.StatusNotStarted},
		}, nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusInProgress).Return(nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusSuccess).Return(nil)
		protocolContractsModel.On("GetByProtocolID", mock.Anything, mock.Anything, "testproto").Return([]data.ProtocolContracts{}, nil)

		// Hands off only on the 4th window (at 111); the first three windows commit cleanly.
		processor := &testCursorAdvancingProcessor{
			testRecordingProcessor: testRecordingProcessor{id: "testproto", ingestStore: ingestStore},
			dbPool:                 dbPool,
			advanceAtSeq:           111,
			cursorNameFunc:         utils.ProtocolHistoryCursorName,
		}

		backend := &multiLedgerBackend{ledgers: map[uint32]xdr.LedgerCloseMeta{
			100: dummyLedgerMeta(100), 101: dummyLedgerMeta(101), 102: dummyLedgerMeta(102),
			103: dummyLedgerMeta(103), 104: dummyLedgerMeta(104), 105: dummyLedgerMeta(105),
			106: dummyLedgerMeta(106), 107: dummyLedgerMeta(107), 108: dummyLedgerMeta(108),
			109: dummyLedgerMeta(109), 110: dummyLedgerMeta(110), 111: dummyLedgerMeta(111),
		}}

		svc, err := NewProtocolMigrateHistoryService(ProtocolMigrateHistoryConfig{
			DB: dbPool, LedgerBackend: backend,
			ProtocolsModel: protocolsModel, ProtocolContractsModel: protocolContractsModel,
			IngestStore: ingestStore, NetworkPassphrase: "Test SDF Network ; September 2015",
			Processors: []ProtocolProcessor{processor}, WindowSize: 3,
		})
		require.NoError(t, err)
		require.NoError(t, svc.Run(ctx, []string{"testproto"}))

		// Three full windows [100,102],[103,105],[106,108] commit once each at their winEnd;
		// the fourth window [109,111] is discarded when its CAS loses to the simulated takeover.
		assert.Equal(t, []uint32{102, 105, 108}, processor.persistedHistorySeqs)
		for _, seq := range []uint32{102, 105, 108} {
			_, ok := getHistorySentinel(t, ctx, dbPool, "testproto", seq)
			assert.True(t, ok, "window boundary %d should be persisted", seq)
		}
		for _, seq := range []uint32{100, 101, 103, 104, 106, 107, 109, 110, 111} {
			_, ok := getHistorySentinel(t, ctx, dbPool, "testproto", seq)
			assert.False(t, ok, "no sentinel expected for non-boundary ledger %d", seq)
		}
		// Reset fires on every flushWindow: 3 commits + 1 discarded window = 4.
		assert.Equal(t, 4, processor.resetCount)
		require.Len(t, processor.processedInputs, 12)
		assert.Equal(t, uint32(211), getIngestStoreValue(t, ctx, dbPool, "protocol_testproto_history_cursor"))
	})
}

func TestProtocolMigrateEngine_HeterogeneousCursorResume(t *testing.T) {
	t.Run("each protocol folds from its own cursor position", func(t *testing.T) {
		ctx := context.Background()
		dbPool, ingestStore := setupTestDB(t)

		setIngestStoreValue(t, ctx, dbPool, "oldest_ingest_ledger", 100)
		setIngestStoreValue(t, ctx, dbPool, "latest_ingest_ledger", 104)
		// proto2 has already processed up to 102; proto1 starts fresh from 99.
		setIngestStoreValue(t, ctx, dbPool, utils.ProtocolHistoryCursorName("proto2"), 102)

		_, err := dbPool.Exec(ctx, `INSERT INTO protocols (id, classification_status) VALUES ('proto1', 'success') ON CONFLICT (id) DO UPDATE SET classification_status = 'success'`)
		require.NoError(t, err)
		_, err = dbPool.Exec(ctx, `INSERT INTO protocols (id, classification_status) VALUES ('proto2', 'success') ON CONFLICT (id) DO UPDATE SET classification_status = 'success'`)
		require.NoError(t, err)

		protocolsModel := data.NewProtocolsModelMock(t)
		protocolContractsModel := data.NewProtocolContractsModelMock(t)
		protocolsModel.On("GetByIDs", mock.Anything, []string{"proto1", "proto2"}).Return([]data.Protocols{
			{ID: "proto1", ClassificationStatus: data.StatusSuccess, HistoryMigrationStatus: data.StatusNotStarted},
			{ID: "proto2", ClassificationStatus: data.StatusSuccess, HistoryMigrationStatus: data.StatusNotStarted},
		}, nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"proto1", "proto2"}, data.StatusInProgress).Return(nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"proto1", "proto2"}, data.StatusSuccess).Return(nil)
		protocolContractsModel.On("GetByProtocolID", mock.Anything, mock.Anything, "proto1").Return([]data.ProtocolContracts{}, nil)
		protocolContractsModel.On("GetByProtocolID", mock.Anything, mock.Anything, "proto2").Return([]data.ProtocolContracts{}, nil)

		// Both processors advance at 104 (last available ledger) so both hand off,
		// terminating the loop via tip-shrink+handoff (WindowSize=10 never fills).
		proc1 := &testCursorAdvancingProcessor{
			testRecordingProcessor: testRecordingProcessor{id: "proto1", ingestStore: ingestStore},
			dbPool:                 dbPool,
			advanceAtSeq:           104,
			cursorNameFunc:         utils.ProtocolHistoryCursorName,
		}
		proc2 := &testCursorAdvancingProcessor{
			testRecordingProcessor: testRecordingProcessor{id: "proto2", ingestStore: ingestStore},
			dbPool:                 dbPool,
			advanceAtSeq:           104,
			cursorNameFunc:         utils.ProtocolHistoryCursorName,
		}

		backend := &multiLedgerBackend{ledgers: map[uint32]xdr.LedgerCloseMeta{
			100: dummyLedgerMeta(100), 101: dummyLedgerMeta(101), 102: dummyLedgerMeta(102),
			103: dummyLedgerMeta(103), 104: dummyLedgerMeta(104),
		}}

		svc, err := NewProtocolMigrateHistoryService(ProtocolMigrateHistoryConfig{
			DB: dbPool, LedgerBackend: backend,
			ProtocolsModel: protocolsModel, ProtocolContractsModel: protocolContractsModel,
			IngestStore: ingestStore, NetworkPassphrase: "Test SDF Network ; September 2015",
			Processors: []ProtocolProcessor{proc1, proc2}, WindowSize: 10,
		})
		require.NoError(t, err)
		require.NoError(t, svc.Run(ctx, []string{"proto1", "proto2"}))

		// proto1 starts from 99+1=100; proto2 resumes from 102+1=103.
		require.NotEmpty(t, proc1.processedInputs)
		require.NotEmpty(t, proc2.processedInputs)
		assert.Equal(t, uint32(100), proc1.processedInputs[0].LedgerSequence)
		assert.Equal(t, uint32(103), proc2.processedInputs[0].LedgerSequence)

		// Both hand off at 104 — cursors are advanceAtSeq + 100 = 204.
		assert.Equal(t, uint32(204), getIngestStoreValue(t, ctx, dbPool, "protocol_proto1_history_cursor"))
		assert.Equal(t, uint32(204), getIngestStoreValue(t, ctx, dbPool, "protocol_proto2_history_cursor"))
	})
}

func TestProtocolMigrateEngine_FrontierGate(t *testing.T) {
	t.Run("gate flushes the open window, waits, and resumes when live's cursor advances", func(t *testing.T) {
		ctx := context.Background()
		dbPool, ingestStore := setupTestDB(t)

		setIngestStoreValue(t, ctx, dbPool, "oldest_ingest_ledger", 100)
		// Live has committed through 101: the engine may fold 100-101 but must gate at 102.
		setIngestStoreValue(t, ctx, dbPool, "latest_ingest_ledger", 101)

		_, err := dbPool.Exec(ctx, `INSERT INTO protocols (id, classification_status) VALUES ('testproto', 'success') ON CONFLICT (id) DO UPDATE SET classification_status = 'success'`)
		require.NoError(t, err)

		protocolsModel := data.NewProtocolsModelMock(t)
		protocolContractsModel := data.NewProtocolContractsModelMock(t)
		protocolsModel.On("GetByIDs", mock.Anything, []string{"testproto"}).Return([]data.Protocols{
			{ID: "testproto", ClassificationStatus: data.StatusSuccess, HistoryMigrationStatus: data.StatusNotStarted},
		}, nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusInProgress).Return(nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusSuccess).Return(nil)
		protocolContractsModel.On("GetByProtocolID", mock.Anything, mock.Anything, "testproto").Return([]data.ProtocolContracts{}, nil)

		// Hand off at 103 (the fold hook bumps the cursor to 203, simulating live
		// winning that ledger) so the run terminates after the gate releases.
		processor := &testCursorAdvancingProcessor{
			testRecordingProcessor: testRecordingProcessor{id: "testproto", ingestStore: ingestStore},
			dbPool:                 dbPool,
			advanceAtSeq:           103,
			cursorNameFunc:         utils.ProtocolHistoryCursorName,
		}

		backend := &multiLedgerBackend{ledgers: map[uint32]xdr.LedgerCloseMeta{
			100: dummyLedgerMeta(100), 101: dummyLedgerMeta(101), 102: dummyLedgerMeta(102), 103: dummyLedgerMeta(103),
		}}

		svc, err := NewProtocolMigrateHistoryService(ProtocolMigrateHistoryConfig{
			DB: dbPool, LedgerBackend: backend,
			ProtocolsModel: protocolsModel, ProtocolContractsModel: protocolContractsModel,
			IngestStore: ingestStore, NetworkPassphrase: "Test SDF Network ; September 2015",
			Processors: []ProtocolProcessor{processor}, WindowSize: 10,
		})
		require.NoError(t, err)

		done := make(chan error, 1)
		go func() { done <- svc.Run(ctx, []string{"testproto"}) }()

		// The engine coalesces 100-101 into one open window (WindowSize 10), then gates
		// at 102. The gate must flush that window before waiting: the sentinel for the
		// window end (101) appearing while live's cursor still sits at 101 proves the
		// flush-before-wait ordering.
		require.Eventually(t, func() bool {
			_, ok := getHistorySentinel(t, ctx, dbPool, "testproto", 101)
			return ok
		}, 10*time.Second, 50*time.Millisecond, "gate should flush the open window before waiting")
		_, ok := getHistorySentinel(t, ctx, dbPool, "testproto", 100)
		assert.False(t, ok, "100 should have no per-ledger sentinel: it coalesced into the window flushed at 101")
		assert.Equal(t, uint32(101), getIngestStoreValue(t, ctx, dbPool, "protocol_testproto_history_cursor"),
			"the gate's flush should commit the cursor to the window end before waiting")

		// Live advances past 103; the gate must release and the engine resume folding.
		setIngestStoreValue(t, ctx, dbPool, "latest_ingest_ledger", 103)

		select {
		case runErr := <-done:
			require.NoError(t, runErr)
		case <-time.After(30 * time.Second):
			t.Fatal("engine did not resume after live's cursor advanced past the gate")
		}

		// After resuming it folded 102-103; the fold hook at 103 moved the cursor to 203,
		// so the [102,103] window's CAS failed -> handoff, window discarded.
		_, ok = getHistorySentinel(t, ctx, dbPool, "testproto", 102)
		assert.False(t, ok, "sentinel for 102 should NOT exist (CAS failed -> handoff)")
		assert.Equal(t, []uint32{101}, processor.persistedHistorySeqs)
		assert.Equal(t, uint32(203), getIngestStoreValue(t, ctx, dbPool, "protocol_testproto_history_cursor"))
	})

	t.Run("context cancellation while gated returns cleanly", func(t *testing.T) {
		ctx := context.Background()
		dbPool, ingestStore := setupTestDB(t)

		setIngestStoreValue(t, ctx, dbPool, "oldest_ingest_ledger", 100)
		setIngestStoreValue(t, ctx, dbPool, "latest_ingest_ledger", 101)

		_, err := dbPool.Exec(ctx, `INSERT INTO protocols (id, classification_status) VALUES ('testproto', 'success') ON CONFLICT (id) DO UPDATE SET classification_status = 'success'`)
		require.NoError(t, err)

		protocolsModel := data.NewProtocolsModelMock(t)
		protocolContractsModel := data.NewProtocolContractsModelMock(t)
		protocolsModel.On("GetByIDs", mock.Anything, []string{"testproto"}).Return([]data.Protocols{
			{ID: "testproto", ClassificationStatus: data.StatusSuccess, HistoryMigrationStatus: data.StatusNotStarted},
		}, nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusInProgress).Return(nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusFailed).Return(nil)
		protocolContractsModel.On("GetByProtocolID", mock.Anything, mock.Anything, "testproto").Return([]data.ProtocolContracts{}, nil)

		processor := &testRecordingProcessor{id: "testproto", ingestStore: ingestStore}
		backend := &multiLedgerBackend{ledgers: map[uint32]xdr.LedgerCloseMeta{
			100: dummyLedgerMeta(100), 101: dummyLedgerMeta(101),
		}}

		svc, err := NewProtocolMigrateHistoryService(ProtocolMigrateHistoryConfig{
			DB: dbPool, LedgerBackend: backend,
			ProtocolsModel: protocolsModel, ProtocolContractsModel: protocolContractsModel,
			IngestStore: ingestStore, NetworkPassphrase: "Test SDF Network ; September 2015",
			Processors: []ProtocolProcessor{processor}, WindowSize: 10,
		})
		require.NoError(t, err)

		runCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- svc.Run(runCtx, []string{"testproto"}) }()

		// Wait until the engine is gated at 102 (its window flush at the gate is the signal),
		// then cancel and require a prompt, descriptive error.
		require.Eventually(t, func() bool {
			_, ok := getHistorySentinel(t, ctx, dbPool, "testproto", 101)
			return ok
		}, 10*time.Second, 50*time.Millisecond, "engine should reach the gate and flush before waiting")
		cancel()

		select {
		case runErr := <-done:
			require.Error(t, runErr)
			assert.ErrorContains(t, runErr, "gated on live ingestion's frontier at ledger 102")
		case <-time.After(30 * time.Second):
			t.Fatal("engine did not return after cancellation while gated")
		}
	})

	t.Run("a window never outlives the frontier value that opened it", func(t *testing.T) {
		// Live commits a ledger that classifies a new contract while the engine is
		// mid-window. The crossing at that ledger must close the open window and
		// re-read membership, so the engine folds the new frontier ledgers WITH the
		// new contract — not off the stale window-start snapshot. Uses an event-only
		// processor, so this also pins that membership refresh is not limited to
		// ContractData-requiring trackers.
		ctx := context.Background()
		dbPool, ingestStore := setupTestDB(t)

		setIngestStoreValue(t, ctx, dbPool, "oldest_ingest_ledger", 100)
		// Live has committed through 102 when the run starts.
		setIngestStoreValue(t, ctx, dbPool, "latest_ingest_ledger", 102)

		_, err := dbPool.Exec(ctx, `INSERT INTO protocols (id, classification_status) VALUES ('testproto', 'success') ON CONFLICT (id) DO UPDATE SET classification_status = 'success'`)
		require.NoError(t, err)

		protocolsModel := data.NewProtocolsModelMock(t)
		protocolContractsModel := data.NewProtocolContractsModelMock(t)
		protocolsModel.On("GetByIDs", mock.Anything, []string{"testproto"}).Return([]data.Protocols{
			{ID: "testproto", ClassificationStatus: data.StatusSuccess, HistoryMigrationStatus: data.StatusNotStarted},
		}, nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusInProgress).Return(nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusSuccess).Return(nil)
		// Membership: empty at the run-start load and at the first window's refresh;
		// contract X exists from the refresh after the crossing onward (live's
		// transaction for ledger 104 classified it).
		contractX := data.ProtocolContracts{ContractID: "78787878"}
		protocolContractsModel.On("GetByProtocolID", mock.Anything, mock.Anything, "testproto").Return([]data.ProtocolContracts{}, nil).Twice()
		protocolContractsModel.On("GetByProtocolID", mock.Anything, mock.Anything, "testproto").Return([]data.ProtocolContracts{contractX}, nil)

		// During the fold of 102, "live" commits ledger 104 (classification of X +
		// cursor together); during the fold of 104, live takes the protocol cursor
		// so the run terminates via handoff at the next crossing.
		processor := &testMidWindowClassificationProcessor{
			testRecordingProcessor: testRecordingProcessor{id: "testproto", ingestStore: ingestStore},
			dbPool:                 dbPool,
			advanceLiveAtSeq:       102,
			liveCursorTarget:       104,
			handoffAtSeq:           104,
		}

		backend := &multiLedgerBackend{ledgers: map[uint32]xdr.LedgerCloseMeta{
			100: dummyLedgerMeta(100), 101: dummyLedgerMeta(101), 102: dummyLedgerMeta(102),
			103: dummyLedgerMeta(103), 104: dummyLedgerMeta(104),
		}}

		svc, err := NewProtocolMigrateHistoryService(ProtocolMigrateHistoryConfig{
			DB: dbPool, LedgerBackend: backend,
			ProtocolsModel: protocolsModel, ProtocolContractsModel: protocolContractsModel,
			IngestStore: ingestStore, NetworkPassphrase: "Test SDF Network ; September 2015",
			Processors: []ProtocolProcessor{processor}, WindowSize: 10,
		})
		require.NoError(t, err)
		require.NoError(t, svc.Run(ctx, []string{"testproto"}))

		// The crossing at 103 must have flushed [100,102] as its own window: the
		// engine's committed cursor reached 102 there, and the window's persist ran
		// (sentinel at the window end). Without the flush-at-crossing, 100-104 would
		// have folded as one window whose CAS fails at the handoff — no sentinel at
		// all — and 103-104 would have used the stale membership below.
		val, ok := getHistorySentinel(t, ctx, dbPool, "testproto", 102)
		require.True(t, ok, "the crossing at 103 should have flushed the open window [100,102]")
		assert.Equal(t, uint32(102), val)
		assert.Equal(t, []uint32{102}, processor.persistedHistorySeqs)

		// Ledgers up to the crossing folded without X; the new window's refresh must
		// deliver X to every ledger after it — the engine saw the classification that
		// committed mid-window.
		membershipByLedger := map[uint32][]data.ProtocolContracts{}
		for _, in := range processor.processedInputs {
			membershipByLedger[in.LedgerSequence] = in.ProtocolContracts
		}
		for _, seq := range []uint32{100, 101, 102} {
			assert.Empty(t, membershipByLedger[seq], "ledger %d folded before X was classified", seq)
		}
		for _, seq := range []uint32{103, 104} {
			require.Len(t, membershipByLedger[seq], 1, "ledger %d must fold with the refreshed membership", seq)
			assert.Equal(t, contractX.ContractID, membershipByLedger[seq][0].ContractID)
		}

		// Handoff: live took 104's cursor mid-fold, so the [103,104] window's CAS
		// failed and its persist was discarded.
		_, ok = getHistorySentinel(t, ctx, dbPool, "testproto", 104)
		assert.False(t, ok, "the handed-off window must not persist")
		assert.Equal(t, uint32(104), getIngestStoreValue(t, ctx, dbPool, "protocol_testproto_history_cursor"))
	})
}
