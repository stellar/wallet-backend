package services

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/utils"
)

// multiLedgerBackend is a test double that serves ledger meta for a range of ledgers.
// When GetLedger is called for a missing sequence, it fires the optional onMiss
// callback once (which can inject new ledgers or advance DB cursors to simulate
// live ingestion racing ahead), re-checks the map, and blocks on ctx.Done() if
// the ledger is still absent.
type multiLedgerBackend struct {
	ledgers    map[uint32]xdr.LedgerCloseMeta
	onMiss     func(seq uint32)
	onMissOnce sync.Once
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
	if b.onMiss != nil {
		b.onMissOnce.Do(func() { b.onMiss(sequence) })
	}
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

// simulateHandoffOnMiss returns an onMiss callback that injects a dummy ledger
// into the backend and advances all protocol cursors in the DB, simulating live
// ingestion racing ahead. This causes the subsequent CAS to fail, handing off
// each tracker.
func simulateHandoffOnMiss(backend *multiLedgerBackend, dbPool db.ConnectionPool, protocolIDs []string) func(uint32) {
	return func(seq uint32) {
		backend.ledgers[seq] = dummyLedgerMeta(seq)
		for _, pid := range protocolIDs {
			cursor := utils.ProtocolHistoryCursorName(pid)
			//nolint:errcheck
			dbPool.ExecContext(context.Background(),
				`UPDATE ingest_store SET value = $1 WHERE key = $2`,
				strconv.FormatUint(uint64(seq+100), 10), cursor)
		}
	}
}

// transientErrorBackend wraps multiLedgerBackend and injects transient errors
// on the initial PrepareRange call before delegating normally. Simulates RPC
// blips that should be retried.
type transientErrorBackend struct {
	multiLedgerBackend
	// prepareFailsLeft counts how many PrepareRange calls should return a
	// transient error before succeeding.
	prepareFailsLeft atomic.Int32
}

func (b *transientErrorBackend) PrepareRange(ctx context.Context, r ledgerbackend.Range) error {
	if b.prepareFailsLeft.Add(-1) >= 0 {
		return fmt.Errorf("transient RPC error: connection refused")
	}
	return b.multiLedgerBackend.PrepareRange(ctx, r)
}

// rangeTrackingBackend wraps multiLedgerBackend and records every
// PrepareRange call. It also exposes an onMiss hook that fires synchronously
// when GetLedger is called for a ledger that's not yet in the map, letting
// tests inject new ledgers mid-run to simulate tip advancement.
type rangeTrackingBackend struct {
	multiLedgerBackend
	mu         sync.Mutex
	ranges     []rangeCall
	onMiss     func(sequence uint32)
	onMissOnce sync.Once
}

type rangeCall struct {
	bounded bool
	r       ledgerbackend.Range
}

func (b *rangeTrackingBackend) PrepareRange(ctx context.Context, r ledgerbackend.Range) error {
	b.mu.Lock()
	b.ranges = append(b.ranges, rangeCall{bounded: r.Bounded(), r: r})
	b.mu.Unlock()
	return b.multiLedgerBackend.PrepareRange(ctx, r)
}

// GetLedger checks the map, runs the onMiss hook (once) if the ledger isn't
// present so tests can inject new ledgers, then re-checks. If still missing,
// falls back to the base blocking behavior.
func (b *rangeTrackingBackend) GetLedger(ctx context.Context, sequence uint32) (xdr.LedgerCloseMeta, error) {
	b.mu.Lock()
	meta, ok := b.multiLedgerBackend.ledgers[sequence]
	b.mu.Unlock()
	if ok {
		return meta, nil
	}
	if b.onMiss != nil {
		b.onMissOnce.Do(func() { b.onMiss(sequence) })
	}
	b.mu.Lock()
	meta, ok = b.multiLedgerBackend.ledgers[sequence]
	b.mu.Unlock()
	if ok {
		return meta, nil
	}
	return b.multiLedgerBackend.GetLedger(ctx, sequence)
}

// prepareEnforcingBackend wraps multiLedgerBackend and errors on any second
// PrepareRange call. Guards against regressing into the pre-refactor behavior
// where the service called PrepareRange multiple times (which RPCLedgerBackend
// rejects in production).
type prepareEnforcingBackend struct {
	multiLedgerBackend
	prepareCalls atomic.Int32
}

func (b *prepareEnforcingBackend) PrepareRange(ctx context.Context, r ledgerbackend.Range) error {
	if n := b.prepareCalls.Add(1); n > 1 {
		return fmt.Errorf("PrepareRange called %d times; RPCLedgerBackend only accepts one", n)
	}
	return b.multiLedgerBackend.PrepareRange(ctx, r)
}

// recordingProcessor is a test double that records all ProcessLedger inputs
// and writes per-ledger sentinel keys to ingest_store during PersistHistory,
// proving that PersistHistory actually committed data inside the transaction.
type recordingProcessor struct {
	id              string
	ingestStore     *data.IngestStoreModel
	processedInputs []ProtocolProcessorInput
	persistedSeqs   []uint32
	lastProcessed   uint32
}

func (p *recordingProcessor) ProtocolID() string { return p.id }

func (p *recordingProcessor) ProcessLedger(_ context.Context, input ProtocolProcessorInput) error {
	p.processedInputs = append(p.processedInputs, input)
	p.lastProcessed = input.LedgerSequence
	return nil
}

func (p *recordingProcessor) PersistHistory(ctx context.Context, dbTx pgx.Tx) error {
	p.persistedSeqs = append(p.persistedSeqs, p.lastProcessed)
	return p.ingestStore.Update(ctx, dbTx, fmt.Sprintf("test_%s_history_%d", p.id, p.lastProcessed), p.lastProcessed)
}

func (p *recordingProcessor) PersistCurrentState(_ context.Context, _ pgx.Tx) error {
	return nil
}

// cursorAdvancingProcessor simulates live ingestion taking over by advancing
// its own cursor in the DB during ProcessLedger, causing the subsequent CAS to fail.
type cursorAdvancingProcessor struct {
	recordingProcessor
	dbPool       db.ConnectionPool
	advanceAtSeq uint32
}

func (p *cursorAdvancingProcessor) ProcessLedger(ctx context.Context, input ProtocolProcessorInput) error {
	if input.LedgerSequence == p.advanceAtSeq {
		if _, err := p.dbPool.ExecContext(ctx,
			`UPDATE ingest_store SET value = $1 WHERE key = $2`,
			strconv.FormatUint(uint64(p.advanceAtSeq+100), 10),
			utils.ProtocolHistoryCursorName(p.id)); err != nil {
			return fmt.Errorf("advancing cursor for test: %w", err)
		}
	}
	return p.recordingProcessor.ProcessLedger(ctx, input)
}

// errorAtSeqProcessor wraps recordingProcessor and returns an error when
// ProcessLedger is called for a specific ledger sequence.
type errorAtSeqProcessor struct {
	recordingProcessor
	errorAtSeq uint32
}

func (p *errorAtSeqProcessor) ProcessLedger(ctx context.Context, input ProtocolProcessorInput) error {
	if input.LedgerSequence == p.errorAtSeq {
		return fmt.Errorf("simulated error at ledger %d", p.errorAtSeq)
	}
	return p.recordingProcessor.ProcessLedger(ctx, input)
}

func getHistorySentinel(t *testing.T, ctx context.Context, dbPool db.ConnectionPool, protocolID string, seq uint32) (uint32, bool) {
	t.Helper()
	var val uint32
	err := dbPool.GetContext(ctx, &val, `SELECT value FROM ingest_store WHERE key = $1`, fmt.Sprintf("test_%s_history_%d", protocolID, seq))
	if err != nil {
		return 0, false
	}
	return val, true
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

func setupTestDB(t *testing.T) (db.ConnectionPool, *data.IngestStoreModel) {
	t.Helper()
	dbt := dbtest.Open(t)
	t.Cleanup(func() { dbt.Close() })

	dbPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	t.Cleanup(func() { dbPool.Close() })

	mockMetrics := metrics.NewMockMetricsService()
	mockMetrics.On("ObserveDBQueryDuration", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
	mockMetrics.On("IncDBQuery", mock.Anything, mock.Anything).Return().Maybe()
	mockMetrics.On("IncDBQueryError", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()

	ingestStore := &data.IngestStoreModel{DB: dbPool, MetricsService: mockMetrics}
	return dbPool, ingestStore
}

func setIngestStoreValue(t *testing.T, ctx context.Context, dbPool db.ConnectionPool, key string, value uint32) {
	t.Helper()
	_, err := dbPool.ExecContext(ctx, `INSERT INTO ingest_store (key, value) VALUES ($1, $2)
		ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value`, key, strconv.FormatUint(uint64(value), 10))
	require.NoError(t, err)
}

func getIngestStoreValue(t *testing.T, ctx context.Context, dbPool db.ConnectionPool, key string) uint32 {
	t.Helper()
	var val uint32
	err := dbPool.GetContext(ctx, &val, `SELECT value FROM ingest_store WHERE key = $1`, key)
	require.NoError(t, err)
	return val
}

func TestProtocolMigrateHistory(t *testing.T) {
	t.Run("happy path — single protocol, 3 ledgers, CAS handoff after last", func(t *testing.T) {
		ctx := context.Background()
		dbPool, ingestStore := setupTestDB(t)

		// Set up ingest cursors
		setIngestStoreValue(t, ctx, dbPool, "oldest_ingest_ledger", 100)
		setIngestStoreValue(t, ctx, dbPool, "latest_ingest_ledger", 102)

		// Set up protocol in DB
		_, err := dbPool.ExecContext(ctx, `INSERT INTO protocols (id, classification_status) VALUES ('testproto', 'success') ON CONFLICT (id) DO UPDATE SET classification_status = 'success'`)
		require.NoError(t, err)

		protocolsModel := data.NewProtocolsModelMock(t)
		protocolContractsModel := data.NewProtocolContractsModelMock(t)
		processor := &recordingProcessor{id: "testproto", ingestStore: ingestStore}

		protocolsModel.On("GetByIDs", ctx, []string{"testproto"}).Return([]data.Protocols{
			{ID: "testproto", ClassificationStatus: data.StatusSuccess, HistoryMigrationStatus: data.StatusNotStarted},
		}, nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusInProgress).Return(nil)
		protocolContractsModel.On("GetByProtocolID", mock.Anything, "testproto").Return([]data.ProtocolContracts{}, nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusSuccess).Return(nil)

		backend := &multiLedgerBackend{
			ledgers: map[uint32]xdr.LedgerCloseMeta{
				100: dummyLedgerMeta(100),
				101: dummyLedgerMeta(101),
				102: dummyLedgerMeta(102),
			},
		}
		// When the migration requests the first ledger past the available set,
		// simulate live ingestion racing ahead: inject a dummy ledger and advance
		// the cursor so the subsequent CAS fails and the tracker is handed off.
		backend.onMiss = simulateHandoffOnMiss(backend, dbPool, []string{"testproto"})

		svc, err := NewProtocolMigrateHistoryService(ProtocolMigrateHistoryConfig{
			DB: dbPool, LedgerBackend: backend,
			ProtocolsModel: protocolsModel, ProtocolContractsModel: protocolContractsModel,
			IngestStore: ingestStore, NetworkPassphrase: "Test SDF Network ; September 2015",
			Processors: []ProtocolProcessor{processor},
		})
		require.NoError(t, err)

		err = svc.Run(ctx, []string{"testproto"})
		require.NoError(t, err)

		// Verify PersistHistory actually committed sentinel values to the DB
		for _, seq := range []uint32{100, 101, 102} {
			val, ok := getHistorySentinel(t, ctx, dbPool, "testproto", seq)
			require.True(t, ok, "sentinel for ledger %d should exist", seq)
			assert.Equal(t, seq, val, "sentinel value for ledger %d", seq)
		}

		// Verify processor recorded all inputs (100-102 real + 103 dummy handoff ledger)
		require.Len(t, processor.processedInputs, 4)
		for i, seq := range []uint32{100, 101, 102, 103} {
			assert.Equal(t, seq, processor.processedInputs[i].LedgerSequence)
			assert.Equal(t, "Test SDF Network ; September 2015", processor.processedInputs[i].NetworkPassphrase)
		}
		// PersistHistory is called for 100-102 (CAS succeeds) but NOT for 103 (CAS fails → handoff)
		assert.Equal(t, []uint32{100, 101, 102}, processor.persistedSeqs)
	})

	t.Run("CAS failure (handoff) — live ingestion advances cursor, CAS fails, status success", func(t *testing.T) {
		ctx := context.Background()
		dbPool, ingestStore := setupTestDB(t)

		setIngestStoreValue(t, ctx, dbPool, "oldest_ingest_ledger", 100)
		setIngestStoreValue(t, ctx, dbPool, "latest_ingest_ledger", 102)

		_, err := dbPool.ExecContext(ctx, `INSERT INTO protocols (id, classification_status) VALUES ('testproto', 'success') ON CONFLICT (id) DO UPDATE SET classification_status = 'success'`)
		require.NoError(t, err)

		protocolsModel := data.NewProtocolsModelMock(t)
		protocolContractsModel := data.NewProtocolContractsModelMock(t)

		// cursorAdvancingProcessor advances the cursor at ledger 100,
		// simulating live ingestion racing ahead and causing CAS failure.
		processor := &cursorAdvancingProcessor{
			recordingProcessor: recordingProcessor{id: "testproto", ingestStore: ingestStore},
			dbPool:             dbPool,
			advanceAtSeq:       100,
		}

		protocolsModel.On("GetByIDs", ctx, []string{"testproto"}).Return([]data.Protocols{
			{ID: "testproto", ClassificationStatus: data.StatusSuccess, HistoryMigrationStatus: data.StatusNotStarted},
		}, nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusInProgress).Return(nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusSuccess).Return(nil)

		protocolContractsModel.On("GetByProtocolID", mock.Anything, "testproto").Return([]data.ProtocolContracts{}, nil)

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
		require.NoError(t, err) // Handoff is success

		// Only ledger 100 was processed (CAS failed there → handoff)
		require.Len(t, processor.processedInputs, 1)
		assert.Equal(t, uint32(100), processor.processedInputs[0].LedgerSequence)
		assert.Empty(t, processor.persistedSeqs) // CAS failed, PersistHistory not called
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

		_, err := dbPool.ExecContext(ctx, `INSERT INTO protocols (id, classification_status) VALUES ('testproto', 'success') ON CONFLICT (id) DO UPDATE SET classification_status = 'success'`)
		require.NoError(t, err)

		protocolsModel := data.NewProtocolsModelMock(t)
		protocolContractsModel := data.NewProtocolContractsModelMock(t)
		processor := &recordingProcessor{id: "testproto", ingestStore: ingestStore}

		// Mock expects the deduplicated slice (single element), not the duplicated input.
		protocolsModel.On("GetByIDs", ctx, []string{"testproto"}).Return([]data.Protocols{
			{ID: "testproto", ClassificationStatus: data.StatusSuccess, HistoryMigrationStatus: data.StatusNotStarted},
		}, nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusInProgress).Return(nil)
		protocolContractsModel.On("GetByProtocolID", mock.Anything, "testproto").Return([]data.ProtocolContracts{}, nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusSuccess).Return(nil)

		backend := &multiLedgerBackend{
			ledgers: map[uint32]xdr.LedgerCloseMeta{
				100: dummyLedgerMeta(100),
				101: dummyLedgerMeta(101),
			},
		}
		backend.onMiss = simulateHandoffOnMiss(backend, dbPool, []string{"testproto"})

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

		// Each ledger processed exactly once (100, 101 real + 102 handoff dummy).
		require.Len(t, processor.processedInputs, 3)
		assert.Equal(t, uint32(100), processor.processedInputs[0].LedgerSequence)
		assert.Equal(t, uint32(101), processor.processedInputs[1].LedgerSequence)
		// PersistHistory called for 100-101 (CAS succeeds), not for 102 (CAS fails → handoff)
		assert.Equal(t, []uint32{100, 101}, processor.persistedSeqs)
	})

	t.Run("resume from cursor — cursor already at N, process from N+1", func(t *testing.T) {
		ctx := context.Background()
		dbPool, ingestStore := setupTestDB(t)

		setIngestStoreValue(t, ctx, dbPool, "oldest_ingest_ledger", 100)
		setIngestStoreValue(t, ctx, dbPool, "latest_ingest_ledger", 103)
		// Cursor already at 101 (previous partial run)
		setIngestStoreValue(t, ctx, dbPool, "protocol_testproto_history_cursor", 101)

		_, err := dbPool.ExecContext(ctx, `INSERT INTO protocols (id, classification_status) VALUES ('testproto', 'success') ON CONFLICT (id) DO UPDATE SET classification_status = 'success'`)
		require.NoError(t, err)

		protocolsModel := data.NewProtocolsModelMock(t)
		protocolContractsModel := data.NewProtocolContractsModelMock(t)
		processor := &recordingProcessor{id: "testproto", ingestStore: ingestStore}

		protocolsModel.On("GetByIDs", ctx, []string{"testproto"}).Return([]data.Protocols{
			{ID: "testproto", ClassificationStatus: data.StatusSuccess, HistoryMigrationStatus: data.StatusNotStarted},
		}, nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusInProgress).Return(nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusSuccess).Return(nil)

		protocolContractsModel.On("GetByProtocolID", mock.Anything, "testproto").Return([]data.ProtocolContracts{}, nil)

		backend := &multiLedgerBackend{
			ledgers: map[uint32]xdr.LedgerCloseMeta{
				102: dummyLedgerMeta(102),
				103: dummyLedgerMeta(103),
			},
		}
		backend.onMiss = simulateHandoffOnMiss(backend, dbPool, []string{"testproto"})

		svc, err := NewProtocolMigrateHistoryService(ProtocolMigrateHistoryConfig{
			DB: dbPool, LedgerBackend: backend,
			ProtocolsModel: protocolsModel, ProtocolContractsModel: protocolContractsModel,
			IngestStore: ingestStore, NetworkPassphrase: "Test SDF Network ; September 2015",
			Processors: []ProtocolProcessor{processor},
		})
		require.NoError(t, err)

		err = svc.Run(ctx, []string{"testproto"})
		require.NoError(t, err)

		// Verify sentinels exist only for 102, 103 (not 100, 101)
		for _, seq := range []uint32{100, 101} {
			_, ok := getHistorySentinel(t, ctx, dbPool, "testproto", seq)
			assert.False(t, ok, "sentinel for ledger %d should NOT exist (already processed)", seq)
		}
		for _, seq := range []uint32{102, 103} {
			val, ok := getHistorySentinel(t, ctx, dbPool, "testproto", seq)
			require.True(t, ok, "sentinel for ledger %d should exist", seq)
			assert.Equal(t, seq, val, "sentinel value for ledger %d", seq)
		}
		assert.Equal(t, []uint32{102, 103}, processor.persistedSeqs)
	})

	t.Run("error during ProcessLedger — status failed", func(t *testing.T) {
		ctx := context.Background()
		dbPool, ingestStore := setupTestDB(t)

		setIngestStoreValue(t, ctx, dbPool, "oldest_ingest_ledger", 100)
		setIngestStoreValue(t, ctx, dbPool, "latest_ingest_ledger", 101)

		_, err := dbPool.ExecContext(ctx, `INSERT INTO protocols (id, classification_status) VALUES ('testproto', 'success') ON CONFLICT (id) DO UPDATE SET classification_status = 'success'`)
		require.NoError(t, err)

		protocolsModel := data.NewProtocolsModelMock(t)
		protocolContractsModel := data.NewProtocolContractsModelMock(t)
		processorMock := NewProtocolProcessorMock(t)

		protocolsModel.On("GetByIDs", ctx, []string{"testproto"}).Return([]data.Protocols{
			{ID: "testproto", ClassificationStatus: data.StatusSuccess, HistoryMigrationStatus: data.StatusNotStarted},
		}, nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusInProgress).Return(nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusFailed).Return(nil)

		protocolContractsModel.On("GetByProtocolID", mock.Anything, "testproto").Return([]data.ProtocolContracts{}, nil)

		processorMock.On("ProtocolID").Return("testproto")
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

		_, err := dbPool.ExecContext(ctx, `INSERT INTO protocols (id, classification_status) VALUES ('testproto', 'success') ON CONFLICT (id) DO UPDATE SET classification_status = 'success'`)
		require.NoError(t, err)

		protocolsModel := data.NewProtocolsModelMock(t)
		protocolContractsModel := data.NewProtocolContractsModelMock(t)
		processorMock := NewProtocolProcessorMock(t)

		protocolsModel.On("GetByIDs", ctx, []string{"testproto"}).Return([]data.Protocols{
			{ID: "testproto", ClassificationStatus: data.StatusSuccess, HistoryMigrationStatus: data.StatusNotStarted},
		}, nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusInProgress).Return(nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusFailed).Return(nil)

		protocolContractsModel.On("GetByProtocolID", mock.Anything, "testproto").Return([]data.ProtocolContracts{}, nil)

		processorMock.On("ProtocolID").Return("testproto")
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

	t.Run("already at tip — cursor equals latest, handoff on first fetch", func(t *testing.T) {
		ctx := context.Background()
		dbPool, ingestStore := setupTestDB(t)

		setIngestStoreValue(t, ctx, dbPool, "oldest_ingest_ledger", 100)
		setIngestStoreValue(t, ctx, dbPool, "latest_ingest_ledger", 105)
		setIngestStoreValue(t, ctx, dbPool, "protocol_testproto_history_cursor", 105)

		_, err := dbPool.ExecContext(ctx, `INSERT INTO protocols (id, classification_status) VALUES ('testproto', 'success') ON CONFLICT (id) DO UPDATE SET classification_status = 'success'`)
		require.NoError(t, err)

		protocolsModel := data.NewProtocolsModelMock(t)
		protocolContractsModel := data.NewProtocolContractsModelMock(t)
		processor := &recordingProcessor{id: "testproto", ingestStore: ingestStore}

		protocolsModel.On("GetByIDs", ctx, []string{"testproto"}).Return([]data.Protocols{
			{ID: "testproto", ClassificationStatus: data.StatusSuccess, HistoryMigrationStatus: data.StatusNotStarted},
		}, nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusInProgress).Return(nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusSuccess).Return(nil)

		protocolContractsModel.On("GetByProtocolID", mock.Anything, "testproto").Return([]data.ProtocolContracts{}, nil)

		backend := &multiLedgerBackend{ledgers: map[uint32]xdr.LedgerCloseMeta{}}
		// Cursor is already at 105; the loop starts at 106 which doesn't exist.
		// Simulate live ingestion taking over so CAS fails on the first fetch.
		backend.onMiss = simulateHandoffOnMiss(backend, dbPool, []string{"testproto"})

		svc, err := NewProtocolMigrateHistoryService(ProtocolMigrateHistoryConfig{
			DB: dbPool, LedgerBackend: backend,
			ProtocolsModel: protocolsModel, ProtocolContractsModel: protocolContractsModel,
			IngestStore: ingestStore, NetworkPassphrase: "Test SDF Network ; September 2015",
			Processors: []ProtocolProcessor{processor},
		})
		require.NoError(t, err)

		err = svc.Run(ctx, []string{"testproto"})
		require.NoError(t, err)

		// Only the handoff dummy ledger was processed; no sentinels persisted
		// because the CAS fails immediately (cursor was advanced by onMiss).
		require.Len(t, processor.processedInputs, 1)
		assert.Empty(t, processor.persistedSeqs)
	})

	t.Run("multiple protocols — both process each ledger via shared fetch", func(t *testing.T) {
		ctx := context.Background()
		dbPool, ingestStore := setupTestDB(t)

		setIngestStoreValue(t, ctx, dbPool, "oldest_ingest_ledger", 100)
		setIngestStoreValue(t, ctx, dbPool, "latest_ingest_ledger", 101)

		_, err := dbPool.ExecContext(ctx, `INSERT INTO protocols (id, classification_status) VALUES ('proto1', 'success') ON CONFLICT (id) DO UPDATE SET classification_status = 'success'`)
		require.NoError(t, err)
		_, err = dbPool.ExecContext(ctx, `INSERT INTO protocols (id, classification_status) VALUES ('proto2', 'success') ON CONFLICT (id) DO UPDATE SET classification_status = 'success'`)
		require.NoError(t, err)

		protocolsModel := data.NewProtocolsModelMock(t)
		protocolContractsModel := data.NewProtocolContractsModelMock(t)
		proc1 := &recordingProcessor{id: "proto1", ingestStore: ingestStore}
		proc2 := &recordingProcessor{id: "proto2", ingestStore: ingestStore}

		protocolsModel.On("GetByIDs", ctx, []string{"proto1", "proto2"}).Return([]data.Protocols{
			{ID: "proto1", ClassificationStatus: data.StatusSuccess, HistoryMigrationStatus: data.StatusNotStarted},
			{ID: "proto2", ClassificationStatus: data.StatusSuccess, HistoryMigrationStatus: data.StatusNotStarted},
		}, nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"proto1", "proto2"}, data.StatusInProgress).Return(nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"proto1", "proto2"}, data.StatusSuccess).Return(nil)

		protocolContractsModel.On("GetByProtocolID", mock.Anything, "proto1").Return([]data.ProtocolContracts{}, nil)
		protocolContractsModel.On("GetByProtocolID", mock.Anything, "proto2").Return([]data.ProtocolContracts{}, nil)

		backend := &multiLedgerBackend{
			ledgers: map[uint32]xdr.LedgerCloseMeta{
				100: dummyLedgerMeta(100),
				101: dummyLedgerMeta(101),
			},
		}
		backend.onMiss = simulateHandoffOnMiss(backend, dbPool, []string{"proto1", "proto2"})

		svc, err := NewProtocolMigrateHistoryService(ProtocolMigrateHistoryConfig{
			DB: dbPool, LedgerBackend: backend,
			ProtocolsModel: protocolsModel, ProtocolContractsModel: protocolContractsModel,
			IngestStore: ingestStore, NetworkPassphrase: "Test SDF Network ; September 2015",
			Processors: []ProtocolProcessor{proc1, proc2},
		})
		require.NoError(t, err)

		err = svc.Run(ctx, []string{"proto1", "proto2"})
		require.NoError(t, err)

		// Verify each protocol has independently keyed sentinels for the real ledgers
		for _, id := range []string{"proto1", "proto2"} {
			for _, seq := range []uint32{100, 101} {
				val, ok := getHistorySentinel(t, ctx, dbPool, id, seq)
				require.True(t, ok, "sentinel for %s ledger %d should exist", id, seq)
				assert.Equal(t, seq, val, "sentinel value for %s ledger %d", id, seq)
			}
		}
		assert.Equal(t, []uint32{100, 101}, proc1.persistedSeqs)
		assert.Equal(t, []uint32{100, 101}, proc2.persistedSeqs)
	})

	t.Run("protocols at different cursors — each starts from its own position", func(t *testing.T) {
		ctx := context.Background()
		dbPool, ingestStore := setupTestDB(t)

		setIngestStoreValue(t, ctx, dbPool, "oldest_ingest_ledger", 50)
		setIngestStoreValue(t, ctx, dbPool, "latest_ingest_ledger", 102)
		// proto1 cursor at 98, proto2 cursor at 100
		setIngestStoreValue(t, ctx, dbPool, "protocol_proto1_history_cursor", 98)
		setIngestStoreValue(t, ctx, dbPool, "protocol_proto2_history_cursor", 100)

		_, err := dbPool.ExecContext(ctx, `INSERT INTO protocols (id, classification_status) VALUES ('proto1', 'success') ON CONFLICT (id) DO UPDATE SET classification_status = 'success'`)
		require.NoError(t, err)
		_, err = dbPool.ExecContext(ctx, `INSERT INTO protocols (id, classification_status) VALUES ('proto2', 'success') ON CONFLICT (id) DO UPDATE SET classification_status = 'success'`)
		require.NoError(t, err)

		protocolsModel := data.NewProtocolsModelMock(t)
		protocolContractsModel := data.NewProtocolContractsModelMock(t)
		proc1 := &recordingProcessor{id: "proto1", ingestStore: ingestStore}
		proc2 := &recordingProcessor{id: "proto2", ingestStore: ingestStore}

		protocolsModel.On("GetByIDs", ctx, []string{"proto1", "proto2"}).Return([]data.Protocols{
			{ID: "proto1", ClassificationStatus: data.StatusSuccess, HistoryMigrationStatus: data.StatusNotStarted},
			{ID: "proto2", ClassificationStatus: data.StatusSuccess, HistoryMigrationStatus: data.StatusNotStarted},
		}, nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"proto1", "proto2"}, data.StatusInProgress).Return(nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"proto1", "proto2"}, data.StatusSuccess).Return(nil)

		protocolContractsModel.On("GetByProtocolID", mock.Anything, "proto1").Return([]data.ProtocolContracts{}, nil)
		protocolContractsModel.On("GetByProtocolID", mock.Anything, "proto2").Return([]data.ProtocolContracts{}, nil)

		backend := &multiLedgerBackend{
			ledgers: map[uint32]xdr.LedgerCloseMeta{
				99:  dummyLedgerMeta(99),
				100: dummyLedgerMeta(100),
				101: dummyLedgerMeta(101),
				102: dummyLedgerMeta(102),
			},
		}
		backend.onMiss = simulateHandoffOnMiss(backend, dbPool, []string{"proto1", "proto2"})

		svc, err := NewProtocolMigrateHistoryService(ProtocolMigrateHistoryConfig{
			DB: dbPool, LedgerBackend: backend,
			ProtocolsModel: protocolsModel, ProtocolContractsModel: protocolContractsModel,
			IngestStore: ingestStore, NetworkPassphrase: "Test SDF Network ; September 2015",
			Processors: []ProtocolProcessor{proc1, proc2},
		})
		require.NoError(t, err)

		err = svc.Run(ctx, []string{"proto1", "proto2"})
		require.NoError(t, err)

		// proto1 should process 99-102 + 103 handoff, proto2 should process 101-102 + 103 handoff
		require.Len(t, proc1.processedInputs, 5)
		for i, seq := range []uint32{99, 100, 101, 102, 103} {
			assert.Equal(t, seq, proc1.processedInputs[i].LedgerSequence)
		}
		require.Len(t, proc2.processedInputs, 3)
		for i, seq := range []uint32{101, 102, 103} {
			assert.Equal(t, seq, proc2.processedInputs[i].LedgerSequence)
		}

		// PersistHistory called for real ledgers only (CAS fails on 103)
		assert.Equal(t, []uint32{99, 100, 101, 102}, proc1.persistedSeqs)
		assert.Equal(t, []uint32{101, 102}, proc2.persistedSeqs)
	})

	t.Run("one protocol hands off, other continues", func(t *testing.T) {
		ctx := context.Background()
		dbPool, ingestStore := setupTestDB(t)

		setIngestStoreValue(t, ctx, dbPool, "oldest_ingest_ledger", 100)
		setIngestStoreValue(t, ctx, dbPool, "latest_ingest_ledger", 102)

		_, err := dbPool.ExecContext(ctx, `INSERT INTO protocols (id, classification_status) VALUES ('proto1', 'success') ON CONFLICT (id) DO UPDATE SET classification_status = 'success'`)
		require.NoError(t, err)
		_, err = dbPool.ExecContext(ctx, `INSERT INTO protocols (id, classification_status) VALUES ('proto2', 'success') ON CONFLICT (id) DO UPDATE SET classification_status = 'success'`)
		require.NoError(t, err)

		protocolsModel := data.NewProtocolsModelMock(t)
		protocolContractsModel := data.NewProtocolContractsModelMock(t)
		// proc1 advances its own cursor during ProcessLedger at seq 100, causing CAS failure
		proc1 := &cursorAdvancingProcessor{
			recordingProcessor: recordingProcessor{id: "proto1", ingestStore: ingestStore},
			dbPool:             dbPool,
			advanceAtSeq:       100,
		}
		proc2 := &recordingProcessor{id: "proto2", ingestStore: ingestStore}

		protocolsModel.On("GetByIDs", ctx, []string{"proto1", "proto2"}).Return([]data.Protocols{
			{ID: "proto1", ClassificationStatus: data.StatusSuccess, HistoryMigrationStatus: data.StatusNotStarted},
			{ID: "proto2", ClassificationStatus: data.StatusSuccess, HistoryMigrationStatus: data.StatusNotStarted},
		}, nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"proto1", "proto2"}, data.StatusInProgress).Return(nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"proto1", "proto2"}, data.StatusSuccess).Return(nil)

		protocolContractsModel.On("GetByProtocolID", mock.Anything, "proto1").Return([]data.ProtocolContracts{}, nil)
		protocolContractsModel.On("GetByProtocolID", mock.Anything, "proto2").Return([]data.ProtocolContracts{}, nil)

		backend := &multiLedgerBackend{
			ledgers: map[uint32]xdr.LedgerCloseMeta{
				100: dummyLedgerMeta(100),
				101: dummyLedgerMeta(101),
				102: dummyLedgerMeta(102),
			},
		}
		// proto1 hands off via cursorAdvancingProcessor at 100; proto2 still needs
		// handoff after processing 102. onMiss advances proto2's cursor.
		backend.onMiss = simulateHandoffOnMiss(backend, dbPool, []string{"proto2"})

		svc, err := NewProtocolMigrateHistoryService(ProtocolMigrateHistoryConfig{
			DB: dbPool, LedgerBackend: backend,
			ProtocolsModel: protocolsModel, ProtocolContractsModel: protocolContractsModel,
			IngestStore: ingestStore, NetworkPassphrase: "Test SDF Network ; September 2015",
			Processors: []ProtocolProcessor{proc1, proc2},
		})
		require.NoError(t, err)

		err = svc.Run(ctx, []string{"proto1", "proto2"})
		require.NoError(t, err)

		// proto2 should have processed 100-102 real + 103 handoff dummy
		assert.Equal(t, []uint32{100, 101, 102}, proc2.persistedSeqs)

		// proto1 should have processed only ledger 100 (then CAS failed, handed off)
		require.Len(t, proc1.processedInputs, 1)
		assert.Equal(t, uint32(100), proc1.processedInputs[0].LedgerSequence)
		// proto1 PersistHistory was NOT called because CAS failed
		assert.Empty(t, proc1.persistedSeqs)

		// Verify proto2 sentinels exist for the real ledgers
		for _, seq := range []uint32{100, 101, 102} {
			val, ok := getHistorySentinel(t, ctx, dbPool, "proto2", seq)
			require.True(t, ok, "sentinel for proto2 ledger %d should exist", seq)
			assert.Equal(t, seq, val)
		}
	})

	t.Run("multi-protocol failure with handoff — handed-off gets success, other gets failed", func(t *testing.T) {
		ctx := context.Background()
		dbPool, ingestStore := setupTestDB(t)

		setIngestStoreValue(t, ctx, dbPool, "oldest_ingest_ledger", 100)
		setIngestStoreValue(t, ctx, dbPool, "latest_ingest_ledger", 102)

		_, err := dbPool.ExecContext(ctx, `INSERT INTO protocols (id, classification_status) VALUES ('proto1', 'success') ON CONFLICT (id) DO UPDATE SET classification_status = 'success'`)
		require.NoError(t, err)
		_, err = dbPool.ExecContext(ctx, `INSERT INTO protocols (id, classification_status) VALUES ('proto2', 'success') ON CONFLICT (id) DO UPDATE SET classification_status = 'success'`)
		require.NoError(t, err)

		protocolsModel := data.NewProtocolsModelMock(t)
		protocolContractsModel := data.NewProtocolContractsModelMock(t)

		// proto1: hands off via CAS failure at ledger 100
		proc1 := &cursorAdvancingProcessor{
			recordingProcessor: recordingProcessor{id: "proto1", ingestStore: ingestStore},
			dbPool:             dbPool,
			advanceAtSeq:       100,
		}
		// proto2: errors at ledger 101
		proc2 := &errorAtSeqProcessor{
			recordingProcessor: recordingProcessor{id: "proto2", ingestStore: ingestStore},
			errorAtSeq:         101,
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

		protocolContractsModel.On("GetByProtocolID", mock.Anything, "proto1").Return([]data.ProtocolContracts{}, nil)
		protocolContractsModel.On("GetByProtocolID", mock.Anything, "proto2").Return([]data.ProtocolContracts{}, nil)

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

	t.Run("transient PrepareRange error retries then succeeds", func(t *testing.T) {
		ctx := context.Background()
		dbPool, ingestStore := setupTestDB(t)

		setIngestStoreValue(t, ctx, dbPool, "oldest_ingest_ledger", 100)
		setIngestStoreValue(t, ctx, dbPool, "latest_ingest_ledger", 101)

		_, err := dbPool.ExecContext(ctx, `INSERT INTO protocols (id, classification_status) VALUES ('testproto', 'success') ON CONFLICT (id) DO UPDATE SET classification_status = 'success'`)
		require.NoError(t, err)

		protocolsModel := data.NewProtocolsModelMock(t)
		protocolContractsModel := data.NewProtocolContractsModelMock(t)
		processor := &recordingProcessor{id: "testproto", ingestStore: ingestStore}

		protocolsModel.On("GetByIDs", ctx, []string{"testproto"}).Return([]data.Protocols{
			{ID: "testproto", ClassificationStatus: data.StatusSuccess, HistoryMigrationStatus: data.StatusNotStarted},
		}, nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusInProgress).Return(nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusSuccess).Return(nil)
		protocolContractsModel.On("GetByProtocolID", mock.Anything, "testproto").Return([]data.ProtocolContracts{}, nil)

		backend := &transientErrorBackend{
			multiLedgerBackend: multiLedgerBackend{
				ledgers: map[uint32]xdr.LedgerCloseMeta{
					100: dummyLedgerMeta(100),
					101: dummyLedgerMeta(101),
				},
			},
		}
		backend.onMiss = simulateHandoffOnMiss(&backend.multiLedgerBackend, dbPool, []string{"testproto"})
		// First PrepareRange call fails transiently; RetryWithBackoff must retry
		// until success. A second call would be the retry and will succeed.
		backend.prepareFailsLeft.Store(1)

		svc, err := NewProtocolMigrateHistoryService(ProtocolMigrateHistoryConfig{
			DB: dbPool, LedgerBackend: backend,
			ProtocolsModel: protocolsModel, ProtocolContractsModel: protocolContractsModel,
			IngestStore: ingestStore, NetworkPassphrase: "Test SDF Network ; September 2015",
			Processors: []ProtocolProcessor{processor},
		})
		require.NoError(t, err)

		err = svc.Run(ctx, []string{"testproto"})
		require.NoError(t, err)

		// Verify all real ledgers were processed.
		assert.Equal(t, []uint32{100, 101}, processor.persistedSeqs)
	})

	t.Run("tip advances mid-run — PrepareRange called once, new ledgers picked up", func(t *testing.T) {
		ctx := context.Background()
		dbPool, ingestStore := setupTestDB(t)

		setIngestStoreValue(t, ctx, dbPool, "oldest_ingest_ledger", 100)

		_, err := dbPool.ExecContext(ctx, `INSERT INTO protocols (id, classification_status) VALUES ('testproto', 'success') ON CONFLICT (id) DO UPDATE SET classification_status = 'success'`)
		require.NoError(t, err)

		protocolsModel := data.NewProtocolsModelMock(t)
		protocolContractsModel := data.NewProtocolContractsModelMock(t)
		processor := &recordingProcessor{id: "testproto", ingestStore: ingestStore}

		protocolsModel.On("GetByIDs", ctx, []string{"testproto"}).Return([]data.Protocols{
			{ID: "testproto", ClassificationStatus: data.StatusSuccess, HistoryMigrationStatus: data.StatusNotStarted},
		}, nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusInProgress).Return(nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusSuccess).Return(nil)
		protocolContractsModel.On("GetByProtocolID", mock.Anything, "testproto").Return([]data.ProtocolContracts{}, nil)

		backend := &rangeTrackingBackend{
			multiLedgerBackend: multiLedgerBackend{
				ledgers: map[uint32]xdr.LedgerCloseMeta{
					100: dummyLedgerMeta(100),
					101: dummyLedgerMeta(101),
				},
			},
		}

		// When the service first calls GetLedger for a missing sequence (102),
		// inject ledgers 102 and 103 synchronously. The refactored design fetches
		// them on the retry path inside the same GetLedger loop; no extra
		// PrepareRange is needed.
		backend.onMiss = func(_ uint32) {
			backend.mu.Lock()
			backend.multiLedgerBackend.ledgers[102] = dummyLedgerMeta(102)
			backend.multiLedgerBackend.ledgers[103] = dummyLedgerMeta(103)
			backend.mu.Unlock()
		}
		// After 100-103 are processed, ledger 104 misses on the inner backend.
		// Simulate live ingestion handoff so the loop terminates.
		backend.multiLedgerBackend.onMiss = simulateHandoffOnMiss(&backend.multiLedgerBackend, dbPool, []string{"testproto"})

		svc, err := NewProtocolMigrateHistoryService(ProtocolMigrateHistoryConfig{
			DB: dbPool, LedgerBackend: backend,
			ProtocolsModel: protocolsModel, ProtocolContractsModel: protocolContractsModel,
			IngestStore: ingestStore, NetworkPassphrase: "Test SDF Network ; September 2015",
			Processors: []ProtocolProcessor{processor},
		})
		require.NoError(t, err)

		err = svc.Run(ctx, []string{"testproto"})
		require.NoError(t, err)

		// The refactor guarantees PrepareRange is called exactly once with an
		// UnboundedRange — anything else would regress the RPCLedgerBackend fix.
		backend.mu.Lock()
		ranges := make([]rangeCall, len(backend.ranges))
		copy(ranges, backend.ranges)
		backend.mu.Unlock()

		require.Len(t, ranges, 1, "PrepareRange must be called exactly once")
		assert.False(t, ranges[0].bounded, "PrepareRange must be unbounded")

		// All ledgers 100-103 processed plus handoff dummy (104).
		assert.Equal(t, []uint32{100, 101, 102, 103}, processor.persistedSeqs)

		for _, seq := range []uint32{100, 101, 102, 103} {
			val, ok := getHistorySentinel(t, ctx, dbPool, "testproto", seq)
			require.True(t, ok, "sentinel for ledger %d should exist", seq)
			assert.Equal(t, seq, val, "sentinel value for ledger %d", seq)
		}
	})

	t.Run("PrepareRange called exactly once — guards against RPCLedgerBackend re-prepare error", func(t *testing.T) {
		// Simulates RPCLedgerBackend's single-prepare constraint. If the service
		// ever regresses and calls PrepareRange a second time, this backend
		// returns an error and Run fails.
		ctx := context.Background()
		dbPool, ingestStore := setupTestDB(t)

		setIngestStoreValue(t, ctx, dbPool, "oldest_ingest_ledger", 100)

		_, err := dbPool.ExecContext(ctx, `INSERT INTO protocols (id, classification_status) VALUES ('testproto', 'success') ON CONFLICT (id) DO UPDATE SET classification_status = 'success'`)
		require.NoError(t, err)

		protocolsModel := data.NewProtocolsModelMock(t)
		protocolContractsModel := data.NewProtocolContractsModelMock(t)
		processor := &recordingProcessor{id: "testproto", ingestStore: ingestStore}

		protocolsModel.On("GetByIDs", ctx, []string{"testproto"}).Return([]data.Protocols{
			{ID: "testproto", ClassificationStatus: data.StatusSuccess, HistoryMigrationStatus: data.StatusNotStarted},
		}, nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusInProgress).Return(nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusSuccess).Return(nil)
		protocolContractsModel.On("GetByProtocolID", mock.Anything, "testproto").Return([]data.ProtocolContracts{}, nil)

		backend := &prepareEnforcingBackend{
			multiLedgerBackend: multiLedgerBackend{
				ledgers: map[uint32]xdr.LedgerCloseMeta{
					100: dummyLedgerMeta(100),
					101: dummyLedgerMeta(101),
					102: dummyLedgerMeta(102),
				},
			},
		}
		backend.multiLedgerBackend.onMiss = simulateHandoffOnMiss(&backend.multiLedgerBackend, dbPool, []string{"testproto"})

		svc, err := NewProtocolMigrateHistoryService(ProtocolMigrateHistoryConfig{
			DB: dbPool, LedgerBackend: backend,
			ProtocolsModel: protocolsModel, ProtocolContractsModel: protocolContractsModel,
			IngestStore: ingestStore, NetworkPassphrase: "Test SDF Network ; September 2015",
			Processors: []ProtocolProcessor{processor},
		})
		require.NoError(t, err)

		err = svc.Run(ctx, []string{"testproto"})
		require.NoError(t, err)

		assert.Equal(t, int32(1), backend.prepareCalls.Load(), "PrepareRange must be called exactly once")
	})

	t.Run("context cancelled during fetch — returns context error, status failed", func(t *testing.T) {
		// Cancel the parent context from inside a GetLedger onMiss hook (ledger
		// 101 is absent), so cancellation fires while RetryWithBackoff is trying
		// to fetch 101 — before any tracker processes it.
		parentCtx := context.Background()
		dbPool, ingestStore := setupTestDB(t)

		setIngestStoreValue(t, parentCtx, dbPool, "oldest_ingest_ledger", 100)

		_, err := dbPool.ExecContext(parentCtx, `INSERT INTO protocols (id, classification_status) VALUES ('testproto', 'success') ON CONFLICT (id) DO UPDATE SET classification_status = 'success'`)
		require.NoError(t, err)

		protocolsModel := data.NewProtocolsModelMock(t)
		protocolContractsModel := data.NewProtocolContractsModelMock(t)
		processor := &recordingProcessor{id: "testproto", ingestStore: ingestStore}

		protocolsModel.On("GetByIDs", mock.Anything, []string{"testproto"}).Return([]data.Protocols{
			{ID: "testproto", ClassificationStatus: data.StatusSuccess, HistoryMigrationStatus: data.StatusNotStarted},
		}, nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusInProgress).Return(nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusFailed).Return(nil)
		protocolContractsModel.On("GetByProtocolID", mock.Anything, "testproto").Return([]data.ProtocolContracts{}, nil)

		cancelCtx, cancel := context.WithCancel(parentCtx)
		backend := &rangeTrackingBackend{
			multiLedgerBackend: multiLedgerBackend{
				ledgers: map[uint32]xdr.LedgerCloseMeta{
					100: dummyLedgerMeta(100),
				},
			},
		}
		// On the first miss (seq 101 is absent), cancel the parent context.
		// GetLedger then unblocks via ctx.Done() and returns a context error.
		backend.onMiss = func(_ uint32) { cancel() }

		svc, err := NewProtocolMigrateHistoryService(ProtocolMigrateHistoryConfig{
			DB: dbPool, LedgerBackend: backend,
			ProtocolsModel: protocolsModel, ProtocolContractsModel: protocolContractsModel,
			IngestStore: ingestStore, NetworkPassphrase: "Test SDF Network ; September 2015",
			Processors: []ProtocolProcessor{processor},
		})
		require.NoError(t, err)

		err = svc.Run(cancelCtx, []string{"testproto"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "context")

		// Ledger 100 was served before cancellation and should be committed.
		// Ledger 101 was the trigger for cancellation and must not be committed.
		assert.Equal(t, []uint32{100}, processor.persistedSeqs, "only ledger 100 persisted")
		cursorVal := getIngestStoreValue(t, parentCtx, dbPool, "protocol_testproto_history_cursor")
		assert.Equal(t, uint32(100), cursorVal, "cursor advances only for committed ledger 100")
	})

	t.Run("oldest_ingest_ledger is 0 — returns error, does not call backend", func(t *testing.T) {
		// Aditya review comment #8: the "ingestion has not started yet" guard
		// has no test coverage.
		ctx := context.Background()
		dbPool, ingestStore := setupTestDB(t)

		// Do NOT set oldest_ingest_ledger; the ingest store returns 0 by default.

		_, err := dbPool.ExecContext(ctx, `INSERT INTO protocols (id, classification_status) VALUES ('testproto', 'success') ON CONFLICT (id) DO UPDATE SET classification_status = 'success'`)
		require.NoError(t, err)

		protocolsModel := data.NewProtocolsModelMock(t)
		protocolContractsModel := data.NewProtocolContractsModelMock(t)
		processorMock := NewProtocolProcessorMock(t)

		processorMock.On("ProtocolID").Return("testproto")
		protocolsModel.On("GetByIDs", ctx, []string{"testproto"}).Return([]data.Protocols{
			{ID: "testproto", ClassificationStatus: data.StatusSuccess, HistoryMigrationStatus: data.StatusNotStarted},
		}, nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusInProgress).Return(nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusFailed).Return(nil)

		backend := &prepareEnforcingBackend{
			multiLedgerBackend: multiLedgerBackend{ledgers: map[uint32]xdr.LedgerCloseMeta{}},
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
		assert.Contains(t, err.Error(), "ingestion has not started yet")

		// Critical: the backend must not be touched if oldest_ingest_ledger is 0.
		assert.Equal(t, int32(0), backend.prepareCalls.Load(), "PrepareRange should not be called when oldest_ingest_ledger is 0")
	})
}

func TestNewProtocolMigrateHistoryService(t *testing.T) {
	t.Run("nil processor returns error", func(t *testing.T) {
		_, err := NewProtocolMigrateHistoryService(ProtocolMigrateHistoryConfig{
			Processors: []ProtocolProcessor{nil},
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "nil")
	})

	t.Run("duplicate processor ID returns error", func(t *testing.T) {
		proc1 := &ProtocolProcessorMock{}
		proc1.On("ProtocolID").Return("dup")
		proc2 := &ProtocolProcessorMock{}
		proc2.On("ProtocolID").Return("dup")

		_, err := NewProtocolMigrateHistoryService(ProtocolMigrateHistoryConfig{
			Processors: []ProtocolProcessor{proc1, proc2},
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "duplicate")
	})
}
