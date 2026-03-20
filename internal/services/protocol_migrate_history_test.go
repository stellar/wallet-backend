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
)

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

// rangeTrackingBackend wraps multiLedgerBackend and records the sequence of
// PrepareRange calls, capturing whether each was bounded or unbounded.
// An optional onUnbounded callback fires synchronously on the first unbounded
// PrepareRange, allowing tests to inject new ledgers deterministically before
// the subsequent GetLedger call.
type rangeTrackingBackend struct {
	multiLedgerBackend
	mu              sync.Mutex
	ranges          []rangeCall
	onUnbounded     func()
	onUnboundedOnce sync.Once
}

type rangeCall struct {
	bounded bool
	r       ledgerbackend.Range
}

func (b *rangeTrackingBackend) PrepareRange(ctx context.Context, r ledgerbackend.Range) error {
	b.mu.Lock()
	b.ranges = append(b.ranges, rangeCall{bounded: r.Bounded(), r: r})
	b.mu.Unlock()
	if !r.Bounded() && b.onUnbounded != nil {
		b.onUnboundedOnce.Do(b.onUnbounded)
	}
	return b.multiLedgerBackend.PrepareRange(ctx, r)
}

// GetLedger checks for ledgers under the mutex (supporting ledgers added by
// the onUnbounded callback), then falls back to the base blocking behavior.
func (b *rangeTrackingBackend) GetLedger(ctx context.Context, sequence uint32) (xdr.LedgerCloseMeta, error) {
	b.mu.Lock()
	meta, ok := b.multiLedgerBackend.ledgers[sequence]
	b.mu.Unlock()
	if ok {
		return meta, nil
	}
	return b.multiLedgerBackend.GetLedger(ctx, sequence)
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
		_, _ = p.dbPool.ExecContext(ctx,
			`UPDATE ingest_store SET value = $1 WHERE key = $2`,
			strconv.FormatUint(uint64(p.advanceAtSeq+100), 10),
			protocolHistoryCursorName(p.id))
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
	t.Run("happy path — single protocol, 3 ledgers, all CAS succeed", func(t *testing.T) {
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

		svc, err := NewProtocolMigrateHistoryService(ProtocolMigrateHistoryConfig{
			DB: dbPool, LedgerBackend: backend,
			ProtocolsModel: protocolsModel, ProtocolContractsModel: protocolContractsModel,
			IngestStore: ingestStore, NetworkPassphrase: "Test SDF Network ; September 2015",

			Processors: []ProtocolProcessor{processor},
		})
		require.NoError(t, err)

		err = svc.Run(ctx, []string{"testproto"})
		require.NoError(t, err)

		// Verify cursor advanced
		cursorVal := getIngestStoreValue(t, ctx, dbPool, "protocol_testproto_history_cursor")
		assert.Equal(t, uint32(102), cursorVal)

		// Verify PersistHistory actually committed sentinel values to the DB
		for _, seq := range []uint32{100, 101, 102} {
			val, ok := getHistorySentinel(t, ctx, dbPool, "testproto", seq)
			require.True(t, ok, "sentinel for ledger %d should exist", seq)
			assert.Equal(t, seq, val, "sentinel value for ledger %d", seq)
		}

		// Verify processor recorded all inputs
		require.Len(t, processor.processedInputs, 3)
		for i, seq := range []uint32{100, 101, 102} {
			assert.Equal(t, seq, processor.processedInputs[i].LedgerSequence)
			assert.Equal(t, "Test SDF Network ; September 2015", processor.processedInputs[i].NetworkPassphrase)
		}
		assert.Equal(t, []uint32{100, 101, 102}, processor.persistedSeqs)
	})

	t.Run("CAS failure (handoff) — CAS fails at ledger N, status success", func(t *testing.T) {
		ctx := context.Background()
		dbPool, ingestStore := setupTestDB(t)

		setIngestStoreValue(t, ctx, dbPool, "oldest_ingest_ledger", 100)
		setIngestStoreValue(t, ctx, dbPool, "latest_ingest_ledger", 102)
		// Pre-set cursor to 100, so processing starts at 101
		// But we'll simulate CAS failure at 101 by having someone else advance it
		setIngestStoreValue(t, ctx, dbPool, "protocol_testproto_history_cursor", 100)

		_, err := dbPool.ExecContext(ctx, `INSERT INTO protocols (id, classification_status) VALUES ('testproto', 'success') ON CONFLICT (id) DO UPDATE SET classification_status = 'success'`)
		require.NoError(t, err)

		protocolsModel := data.NewProtocolsModelMock(t)
		protocolContractsModel := data.NewProtocolContractsModelMock(t)
		processorMock := NewProtocolProcessorMock(t)

		protocolsModel.On("GetByIDs", ctx, []string{"testproto"}).Return([]data.Protocols{
			{ID: "testproto", ClassificationStatus: data.StatusSuccess, HistoryMigrationStatus: data.StatusNotStarted},
		}, nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusInProgress).Return(nil)
		protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusSuccess).Return(nil)

		protocolContractsModel.On("GetByProtocolID", mock.Anything, "testproto").Return([]data.ProtocolContracts{}, nil).Maybe()

		processorMock.On("ProtocolID").Return("testproto")
		processorMock.On("ProcessLedger", mock.Anything, mock.Anything).Return(nil).Maybe()
		processorMock.On("PersistHistory", mock.Anything, mock.Anything).Return(nil).Maybe()

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

			Processors: []ProtocolProcessor{processorMock},
		})
		require.NoError(t, err)

		// Simulate CAS failure: advance cursor externally before service runs
		setIngestStoreValue(t, ctx, dbPool, "protocol_testproto_history_cursor", 105)

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

		svc, err := NewProtocolMigrateHistoryService(ProtocolMigrateHistoryConfig{
			DB: dbPool, LedgerBackend: backend,
			ProtocolsModel: protocolsModel, ProtocolContractsModel: protocolContractsModel,
			IngestStore: ingestStore, NetworkPassphrase: "Test SDF Network ; September 2015",

			Processors: []ProtocolProcessor{processor},
		})
		require.NoError(t, err)

		err = svc.Run(ctx, []string{"testproto"})
		require.NoError(t, err)

		cursorVal := getIngestStoreValue(t, ctx, dbPool, "protocol_testproto_history_cursor")
		assert.Equal(t, uint32(103), cursorVal)

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

	t.Run("already at tip — cursor equals latest, immediate success", func(t *testing.T) {
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

		svc, err := NewProtocolMigrateHistoryService(ProtocolMigrateHistoryConfig{
			DB: dbPool, LedgerBackend: backend,
			ProtocolsModel: protocolsModel, ProtocolContractsModel: protocolContractsModel,
			IngestStore: ingestStore, NetworkPassphrase: "Test SDF Network ; September 2015",

			Processors: []ProtocolProcessor{processor},
		})
		require.NoError(t, err)

		err = svc.Run(ctx, []string{"testproto"})
		require.NoError(t, err)

		// No processing happened — no sentinels should exist
		_, ok := getHistorySentinel(t, ctx, dbPool, "testproto", 105)
		assert.False(t, ok, "no sentinel should exist when already at tip")
		assert.Empty(t, processor.processedInputs)
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

		svc, err := NewProtocolMigrateHistoryService(ProtocolMigrateHistoryConfig{
			DB: dbPool, LedgerBackend: backend,
			ProtocolsModel: protocolsModel, ProtocolContractsModel: protocolContractsModel,
			IngestStore: ingestStore, NetworkPassphrase: "Test SDF Network ; September 2015",

			Processors: []ProtocolProcessor{proc1, proc2},
		})
		require.NoError(t, err)

		err = svc.Run(ctx, []string{"proto1", "proto2"})
		require.NoError(t, err)

		cursor1 := getIngestStoreValue(t, ctx, dbPool, "protocol_proto1_history_cursor")
		cursor2 := getIngestStoreValue(t, ctx, dbPool, "protocol_proto2_history_cursor")
		assert.Equal(t, uint32(101), cursor1)
		assert.Equal(t, uint32(101), cursor2)

		// Verify each protocol has independently keyed sentinels
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

		svc, err := NewProtocolMigrateHistoryService(ProtocolMigrateHistoryConfig{
			DB: dbPool, LedgerBackend: backend,
			ProtocolsModel: protocolsModel, ProtocolContractsModel: protocolContractsModel,
			IngestStore: ingestStore, NetworkPassphrase: "Test SDF Network ; September 2015",

			Processors: []ProtocolProcessor{proc1, proc2},
		})
		require.NoError(t, err)

		err = svc.Run(ctx, []string{"proto1", "proto2"})
		require.NoError(t, err)

		cursor1 := getIngestStoreValue(t, ctx, dbPool, "protocol_proto1_history_cursor")
		cursor2 := getIngestStoreValue(t, ctx, dbPool, "protocol_proto2_history_cursor")
		assert.Equal(t, uint32(102), cursor1)
		assert.Equal(t, uint32(102), cursor2)

		// proto1 should process 99-102, proto2 should process 101-102
		require.Len(t, proc1.processedInputs, 4)
		for i, seq := range []uint32{99, 100, 101, 102} {
			assert.Equal(t, seq, proc1.processedInputs[i].LedgerSequence)
		}
		require.Len(t, proc2.processedInputs, 2)
		for i, seq := range []uint32{101, 102} {
			assert.Equal(t, seq, proc2.processedInputs[i].LedgerSequence)
		}

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

		svc, err := NewProtocolMigrateHistoryService(ProtocolMigrateHistoryConfig{
			DB: dbPool, LedgerBackend: backend,
			ProtocolsModel: protocolsModel, ProtocolContractsModel: protocolContractsModel,
			IngestStore: ingestStore, NetworkPassphrase: "Test SDF Network ; September 2015",

			Processors: []ProtocolProcessor{proc1, proc2},
		})
		require.NoError(t, err)

		err = svc.Run(ctx, []string{"proto1", "proto2"})
		require.NoError(t, err)

		// proto2 should have processed all 3 ledgers
		cursor2 := getIngestStoreValue(t, ctx, dbPool, "protocol_proto2_history_cursor")
		assert.Equal(t, uint32(102), cursor2)
		assert.Equal(t, []uint32{100, 101, 102}, proc2.persistedSeqs)

		// proto1 should have processed only ledger 100 (then CAS failed, handed off)
		require.Len(t, proc1.processedInputs, 1)
		assert.Equal(t, uint32(100), proc1.processedInputs[0].LedgerSequence)
		// proto1 PersistHistory was NOT called because CAS failed
		assert.Empty(t, proc1.persistedSeqs)

		// Verify proto2 sentinels exist for all ledgers
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

	t.Run("transient PrepareRange error retries then converges", func(t *testing.T) {
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
		// First PrepareRange call for the convergence poll will fail transiently.
		// The bounded-range PrepareRange calls (for processing) always succeed because
		// the counter is only 1 and multiLedgerBackend.PrepareRange is a no-op.
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

		// Verify all ledgers were processed — the transient error did not cause premature convergence.
		cursorVal := getIngestStoreValue(t, ctx, dbPool, "protocol_testproto_history_cursor")
		assert.Equal(t, uint32(101), cursorVal)
		assert.Equal(t, []uint32{100, 101}, processor.persistedSeqs)
	})

	t.Run("transient GetLedger error retries then converges", func(t *testing.T) {
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
		// First GetLedger call for the convergence poll (ledger 102, which doesn't exist)
		// will fail transiently instead of blocking until context done.
		backend.missingGetLedgerFailsLeft.Store(1)

		svc, err := NewProtocolMigrateHistoryService(ProtocolMigrateHistoryConfig{
			DB: dbPool, LedgerBackend: backend,
			ProtocolsModel: protocolsModel, ProtocolContractsModel: protocolContractsModel,
			IngestStore: ingestStore, NetworkPassphrase: "Test SDF Network ; September 2015",
			Processors: []ProtocolProcessor{processor},
		})
		require.NoError(t, err)

		err = svc.Run(ctx, []string{"testproto"})
		require.NoError(t, err)

		// Verify all ledgers were processed — the transient error did not cause premature convergence.
		cursorVal := getIngestStoreValue(t, ctx, dbPool, "protocol_testproto_history_cursor")
		assert.Equal(t, uint32(101), cursorVal)
		assert.Equal(t, []uint32{100, 101}, processor.persistedSeqs)
	})

	t.Run("tip advances during convergence poll triggers bounded-unbounded-bounded transition", func(t *testing.T) {
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

		backend := &rangeTrackingBackend{
			multiLedgerBackend: multiLedgerBackend{
				ledgers: map[uint32]xdr.LedgerCloseMeta{
					100: dummyLedgerMeta(100),
					101: dummyLedgerMeta(101),
				},
			},
		}

		// When the service reaches the convergence poll and calls
		// PrepareRange(UnboundedRange), this callback fires synchronously
		// to simulate tip advancement: it adds new ledgers and updates the
		// ingest store before GetLedger is called.
		backend.onUnbounded = func() {
			backend.mu.Lock()
			backend.multiLedgerBackend.ledgers[102] = dummyLedgerMeta(102)
			backend.multiLedgerBackend.ledgers[103] = dummyLedgerMeta(103)
			backend.mu.Unlock()
			setIngestStoreValue(t, ctx, dbPool, "latest_ingest_ledger", 103)
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

		// Verify Bounded → Unbounded → Bounded range transition sequence
		backend.mu.Lock()
		ranges := make([]rangeCall, len(backend.ranges))
		copy(ranges, backend.ranges)
		backend.mu.Unlock()

		require.GreaterOrEqual(t, len(ranges), 3, "expected at least 3 PrepareRange calls, got %d", len(ranges))
		assert.True(t, ranges[0].bounded, "first PrepareRange should be bounded")
		assert.False(t, ranges[1].bounded, "second PrepareRange should be unbounded (convergence poll)")
		assert.True(t, ranges[2].bounded, "third PrepareRange should be bounded (re-entered loop after tip advance)")

		// Verify all ledgers 100-103 were processed and persisted
		cursorVal := getIngestStoreValue(t, ctx, dbPool, "protocol_testproto_history_cursor")
		assert.Equal(t, uint32(103), cursorVal)
		assert.Equal(t, []uint32{100, 101, 102, 103}, processor.persistedSeqs)

		for _, seq := range []uint32{100, 101, 102, 103} {
			val, ok := getHistorySentinel(t, ctx, dbPool, "testproto", seq)
			require.True(t, ok, "sentinel for ledger %d should exist", seq)
			assert.Equal(t, seq, val, "sentinel value for ledger %d", seq)
		}
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
