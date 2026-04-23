package services

import (
	"context"
	"fmt"
	"strconv"
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
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/utils"
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
}

func (p *testRecordingProcessor) ProtocolID() string { return p.id }

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

func (p *testRecordingProcessor) LoadCurrentState(_ context.Context, _ pgx.Tx) error {
	return nil
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
			assert.Equal(t, "Test SDF Network ; September 2015", processor.processedInputs[i].NetworkPassphrase)
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

		protocolContractsModel.On("GetByProtocolID", mock.Anything, "testproto").Return([]data.ProtocolContracts{}, nil).Maybe()

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
		protocolContractsModel.On("GetByProtocolID", mock.Anything, "testproto").Return([]data.ProtocolContracts{}, nil)
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

		protocolContractsModel.On("GetByProtocolID", mock.Anything, "testproto").Return([]data.ProtocolContracts{}, nil)

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
		protocolContractsModel.On("GetByProtocolID", mock.Anything, "testproto").Return([]data.ProtocolContracts{}, nil)

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
}
