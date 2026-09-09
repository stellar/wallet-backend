package services

import (
	"context"
	"testing"
	"time"

	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/utils"
)

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

// TestHistoryStrategySpecifics verifies history-specific wiring: uses HistoryMigrationStatus,
// ProtocolHistoryCursorName, PersistHistory, UpdateHistoryMigrationStatus, and reads oldest_ingest_ledger.
func TestHistoryStrategySpecifics(t *testing.T) {
	ctx := context.Background()
	dbPool, ingestStore := setupTestDB(t)

	// History strategy reads oldest_ingest_ledger
	setIngestStoreValue(t, ctx, dbPool, "oldest_ingest_ledger", 100)
	setIngestStoreValue(t, ctx, dbPool, "latest_ingest_ledger", 100)

	_, err := dbPool.Exec(ctx, `INSERT INTO protocols (id, classification_status) VALUES ('testproto', 'success') ON CONFLICT (id) DO UPDATE SET classification_status = 'success'`)
	require.NoError(t, err)

	protocolsModel := data.NewProtocolsModelMock(t)
	protocolContractsModel := data.NewProtocolContractsModelMock(t)
	// Use testCursorAdvancingProcessor to trigger CAS handoff on ledger 100,
	// allowing the unbounded loop to terminate.
	processor := &testCursorAdvancingProcessor{
		testRecordingProcessor: testRecordingProcessor{id: "testproto", ingestStore: ingestStore},
		dbPool:                 dbPool,
		advanceAtSeq:           100,
		cursorNameFunc:         utils.ProtocolHistoryCursorName,
	}

	protocolsModel.On("GetByIDs", mock.Anything, []string{"testproto"}).Return([]data.Protocols{
		{ID: "testproto", ClassificationStatus: data.StatusSuccess, HistoryMigrationStatus: data.StatusNotStarted},
	}, nil)
	// Verify it calls UpdateHistoryMigrationStatus (not current state)
	protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusInProgress).Return(nil)
	protocolsModel.On("UpdateHistoryMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusSuccess).Return(nil)
	protocolContractsModel.On("GetByProtocolID", mock.Anything, mock.Anything, "testproto").Return([]data.ProtocolContracts{}, nil)

	backend := &multiLedgerBackend{
		ledgers: map[uint32]xdr.LedgerCloseMeta{
			100: dummyLedgerMeta(100),
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

	// DB cursor is 200 (advanceAtSeq=100 + 100) — CAS failed on 100 due to handoff.
	// The processor advanced it to simulate live ingestion takeover.
	verifyCtx := context.Background()
	cursorVal := getIngestStoreValue(t, verifyCtx, dbPool, utils.ProtocolHistoryCursorName("testproto"))
	assert.Equal(t, uint32(200), cursorVal)

	// PersistHistory was NOT called because CAS failed on ledger 100
	assert.Empty(t, processor.persistedHistorySeqs)
	assert.Empty(t, processor.persistedCurrentStateSeqs)

	// No history sentinel written (CAS failed)
	_, ok := getHistorySentinel(t, verifyCtx, dbPool, "testproto", 100)
	assert.False(t, ok)

	// Verify no current state sentinel written
	_, ok = getCurrentStateSentinel(t, verifyCtx, dbPool, "testproto", 100)
	assert.False(t, ok)

	protocolsModel.AssertExpectations(t)
}

// TestHistoryMigrationRefusesWhileLockHeld pins the history advisory-lock
// exclusion from the migrate side: a held per-protocol lock (another migration
// or a rebuild) makes Run fail before it reads or marks anything.
func TestHistoryMigrationRefusesWhileLockHeld(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	dbPool, ingestStore := setupTestDB(t)

	svc, err := NewProtocolMigrateHistoryService(ProtocolMigrateHistoryConfig{
		DB: dbPool, LedgerBackend: &multiLedgerBackend{},
		ProtocolsModel: data.NewProtocolsModelMock(t), ProtocolContractsModel: data.NewProtocolContractsModelMock(t),
		IngestStore: ingestStore, NetworkPassphrase: "Test SDF Network ; September 2015",
		Processors: []ProtocolProcessor{&testRecordingProcessor{id: "testproto", ingestStore: ingestStore}},
	})
	require.NoError(t, err)

	// Hold the lock on a raw connection, as a concurrent rebuild would.
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
