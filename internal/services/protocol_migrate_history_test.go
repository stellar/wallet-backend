package services

import (
	"context"
	"testing"

	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
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
	protocolContractsModel.On("GetByProtocolID", mock.Anything, "testproto").Return([]data.ProtocolContracts{}, nil)

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
