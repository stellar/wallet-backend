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

	_, err := dbPool.ExecContext(ctx, `INSERT INTO protocols (id, classification_status) VALUES ('testproto', 'success') ON CONFLICT (id) DO UPDATE SET classification_status = 'success'`)
	require.NoError(t, err)

	protocolsModel := data.NewProtocolsModelMock(t)
	protocolContractsModel := data.NewProtocolContractsModelMock(t)
	processor := &testRecordingProcessor{id: "testproto", ingestStore: ingestStore}

	protocolsModel.On("GetByIDs", ctx, []string{"testproto"}).Return([]data.Protocols{
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

	// Verify history cursor name used
	cursorVal := getIngestStoreValue(t, ctx, dbPool, utils.ProtocolHistoryCursorName("testproto"))
	assert.Equal(t, uint32(100), cursorVal)

	// Verify PersistHistory was called (not PersistCurrentState)
	assert.Equal(t, []uint32{100}, processor.persistedHistorySeqs)
	assert.Empty(t, processor.persistedCurrentStateSeqs)

	// Verify history sentinel written
	val, ok := getHistorySentinel(t, ctx, dbPool, "testproto", 100)
	require.True(t, ok)
	assert.Equal(t, uint32(100), val)

	// Verify no current state sentinel written
	_, ok = getCurrentStateSentinel(t, ctx, dbPool, "testproto", 100)
	assert.False(t, ok)

	protocolsModel.AssertExpectations(t)
}
