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
	"github.com/stellar/wallet-backend/internal/utils"
)

func TestNewProtocolMigrateCurrentStateService(t *testing.T) {
	t.Run("nil processor returns error", func(t *testing.T) {
		_, err := NewProtocolMigrateCurrentStateService(ProtocolMigrateCurrentStateConfig{
			StartLedger: 100,
			Processors:  []ProtocolProcessor{nil},
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "nil")
	})

	t.Run("duplicate processor ID returns error", func(t *testing.T) {
		proc1 := &ProtocolProcessorMock{}
		proc1.On("ProtocolID").Return("dup")
		proc2 := &ProtocolProcessorMock{}
		proc2.On("ProtocolID").Return("dup")

		_, err := NewProtocolMigrateCurrentStateService(ProtocolMigrateCurrentStateConfig{
			StartLedger: 100,
			Processors:  []ProtocolProcessor{proc1, proc2},
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "duplicate")
	})

	t.Run("start ledger zero returns error", func(t *testing.T) {
		_, err := NewProtocolMigrateCurrentStateService(ProtocolMigrateCurrentStateConfig{
			StartLedger: 0,
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "start ledger must be > 0")
	})
}

// TestCurrentStateStrategySpecifics verifies current-state-specific wiring: uses
// CurrentStateMigrationStatus, ProtocolCurrentStateCursorName, PersistCurrentState,
// UpdateCurrentStateMigrationStatus, and uses StartLedger config (no oldest_ingest_ledger read).
func TestCurrentStateStrategySpecifics(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	dbPool, ingestStore := setupTestDB(t)

	// Current-state strategy does NOT read oldest_ingest_ledger — uses StartLedger config.
	// Only need latest_ingest_ledger for the processing loop.
	setIngestStoreValue(t, ctx, dbPool, "latest_ingest_ledger", 100)

	_, err := dbPool.Exec(ctx, `INSERT INTO protocols (id, classification_status) VALUES ('testproto', 'success') ON CONFLICT (id) DO UPDATE SET classification_status = 'success'`)
	require.NoError(t, err)

	protocolsModel := data.NewProtocolsModelMock(t)
	protocolContractsModel := data.NewProtocolContractsModelMock(t)
	processor := &testRecordingProcessor{id: "testproto", ingestStore: ingestStore}

	protocolsModel.On("GetByIDs", ctx, []string{"testproto"}).Return([]data.Protocols{
		{ID: "testproto", ClassificationStatus: data.StatusSuccess, CurrentStateMigrationStatus: data.StatusNotStarted},
	}, nil)
	// Verify it calls UpdateCurrentStateMigrationStatus (not history)
	protocolsModel.On("UpdateCurrentStateMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusInProgress).Return(nil)
	// After context cancellation the engine marks the protocol as failed
	protocolsModel.On("UpdateCurrentStateMigrationStatus", mock.Anything, mock.Anything, []string{"testproto"}, data.StatusFailed).Return(nil)
	protocolContractsModel.On("GetByProtocolID", mock.Anything, "testproto").Return([]data.ProtocolContracts{}, nil)

	backend := &multiLedgerBackend{
		ledgers: map[uint32]xdr.LedgerCloseMeta{
			100: dummyLedgerMeta(100),
		},
	}

	svc, err := NewProtocolMigrateCurrentStateService(ProtocolMigrateCurrentStateConfig{
		DB: dbPool, LedgerBackend: backend,
		ProtocolsModel: protocolsModel, ProtocolContractsModel: protocolContractsModel,
		IngestStore: ingestStore, NetworkPassphrase: "Test SDF Network ; September 2015",
		Processors:  []ProtocolProcessor{processor},
		StartLedger: 100,
	})
	require.NoError(t, err)

	// With unbounded range, GetLedger blocks when no more ledgers are available.
	// The context timeout causes the engine to return a context error after
	// processing all available ledgers.
	err = svc.Run(ctx, []string{"testproto"})
	require.Error(t, err)
	require.ErrorIs(t, context.DeadlineExceeded, ctx.Err())

	// Verify current-state cursor advanced for the available ledger
	verifyCtx := context.Background()
	cursorVal := getIngestStoreValue(t, verifyCtx, dbPool, utils.ProtocolCurrentStateCursorName("testproto"))
	assert.Equal(t, uint32(100), cursorVal)

	// Verify PersistCurrentState was called (not PersistHistory)
	assert.Equal(t, []uint32{100}, processor.persistedCurrentStateSeqs)
	assert.Empty(t, processor.persistedHistorySeqs)

	// Verify current state sentinel written
	val, ok := getCurrentStateSentinel(t, verifyCtx, dbPool, "testproto", 100)
	require.True(t, ok)
	assert.Equal(t, uint32(100), val)

	// Verify no history sentinel written
	_, ok = getHistorySentinel(t, verifyCtx, dbPool, "testproto", 100)
	assert.False(t, ok)

	protocolsModel.AssertExpectations(t)
}
