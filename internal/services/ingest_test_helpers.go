package services

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/apptracker"
	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/signing/store"
)

var (
	testKP1   = keypair.MustRandom()
	testKP2   = keypair.MustRandom()
	testAddr1 = testKP1.Address()
	testAddr2 = testKP2.Address()
)

const (
	defaultGetLedgersLimit = 50

	// Test hash constants for ingest tests (64-char hex strings for BYTEA storage)
	flushTxHash1 = "f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f101"
	flushTxHash2 = "f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f202"
	flushTxHash3 = "f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f303"
	flushTxHash4 = "f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f404"
	// Test fixtures for ledger metadata
	ledgerMetadataWith0Tx = "AAAAAQAAAACB7Zh2o0NTFwl1nvs7xr3SJ7w8PpwnSRb8QyG9k6acEwAAABaeASPlzu/ZFxwyyWsxtGoj3KCrybm2yN7WOweR0BWdLYjyoO5BI41g1PFT+iHW68giP49Koo+q3VmH8I4GdtW2AAAAAGhTTB8AAAAAAAAAAQAAAAC1XRCyu30oTtXAOkel4bWQyQ9Xg1VHHMRQe76CBNI8iwAAAEDSH4sE7cL7UJyOqUo9ZZeNqPT7pt7su8iijHjWYg4MbeFUh/gkGf6N40bZjP/dlIuGXmuEhWoEX0VTV58xOB4C3z9hmASpL9tAVxktxD3XSOp3itxSvEmM6AUkwBS4ERm+pITz+1V1m+3/v6eaEKglCnon3a5xkn02sLltJ9CSzwAAEYIN4Lazp2QAAAAAAAMtYtQzAAAAAAAAAAAAAAAMAAAAZABMS0AAAADIXukLfWC53MCmzxKd/+LBbaYxQkgxATFDLI3hWj7EqWgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAGeASPlzu/ZFxwyyWsxtGoj3KCrybm2yN7WOweR0BWdLQAAAAIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA9yHMAAAAAAAAAAA=="
	ledgerMetadataWith1Tx = "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"
)

// ==================== Test Helper Functions ====================

// testBackfillOpts configures newTestBackfillService. All fields are optional.
type testBackfillOpts struct {
	models            *data.Models
	factory           LedgerBackendFactory
	batchSize         int
	dbInsertBatchSize int
}

// newTestBackfillService creates an *ingestService with all mocks wired up.
// Pass opts.models = nil for pure mock tests (no DB).
func newTestBackfillService(t *testing.T, opts testBackfillOpts) *ingestService {
	t.Helper()

	mockMetrics := metrics.NewMockMetricsService()
	mockMetrics.On("RegisterPoolMetrics", mock.Anything, mock.Anything).Return().Maybe()
	mockMetrics.On("ObserveDBQueryDuration", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
	mockMetrics.On("IncDBQuery", mock.Anything, mock.Anything).Return().Maybe()
	mockMetrics.On("IncDBTransaction", mock.Anything).Return().Maybe()
	mockMetrics.On("ObserveDBTransactionDuration", mock.Anything, mock.Anything).Return().Maybe()
	mockMetrics.On("ObserveDBBatchSize", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
	mockMetrics.On("IncStateChanges", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
	mockMetrics.On("ObserveIngestionParticipantsCount", mock.Anything).Return().Maybe()
	mockMetrics.On("SetOldestLedgerIngested", mock.Anything).Return().Maybe()
	mockMetrics.On("ObserveIngestionDuration", mock.Anything, mock.Anything).Return().Maybe()
	mockMetrics.On("IncDBQueryError", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
	mockMetrics.On("ObserveStateChangeProcessingDuration", mock.Anything, mock.Anything).Return().Maybe()

	mockRPC := &RPCServiceMock{}
	mockRPC.On("NetworkPassphrase").Return(network.TestNetworkPassphrase).Maybe()

	cfg := IngestServiceConfig{
		IngestionMode:             IngestionModeBackfill,
		Models:                    opts.models,
		LatestLedgerCursorName:    "latest_ledger_cursor",
		OldestLedgerCursorName:    "oldest_ledger_cursor",
		AppTracker:                &apptracker.MockAppTracker{},
		RPCService:                mockRPC,
		LedgerBackend:             &LedgerBackendMock{},
		LedgerBackendFactory:      opts.factory,
		ChannelAccountStore:       &store.ChannelAccountStoreMock{},
		MetricsService:            mockMetrics,
		GetLedgersLimit:           defaultGetLedgersLimit,
		Network:                   network.TestNetworkPassphrase,
		NetworkPassphrase:         network.TestNetworkPassphrase,
		Archive:                   &HistoryArchiveMock{},
		SkipTxMeta:                false,
		SkipTxEnvelope:            false,
		BackfillBatchSize:         opts.batchSize,
		BackfillDBInsertBatchSize: opts.dbInsertBatchSize,
	}

	svc, err := NewIngestService(cfg)
	require.NoError(t, err)
	return svc
}

// makeLedgerCloseMeta creates a minimal valid V1 LedgerCloseMeta with the given
// sequence number and close time.
func makeLedgerCloseMeta(seq uint32, closeTime time.Time) xdr.LedgerCloseMeta {
	return xdr.LedgerCloseMeta{
		V: 1,
		V1: &xdr.LedgerCloseMetaV1{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Header: xdr.LedgerHeader{
					LedgerSeq: xdr.Uint32(seq),
					ScpValue: xdr.StellarValue{
						CloseTime: xdr.TimePoint(closeTime.Unix()),
					},
				},
			},
		},
	}
}

// makeLedgerBuffer creates a *LedgerBuffer with an empty IndexerBuffer and the
// given sequence/time. Useful for watermarkTracker and consumeAndFlush tests.
func makeLedgerBuffer(seq uint32, closeTime time.Time) *LedgerBuffer {
	return &LedgerBuffer{
		buffer:    indexer.NewIndexerBuffer(),
		ledgerSeq: seq,
		closeTime: closeTime,
	}
}

// setupDBCursors sets up ingest_store cursors for testing
func setupDBCursors(t *testing.T, ctx context.Context, pool *pgxpool.Pool, latestLedger, oldestLedger uint32) {
	_, err := pool.Exec(ctx, `DELETE FROM ingest_store`)
	require.NoError(t, err)
	if latestLedger > 0 {
		_, err = pool.Exec(ctx, `INSERT INTO ingest_store (key, value) VALUES ('latest_ledger_cursor', $1)`, fmt.Sprintf("%d", latestLedger))
		require.NoError(t, err)
	}
	if oldestLedger > 0 {
		_, err = pool.Exec(ctx, `INSERT INTO ingest_store (key, value) VALUES ('oldest_ledger_cursor', $1)`, fmt.Sprintf("%d", oldestLedger))
		require.NoError(t, err)
	}
}

// createTestTransaction creates a transaction with required fields for testing.
func createTestTransaction(hash string, toID int64) types.Transaction {
	now := time.Now()
	envelope := "test_envelope_xdr"
	meta := "test_meta_xdr"
	return types.Transaction{
		Hash:            types.HashBytea(hash),
		ToID:            toID,
		EnvelopeXDR:     &envelope,
		FeeCharged:      100,
		ResultCode:      "TransactionResultCodeTxSuccess",
		MetaXDR:         &meta,
		LedgerNumber:    1000,
		LedgerCreatedAt: now,
		IngestedAt:      now,
	}
}

// createTestOperation creates an operation with required fields for testing.
func createTestOperation(id int64) types.Operation {
	now := time.Now()
	return types.Operation{
		ID:              id,
		OperationType:   types.OperationTypePayment,
		OperationXDR:    types.XDRBytea([]byte("test_operation_xdr")),
		LedgerNumber:    1000,
		LedgerCreatedAt: now,
		IngestedAt:      now,
	}
}

// createTestStateChange creates a state change with required fields for testing.
func createTestStateChange(toID int64, accountID string, opID int64) types.StateChange {
	now := time.Now()
	reason := types.StateChangeReasonCredit
	return types.StateChange{
		ToID:                toID,
		StateChangeOrder:    1,
		StateChangeCategory: types.StateChangeCategoryBalance,
		StateChangeReason:   &reason,
		AccountID:           types.AddressBytea(accountID),
		OperationID:         opID,
		LedgerNumber:        1000,
		LedgerCreatedAt:     now,
		IngestedAt:          now,
	}
}
