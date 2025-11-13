package services

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	"github.com/stellar/go/keypair"
	"github.com/stellar/go/network"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/apptracker"
	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/signing/store"
	cache "github.com/stellar/wallet-backend/internal/store"
)

const (
	testInnerTxHash        = "2ba55208f8ccc21e67c3c515ee8e793bfebb5ff20e3a14264c0890897560e368"
	testInnerTxXDR         = "AAAAAgAAAAC//CoiAsv/SQfZHUYwg1/5F127eo+Rv6b9lf6GbIJNygAAAGQAAAAAAAAAAgAAAAEAAAAAAAAAAAAAAABoI7JqAAAAAAAAAAEAAAAAAAAAAAAAAADcrhjQMMeoVosXGSgLrC4WhXYLHl1HcUniEWKOGTyPEAAAAAAAmJaAAAAAAAAAAAFsgk3KAAAAQEYaesICeGfKcUiMEYoZZrKptMmMcW8636peWLpChKukfqTxSujQilalxe6ab+en9Bhf8iGMF8jb5JqIIYlYjQs="
	testFeeBumpTxHash      = "b99e17610372fd66968cc3124c4d29a9dd856c2b8ffa1c446f6aefc5657f5a82"
	testFeeBumpTxXDR       = "AAAABQAAAACRDhlb19H9O6EVQnLPSBX5kH4+ycO03nl6OOK1drSinwAAAAAAAAGQAAAAAgAAAAC//CoiAsv/SQfZHUYwg1/5F127eo+Rv6b9lf6GbIJNygAAAGQAAAAAAAAAAgAAAAEAAAAAAAAAAAAAAABoI7JqAAAAAAAAAAEAAAAAAAAAAAAAAADcrhjQMMeoVosXGSgLrC4WhXYLHl1HcUniEWKOGTyPEAAAAAAAmJaAAAAAAAAAAAFsgk3KAAAAQEYaesICeGfKcUiMEYoZZrKptMmMcW8636peWLpChKukfqTxSujQilalxe6ab+en9Bhf8iGMF8jb5JqIIYlYjQsAAAAAAAAAAXa0op8AAABADjCsmF/xr9jXwNStUM7YqXEd49qfbvGZPJPplANW7aiErkHWxEj6C2RVOyPyK8KBr1fjCleBSmDZjD1X0kkJCQ=="
	defaultGetLedgersLimit = 50
)

func Test_getLedgerSeqRange(t *testing.T) {
	const getLedgersLimit = 50
	testCases := []struct {
		name               string
		latestLedgerSynced uint32
		rpcOldestLedger    uint32
		rpcNewestLedger    uint32
		wantInSync         bool
		wantResult         LedgerSeqRange
	}{
		{
			name:               "latest_synced_behind_rpc_oldest",
			latestLedgerSynced: 5,
			rpcOldestLedger:    10,
			rpcNewestLedger:    20,
			wantInSync:         false,
			wantResult: LedgerSeqRange{
				StartLedger: 10,
				Limit:       getLedgersLimit,
			},
		},
		{
			name:               "latest_synced_equals_rpc_oldest",
			latestLedgerSynced: 10,
			rpcOldestLedger:    10,
			rpcNewestLedger:    20,
			wantInSync:         false,
			wantResult: LedgerSeqRange{
				StartLedger: 11,
				Limit:       getLedgersLimit,
			},
		},
		{
			name:               "latest_synced_ahead_of_rpc_oldest",
			rpcOldestLedger:    10,
			rpcNewestLedger:    20,
			latestLedgerSynced: 15,
			wantInSync:         false,
			wantResult: LedgerSeqRange{
				StartLedger: 16,
				Limit:       getLedgersLimit,
			},
		},
		{
			name:               "latest_synced_equals_rpc_newest",
			rpcOldestLedger:    10,
			rpcNewestLedger:    20,
			latestLedgerSynced: 20,
			wantInSync:         true,
			wantResult:         LedgerSeqRange{},
		},
		{
			name:               "latest_synced_ahead_of_rpc_newest",
			rpcOldestLedger:    10,
			rpcNewestLedger:    20,
			latestLedgerSynced: 25,
			wantInSync:         true,
			wantResult:         LedgerSeqRange{},
		},
	}

	ingestService := &ingestService{getLedgersLimit: getLedgersLimit}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ledgerRange, inSync := ingestService.getLedgerSeqRange(tc.rpcOldestLedger, tc.rpcNewestLedger, tc.latestLedgerSynced)
			assert.Equal(t, tc.wantResult, ledgerRange)
			assert.Equal(t, tc.wantInSync, inSync)
		})
	}
}

func TestGetLedgerTransactions(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := metrics.NewMockMetricsService()
	mockMetricsService.On("RegisterPoolMetrics", "ledger_indexer", mock.Anything).Return()
	mockMetricsService.On("RegisterPoolMetrics", "ingest", mock.Anything).Return()
	models, err := data.NewModels(dbConnectionPool, mockMetricsService)
	require.NoError(t, err)
	mockAppTracker := apptracker.MockAppTracker{}
	mockRPCService := RPCServiceMock{}
	mockRPCService.On("NetworkPassphrase").Return(network.TestNetworkPassphrase)
	mockChAccStore := &store.ChannelAccountStoreMock{}
	mockContractStore := &cache.MockTokenContractStore{}
	ingestService, err := NewIngestService(models, "ingestionLedger", &mockAppTracker, &mockRPCService, mockChAccStore, mockContractStore, mockMetricsService, defaultGetLedgersLimit, network.TestNetworkPassphrase)
	require.NoError(t, err)
	t.Run("all_ledger_transactions_in_single_gettransactions_call", func(t *testing.T) {
		defer mockMetricsService.AssertExpectations(t)

		rpcGetTransactionsResult := entities.RPCGetTransactionsResult{
			Cursor: "51",
			Transactions: []entities.Transaction{
				{
					Status: entities.SuccessStatus,
					Hash:   "hash1",
					Ledger: 1,
				},
				{
					Status: entities.FailedStatus,
					Hash:   "hash2",
					Ledger: 2,
				},
			},
		}
		mockRPCService.
			On("GetTransactions", int64(1), "", 50).
			Return(rpcGetTransactionsResult, nil).
			Once()

		txns, err := ingestService.GetLedgerTransactions(1)
		assert.Equal(t, 1, len(txns))
		assert.Equal(t, txns[0].Hash, "hash1")
		assert.NoError(t, err)
	})

	t.Run("ledger_transactions_split_between_multiple_gettransactions_calls", func(t *testing.T) {
		defer mockMetricsService.AssertExpectations(t)

		rpcGetTransactionsResult1 := entities.RPCGetTransactionsResult{
			Cursor: "51",
			Transactions: []entities.Transaction{
				{
					Status: entities.SuccessStatus,
					Hash:   "hash1",
					Ledger: 1,
				},
				{
					Status: entities.FailedStatus,
					Hash:   "hash2",
					Ledger: 1,
				},
			},
		}
		rpcGetTransactionsResult2 := entities.RPCGetTransactionsResult{
			Cursor: "51",
			Transactions: []entities.Transaction{
				{
					Status: entities.SuccessStatus,
					Hash:   "hash3",
					Ledger: 1,
				},
				{
					Status: entities.FailedStatus,
					Hash:   "hash4",
					Ledger: 2,
				},
			},
		}

		mockRPCService.
			On("GetTransactions", int64(1), "", 50).
			Return(rpcGetTransactionsResult1, nil).
			Once()

		mockRPCService.
			On("GetTransactions", int64(1), "51", 50).
			Return(rpcGetTransactionsResult2, nil).
			Once()

		txns, err := ingestService.GetLedgerTransactions(1)
		assert.Equal(t, 3, len(txns))
		assert.Equal(t, txns[0].Hash, "hash1")
		assert.Equal(t, txns[1].Hash, "hash2")
		assert.Equal(t, txns[2].Hash, "hash3")
		assert.NoError(t, err)
	})
}

func TestIngest_LatestSyncedLedgerBehindRPC(t *testing.T) {
	dbt := dbtest.Open(t)
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		require.NoError(t, dbConnectionPool.Close())
		dbt.Close()
	}()

	mockMetricsService := metrics.NewMockMetricsService()
	mockMetricsService.On("RegisterPoolMetrics", "ledger_indexer", mock.Anything).Return()
	mockMetricsService.On("RegisterPoolMetrics", "ingest", mock.Anything).Return()
	mockMetricsService.On("ObserveDBQueryDuration", "UpdateLatestLedgerSynced", "ingest_store", mock.AnythingOfType("float64")).Once()
	mockMetricsService.On("IncDBQuery", "UpdateLatestLedgerSynced", "ingest_store").Once()
	mockMetricsService.On("ObserveDBQueryDuration", "GetLatestLedgerSynced", "ingest_store", mock.AnythingOfType("float64")).Once()
	mockMetricsService.On("IncDBQuery", "GetLatestLedgerSynced", "ingest_store").Once()
	mockMetricsService.On("SetLatestLedgerIngested", float64(50)).Once()
	mockMetricsService.On("ObserveIngestionDuration", "total", mock.AnythingOfType("float64")).Once()
	defer mockMetricsService.AssertExpectations(t)

	models, err := data.NewModels(dbConnectionPool, mockMetricsService)
	require.NoError(t, err)
	mockAppTracker := apptracker.MockAppTracker{}
	mockRPCService := RPCServiceMock{}
	mockRPCService.
		On("TrackRPCServiceHealth", ctx, mock.Anything).Once().
		On("NetworkPassphrase").Return(network.TestNetworkPassphrase)
	mockChAccStore := &store.ChannelAccountStoreMock{}
	mockContractStore := &cache.MockTokenContractStore{}
	ingestService, err := NewIngestService(models, "ingestionLedger", &mockAppTracker, &mockRPCService, mockChAccStore, mockContractStore, mockMetricsService, defaultGetLedgersLimit, network.TestNetworkPassphrase)
	require.NoError(t, err)

	srcAccount := keypair.MustRandom().Address()
	destAccount := keypair.MustRandom().Address()

	transaction, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount: &txnbuild.SimpleAccount{
			AccountID: keypair.MustRandom().Address(),
		},
		Operations: []txnbuild.Operation{&txnbuild.Payment{
			SourceAccount: srcAccount,
			Destination:   destAccount,
			Amount:        "10",
			Asset:         txnbuild.NativeAsset{},
		}},
		Preconditions: txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(10)},
	})
	require.NoError(t, err)
	txHash, err := transaction.HashHex(network.TestNetworkPassphrase)
	require.NoError(t, err)
	mockChAccStore.On("UnassignTxAndUnlockChannelAccounts", mock.Anything, mock.Anything, txHash).Return(int64(1), nil).Once()
	txEnvXDR, err := transaction.Base64()
	require.NoError(t, err)

	mockResult := entities.RPCGetTransactionsResult{
		Transactions: []entities.Transaction{{
			Status:           entities.SuccessStatus,
			Hash:             txHash,
			ApplicationOrder: 1,
			FeeBump:          false,
			EnvelopeXDR:      txEnvXDR,
			ResultXDR:        "AAAAAAAAAMj////9AAAAAA==",
			Ledger:           50,
		}, {
			Status:           entities.SuccessStatus,
			Hash:             "some-other-tx-hash",
			ApplicationOrder: 1,
			FeeBump:          false,
			EnvelopeXDR:      txEnvXDR,
			ResultXDR:        "AAAAAAAAAMj////9AAAAAA==",
			Ledger:           51,
		}},
		LatestLedger:          int64(100),
		LatestLedgerCloseTime: int64(1),
		OldestLedger:          int64(50),
		OldestLedgerCloseTime: int64(1),
	}
	mockRPCService.On("GetTransactions", int64(50), "", 50).Return(mockResult, nil).Once()
	heartbeatChan := make(chan entities.RPCGetHealthResult, 1)
	heartbeatChan <- entities.RPCGetHealthResult{
		Status:       "healthy",
		LatestLedger: 100,
		OldestLedger: 50,
	}
	mockRPCService.On("GetHeartbeatChannel").Return(heartbeatChan)

	err = ingestService.DeprecatedRun(ctx, uint32(49), uint32(50))
	require.NoError(t, err)

	mockRPCService.AssertNotCalled(t, "GetTransactions", int64(49), "", int64(50))
	mockRPCService.AssertExpectations(t)

	ledger, err := models.IngestStore.GetLatestLedgerSynced(context.Background(), "ingestionLedger")
	require.NoError(t, err)
	assert.Equal(t, uint32(50), ledger)
}

func TestIngest_LatestSyncedLedgerAheadOfRPC(t *testing.T) {
	dbt := dbtest.Open(t)
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		require.NoError(t, dbConnectionPool.Close())
		dbt.Close()
		log.DefaultLogger.SetOutput(os.Stderr)
	}()

	mockMetricsService := metrics.NewMockMetricsService()
	mockMetricsService.On("RegisterPoolMetrics", "ledger_indexer", mock.Anything).Return()
	mockMetricsService.On("RegisterPoolMetrics", "ingest", mock.Anything).Return()
	models, err := data.NewModels(dbConnectionPool, mockMetricsService)
	require.NoError(t, err)
	mockAppTracker := apptracker.MockAppTracker{}
	mockRPCService := RPCServiceMock{}
	mockRPCService.
		On("TrackRPCServiceHealth", ctx, mock.Anything).Once().
		On("NetworkPassphrase").Return(network.TestNetworkPassphrase)
	mockChAccStore := &store.ChannelAccountStoreMock{}
	mockChAccStore.On("UnassignTxAndUnlockChannelAccounts", mock.Anything, mock.Anything, testInnerTxHash).Return(int64(1), nil).Twice()
	mockContractStore := &cache.MockTokenContractStore{}
	ingestService, err := NewIngestService(models, "ingestionLedger", &mockAppTracker, &mockRPCService, mockChAccStore, mockContractStore, mockMetricsService, defaultGetLedgersLimit, network.TestNetworkPassphrase)
	require.NoError(t, err)

	mockMetricsService.On("ObserveDBQueryDuration", "UpdateLatestLedgerSynced", "ingest_store", mock.AnythingOfType("float64")).Once()
	mockMetricsService.On("IncDBQuery", "UpdateLatestLedgerSynced", "ingest_store").Once()
	mockMetricsService.On("ObserveDBQueryDuration", "GetLatestLedgerSynced", "ingest_store", mock.AnythingOfType("float64")).Once()
	mockMetricsService.On("IncDBQuery", "GetLatestLedgerSynced", "ingest_store").Once()
	mockMetricsService.On("SetLatestLedgerIngested", float64(100)).Once()
	mockMetricsService.On("ObserveIngestionDuration", "total", mock.AnythingOfType("float64")).Once()
	defer mockMetricsService.AssertExpectations(t)

	heartbeatChan := make(chan entities.RPCGetHealthResult, 1)
	mockRPCService.On("GetHeartbeatChannel").Return(heartbeatChan)

	// Send first heartbeat showing RPC is behind
	heartbeatChan <- entities.RPCGetHealthResult{
		Status:       "healthy",
		LatestLedger: 50,
		OldestLedger: 1,
	}

	// After a delay, send second heartbeat showing RPC has caught up
	go func() {
		time.Sleep(6 * time.Second) // Sleep longer than the service's 5 second wait
		heartbeatChan <- entities.RPCGetHealthResult{
			Status:       "healthy",
			LatestLedger: 100,
			OldestLedger: 50,
		}
	}()

	// Capture debug logs to verify waiting message
	var logBuffer bytes.Buffer
	log.DefaultLogger.SetOutput(&logBuffer)
	log.SetLevel(log.DebugLevel)

	mockResult := entities.RPCGetTransactionsResult{
		Transactions: []entities.Transaction{{
			Status:           entities.SuccessStatus,
			Hash:             testInnerTxHash,
			ApplicationOrder: 1,
			FeeBump:          false,
			EnvelopeXDR:      testInnerTxXDR,
			ResultXDR:        "AAAAAAAAAMj////9AAAAAA==",
			Ledger:           100,
		}, {
			Status:           entities.SuccessStatus,
			Hash:             testFeeBumpTxHash,
			ApplicationOrder: 1,
			FeeBump:          false,
			EnvelopeXDR:      testFeeBumpTxXDR,
			ResultXDR:        "AAAAAAAAAMj////9AAAAAA==",
			Ledger:           101,
		}},
		LatestLedger:          int64(100),
		LatestLedgerCloseTime: int64(1),
		OldestLedger:          int64(50),
		OldestLedgerCloseTime: int64(1),
	}
	mockRPCService.On("GetTransactions", int64(100), "", 50).Return(mockResult, nil).Once()
	mockAppTracker.On("CaptureMessage", mock.Anything).Maybe().Return(nil)

	// Start ingestion at ledger 100 (ahead of RPC's initial position at 50)
	err = ingestService.DeprecatedRun(ctx, uint32(100), uint32(100))
	require.NoError(t, err)

	// Verify the debug log message was written
	logOutput := logBuffer.String()
	expectedLog := "waiting for RPC to catchup to ledger 100 (latest: 50)"
	assert.Contains(t, logOutput, expectedLog)

	// Verify the ledger was eventually processed
	ledger, err := models.IngestStore.GetLatestLedgerSynced(context.Background(), "ingestionLedger")
	require.NoError(t, err)
	assert.Equal(t, uint32(100), ledger)

	mockRPCService.AssertExpectations(t)
}

func Test_ingestService_extractInnerTxHash(t *testing.T) {
	networkPassphrase := network.TestNetworkPassphrase
	sourceAccountKP := keypair.MustRandom()
	destAccountKP := keypair.MustRandom()

	// Create a simple inner transaction
	innerTx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount: &txnbuild.SimpleAccount{
			AccountID: sourceAccountKP.Address(),
			Sequence:  1,
		},
		Operations: []txnbuild.Operation{&txnbuild.CreateAccount{
			Destination: destAccountKP.Address(),
			Amount:      "1",
		}},
		Preconditions:        txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(10)},
		IncrementSequenceNum: true,
		BaseFee:              txnbuild.MinBaseFee,
	})
	require.NoError(t, err)
	innerTx, err = innerTx.Sign(networkPassphrase, sourceAccountKP)
	require.NoError(t, err)

	innerTxHash, err := innerTx.HashHex(networkPassphrase)
	require.NoError(t, err)
	innerTxXDR, err := innerTx.Base64()
	require.NoError(t, err)

	// Create a fee bump transaction
	feeBumpAccountKP := keypair.MustRandom()
	feeBumpTx, err := txnbuild.NewFeeBumpTransaction(txnbuild.FeeBumpTransactionParams{
		Inner:      innerTx,
		FeeAccount: feeBumpAccountKP.Address(),
		BaseFee:    2 * txnbuild.MinBaseFee,
	})
	require.NoError(t, err)
	feeBumpTx, err = feeBumpTx.Sign(networkPassphrase, feeBumpAccountKP)
	require.NoError(t, err)
	feeBumpTxXDR, err := feeBumpTx.Base64()
	require.NoError(t, err)

	ingestSvc := ingestService{rpcService: &rpcService{networkPassphrase: networkPassphrase}}

	t.Run("🟢inner_tx_hash", func(t *testing.T) {
		gotTxHash, err := ingestSvc.extractInnerTxHash(innerTxXDR)
		require.NoError(t, err)
		assert.Equal(t, innerTxHash, gotTxHash)
	})

	t.Run("🟢fee_bump_tx_hash", func(t *testing.T) {
		gotTxHash, err := ingestSvc.extractInnerTxHash(feeBumpTxXDR)
		require.NoError(t, err)
		assert.Equal(t, innerTxHash, gotTxHash)
	})
}

func Test_ingestService_getLedgerTransactions(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()

	const ledgerMetadataWith0Tx = "AAAAAQAAAACB7Zh2o0NTFwl1nvs7xr3SJ7w8PpwnSRb8QyG9k6acEwAAABaeASPlzu/ZFxwyyWsxtGoj3KCrybm2yN7WOweR0BWdLYjyoO5BI41g1PFT+iHW68giP49Koo+q3VmH8I4GdtW2AAAAAGhTTB8AAAAAAAAAAQAAAAC1XRCyu30oTtXAOkel4bWQyQ9Xg1VHHMRQe76CBNI8iwAAAEDSH4sE7cL7UJyOqUo9ZZeNqPT7pt7su8iijHjWYg4MbeFUh/gkGf6N40bZjP/dlIuGXmuEhWoEX0VTV58xOB4C3z9hmASpL9tAVxktxD3XSOp3itxSvEmM6AUkwBS4ERm+pITz+1V1m+3/v6eaEKglCnon3a5xkn02sLltJ9CSzwAAEYIN4Lazp2QAAAAAAAMtYtQzAAAAAAAAAAAAAAAMAAAAZABMS0AAAADIXukLfWC53MCmzxKd/+LBbaYxQkgxATFDLI3hWj7EqWgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAGeASPlzu/ZFxwyyWsxtGoj3KCrybm2yN7WOweR0BWdLQAAAAIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA9yHMAAAAAAAAAAA=="
	const ledgerMetadataWith1Tx = "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"

	testCases := []struct {
		name                    string
		inputLedgerCloseMetaStr string
		wantErrContains         string
		wantResultTxHashes      []string
	}{
		{
			name:                    "🟢successful_transaction_reading_0_tx",
			inputLedgerCloseMetaStr: ledgerMetadataWith0Tx,
		},
		{
			name:                    "🟢successful_transaction_reading_1_tx",
			inputLedgerCloseMetaStr: ledgerMetadataWith1Tx,
			wantErrContains:         "",
			wantResultTxHashes:      []string{"5c54a259cc690d16bc7eb06cd706c468c530e2131eedcb45b5dc370a2ef7204c"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockMetricsService := metrics.NewMockMetricsService()
			mockMetricsService.On("RegisterPoolMetrics", "ledger_indexer", mock.Anything).Return()
			mockMetricsService.On("RegisterPoolMetrics", "ingest", mock.Anything).Return()
			defer mockMetricsService.AssertExpectations(t)
			models, err := data.NewModels(dbConnectionPool, mockMetricsService)
			require.NoError(t, err)

			mockAppTracker := apptracker.MockAppTracker{}
			mockRPCService := RPCServiceMock{}
			mockRPCService.On("NetworkPassphrase").Return(network.TestNetworkPassphrase)
			mockChAccStore := &store.ChannelAccountStoreMock{}
			mockContractStore := &cache.MockTokenContractStore{}
			ingestService, err := NewIngestService(models, "testCursor", &mockAppTracker, &mockRPCService, mockChAccStore, mockContractStore, mockMetricsService, defaultGetLedgersLimit, network.TestNetworkPassphrase)
			require.NoError(t, err)

			var xdrLedgerCloseMeta xdr.LedgerCloseMeta
			err = xdr.SafeUnmarshalBase64(tc.inputLedgerCloseMetaStr, &xdrLedgerCloseMeta)
			require.NoError(t, err)
			transactions, err := ingestService.getLedgerTransactions(ctx, xdrLedgerCloseMeta)

			// Verify results
			if tc.wantErrContains != "" {
				require.Error(t, err)
				assert.ErrorContains(t, err, tc.wantErrContains)
			} else {
				require.NoError(t, err)
				assert.Len(t, transactions, len(tc.wantResultTxHashes))

				// Verify transaction hashes if we have expected results
				if len(tc.wantResultTxHashes) > 0 {
					for i, expectedHash := range tc.wantResultTxHashes {
						assert.Equal(t, expectedHash, transactions[i].Hash.HexString())
					}
				}
			}
		})
	}
}
