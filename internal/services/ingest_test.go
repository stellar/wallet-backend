package services

import (
	"context"
	"testing"

	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/txnbuild"
	"github.com/stellar/go-stellar-sdk/xdr"
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
)

const (
	defaultGetLedgersLimit = 50
)

func TestGetLedgerTransactions(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := metrics.NewMockMetricsService()
	mockMetricsService.On("RegisterPoolMetrics", "ledger_indexer", mock.Anything).Return()
	models, err := data.NewModels(dbConnectionPool, mockMetricsService)
	require.NoError(t, err)
	mockAppTracker := apptracker.MockAppTracker{}
	mockRPCService := RPCServiceMock{}
	mockRPCService.On("NetworkPassphrase").Return(network.TestNetworkPassphrase)
	mockChAccStore := &store.ChannelAccountStoreMock{}
	mockLedgerBackend := &LedgerBackendMock{}
	mockArchive := &HistoryArchiveMock{}
	ingestService, err := NewIngestService(models, "ingestionLedger", "accountTokensCursor", &mockAppTracker, &mockRPCService, mockLedgerBackend, mockChAccStore, nil, nil, mockMetricsService, defaultGetLedgersLimit, network.TestNetworkPassphrase, network.TestNetworkPassphrase, mockArchive, false, false, false)
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
			defer mockMetricsService.AssertExpectations(t)
			models, err := data.NewModels(dbConnectionPool, mockMetricsService)
			require.NoError(t, err)

			mockAppTracker := apptracker.MockAppTracker{}
			mockRPCService := RPCServiceMock{}
			mockRPCService.On("NetworkPassphrase").Return(network.TestNetworkPassphrase)
			mockChAccStore := &store.ChannelAccountStoreMock{}
			mockLedgerBackend := &LedgerBackendMock{}
			mockArchive := &HistoryArchiveMock{}
			ingestService, err := NewIngestService(models, "testCursor", "accountTokensCursor", &mockAppTracker, &mockRPCService, mockLedgerBackend, mockChAccStore, nil, nil, mockMetricsService, defaultGetLedgersLimit, network.TestNetworkPassphrase, network.TestNetworkPassphrase, mockArchive, false, false, false)
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
