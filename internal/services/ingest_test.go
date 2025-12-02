package services

import (
	"context"
	"testing"

	"github.com/stellar/go/keypair"
	"github.com/stellar/go/network"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/apptracker"
	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/signing/store"
)

const (
	defaultGetLedgersLimit = 50
)

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
			mockMetricsService.On("RegisterPoolMetrics", "backfill", mock.Anything).Return()
			defer mockMetricsService.AssertExpectations(t)
			models, err := data.NewModels(dbConnectionPool, mockMetricsService)
			require.NoError(t, err)

			mockAppTracker := apptracker.MockAppTracker{}
			mockRPCService := RPCServiceMock{}
			mockRPCService.On("NetworkPassphrase").Return(network.TestNetworkPassphrase)
			mockChAccStore := &store.ChannelAccountStoreMock{}
			mockLedgerBackend := &LedgerBackendMock{}
			mockArchive := &HistoryArchiveMock{}
			ingestService, err := NewIngestService(IngestServiceConfig{
				Models:                  models,
				LatestLedgerCursorName:  "testCursor",
				OldestLedgerCursorName:  "oldestTestCursor",
				AccountTokensCursorName: "accountTokensCursor",
				AppTracker:              &mockAppTracker,
				RPCService:              &mockRPCService,
				LedgerBackend:           mockLedgerBackend,
				LedgerBackendFactory:    nil,
				ChannelAccountStore:     mockChAccStore,
				AccountTokenService:     nil,
				ContractMetadataService: nil,
				MetricsService:          mockMetricsService,
				GetLedgersLimit:         defaultGetLedgersLimit,
				Network:                 network.TestNetworkPassphrase,
				NetworkPassphrase:       network.TestNetworkPassphrase,
				Archive:                 mockArchive,
				SkipTxMeta:              false,
			})
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

func Test_splitGapsIntoBatches(t *testing.T) {
	testCases := []struct {
		name       string
		gaps       []data.LedgerRange
		batchSize  uint32
		wantResult []BackfillBatch
	}{
		{
			name:       "empty_gaps",
			gaps:       []data.LedgerRange{},
			batchSize:  100,
			wantResult: nil,
		},
		{
			name: "single_gap_smaller_than_batch",
			gaps: []data.LedgerRange{
				{GapStart: 100, GapEnd: 150},
			},
			batchSize: 100,
			wantResult: []BackfillBatch{
				{StartLedger: 100, EndLedger: 150},
			},
		},
		{
			name: "single_gap_exactly_batch_size",
			gaps: []data.LedgerRange{
				{GapStart: 100, GapEnd: 199},
			},
			batchSize: 100,
			wantResult: []BackfillBatch{
				{StartLedger: 100, EndLedger: 199},
			},
		},
		{
			name: "single_gap_larger_than_batch",
			gaps: []data.LedgerRange{
				{GapStart: 100, GapEnd: 350},
			},
			batchSize: 100,
			wantResult: []BackfillBatch{
				{StartLedger: 100, EndLedger: 199},
				{StartLedger: 200, EndLedger: 299},
				{StartLedger: 300, EndLedger: 350},
			},
		},
		{
			name: "multiple_gaps",
			gaps: []data.LedgerRange{
				{GapStart: 100, GapEnd: 150},
				{GapStart: 500, GapEnd: 650},
			},
			batchSize: 100,
			wantResult: []BackfillBatch{
				{StartLedger: 100, EndLedger: 150},
				{StartLedger: 500, EndLedger: 599},
				{StartLedger: 600, EndLedger: 650},
			},
		},
		{
			name: "single_ledger_gap",
			gaps: []data.LedgerRange{
				{GapStart: 100, GapEnd: 100},
			},
			batchSize: 100,
			wantResult: []BackfillBatch{
				{StartLedger: 100, EndLedger: 100},
			},
		},
	}

	svc := &ingestService{}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := svc.splitGapsIntoBatches(tc.gaps, tc.batchSize)
			assert.Equal(t, tc.wantResult, result)
		})
	}
}
