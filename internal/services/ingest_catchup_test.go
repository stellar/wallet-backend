package services

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/toid"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/apptracker"
	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/indexer"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/signing/store"
)

func Test_Catchup_Validation(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	testCases := []struct {
		name                  string
		startLedger           uint32
		endLedger             uint32
		latestIngested        uint32
		expectValidationError bool
		errorContains         string
	}{
		{
			name:                  "valid_range",
			startLedger:           101,
			endLedger:             150,
			latestIngested:        100,
			expectValidationError: false,
		},
		{
			name:                  "start_not_next_ledger",
			startLedger:           105,
			endLedger:             150,
			latestIngested:        100,
			expectValidationError: true,
			errorContains:         "catchup must start from ledger 101",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := dbConnectionPool.Exec(ctx, "DELETE FROM transactions")
			require.NoError(t, err)
			_, err = dbConnectionPool.Exec(ctx, "DELETE FROM ingest_store")
			require.NoError(t, err)

			_, err = dbConnectionPool.Exec(ctx,
				`INSERT INTO ingest_store (key, value) VALUES ('latest_ledger_cursor', $1)`,
				fmt.Sprintf("%d", tc.latestIngested))
			require.NoError(t, err)
			_, err = dbConnectionPool.Exec(ctx,
				`INSERT INTO ingest_store (key, value) VALUES ('oldest_ledger_cursor', $1)`,
				fmt.Sprintf("%d", tc.latestIngested))
			require.NoError(t, err)

			mockMetricsService := metrics.NewMockMetricsService()
			mockMetricsService.On("RegisterPoolMetrics", "ledger_indexer", mock.Anything).Return()
			mockMetricsService.On("RegisterPoolMetrics", "backfill", mock.Anything).Return()
			mockMetricsService.On("ObserveDBQueryDuration", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
			mockMetricsService.On("IncDBQuery", mock.Anything, mock.Anything).Return().Maybe()
			defer mockMetricsService.AssertExpectations(t)

			models, err := data.NewModels(dbConnectionPool, mockMetricsService)
			require.NoError(t, err)

			mockBackendFactory := func(ctx context.Context) (ledgerbackend.LedgerBackend, error) {
				return nil, fmt.Errorf("mock backend factory error")
			}

			svc, err := NewIngestService(IngestServiceConfig{
				IngestionMode:          IngestionModeBackfill,
				Models:                 models,
				LatestLedgerCursorName: "latest_ledger_cursor",
				OldestLedgerCursorName: "oldest_ledger_cursor",
				AppTracker:             &apptracker.MockAppTracker{},
				RPCService:             &RPCServiceMock{},
				LedgerBackend:          &LedgerBackendMock{},
				LedgerBackendFactory:   mockBackendFactory,
				ChannelAccountStore:    &store.ChannelAccountStoreMock{},
				MetricsService:         mockMetricsService,
				GetLedgersLimit:        defaultGetLedgersLimit,
				Network:                network.TestNetworkPassphrase,
				NetworkPassphrase:      network.TestNetworkPassphrase,
				Archive:                &HistoryArchiveMock{},
				SkipTxMeta:             false,
				SkipTxEnvelope:         false,
				BackfillBatchSize:      100,
			})
			require.NoError(t, err)

			err = svc.startCatchup(ctx, tc.startLedger, tc.endLedger)
			if tc.expectValidationError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.errorContains)
			} else {
				// Catchup mode returns error when batches fail
				require.Error(t, err)
				assert.Contains(t, err.Error(), "optimized catchup failed")
				assert.NotContains(t, err.Error(), "catchup must start from ledger")
			}
		})
	}
}

// Test_ingestService_startBackfilling_CatchupMode_PartialFailure_ReturnsError verifies
// that catchup mode returns an error when any batch fails.
func Test_ingestService_startBackfilling_CatchupMode_PartialFailure_ReturnsError(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	// Clean up database
	_, err = dbConnectionPool.Exec(ctx, `DELETE FROM transactions`)
	require.NoError(t, err)

	// Set up initial cursors: latest = 99, so catchup starts at 100
	setupDBCursors(t, ctx, dbConnectionPool, 99, 50)

	mockMetricsService := metrics.NewMockMetricsService()
	mockMetricsService.On("RegisterPoolMetrics", "ledger_indexer", mock.Anything).Return()
	mockMetricsService.On("RegisterPoolMetrics", "backfill", mock.Anything).Return()
	mockMetricsService.On("ObserveDBQueryDuration", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
	mockMetricsService.On("IncDBQuery", mock.Anything, mock.Anything).Return().Maybe()
	mockMetricsService.On("IncDBTransaction", mock.Anything).Return().Maybe()
	mockMetricsService.On("ObserveDBTransactionDuration", mock.Anything, mock.Anything).Return().Maybe()
	mockMetricsService.On("ObserveDBBatchSize", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
	mockMetricsService.On("ObserveIngestionParticipantsCount", mock.Anything).Return().Maybe()
	mockMetricsService.On("IncStateChanges", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
	defer mockMetricsService.AssertExpectations(t)

	models, modelsErr := data.NewModels(dbConnectionPool, mockMetricsService)
	require.NoError(t, modelsErr)

	mockRPCService := &RPCServiceMock{}
	mockRPCService.On("NetworkPassphrase").Return(network.TestNetworkPassphrase).Maybe()

	// Factory that fails for the middle batch
	factory := func(ctx context.Context) (ledgerbackend.LedgerBackend, error) {
		mockBackend := &LedgerBackendMock{}
		// Fail for batch starting at 110
		mockBackend.On("PrepareRange", mock.Anything, mock.MatchedBy(func(r ledgerbackend.Range) bool {
			return r.From() == 110
		})).Return(fmt.Errorf("simulated catchup failure"))
		mockBackend.On("PrepareRange", mock.Anything, mock.Anything).Return(nil).Maybe()
		mockBackend.On("GetLedger", mock.Anything, mock.Anything).Return(xdr.LedgerCloseMeta{
			V: 0,
			V0: &xdr.LedgerCloseMetaV0{
				LedgerHeader: xdr.LedgerHeaderHistoryEntry{
					Header: xdr.LedgerHeader{
						LedgerSeq: xdr.Uint32(100),
					},
				},
			},
		}, nil).Maybe()
		mockBackend.On("Close").Return(nil).Maybe()
		return mockBackend, nil
	}

	svc, svcErr := NewIngestService(IngestServiceConfig{
		IngestionMode:          IngestionModeBackfill,
		Models:                 models,
		LatestLedgerCursorName: "latest_ledger_cursor",
		OldestLedgerCursorName: "oldest_ledger_cursor",
		AppTracker:             &apptracker.MockAppTracker{},
		RPCService:             mockRPCService,
		LedgerBackend:          &LedgerBackendMock{},
		LedgerBackendFactory:   factory,
		MetricsService:         mockMetricsService,
		GetLedgersLimit:        defaultGetLedgersLimit,
		Network:                network.TestNetworkPassphrase,
		NetworkPassphrase:      network.TestNetworkPassphrase,
		Archive:                &HistoryArchiveMock{},
		BackfillBatchSize:      10,
	})
	require.NoError(t, svcErr)

	// Run catchup backfilling: starts at latest+1 = 100, ends at 129 (3 batches)
	backfillErr := svc.startCatchup(ctx, 100, 129)

	// Verify error is returned for catchup mode
	require.Error(t, backfillErr)
	assert.Contains(t, backfillErr.Error(), "optimized catchup failed")
	assert.Contains(t, backfillErr.Error(), "batches failed")

	// Verify latest cursor was NOT updated
	finalLatest, getErr := models.IngestStore.Get(ctx, "latest_ledger_cursor")
	require.NoError(t, getErr)
	assert.Equal(t, uint32(99), finalLatest, "latest cursor should NOT be updated when catchup fails")
}

// Test_ingestService_processBatchChanges tests the processBatchChanges method which processes
// aggregated batch changes after all parallel batches complete.
// NOTE: The implementation now:
// 1. Calls models.TrustlineAsset.BatchInsert for trustline assets (real DB operation)
// 2. Calls contractMetadataService.FetchMetadata for new contracts (mocked)
// 3. Calls models.Contract.BatchInsert for contracts (real DB operation)
// 4. Calls tokenIngestionService.ProcessTokenChanges(ctx, dbTx, trustlineChanges, contractChanges) (mocked)
func Test_ingestService_processBatchChanges(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	testCases := []struct {
		name                 string
		trustlineChanges     map[indexer.TrustlineChangeKey]types.TrustlineChange
		contractChanges      []types.ContractChange
		uniqueAssets         map[uuid.UUID]data.TrustlineAsset
		uniqueContractTokens map[string]types.ContractType
		setupMocks           func(t *testing.T, tokenIngestionService *TokenIngestionServiceMock, contractMetadataSvc *ContractMetadataServiceMock)
		wantErr              bool
		wantErrContains      string
		verifySortedChanges  func(t *testing.T, tokenIngestionService *TokenIngestionServiceMock)
	}{
		{
			name:                 "empty_data_calls_ProcessTokenChanges",
			trustlineChanges:     map[indexer.TrustlineChangeKey]types.TrustlineChange{},
			contractChanges:      []types.ContractChange{},
			uniqueAssets:         map[uuid.UUID]data.TrustlineAsset{},
			uniqueContractTokens: map[string]types.ContractType{},
			setupMocks: func(t *testing.T, tokenIngestionService *TokenIngestionServiceMock, contractMetadataSvc *ContractMetadataServiceMock) {
				// ProcessTokenChanges is called with empty map and slice
				tokenIngestionService.On("ProcessTokenChanges", mock.Anything, mock.Anything, mock.MatchedBy(func(changes map[indexer.TrustlineChangeKey]types.TrustlineChange) bool {
					return len(changes) == 0
				}), []types.ContractChange{}, mock.MatchedBy(func(m map[string]types.AccountChange) bool { return true }), mock.Anything).Return(nil)
			},
			wantErr: false,
		},
		{
			// Data is now pre-deduplicated by caller
			name: "passes_through_deduplicated_trustline_changes",
			trustlineChanges: map[indexer.TrustlineChangeKey]types.TrustlineChange{
				{AccountID: "GA1", TrustlineID: data.DeterministicAssetID("USD", "GA1")}: {AccountID: "GA1", Asset: "USD:GA1", OperationID: toid.New(100, 0, 10).ToInt64(), LedgerNumber: 100},
				{AccountID: "GA2", TrustlineID: data.DeterministicAssetID("EUR", "GA2")}: {AccountID: "GA2", Asset: "EUR:GA2", OperationID: toid.New(101, 0, 1).ToInt64(), LedgerNumber: 101},
				{AccountID: "GA3", TrustlineID: data.DeterministicAssetID("GBP", "GA3")}: {AccountID: "GA3", Asset: "GBP:GA3", OperationID: toid.New(100, 0, 5).ToInt64(), LedgerNumber: 100},
			},
			contractChanges:      []types.ContractChange{},
			uniqueAssets:         map[uuid.UUID]data.TrustlineAsset{},
			uniqueContractTokens: map[string]types.ContractType{},
			setupMocks: func(t *testing.T, tokenIngestionService *TokenIngestionServiceMock, contractMetadataSvc *ContractMetadataServiceMock) {
				// Verify all 3 changes are in the map (different keys)
				tokenIngestionService.On("ProcessTokenChanges", mock.Anything, mock.Anything, mock.MatchedBy(func(changes map[indexer.TrustlineChangeKey]types.TrustlineChange) bool {
					return len(changes) == 3
				}), []types.ContractChange{}, mock.MatchedBy(func(m map[string]types.AccountChange) bool { return true }), mock.Anything).Return(nil)
			},
			wantErr: false,
		},
		{
			// Contract changes are passed through (not deduplicated here)
			name:             "passes_contract_changes_through",
			trustlineChanges: map[indexer.TrustlineChangeKey]types.TrustlineChange{},
			contractChanges: []types.ContractChange{
				{AccountID: "GA1", ContractID: "C1", OperationID: toid.New(200, 0, 20).ToInt64(), LedgerNumber: 200, ContractType: types.ContractTypeUnknown},
				{AccountID: "GA2", ContractID: "C2", OperationID: toid.New(200, 0, 5).ToInt64(), LedgerNumber: 200, ContractType: types.ContractTypeUnknown},
				{AccountID: "GA3", ContractID: "C3", OperationID: toid.New(201, 0, 1).ToInt64(), LedgerNumber: 201, ContractType: types.ContractTypeUnknown},
			},
			uniqueAssets:         map[uuid.UUID]data.TrustlineAsset{},
			uniqueContractTokens: map[string]types.ContractType{},
			setupMocks: func(t *testing.T, tokenIngestionService *TokenIngestionServiceMock, contractMetadataSvc *ContractMetadataServiceMock) {
				// All 3 contract changes are passed through
				tokenIngestionService.On("ProcessTokenChanges", mock.Anything, mock.Anything, mock.MatchedBy(func(changes map[indexer.TrustlineChangeKey]types.TrustlineChange) bool {
					return len(changes) == 0
				}), mock.MatchedBy(func(changes []types.ContractChange) bool {
					return len(changes) == 3
				}), mock.MatchedBy(func(m map[string]types.AccountChange) bool { return true }), mock.Anything).Return(nil)
			},
			wantErr: false,
		},
		{
			name: "inserts_trustline_assets_and_calls_ProcessTokenChanges",
			trustlineChanges: map[indexer.TrustlineChangeKey]types.TrustlineChange{
				{AccountID: "GA1", TrustlineID: data.DeterministicAssetID("USDC", "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN")}: {AccountID: "GA1", Asset: "USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN", OperationID: 1, LedgerNumber: 100, Operation: types.TrustlineOpAdd},
			},
			contractChanges: []types.ContractChange{},
			uniqueAssets: map[uuid.UUID]data.TrustlineAsset{
				data.DeterministicAssetID("USDC", "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN"): {ID: data.DeterministicAssetID("USDC", "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN"), Code: "USDC", Issuer: "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN"},
			},
			uniqueContractTokens: map[string]types.ContractType{},
			setupMocks: func(t *testing.T, tokenIngestionService *TokenIngestionServiceMock, contractMetadataSvc *ContractMetadataServiceMock) {
				// ProcessTokenChanges is called after trustline assets are inserted to DB
				tokenIngestionService.On("ProcessTokenChanges", mock.Anything, mock.Anything, mock.MatchedBy(func(changes map[indexer.TrustlineChangeKey]types.TrustlineChange) bool {
					if len(changes) != 1 {
						return false
					}
					for _, change := range changes {
						return change.Asset == "USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN"
					}
					return false
				}), []types.ContractChange{}, mock.MatchedBy(func(m map[string]types.AccountChange) bool { return true }), mock.Anything).Return(nil)
			},
			wantErr: false,
		},
		{
			// Note: Unknown contracts don't get inserted into contracts table but are still passed to ProcessTokenChanges
			name:                 "processes_unknown_contract_changes",
			trustlineChanges:     map[indexer.TrustlineChangeKey]types.TrustlineChange{},
			uniqueAssets:         map[uuid.UUID]data.TrustlineAsset{},
			uniqueContractTokens: map[string]types.ContractType{},
			contractChanges: []types.ContractChange{
				{AccountID: "GA1", ContractID: "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC", OperationID: toid.New(100, 0, 1).ToInt64(), LedgerNumber: 100, ContractType: types.ContractTypeUnknown},
			},
			setupMocks: func(t *testing.T, tokenIngestionService *TokenIngestionServiceMock, contractMetadataSvc *ContractMetadataServiceMock) {
				// Unknown contracts don't trigger FetchMetadata but are passed to ProcessTokenChanges
				tokenIngestionService.On("ProcessTokenChanges", mock.Anything, mock.Anything, mock.MatchedBy(func(changes map[indexer.TrustlineChangeKey]types.TrustlineChange) bool {
					return len(changes) == 0
				}), mock.MatchedBy(func(changes []types.ContractChange) bool {
					return len(changes) == 1 && changes[0].ContractType == types.ContractTypeUnknown
				}), mock.MatchedBy(func(m map[string]types.AccountChange) bool { return true }), mock.Anything).Return(nil)
			},
			wantErr: false,
		},
		{
			name: "calls_ProcessTokenChanges_with_trustline_and_contract_changes",
			trustlineChanges: map[indexer.TrustlineChangeKey]types.TrustlineChange{
				{AccountID: "GA1", TrustlineID: data.DeterministicAssetID("USDC", "GA")}: {AccountID: "GA1", Asset: "USDC:GA", OperationID: toid.New(100, 0, 1).ToInt64(), LedgerNumber: 100},
			},
			contractChanges: []types.ContractChange{
				{AccountID: "GA2", ContractID: "C1", OperationID: toid.New(100, 0, 2).ToInt64(), LedgerNumber: 100, ContractType: types.ContractTypeUnknown},
			},
			uniqueAssets:         map[uuid.UUID]data.TrustlineAsset{},
			uniqueContractTokens: map[string]types.ContractType{},
			setupMocks: func(t *testing.T, tokenIngestionService *TokenIngestionServiceMock, contractMetadataSvc *ContractMetadataServiceMock) {
				// ProcessTokenChanges receives both trustline and contract changes
				tokenIngestionService.On("ProcessTokenChanges", mock.Anything, mock.Anything, mock.MatchedBy(func(changes map[indexer.TrustlineChangeKey]types.TrustlineChange) bool {
					if len(changes) != 1 {
						return false
					}
					for _, change := range changes {
						return change.Asset == "USDC:GA"
					}
					return false
				}), mock.MatchedBy(func(changes []types.ContractChange) bool {
					return len(changes) == 1 && changes[0].ContractID == "C1"
				}), mock.MatchedBy(func(m map[string]types.AccountChange) bool { return true }), mock.Anything).Return(nil)
			},
			wantErr: false,
		},
		{
			name: "propagates_ProcessTokenChanges_error",
			trustlineChanges: map[indexer.TrustlineChangeKey]types.TrustlineChange{
				{AccountID: "GA1", TrustlineID: data.DeterministicAssetID("USDC", "GA")}: {AccountID: "GA1", Asset: "USDC:GA", OperationID: toid.New(100, 0, 1).ToInt64(), LedgerNumber: 100},
			},
			contractChanges:      []types.ContractChange{},
			uniqueAssets:         map[uuid.UUID]data.TrustlineAsset{},
			uniqueContractTokens: map[string]types.ContractType{},
			setupMocks: func(t *testing.T, tokenIngestionService *TokenIngestionServiceMock, contractMetadataSvc *ContractMetadataServiceMock) {
				// ProcessTokenChanges receives the actual trustline changes
				tokenIngestionService.On("ProcessTokenChanges", mock.Anything, mock.Anything, mock.MatchedBy(func(changes map[indexer.TrustlineChangeKey]types.TrustlineChange) bool {
					return len(changes) == 1
				}), []types.ContractChange{}, mock.MatchedBy(func(m map[string]types.AccountChange) bool { return true }), mock.Anything).Return(fmt.Errorf("db error"))
			},
			wantErr:         true,
			wantErrContains: "processing token changes",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockMetricsService := metrics.NewMockMetricsService()
			mockMetricsService.On("RegisterPoolMetrics", "ledger_indexer", mock.Anything).Return()
			mockMetricsService.On("RegisterPoolMetrics", "backfill", mock.Anything).Return()
			mockMetricsService.On("ObserveDBQueryDuration", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
			mockMetricsService.On("IncDBQuery", mock.Anything, mock.Anything).Return().Maybe()
			mockMetricsService.On("IncDBTransaction", mock.Anything).Return().Maybe()
			mockMetricsService.On("ObserveDBTransactionDuration", mock.Anything, mock.Anything).Return().Maybe()
			defer mockMetricsService.AssertExpectations(t)

			models, err := data.NewModels(dbConnectionPool, mockMetricsService)
			require.NoError(t, err)

			mockRPCService := &RPCServiceMock{}
			mockRPCService.On("NetworkPassphrase").Return(network.TestNetworkPassphrase).Maybe()

			mockTokenIngestionService := NewTokenIngestionServiceMock(t)
			mockContractMetadataSvc := NewContractMetadataServiceMock(t)

			tc.setupMocks(t, mockTokenIngestionService, mockContractMetadataSvc)

			svc, err := NewIngestService(IngestServiceConfig{
				IngestionMode:           IngestionModeBackfill,
				Models:                  models,
				LatestLedgerCursorName:  "latest_ledger_cursor",
				OldestLedgerCursorName:  "oldest_ledger_cursor",
				AppTracker:              &apptracker.MockAppTracker{},
				RPCService:              mockRPCService,
				LedgerBackend:           &LedgerBackendMock{},
				TokenIngestionService:   mockTokenIngestionService,
				ContractMetadataService: mockContractMetadataSvc,
				MetricsService:          mockMetricsService,
				GetLedgersLimit:         defaultGetLedgersLimit,
				Network:                 network.TestNetworkPassphrase,
				NetworkPassphrase:       network.TestNetworkPassphrase,
				Archive:                 &HistoryArchiveMock{},
			})
			require.NoError(t, err)

			err = db.RunInTransaction(ctx, models.DB, func(dbTx pgx.Tx) error {
				return svc.processBatchChanges(ctx, dbTx, tc.trustlineChanges, tc.contractChanges, make(map[string]types.AccountChange), make(map[indexer.SACBalanceChangeKey]types.SACBalanceChange), tc.uniqueAssets, tc.uniqueContractTokens, make(map[string]*data.Contract))
			})

			if tc.wantErr {
				require.Error(t, err)
				if tc.wantErrContains != "" {
					assert.Contains(t, err.Error(), tc.wantErrContains)
				}
			} else {
				require.NoError(t, err)
			}

			if tc.verifySortedChanges != nil {
				tc.verifySortedChanges(t, mockTokenIngestionService)
			}
		})
	}
}

// Test_ingestService_flushCatchupBatch tests that batch changes are collected
// when flushCatchupBatch is called.
func Test_ingestService_flushCatchupBatch(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	testCases := []struct {
		name                      string
		setupBuffer               func() *indexer.IndexerBuffer
		batchChanges              *BatchChanges
		wantTrustlineChangesCount int
		wantContractChanges       []types.ContractChange
	}{
		{
			name: "collects_trustline_changes_when_batchChanges_provided",
			setupBuffer: func() *indexer.IndexerBuffer {
				buf := indexer.NewIndexerBuffer()
				tx1 := createTestTransaction(catchupTxHash1, 1)
				buf.PushTransaction(testAddr1, tx1)
				buf.PushTrustlineChange(types.TrustlineChange{
					AccountID:    testAddr1,
					Asset:        "USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN",
					OperationID:  100,
					LedgerNumber: 1000,
					Operation:    types.TrustlineOpAdd,
				})
				return buf
			},
			batchChanges:              &BatchChanges{TrustlineChangesByKey: make(map[indexer.TrustlineChangeKey]types.TrustlineChange), UniqueTrustlineAssets: make(map[uuid.UUID]data.TrustlineAsset), UniqueContractTokensByID: make(map[string]types.ContractType)},
			wantTrustlineChangesCount: 1,
			wantContractChanges:       nil,
		},
		{
			name: "collects_contract_changes_when_batchChanges_provided",
			setupBuffer: func() *indexer.IndexerBuffer {
				buf := indexer.NewIndexerBuffer()
				tx1 := createTestTransaction(catchupTxHash2, 2)
				buf.PushTransaction(testAddr2, tx1)
				buf.PushContractChange(types.ContractChange{
					AccountID:    testAddr2,
					ContractID:   "CCONTRACTID",
					OperationID:  101,
					LedgerNumber: 1001,
					ContractType: types.ContractTypeSAC,
				})
				return buf
			},
			batchChanges:              &BatchChanges{TrustlineChangesByKey: make(map[indexer.TrustlineChangeKey]types.TrustlineChange), UniqueTrustlineAssets: make(map[uuid.UUID]data.TrustlineAsset), UniqueContractTokensByID: make(map[string]types.ContractType)},
			wantTrustlineChangesCount: 0,
			wantContractChanges: []types.ContractChange{
				{
					AccountID:    testAddr2,
					ContractID:   "CCONTRACTID",
					OperationID:  101,
					LedgerNumber: 1001,
					ContractType: types.ContractTypeSAC,
				},
			},
		},
		{
			name: "accumulates_across_multiple_flushes",
			setupBuffer: func() *indexer.IndexerBuffer {
				buf := indexer.NewIndexerBuffer()
				tx1 := createTestTransaction(catchupTxHash6, 6)
				buf.PushTransaction(testAddr1, tx1)
				buf.PushTrustlineChange(types.TrustlineChange{
					AccountID:    testAddr1,
					Asset:        "GBP:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN",
					OperationID:  103,
					LedgerNumber: 1003,
					Operation:    types.TrustlineOpAdd,
				})
				return buf
			},
			// Pre-populate batchChanges to simulate accumulation from previous flush
			batchChanges: &BatchChanges{
				TrustlineChangesByKey: map[indexer.TrustlineChangeKey]types.TrustlineChange{
					{AccountID: "GPREV", TrustlineID: data.DeterministicAssetID("PREV", "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN")}: {AccountID: "GPREV", Asset: "PREV:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN", OperationID: 50, LedgerNumber: 999, Operation: types.TrustlineOpAdd},
				},
				UniqueTrustlineAssets:    make(map[uuid.UUID]data.TrustlineAsset),
				UniqueContractTokensByID: make(map[string]types.ContractType),
			},
			wantTrustlineChangesCount: 2, // Pre-existing + new change
			wantContractChanges:       nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Clean up test data from previous runs (using HashBytea for BYTEA column)
			for _, hash := range []string{catchupTxHash1, catchupTxHash2, catchupTxHash3, catchupTxHash4, catchupTxHash5, catchupTxHash6, prevTxHash} {
				_, err = dbConnectionPool.Exec(ctx, `DELETE FROM state_changes WHERE to_id IN (SELECT to_id FROM transactions WHERE hash = $1)`, types.HashBytea(hash))
				require.NoError(t, err)
				_, err = dbConnectionPool.Exec(ctx, `DELETE FROM transactions WHERE hash = $1`, types.HashBytea(hash))
				require.NoError(t, err)
			}
			// Also clean up any orphan operations
			_, err = dbConnectionPool.Exec(ctx, `TRUNCATE operations, operations_accounts CASCADE`)
			require.NoError(t, err)

			// Set up cursors
			setupDBCursors(t, ctx, dbConnectionPool, 200, 100)

			mockMetricsService := metrics.NewMockMetricsService()
			mockMetricsService.On("RegisterPoolMetrics", "ledger_indexer", mock.Anything).Return()
			mockMetricsService.On("RegisterPoolMetrics", "backfill", mock.Anything).Return()
			mockMetricsService.On("ObserveDBQueryDuration", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
			mockMetricsService.On("IncDBQuery", mock.Anything, mock.Anything).Return().Maybe()
			mockMetricsService.On("IncDBTransaction", mock.Anything).Return().Maybe()
			mockMetricsService.On("ObserveDBTransactionDuration", mock.Anything, mock.Anything).Return().Maybe()
			mockMetricsService.On("ObserveDBBatchSize", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
			mockMetricsService.On("IncStateChanges", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
			defer mockMetricsService.AssertExpectations(t)

			models, err := data.NewModels(dbConnectionPool, mockMetricsService)
			require.NoError(t, err)

			mockRPCService := &RPCServiceMock{}
			mockRPCService.On("NetworkPassphrase").Return(network.TestNetworkPassphrase).Maybe()

			svc, err := NewIngestService(IngestServiceConfig{
				IngestionMode:          IngestionModeBackfill,
				Models:                 models,
				LatestLedgerCursorName: "latest_ledger_cursor",
				OldestLedgerCursorName: "oldest_ledger_cursor",
				AppTracker:             &apptracker.MockAppTracker{},
				RPCService:             mockRPCService,
				LedgerBackend:          &LedgerBackendMock{},
				MetricsService:         mockMetricsService,
				GetLedgersLimit:        defaultGetLedgersLimit,
				Network:                network.TestNetworkPassphrase,
				NetworkPassphrase:      network.TestNetworkPassphrase,
				Archive:                &HistoryArchiveMock{},
			})
			require.NoError(t, err)

			buffer := tc.setupBuffer()

			err = svc.flushCatchupBatch(ctx, []*indexer.IndexerBuffer{buffer}, tc.batchChanges)
			require.NoError(t, err)

			// Verify collected token changes match expected values
			require.Len(t, tc.batchChanges.TrustlineChangesByKey, tc.wantTrustlineChangesCount, "trustline changes count mismatch")

			require.Len(t, tc.batchChanges.ContractChanges, len(tc.wantContractChanges), "contract changes count mismatch")
			for i, want := range tc.wantContractChanges {
				got := tc.batchChanges.ContractChanges[i]
				assert.Equal(t, want.AccountID, got.AccountID, "ContractChange[%d].AccountID mismatch", i)
				assert.Equal(t, want.ContractID, got.ContractID, "ContractChange[%d].ContractID mismatch", i)
				assert.Equal(t, want.OperationID, got.OperationID, "ContractChange[%d].OperationID mismatch", i)
				assert.Equal(t, want.LedgerNumber, got.LedgerNumber, "ContractChange[%d].LedgerNumber mismatch", i)
				assert.Equal(t, want.ContractType, got.ContractType, "ContractChange[%d].ContractType mismatch", i)
			}
		})
	}
}

// Test_ingestService_processLedgersInBatch_Catchup tests that the pipelined
// processor with flushCatchupBatch returns non-nil BatchChanges.
func Test_ingestService_processLedgersInBatch_Catchup(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	var ledgerMeta xdr.LedgerCloseMeta
	err = xdr.SafeUnmarshalBase64(ledgerMetadataWith0Tx, &ledgerMeta)
	require.NoError(t, err)

	setupDBCursors(t, ctx, dbConnectionPool, 200, 100)

	mockMetricsService := metrics.NewMockMetricsService()
	mockMetricsService.On("RegisterPoolMetrics", "ledger_indexer", mock.Anything).Return()
	mockMetricsService.On("RegisterPoolMetrics", "backfill", mock.Anything).Return()
	mockMetricsService.On("ObserveDBQueryDuration", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
	mockMetricsService.On("IncDBQuery", mock.Anything, mock.Anything).Return().Maybe()
	mockMetricsService.On("IncDBTransaction", mock.Anything).Return().Maybe()
	mockMetricsService.On("ObserveDBTransactionDuration", mock.Anything, mock.Anything).Return().Maybe()
	mockMetricsService.On("ObserveDBBatchSize", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
	mockMetricsService.On("IncStateChanges", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
	mockMetricsService.On("ObserveIngestionParticipantsCount", mock.Anything).Return().Maybe()
	defer mockMetricsService.AssertExpectations(t)

	models, err := data.NewModels(dbConnectionPool, mockMetricsService)
	require.NoError(t, err)

	mockLedgerBackend := &LedgerBackendMock{}
	mockLedgerBackend.On("GetLedger", mock.Anything, uint32(4599)).Return(ledgerMeta, nil)

	mockChAccStore := &store.ChannelAccountStoreMock{}
	mockChAccStore.On("UnassignTxAndUnlockChannelAccounts", mock.Anything, mock.Anything).Return(int64(0), nil).Maybe()

	svc, err := NewIngestService(IngestServiceConfig{
		IngestionMode:          IngestionModeBackfill,
		Models:                 models,
		LatestLedgerCursorName: "latest_ledger_cursor",
		OldestLedgerCursorName: "oldest_ledger_cursor",
		AppTracker:             &apptracker.MockAppTracker{},
		RPCService:             &RPCServiceMock{},
		LedgerBackend:          mockLedgerBackend,
		ChannelAccountStore:    mockChAccStore,
		MetricsService:         mockMetricsService,
		GetLedgersLimit:        defaultGetLedgersLimit,
		Network:                network.TestNetworkPassphrase,
		NetworkPassphrase:      network.TestNetworkPassphrase,
		Archive:                &HistoryArchiveMock{},
	})
	require.NoError(t, err)

	batch := BackfillBatch{StartLedger: 4599, EndLedger: 4599}
	batchChanges := &BatchChanges{
		TrustlineChangesByKey:     make(map[indexer.TrustlineChangeKey]types.TrustlineChange),
		AccountChangesByAccountID: make(map[string]types.AccountChange),
		SACBalanceChangesByKey:    make(map[indexer.SACBalanceChangeKey]types.SACBalanceChange),
		UniqueTrustlineAssets:     make(map[uuid.UUID]data.TrustlineAsset),
		UniqueContractTokensByID:  make(map[string]types.ContractType),
		SACContractsByID:          make(map[string]*data.Contract),
	}
	result := svc.processLedgersInBatch(ctx, mockLedgerBackend, batch, func(ctx context.Context, buffers []*indexer.IndexerBuffer) error {
		return svc.flushCatchupBatch(ctx, buffers, batchChanges)
	})
	result.BatchChanges = batchChanges

	require.NoError(t, result.Error)
	assert.Equal(t, 1, result.LedgersCount)
	assert.NotNil(t, result.BatchChanges, "expected non-nil batch changes for catchup mode")
}

// Test_ingestService_startCatchup_ProcessesBatchChanges tests the full catchup
// flow including batch change processing and cursor updates.
//
//nolint:unparam // Test backend factory always returns nil error - this is intentional for testing
func Test_ingestService_startCatchup_ProcessesBatchChanges(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	// Prepare a ledger metadata for the test
	var ledgerMeta xdr.LedgerCloseMeta
	err = xdr.SafeUnmarshalBase64(ledgerMetadataWith0Tx, &ledgerMeta)
	require.NoError(t, err)
	ledgerSeq := ledgerMeta.LedgerSequence()

	// NOTE: processTokenChanges now only calls:
	// 1. models.TrustlineAsset.BatchInsert (real DB operation)
	// 2. contractMetadataService.FetchMetadata (mocked, but not triggered with empty data)
	// 3. models.Contract.BatchInsert (real DB operation)
	// 4. tokenIngestionService.ProcessTokenChanges(ctx, dbTx, trustlineChanges, contractChanges)
	testCases := []struct {
		name             string
		setupMocks       func(t *testing.T, tokenIngestionService *TokenIngestionServiceMock, backendFactory *func(ctx context.Context) (ledgerbackend.LedgerBackend, error))
		wantErr          bool
		wantErrContains  string
		wantLatestCursor uint32
	}{
		{
			name: "successful_catchup_processes_token_changes",
			setupMocks: func(t *testing.T, tokenIngestionService *TokenIngestionServiceMock, backendFactory *func(ctx context.Context) (ledgerbackend.LedgerBackend, error)) {
				// ProcessTokenChanges is the only mock needed on tokenIngestionService
				tokenIngestionService.On("ProcessTokenChanges", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

				// Backend factory
				*backendFactory = func(_ context.Context) (ledgerbackend.LedgerBackend, error) {
					mockBackend := &LedgerBackendMock{}
					mockBackend.On("PrepareRange", mock.Anything, mock.Anything).Return(nil)
					mockBackend.On("GetLedger", mock.Anything, ledgerSeq).Return(ledgerMeta, nil)
					mockBackend.On("Close").Return(nil)
					return mockBackend, nil
				}
			},
			wantErr:          false,
			wantLatestCursor: ledgerSeq,
		},
		{
			name: "token_processing_error_returns_error",
			setupMocks: func(t *testing.T, tokenIngestionService *TokenIngestionServiceMock, backendFactory *func(ctx context.Context) (ledgerbackend.LedgerBackend, error)) {
				// ProcessTokenChanges fails
				tokenIngestionService.On("ProcessTokenChanges", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(fmt.Errorf("db connection error"))

				// Backend factory
				*backendFactory = func(_ context.Context) (ledgerbackend.LedgerBackend, error) {
					mockBackend := &LedgerBackendMock{}
					mockBackend.On("PrepareRange", mock.Anything, mock.Anything).Return(nil)
					mockBackend.On("GetLedger", mock.Anything, ledgerSeq).Return(ledgerMeta, nil)
					mockBackend.On("Close").Return(nil)
					return mockBackend, nil
				}
			},
			wantErr:          true,
			wantErrContains:  "processing token changes",
			wantLatestCursor: ledgerSeq - 1, // Cursor should NOT be updated
		},
		{
			name: "cursor_not_updated_if_token_processing_fails",
			setupMocks: func(t *testing.T, tokenIngestionService *TokenIngestionServiceMock, backendFactory *func(ctx context.Context) (ledgerbackend.LedgerBackend, error)) {
				// ProcessTokenChanges fails - cursor should not be updated
				tokenIngestionService.On("ProcessTokenChanges", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(fmt.Errorf("db error"))

				// Backend factory
				*backendFactory = func(_ context.Context) (ledgerbackend.LedgerBackend, error) {
					mockBackend := &LedgerBackendMock{}
					mockBackend.On("PrepareRange", mock.Anything, mock.Anything).Return(nil)
					mockBackend.On("GetLedger", mock.Anything, ledgerSeq).Return(ledgerMeta, nil)
					mockBackend.On("Close").Return(nil)
					return mockBackend, nil
				}
			},
			wantErr:          true,
			wantErrContains:  "processing token changes",
			wantLatestCursor: ledgerSeq - 1, // Cursor should NOT be updated
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Clean up and set up cursors (latest = startLedger - 1 for catchup mode validation)
			setupDBCursors(t, ctx, dbConnectionPool, ledgerSeq-1, ledgerSeq-1)

			mockMetricsService := metrics.NewMockMetricsService()
			mockMetricsService.On("RegisterPoolMetrics", "ledger_indexer", mock.Anything).Return()
			mockMetricsService.On("RegisterPoolMetrics", "backfill", mock.Anything).Return()
			mockMetricsService.On("ObserveDBQueryDuration", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
			mockMetricsService.On("IncDBQuery", mock.Anything, mock.Anything).Return().Maybe()
			mockMetricsService.On("IncDBTransaction", mock.Anything).Return().Maybe()
			mockMetricsService.On("ObserveDBTransactionDuration", mock.Anything, mock.Anything).Return().Maybe()
			mockMetricsService.On("ObserveDBBatchSize", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
			mockMetricsService.On("IncStateChanges", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
			mockMetricsService.On("ObserveIngestionParticipantsCount", mock.Anything).Return().Maybe()
			defer mockMetricsService.AssertExpectations(t)

			models, err := data.NewModels(dbConnectionPool, mockMetricsService)
			require.NoError(t, err)

			mockRPCService := &RPCServiceMock{}
			mockRPCService.On("NetworkPassphrase").Return(network.TestNetworkPassphrase).Maybe()

			mockTokenIngestionService := NewTokenIngestionServiceMock(t)
			var backendFactory func(ctx context.Context) (ledgerbackend.LedgerBackend, error)

			tc.setupMocks(t, mockTokenIngestionService, &backendFactory)

			svc, err := NewIngestService(IngestServiceConfig{
				IngestionMode:          IngestionModeBackfill,
				Models:                 models,
				LatestLedgerCursorName: "latest_ledger_cursor",
				OldestLedgerCursorName: "oldest_ledger_cursor",
				AppTracker:             &apptracker.MockAppTracker{},
				RPCService:             mockRPCService,
				LedgerBackend:          &LedgerBackendMock{},
				LedgerBackendFactory:   backendFactory,
				ChannelAccountStore:    &store.ChannelAccountStoreMock{},
				TokenIngestionService:  mockTokenIngestionService,
				MetricsService:         mockMetricsService,
				GetLedgersLimit:        defaultGetLedgersLimit,
				Network:                network.TestNetworkPassphrase,
				NetworkPassphrase:      network.TestNetworkPassphrase,
				Archive:                &HistoryArchiveMock{},
				BackfillBatchSize:      100,
			})
			require.NoError(t, err)

			err = svc.startCatchup(ctx, ledgerSeq, ledgerSeq)

			if tc.wantErr {
				require.Error(t, err)
				if tc.wantErrContains != "" {
					assert.Contains(t, err.Error(), tc.wantErrContains)
				}
			} else {
				require.NoError(t, err)
			}

			// Verify cursor state
			cursor, err := models.IngestStore.Get(ctx, "latest_ledger_cursor")
			require.NoError(t, err)
			assert.Equal(t, tc.wantLatestCursor, cursor, "latest ledger cursor mismatch")
		})
	}
}

// Test_ingestService_processBackfillBatchesParallel_Catchup verifies
// that catchup mode processes batches successfully.
func Test_ingestService_processBackfillBatchesParallel_Catchup(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := metrics.NewMockMetricsService()
	mockMetricsService.On("RegisterPoolMetrics", "ledger_indexer", mock.Anything).Return()
	mockMetricsService.On("RegisterPoolMetrics", "backfill", mock.Anything).Return()
	mockMetricsService.On("ObserveDBQueryDuration", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
	mockMetricsService.On("IncDBQuery", mock.Anything, mock.Anything).Return().Maybe()
	mockMetricsService.On("IncDBTransaction", mock.Anything).Return().Maybe()
	mockMetricsService.On("ObserveDBTransactionDuration", mock.Anything, mock.Anything).Return().Maybe()
	mockMetricsService.On("ObserveDBBatchSize", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
	mockMetricsService.On("ObserveIngestionParticipantsCount", mock.Anything).Return().Maybe()
	mockMetricsService.On("IncStateChanges", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
	defer mockMetricsService.AssertExpectations(t)

	models, modelsErr := data.NewModels(dbConnectionPool, mockMetricsService)
	require.NoError(t, modelsErr)

	factory := func(ctx context.Context) (ledgerbackend.LedgerBackend, error) {
		mockBackend := &LedgerBackendMock{}
		mockBackend.On("PrepareRange", mock.Anything, mock.Anything).Return(nil)
		mockBackend.On("GetLedger", mock.Anything, mock.Anything).Return(xdr.LedgerCloseMeta{
			V: 0,
			V0: &xdr.LedgerCloseMetaV0{
				LedgerHeader: xdr.LedgerHeaderHistoryEntry{
					Header: xdr.LedgerHeader{
						LedgerSeq: xdr.Uint32(100),
					},
				},
			},
		}, nil)
		mockBackend.On("Close").Return(nil)
		return mockBackend, nil
	}

	svc, svcErr := NewIngestService(IngestServiceConfig{
		IngestionMode:          IngestionModeBackfill,
		Models:                 models,
		LatestLedgerCursorName: "latest_ledger_cursor",
		OldestLedgerCursorName: "oldest_ledger_cursor",
		AppTracker:             &apptracker.MockAppTracker{},
		RPCService:             &RPCServiceMock{},
		LedgerBackend:          &LedgerBackendMock{},
		LedgerBackendFactory:   factory,
		MetricsService:         mockMetricsService,
		GetLedgersLimit:        defaultGetLedgersLimit,
		Network:                network.TestNetworkPassphrase,
		NetworkPassphrase:      network.TestNetworkPassphrase,
		Archive:                &HistoryArchiveMock{},
		BackfillBatchSize:      10,
	})
	require.NoError(t, svcErr)

	batches := []BackfillBatch{
		{StartLedger: 100, EndLedger: 100},
		{StartLedger: 101, EndLedger: 101},
	}

	catchupProcessor := func(ctx context.Context, backend ledgerbackend.LedgerBackend, batch BackfillBatch) BackfillResult {
		batchChanges := &BatchChanges{
			TrustlineChangesByKey:     make(map[indexer.TrustlineChangeKey]types.TrustlineChange),
			AccountChangesByAccountID: make(map[string]types.AccountChange),
			SACBalanceChangesByKey:    make(map[indexer.SACBalanceChangeKey]types.SACBalanceChange),
			UniqueTrustlineAssets:     make(map[uuid.UUID]data.TrustlineAsset),
			UniqueContractTokensByID:  make(map[string]types.ContractType),
			SACContractsByID:          make(map[string]*data.Contract),
		}
		result := svc.processLedgersInBatch(ctx, backend, batch, func(ctx context.Context, buffers []*indexer.IndexerBuffer) error {
			return svc.flushCatchupBatch(ctx, buffers, batchChanges)
		})
		result.BatchChanges = batchChanges
		return result
	}
	results := svc.processBackfillBatchesParallel(ctx, batches, catchupProcessor, nil)

	require.Len(t, results, 2)
	for i, result := range results {
		assert.NoError(t, result.Error, "batch %d should succeed", i)
	}
}
