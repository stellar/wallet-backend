// Tests for account token caching service using PostgreSQL.
package services

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	wbdata "github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
)

// Helper functions for creating test XDR values
func ptrToScSymbol(s string) *xdr.ScSymbol {
	sym := xdr.ScSymbol(s)
	return &sym
}

func ptrToUint32(n uint32) *xdr.Uint32 {
	u := xdr.Uint32(n)
	return &u
}

func ptrToAccountID(address string) *xdr.AccountId { //nolint:unparam // test helper used with consistent values
	addr := xdr.MustAddress(address)
	accountID := addr.MustEd25519()
	accID := xdr.AccountId{
		Type:    xdr.PublicKeyTypePublicKeyTypeEd25519,
		Ed25519: &accountID,
	}
	return &accID
}

func ptrToScVec(vals []xdr.ScVal) **xdr.ScVec {
	vec := xdr.ScVec(vals)
	ptr := &vec
	return &ptr
}

func TestExtractHolderAddress(t *testing.T) {
	service := &tokenCacheService{}

	tests := []struct {
		name    string
		key     xdr.ScVal
		want    string
		wantErr bool
	}{
		{
			name: "valid balance entry",
			key: xdr.ScVal{
				Type: xdr.ScValTypeScvVec,
				Vec: ptrToScVec([]xdr.ScVal{
					{
						Type: xdr.ScValTypeScvSymbol,
						Sym:  ptrToScSymbol("Balance"),
					},
					{
						Type: xdr.ScValTypeScvAddress,
						Address: &xdr.ScAddress{
							Type:      xdr.ScAddressTypeScAddressTypeAccount,
							AccountId: ptrToAccountID("GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"),
						},
					},
				}),
			},
			want:    "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N",
			wantErr: false,
		},
		{
			name: "not a vector",
			key: xdr.ScVal{
				Type: xdr.ScValTypeScvU32,
				U32:  ptrToUint32(123),
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "wrong vector length - too short",
			key: xdr.ScVal{
				Type: xdr.ScValTypeScvVec,
				Vec: ptrToScVec([]xdr.ScVal{
					{
						Type: xdr.ScValTypeScvSymbol,
						Sym:  ptrToScSymbol("Balance"),
					},
				}),
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "wrong vector length - too long",
			key: xdr.ScVal{
				Type: xdr.ScValTypeScvVec,
				Vec: ptrToScVec([]xdr.ScVal{
					{Type: xdr.ScValTypeScvSymbol, Sym: ptrToScSymbol("Balance")},
					{Type: xdr.ScValTypeScvU32, U32: ptrToUint32(1)},
					{Type: xdr.ScValTypeScvU32, U32: ptrToUint32(2)},
				}),
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "wrong first element - not 'Balance' symbol",
			key: xdr.ScVal{
				Type: xdr.ScValTypeScvVec,
				Vec: ptrToScVec([]xdr.ScVal{
					{
						Type: xdr.ScValTypeScvSymbol,
						Sym:  ptrToScSymbol("NotBalance"),
					},
					{
						Type: xdr.ScValTypeScvAddress,
						Address: &xdr.ScAddress{
							Type:      xdr.ScAddressTypeScAddressTypeAccount,
							AccountId: ptrToAccountID("GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"),
						},
					},
				}),
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "second element not an address",
			key: xdr.ScVal{
				Type: xdr.ScValTypeScvVec,
				Vec: ptrToScVec([]xdr.ScVal{
					{
						Type: xdr.ScValTypeScvSymbol,
						Sym:  ptrToScSymbol("Balance"),
					},
					{
						Type: xdr.ScValTypeScvU32,
						U32:  ptrToUint32(123),
					},
				}),
			},
			want:    "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := service.extractHolderAddress(tt.key)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Empty(t, got)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestParseAssetString(t *testing.T) {
	tests := []struct {
		name       string
		asset      string
		wantCode   string
		wantIssuer string
		wantErr    bool
	}{
		{
			name:       "valid CODE:ISSUER format with 4 char code",
			asset:      "USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN",
			wantCode:   "USDC",
			wantIssuer: "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN",
			wantErr:    false,
		},
		{
			name:       "valid CODE:ISSUER format with 12 char code",
			asset:      "LONGERCODE12:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN",
			wantCode:   "LONGERCODE12",
			wantIssuer: "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN",
			wantErr:    false,
		},
		{
			name:    "missing colon",
			asset:   "USDCGA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN",
			wantErr: true,
		},
		{
			name:    "empty string",
			asset:   "",
			wantErr: true,
		},
		{
			name:    "empty code",
			asset:   ":GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN",
			wantErr: true,
		},
		{
			name:    "empty issuer",
			asset:   "USDC:",
			wantErr: true,
		},
		{
			name:    "invalid issuer format",
			asset:   "USDC:NOTAVALIDISSUER",
			wantErr: true,
		},
		{
			name:    "code too long (13 chars)",
			asset:   "THIRTEENCHARS:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN",
			wantErr: true,
		},
		{
			name:    "multiple colons - invalid issuer",
			asset:   "USD:C:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, issuer, err := parseAssetString(tt.asset)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.wantCode, code)
				assert.Equal(t, tt.wantIssuer, issuer)
			}
		})
	}
}

func TestGetAccountTrustlines(t *testing.T) {
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	cleanUpDB := func() {
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM account_trustlines`)
		require.NoError(t, err)
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM trustline_assets`)
		require.NoError(t, err)
	}

	t.Run("account with no trustlines returns empty", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", mock.Anything, mock.Anything, mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", mock.Anything, mock.Anything).Return()
		defer mockMetricsService.AssertExpectations(t)

		accountTokensModel := &wbdata.AccountTokensModel{DB: dbConnectionPool, MetricsService: mockMetricsService}
		trustlineAssetModel := &wbdata.TrustlineAssetModel{DB: dbConnectionPool, MetricsService: mockMetricsService}
		contractModel := &wbdata.ContractModel{DB: dbConnectionPool, MetricsService: mockMetricsService}

		service := NewTokenCacheReader(dbConnectionPool, trustlineAssetModel, accountTokensModel, contractModel)

		got, err := service.GetAccountTrustlines(ctx, "GBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB")
		assert.NoError(t, err)
		assert.Empty(t, got)
	})

	t.Run("account with trustlines returns assets", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", mock.Anything, mock.Anything, mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", mock.Anything, mock.Anything).Return()
		defer mockMetricsService.AssertExpectations(t)

		accountTokensModel := &wbdata.AccountTokensModel{DB: dbConnectionPool, MetricsService: mockMetricsService}
		trustlineAssetModel := &wbdata.TrustlineAssetModel{DB: dbConnectionPool, MetricsService: mockMetricsService}
		contractModel := &wbdata.ContractModel{DB: dbConnectionPool, MetricsService: mockMetricsService}

		accountAddress := "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"

		// Insert trustline assets first using deterministic IDs
		assetID := wbdata.DeterministicAssetID("USDC", "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN")
		err = db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return trustlineAssetModel.BatchInsert(ctx, dbTx, []wbdata.TrustlineAsset{
				{ID: assetID, Code: "USDC", Issuer: "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN"},
			})
		})
		require.NoError(t, err)

		// Insert account trustlines
		err = db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return accountTokensModel.BulkInsertTrustlines(ctx, dbTx, map[string][]int64{
				accountAddress: {assetID},
			})
		})
		require.NoError(t, err)

		service := NewTokenCacheReader(dbConnectionPool, trustlineAssetModel, accountTokensModel, contractModel)

		got, err := service.GetAccountTrustlines(ctx, accountAddress)
		assert.NoError(t, err)
		assert.Len(t, got, 1)
		assert.Equal(t, "USDC", got[0].Code)
		assert.Equal(t, "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN", got[0].Issuer)
	})
}

func TestGetAccountContracts(t *testing.T) {
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	cleanUpDB := func() {
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM account_contracts`)
		require.NoError(t, err)
	}

	t.Run("account with no contracts returns empty", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", mock.Anything, mock.Anything, mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", mock.Anything, mock.Anything).Return()
		defer mockMetricsService.AssertExpectations(t)

		accountTokensModel := &wbdata.AccountTokensModel{DB: dbConnectionPool, MetricsService: mockMetricsService}
		trustlineAssetModel := &wbdata.TrustlineAssetModel{DB: dbConnectionPool, MetricsService: mockMetricsService}
		contractModel := &wbdata.ContractModel{DB: dbConnectionPool, MetricsService: mockMetricsService}

		service := NewTokenCacheReader(dbConnectionPool, trustlineAssetModel, accountTokensModel, contractModel)

		got, err := service.GetAccountContracts(ctx, "GBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB")
		assert.NoError(t, err)
		assert.Empty(t, got)
	})

	t.Run("account with contracts returns contract IDs", func(t *testing.T) {
		cleanUpDB()
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM contract_tokens`)
		require.NoError(t, err)
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", mock.Anything, mock.Anything, mock.Anything).Return()
		mockMetricsService.On("ObserveDBBatchSize", mock.Anything, mock.Anything, mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", mock.Anything, mock.Anything).Return()
		defer mockMetricsService.AssertExpectations(t)

		accountTokensModel := &wbdata.AccountTokensModel{DB: dbConnectionPool, MetricsService: mockMetricsService}
		trustlineAssetModel := &wbdata.TrustlineAssetModel{DB: dbConnectionPool, MetricsService: mockMetricsService}
		contractModel := &wbdata.ContractModel{DB: dbConnectionPool, MetricsService: mockMetricsService}

		accountAddress := "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"
		contractID := "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4"

		// Insert contract into contract_tokens using deterministic ID
		numericID := wbdata.DeterministicContractID(contractID)
		err = db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return contractModel.BatchInsert(ctx, dbTx, []*wbdata.Contract{
				{ID: numericID, ContractID: contractID, Type: "SAC"},
			})
		})
		require.NoError(t, err)

		// Insert account contracts using deterministic numeric ID
		err = db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return accountTokensModel.BulkInsertContracts(ctx, dbTx, map[string][]int64{
				accountAddress: {numericID},
			})
		})
		require.NoError(t, err)

		service := NewTokenCacheReader(dbConnectionPool, trustlineAssetModel, accountTokensModel, contractModel)

		got, err := service.GetAccountContracts(ctx, accountAddress)
		assert.NoError(t, err)
		assert.Len(t, got, 1)
		assert.Equal(t, contractID, got[0].ContractID)
	})
}

// Note: The streaming checkpoint functionality (streamCheckpointData) is tested via
// integration tests since it requires database transactions for streaming inserts.
// Unit tests for helper functions like processTrustlineChange are kept below.

func TestProcessTokenChanges(t *testing.T) {
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	cleanUpDB := func() {
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM account_trustlines`)
		require.NoError(t, err)
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM account_contracts`)
		require.NoError(t, err)
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM trustline_assets`)
		require.NoError(t, err)
	}

	t.Run("empty changes returns no error", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		defer mockMetricsService.AssertExpectations(t)

		accountTokensModel := &wbdata.AccountTokensModel{DB: dbConnectionPool, MetricsService: mockMetricsService}
		trustlineAssetModel := &wbdata.TrustlineAssetModel{DB: dbConnectionPool, MetricsService: mockMetricsService}
		contractModel := &wbdata.ContractModel{DB: dbConnectionPool, MetricsService: mockMetricsService}

		service := NewTokenCacheWriter(dbConnectionPool, "Test SDF Network ; September 2015", nil, nil, nil, trustlineAssetModel, accountTokensModel, contractModel)

		err = db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return service.ProcessTokenChanges(ctx, dbTx, []types.TrustlineChange{}, []types.ContractChange{})
		})
		assert.NoError(t, err)
	})

	t.Run("add contract stores contract ID", func(t *testing.T) {
		cleanUpDB()
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM contract_tokens`)
		require.NoError(t, err)
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", mock.Anything, mock.Anything, mock.Anything).Return()
		mockMetricsService.On("ObserveDBBatchSize", mock.Anything, mock.Anything, mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", mock.Anything, mock.Anything).Return()
		defer mockMetricsService.AssertExpectations(t)

		accountTokensModel := &wbdata.AccountTokensModel{DB: dbConnectionPool, MetricsService: mockMetricsService}
		trustlineAssetModel := &wbdata.TrustlineAssetModel{DB: dbConnectionPool, MetricsService: mockMetricsService}
		contractModel := &wbdata.ContractModel{DB: dbConnectionPool, MetricsService: mockMetricsService}

		service := NewTokenCacheWriter(dbConnectionPool, "Test SDF Network ; September 2015", nil, nil, nil, trustlineAssetModel, accountTokensModel, contractModel)

		accountAddress := "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"
		contractID := "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4"

		// First insert the contract into contract_tokens using deterministic ID (simulating FetchAndStoreMetadata)
		numericID := wbdata.DeterministicContractID(contractID)
		err = db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return contractModel.BatchInsert(ctx, dbTx, []*wbdata.Contract{
				{ID: numericID, ContractID: contractID, Type: "SAC"},
			})
		})
		require.NoError(t, err)

		// ProcessTokenChanges now computes IDs internally using DeterministicContractID
		err = db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return service.ProcessTokenChanges(ctx, dbTx, []types.TrustlineChange{}, []types.ContractChange{
				{
					AccountID:    accountAddress,
					ContractID:   contractID,
					ContractType: types.ContractTypeSAC,
				},
			})
		})
		assert.NoError(t, err)

		// Verify contract relationship is stored
		reader := NewTokenCacheReader(dbConnectionPool, trustlineAssetModel, accountTokensModel, contractModel)
		contracts, err := reader.GetAccountContracts(ctx, accountAddress)
		assert.NoError(t, err)
		require.Len(t, contracts, 1)
		assert.Equal(t, contractID, contracts[0].ContractID)
	})
}

func TestEnsureTrustlineAssetsExist(t *testing.T) {
	ctx := context.Background()

	t.Run("empty changes returns nil error", func(t *testing.T) {
		service := &tokenCacheService{}

		// nil dbTx is fine for empty changes since function returns early
		err := service.EnsureTrustlineAssetsExist(ctx, nil, []types.TrustlineChange{})
		require.NoError(t, err)
	})

	t.Run("invalid asset format returns error", func(t *testing.T) {
		dbt := dbtest.Open(t)
		defer dbt.Close()
		dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
		require.NoError(t, err)
		defer dbConnectionPool.Close()

		mockMetrics := &metrics.MockMetricsService{}

		assetModel := &wbdata.TrustlineAssetModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetrics,
		}

		service := &tokenCacheService{
			db:                  dbConnectionPool,
			trustlineAssetModel: assetModel,
		}

		changes := []types.TrustlineChange{
			{AccountID: "GTEST1", Asset: "INVALID_NO_COLON", Operation: types.TrustlineOpAdd},
		}

		// nil dbTx is fine since error occurs during asset parsing, before DB access
		err = service.EnsureTrustlineAssetsExist(ctx, nil, changes)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "parsing asset")
	})

	t.Run("inserts assets into PostgreSQL with deterministic IDs", func(t *testing.T) {
		dbt := dbtest.Open(t)
		defer dbt.Close()
		dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
		require.NoError(t, err)
		defer dbConnectionPool.Close()

		// Clean up
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM trustline_assets`)
		require.NoError(t, err)

		mockMetrics := &metrics.MockMetricsService{}
		mockMetrics.On("ObserveDBQueryDuration", "BatchInsert", "trustline_assets", mock.Anything).Return()
		mockMetrics.On("IncDBQuery", "BatchInsert", "trustline_assets").Return()
		mockMetrics.On("IncDBTransaction", mock.Anything).Return()
		mockMetrics.On("ObserveDBTransactionDuration", mock.Anything, mock.Anything).Return()
		mockMetrics.On("ObserveDBQueryDuration", "BatchGetByIDs", "trustline_assets", mock.Anything).Return()
		mockMetrics.On("IncDBQuery", "BatchGetByIDs", "trustline_assets").Return()

		assetModel := &wbdata.TrustlineAssetModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetrics,
		}

		service := &tokenCacheService{
			db:                  dbConnectionPool,
			trustlineAssetModel: assetModel,
		}

		changes := []types.TrustlineChange{
			{AccountID: "GTEST1", Asset: "USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN", Operation: types.TrustlineOpAdd},
			{AccountID: "GTEST2", Asset: "EURC:GDHU6WRG4IEQXM5NZ4BMPKOXHW76MZM4Y2IEMFDVXBSDP6SJY4ITNPP2", Operation: types.TrustlineOpAdd},
		}

		// nil dbTx is fine for this test - it creates its own transaction internally
		err = service.EnsureTrustlineAssetsExist(ctx, nil, changes)
		require.NoError(t, err)

		// Verify assets were inserted with correct deterministic IDs
		expectedID1 := wbdata.DeterministicAssetID("USDC", "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN")
		expectedID2 := wbdata.DeterministicAssetID("EURC", "GDHU6WRG4IEQXM5NZ4BMPKOXHW76MZM4Y2IEMFDVXBSDP6SJY4ITNPP2")

		assets, err := assetModel.BatchGetByIDs(ctx, []int64{expectedID1, expectedID2})
		require.NoError(t, err)
		assert.Len(t, assets, 2)

		// Clean up
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM trustline_assets`)
		require.NoError(t, err)
	})

	t.Run("idempotent - calling twice with same assets succeeds", func(t *testing.T) {
		dbt := dbtest.Open(t)
		defer dbt.Close()
		dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
		require.NoError(t, err)
		defer dbConnectionPool.Close()

		// Clean up
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM trustline_assets`)
		require.NoError(t, err)

		mockMetrics := &metrics.MockMetricsService{}
		mockMetrics.On("ObserveDBQueryDuration", "BatchInsert", "trustline_assets", mock.Anything).Return()
		mockMetrics.On("IncDBQuery", "BatchInsert", "trustline_assets").Return()
		mockMetrics.On("IncDBTransaction", mock.Anything).Return()
		mockMetrics.On("ObserveDBTransactionDuration", mock.Anything, mock.Anything).Return()

		assetModel := &wbdata.TrustlineAssetModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetrics,
		}

		service := &tokenCacheService{
			db:                  dbConnectionPool,
			trustlineAssetModel: assetModel,
		}

		changes := []types.TrustlineChange{
			{AccountID: "GTEST1", Asset: "USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN", Operation: types.TrustlineOpAdd},
		}

		// First call - nil dbTx creates its own transaction internally
		err = service.EnsureTrustlineAssetsExist(ctx, nil, changes)
		require.NoError(t, err)

		// Second call - should succeed due to ON CONFLICT DO NOTHING
		err = service.EnsureTrustlineAssetsExist(ctx, nil, changes)
		require.NoError(t, err)

		// Clean up
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM trustline_assets`)
		require.NoError(t, err)
	})
}
