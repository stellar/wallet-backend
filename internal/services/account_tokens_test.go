// Tests for account token caching service using PostgreSQL.
package services

import (
	"context"
	"io"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/ingest/sac"
	"github.com/stellar/go-stellar-sdk/strkey"
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

// Mock ChangeReader for testing
type mockChangeReader struct {
	changes []ingest.Change
	index   int
	err     error
}

func (m *mockChangeReader) Read() (ingest.Change, error) {
	if m.err != nil {
		return ingest.Change{}, m.err
	}
	if m.index >= len(m.changes) {
		return ingest.Change{}, io.EOF
	}
	change := m.changes[m.index]
	m.index++
	return change, nil
}

func (m *mockChangeReader) Close() error {
	return nil
}

// mockContractValidator implements ContractValidator for testing
type mockContractValidator struct {
	returnType types.ContractType
	returnErr  error
}

func (m *mockContractValidator) ValidateFromContractCode(_ context.Context, _ []byte) (types.ContractType, error) {
	return m.returnType, m.returnErr
}

func (m *mockContractValidator) Close(_ context.Context) error {
	return nil
}

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

		// Insert trustline assets first
		var assetIDMap map[string]int64
		err = db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			assetIDMap, err = trustlineAssetModel.BatchInsert(ctx, dbTx, []wbdata.TrustlineAsset{
				{Code: "USDC", Issuer: "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN"},
			})
			return err
		})
		require.NoError(t, err)

		// Insert account trustlines
		err = db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return accountTokensModel.BulkInsertTrustlines(ctx, dbTx, map[string][]int64{
				accountAddress: {assetIDMap["USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN"]},
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
		mockMetricsService.On("IncDBQuery", mock.Anything, mock.Anything).Return()
		defer mockMetricsService.AssertExpectations(t)

		accountTokensModel := &wbdata.AccountTokensModel{DB: dbConnectionPool, MetricsService: mockMetricsService}
		trustlineAssetModel := &wbdata.TrustlineAssetModel{DB: dbConnectionPool, MetricsService: mockMetricsService}
		contractModel := &wbdata.ContractModel{DB: dbConnectionPool, MetricsService: mockMetricsService}

		accountAddress := "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"
		contractID := "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4"

		// Insert contract into contract_tokens first to get a numeric ID
		var numericID int64
		err = db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			inserted, insertErr := contractModel.BatchInsert(ctx, dbTx, []wbdata.Contract{
				{ContractID: contractID, ContractType: "SAC"},
			})
			if insertErr != nil {
				return insertErr
			}
			numericID = inserted[contractID]
			return nil
		})
		require.NoError(t, err)

		// Insert account contracts using numeric ID
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
		assert.Equal(t, contractID, got[0])
	})
}

func TestCollectAccountTokensFromCheckpoint(t *testing.T) {
	ctx := context.Background()
	service := &tokenCacheService{
		networkPassphrase: "Test SDF Network ; September 2015",
		contractValidator: &mockContractValidator{returnType: types.ContractTypeUnknown, returnErr: nil},
	}

	t.Run("reads trustline entries", func(t *testing.T) {
		// Create a trustline entry
		accountID := xdr.MustAddress("GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N")
		asset := xdr.MustNewCreditAsset("USDC", "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN")
		trustlineAsset := asset.ToTrustLineAsset()

		trustlineEntry := xdr.LedgerEntry{
			Data: xdr.LedgerEntryData{
				Type: xdr.LedgerEntryTypeTrustline,
				TrustLine: &xdr.TrustLineEntry{
					AccountId: accountID,
					Asset:     trustlineAsset,
					Balance:   100,
				},
			},
		}

		changes := []ingest.Change{
			{
				Type: xdr.LedgerEntryTypeTrustline,
				Post: &trustlineEntry,
			},
		}

		reader := &mockChangeReader{changes: changes}
		cpData, err := service.collectAccountTokensFromCheckpoint(ctx, reader)

		require.NoError(t, err)
		assert.Len(t, cpData.TrustlinesByAccountAddress, 1)
		assert.Contains(t, cpData.TrustlinesByAccountAddress, "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N")
		expectedAsset := "USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN"
		assert.Contains(t, cpData.TrustlinesByAccountAddress["GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"], expectedAsset)
		assert.Empty(t, cpData.ContractsByHolderAddress)
	})

	t.Run("reads contract balance entries", func(t *testing.T) {
		contractHash := xdr.Hash{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}
		contractID := xdr.ContractId(contractHash)

		balanceKey := xdr.ScVal{
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
		}

		contractDataEntry := xdr.LedgerEntry{
			Data: xdr.LedgerEntryData{
				Type: xdr.LedgerEntryTypeContractData,
				ContractData: &xdr.ContractDataEntry{
					Contract: xdr.ScAddress{
						Type:       xdr.ScAddressTypeScAddressTypeContract,
						ContractId: &contractID,
					},
					Key:        balanceKey,
					Durability: xdr.ContractDataDurabilityPersistent,
					Val: xdr.ScVal{
						Type: xdr.ScValTypeScvI128,
					},
				},
			},
		}

		changes := []ingest.Change{
			{
				Type: xdr.LedgerEntryTypeContractData,
				Post: &contractDataEntry,
			},
		}

		reader := &mockChangeReader{changes: changes}
		cpData, err := service.collectAccountTokensFromCheckpoint(ctx, reader)

		require.NoError(t, err)
		assert.Empty(t, cpData.TrustlinesByAccountAddress)
		assert.Len(t, cpData.ContractsByHolderAddress, 1)
		assert.Contains(t, cpData.ContractsByHolderAddress, "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N")
	})

	t.Run("reads valid SAC contract instance entries", func(t *testing.T) {
		issuer := "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN"
		asset := xdr.MustNewCreditAsset("USDC", issuer)

		contractID, err := asset.ContractID(service.networkPassphrase)
		require.NoError(t, err)

		contractData, err := sac.AssetToContractData(false, "USDC", issuer, contractID)
		require.NoError(t, err)

		contractDataEntry := xdr.LedgerEntry{
			Data: contractData,
		}

		changes := []ingest.Change{
			{
				Type: xdr.LedgerEntryTypeContractData,
				Post: &contractDataEntry,
			},
		}

		reader := &mockChangeReader{changes: changes}
		cpData, err := service.collectAccountTokensFromCheckpoint(ctx, reader)

		require.NoError(t, err)
		assert.Empty(t, cpData.TrustlinesByAccountAddress)
		assert.Empty(t, cpData.ContractsByHolderAddress)

		assert.Len(t, cpData.ContractTypesByContractID, 1)
		contractAddress, err := strkey.Encode(strkey.VersionByteContract, contractID[:])
		require.NoError(t, err)
		assert.Contains(t, cpData.ContractTypesByContractID, contractAddress)
		assert.Equal(t, types.ContractTypeSAC, cpData.ContractTypesByContractID[contractAddress])
	})

	t.Run("handles context cancellation", func(t *testing.T) {
		cancelledCtx, cancel := context.WithCancel(ctx)
		cancel()

		reader := &mockChangeReader{
			changes: []ingest.Change{},
		}

		_, err := service.collectAccountTokensFromCheckpoint(cancelledCtx, reader)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cancelled")
	})

	t.Run("handles reader errors", func(t *testing.T) {
		reader := &mockChangeReader{
			err: assert.AnError,
		}

		_, err := service.collectAccountTokensFromCheckpoint(ctx, reader)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "reading checkpoint changes")
	})
}

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

		err := service.ProcessTokenChanges(ctx, nil, []types.TrustlineChange{}, []types.ContractChange{})
		assert.NoError(t, err)
	})

	t.Run("add contract stores contract ID", func(t *testing.T) {
		cleanUpDB()
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM contract_tokens`)
		require.NoError(t, err)
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", mock.Anything, mock.Anything, mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", mock.Anything, mock.Anything).Return()
		defer mockMetricsService.AssertExpectations(t)

		accountTokensModel := &wbdata.AccountTokensModel{DB: dbConnectionPool, MetricsService: mockMetricsService}
		trustlineAssetModel := &wbdata.TrustlineAssetModel{DB: dbConnectionPool, MetricsService: mockMetricsService}
		contractModel := &wbdata.ContractModel{DB: dbConnectionPool, MetricsService: mockMetricsService}

		service := NewTokenCacheWriter(dbConnectionPool, "Test SDF Network ; September 2015", nil, nil, nil, trustlineAssetModel, accountTokensModel, contractModel)

		accountAddress := "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"
		contractID := "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4"

		err := service.ProcessTokenChanges(ctx, nil, []types.TrustlineChange{}, []types.ContractChange{
			{
				AccountID:    accountAddress,
				ContractID:   contractID,
				ContractType: types.ContractTypeSAC,
			},
		})
		assert.NoError(t, err)

		// Verify contract is stored
		reader := NewTokenCacheReader(dbConnectionPool, trustlineAssetModel, accountTokensModel, contractModel)
		contracts, err := reader.GetAccountContracts(ctx, accountAddress)
		assert.NoError(t, err)
		assert.Contains(t, contracts, contractID)
	})
}

func TestGetOrInsertTrustlineAssets(t *testing.T) {
	ctx := context.Background()

	t.Run("empty changes returns empty map", func(t *testing.T) {
		service := &tokenCacheService{}

		assetIDMap, err := service.GetOrInsertTrustlineAssets(ctx, []types.TrustlineChange{})
		require.NoError(t, err)
		assert.Empty(t, assetIDMap)
	})

	t.Run("invalid asset format returns error", func(t *testing.T) {
		service := &tokenCacheService{}

		changes := []types.TrustlineChange{
			{AccountID: "GTEST1", Asset: "INVALID_NO_COLON", Operation: types.TrustlineOpAdd},
		}

		_, err := service.GetOrInsertTrustlineAssets(ctx, changes)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "parsing asset")
	})

	t.Run("returns asset IDs from PostgreSQL", func(t *testing.T) {
		dbt := dbtest.Open(t)
		defer dbt.Close()
		dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
		require.NoError(t, err)
		defer dbConnectionPool.Close()

		mockMetrics := &metrics.MockMetricsService{}
		mockMetrics.On("ObserveDBQueryDuration", "BatchGetOrInsert", "trustline_assets", mock.Anything).Return()
		mockMetrics.On("IncDBQuery", "BatchGetOrInsert", "trustline_assets").Return()

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

		assetIDMap, err := service.GetOrInsertTrustlineAssets(ctx, changes)
		require.NoError(t, err)

		assert.Len(t, assetIDMap, 2)
		assert.Contains(t, assetIDMap, "USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN")
		assert.Contains(t, assetIDMap, "EURC:GDHU6WRG4IEQXM5NZ4BMPKOXHW76MZM4Y2IEMFDVXBSDP6SJY4ITNPP2")
		assert.Greater(t, assetIDMap["USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN"], int64(0))
		assert.Greater(t, assetIDMap["EURC:GDHU6WRG4IEQXM5NZ4BMPKOXHW76MZM4Y2IEMFDVXBSDP6SJY4ITNPP2"], int64(0))
	})
}
