// Tests for account token caching service using PostgreSQL.
package services

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	wbdata "github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/indexer"
	"github.com/stellar/wallet-backend/internal/indexer/processors"
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
	service := &tokenIngestionService{}

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
			code, issuer, err := indexer.ParseAssetString(tt.asset)
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

func TestGetAccountTrustlineBalances(t *testing.T) {
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	cleanUpDB := func() {
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM trustline_balances`)
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

		trustlineBalanceModel := &wbdata.TrustlineBalanceModel{DB: dbConnectionPool, MetricsService: mockMetricsService}

		got, err := trustlineBalanceModel.GetByAccount(ctx, "GBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB")
		assert.NoError(t, err)
		assert.Empty(t, got)
	})

	t.Run("account with trustlines returns assets", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", mock.Anything, mock.Anything, mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", mock.Anything, mock.Anything).Return()
		defer mockMetricsService.AssertExpectations(t)

		trustlineBalanceModel := &wbdata.TrustlineBalanceModel{DB: dbConnectionPool, MetricsService: mockMetricsService}
		trustlineAssetModel := &wbdata.TrustlineAssetModel{DB: dbConnectionPool, MetricsService: mockMetricsService}

		accountAddress := "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"

		// Insert trustline assets first using deterministic IDs
		assetID := wbdata.DeterministicAssetID("USDC", "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN")
		err = db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return trustlineAssetModel.BatchInsert(ctx, dbTx, []wbdata.TrustlineAsset{
				{ID: assetID, Code: "USDC", Issuer: "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN"},
			})
		})
		require.NoError(t, err)

		// Insert account trustline balances
		err = db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return trustlineBalanceModel.BatchCopy(ctx, dbTx, []wbdata.TrustlineBalance{
				{AccountAddress: accountAddress, AssetID: assetID, Balance: 0, Limit: 0, BuyingLiabilities: 0, SellingLiabilities: 0, Flags: 0, LedgerNumber: 100},
			})
		})
		require.NoError(t, err)

		got, err := trustlineBalanceModel.GetByAccount(ctx, accountAddress)
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
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM account_contract_tokens`)
		require.NoError(t, err)
	}

	t.Run("account with no contracts returns empty", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", mock.Anything, mock.Anything, mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", mock.Anything, mock.Anything).Return()
		defer mockMetricsService.AssertExpectations(t)

		accountContractTokensModel := &wbdata.AccountContractTokensModel{DB: dbConnectionPool, MetricsService: mockMetricsService}

		got, err := accountContractTokensModel.GetByAccount(ctx, "GBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB")
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

		accountContractTokensModel := &wbdata.AccountContractTokensModel{DB: dbConnectionPool, MetricsService: mockMetricsService}
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

		// Insert account contracts using deterministic UUID
		err = db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return accountContractTokensModel.BatchInsert(ctx, dbTx, map[string][]uuid.UUID{
				accountAddress: {numericID},
			})
		})
		require.NoError(t, err)

		got, err := accountContractTokensModel.GetByAccount(ctx, accountAddress)
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
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM trustline_balances`)
		require.NoError(t, err)
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM account_contract_tokens`)
		require.NoError(t, err)
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM trustline_assets`)
		require.NoError(t, err)
	}

	t.Run("empty changes returns no error", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		defer mockMetricsService.AssertExpectations(t)

		trustlineBalanceModel := &wbdata.TrustlineBalanceModel{DB: dbConnectionPool, MetricsService: mockMetricsService}
		accountContractTokensModel := &wbdata.AccountContractTokensModel{DB: dbConnectionPool, MetricsService: mockMetricsService}
		trustlineAssetModel := &wbdata.TrustlineAssetModel{DB: dbConnectionPool, MetricsService: mockMetricsService}
		contractModel := &wbdata.ContractModel{DB: dbConnectionPool, MetricsService: mockMetricsService}
		nativeBalanceModel := &wbdata.NativeBalanceModel{DB: dbConnectionPool, MetricsService: mockMetricsService}
		sacBalanceModel := &wbdata.SACBalanceModel{DB: dbConnectionPool, MetricsService: mockMetricsService}

		service := NewTokenIngestionService(nil, trustlineAssetModel, trustlineBalanceModel, nativeBalanceModel, sacBalanceModel, accountContractTokensModel, contractModel, "Test SDF Network ; September 2015")

		err = db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return service.ProcessTokenChanges(ctx, dbTx, map[indexer.TrustlineChangeKey]types.TrustlineChange{}, []types.ContractChange{}, make(map[string]types.AccountChange), make(map[indexer.SACBalanceChangeKey]types.SACBalanceChange))
		})
		assert.NoError(t, err)
	})

	t.Run("add SEP-41 contract stores contract ID", func(t *testing.T) {
		cleanUpDB()
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM contract_tokens`)
		require.NoError(t, err)
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", mock.Anything, mock.Anything, mock.Anything).Return()
		mockMetricsService.On("ObserveDBBatchSize", mock.Anything, mock.Anything, mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", mock.Anything, mock.Anything).Return()
		defer mockMetricsService.AssertExpectations(t)

		trustlineBalanceModel := &wbdata.TrustlineBalanceModel{DB: dbConnectionPool, MetricsService: mockMetricsService}
		accountContractTokensModel := &wbdata.AccountContractTokensModel{DB: dbConnectionPool, MetricsService: mockMetricsService}
		trustlineAssetModel := &wbdata.TrustlineAssetModel{DB: dbConnectionPool, MetricsService: mockMetricsService}
		contractModel := &wbdata.ContractModel{DB: dbConnectionPool, MetricsService: mockMetricsService}
		nativeBalanceModel := &wbdata.NativeBalanceModel{DB: dbConnectionPool, MetricsService: mockMetricsService}
		sacBalanceModel := &wbdata.SACBalanceModel{DB: dbConnectionPool, MetricsService: mockMetricsService}

		service := NewTokenIngestionService(nil, trustlineAssetModel, trustlineBalanceModel, nativeBalanceModel, sacBalanceModel, accountContractTokensModel, contractModel, "Test SDF Network ; September 2015")

		accountAddress := "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"
		contractID := "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4"

		// First insert the contract into contract_tokens using deterministic ID (simulating FetchAndStoreMetadata)
		numericID := wbdata.DeterministicContractID(contractID)
		err = db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return contractModel.BatchInsert(ctx, dbTx, []*wbdata.Contract{
				{ID: numericID, ContractID: contractID, Type: "SEP41"},
			})
		})
		require.NoError(t, err)

		// ProcessTokenChanges processes SEP-41 contracts via contractChanges parameter
		// SAC contracts are processed via sacBalanceChangesByKey parameter instead
		err = db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return service.ProcessTokenChanges(ctx, dbTx, map[indexer.TrustlineChangeKey]types.TrustlineChange{}, []types.ContractChange{
				{
					AccountID:    accountAddress,
					ContractID:   contractID,
					ContractType: types.ContractTypeSEP41,
				},
			}, make(map[string]types.AccountChange), make(map[indexer.SACBalanceChangeKey]types.SACBalanceChange))
		})
		assert.NoError(t, err)

		// Verify contract relationship is stored
		contracts, err := accountContractTokensModel.GetByAccount(ctx, accountAddress)
		assert.NoError(t, err)
		require.Len(t, contracts, 1)
		assert.Equal(t, contractID, contracts[0].ContractID)
	})
}

func TestTokenProcessor_ProcessContractCode(t *testing.T) {
	ctx := context.Background()

	t.Run("valid_sep41_contract", func(t *testing.T) {
		contractValidatorMock := NewContractValidatorMock(t)
		tp := &tokenProcessor{
			contractValidator: contractValidatorMock,
			data: checkpointData{
				contractTypesByWasmHash: make(map[xdr.Hash]types.ContractType),
			},
		}

		hash := xdr.Hash{1, 2, 3}
		code := []byte{0xDE, 0xAD}
		contractValidatorMock.On("ValidateFromContractCode", mock.Anything, code).
			Return(types.ContractTypeSEP41, nil).Once()

		err := tp.ProcessContractCode(ctx, hash, code)
		require.NoError(t, err)
		assert.Equal(t, types.ContractTypeSEP41, tp.data.contractTypesByWasmHash[hash])
		assert.Equal(t, 1, tp.entries)
	})

	t.Run("validator_error_skips_entry", func(t *testing.T) {
		contractValidatorMock := NewContractValidatorMock(t)
		tp := &tokenProcessor{
			contractValidator: contractValidatorMock,
			data: checkpointData{
				contractTypesByWasmHash: make(map[xdr.Hash]types.ContractType),
			},
		}

		hash := xdr.Hash{4, 5, 6}
		code := []byte{0xBA, 0xD0}
		contractValidatorMock.On("ValidateFromContractCode", mock.Anything, code).
			Return(types.ContractTypeUnknown, errors.New("invalid WASM")).Once()

		err := tp.ProcessContractCode(ctx, hash, code)
		require.NoError(t, err, "validator error should not propagate")
		assert.Empty(t, tp.data.contractTypesByWasmHash, "no entry should be stored on error")
		assert.Equal(t, 0, tp.entries, "entries counter should not be incremented")
	})

	t.Run("multiple_contract_codes", func(t *testing.T) {
		contractValidatorMock := NewContractValidatorMock(t)
		tp := &tokenProcessor{
			contractValidator: contractValidatorMock,
			data: checkpointData{
				contractTypesByWasmHash: make(map[xdr.Hash]types.ContractType),
			},
		}

		hash1 := xdr.Hash{10}
		code1 := []byte{0x01}
		hash2 := xdr.Hash{20}
		code2 := []byte{0x02}

		contractValidatorMock.On("ValidateFromContractCode", mock.Anything, code1).
			Return(types.ContractTypeSEP41, nil).Once()
		contractValidatorMock.On("ValidateFromContractCode", mock.Anything, code2).
			Return(types.ContractTypeSEP41, nil).Once()

		require.NoError(t, tp.ProcessContractCode(ctx, hash1, code1))
		require.NoError(t, tp.ProcessContractCode(ctx, hash2, code2))

		assert.Len(t, tp.data.contractTypesByWasmHash, 2)
		assert.Equal(t, types.ContractTypeSEP41, tp.data.contractTypesByWasmHash[hash1])
		assert.Equal(t, types.ContractTypeSEP41, tp.data.contractTypesByWasmHash[hash2])
		assert.Equal(t, 2, tp.entries)
	})
}

// newTestTokenProcessor creates a tokenProcessor with minimal dependencies for unit testing.
// No DB models are needed since we only inspect accumulated slices, never flush.
func newTestTokenProcessor() *tokenProcessor {
	svc := &tokenIngestionService{networkPassphrase: network.TestNetworkPassphrase}
	return &tokenProcessor{
		service:          svc,
		checkpointLedger: 100,
		data: checkpointData{
			contractTokensByHolderAddress: make(map[string][]uuid.UUID),
			contractIDsByWasmHash:         make(map[xdr.Hash][]string),
			contractTypesByWasmHash:       make(map[xdr.Hash]types.ContractType),
			uniqueAssets:                  make(map[uuid.UUID]*wbdata.TrustlineAsset),
			uniqueContractTokens:          make(map[uuid.UUID]*wbdata.Contract),
		},
		batch: &batch{
			nativeBalances:    make([]wbdata.NativeBalance, 0),
			trustlineBalances: make([]wbdata.TrustlineBalance, 0),
			sacBalances:       make([]wbdata.SACBalance, 0),
		},
	}
}

// makeAccountChangeWithBalance builds an ingest.Change for an Account entry with the given balance and liabilities.
func makeAccountChangeWithBalance(address string, balance xdr.Int64, numSubEntries xdr.Uint32, buyingLiab, sellingLiab xdr.Int64) ingest.Change {
	accountID := xdr.MustAddress(address)
	return ingest.Change{
		Type: xdr.LedgerEntryTypeAccount,
		Post: &xdr.LedgerEntry{
			Data: xdr.LedgerEntryData{
				Type: xdr.LedgerEntryTypeAccount,
				Account: &xdr.AccountEntry{
					AccountId:     accountID,
					Balance:       balance,
					NumSubEntries: numSubEntries,
					Ext: xdr.AccountEntryExt{
						V: 1,
						V1: &xdr.AccountEntryExtensionV1{
							Liabilities: xdr.Liabilities{
								Buying:  buyingLiab,
								Selling: sellingLiab,
							},
							Ext: xdr.AccountEntryExtensionV1Ext{V: 0},
						},
					},
				},
			},
		},
	}
}

// makeTrustlineChange builds an ingest.Change for a CreditAlphanum4 trustline.
func makeTrustlineChange(address, assetCode, assetIssuer string, balance, limit xdr.Int64) ingest.Change {
	accountID := xdr.MustAddress(address)
	asset, err := xdr.NewCreditAsset(assetCode, assetIssuer)
	if err != nil {
		panic(err)
	}
	trustlineAsset := asset.ToTrustLineAsset()

	return ingest.Change{
		Type: xdr.LedgerEntryTypeTrustline,
		Post: &xdr.LedgerEntry{
			Data: xdr.LedgerEntryData{
				Type: xdr.LedgerEntryTypeTrustline,
				TrustLine: &xdr.TrustLineEntry{
					AccountId: accountID,
					Asset:     trustlineAsset,
					Balance:   balance,
					Limit:     limit,
					Flags:     xdr.Uint32(xdr.TrustLineFlagsAuthorizedFlag),
					Ext: xdr.TrustLineEntryExt{
						V: 1,
						V1: &xdr.TrustLineEntryV1{
							Liabilities: xdr.Liabilities{Buying: 100, Selling: 200},
							Ext:         xdr.TrustLineEntryV1Ext{V: 0},
						},
					},
				},
			},
		},
	}
}

// makePoolShareTrustlineChange builds an ingest.Change for a pool share trustline (should be skipped).
func makePoolShareTrustlineChange(address string) ingest.Change {
	accountID := xdr.MustAddress(address)
	poolID := xdr.PoolId{1, 2, 3}
	return ingest.Change{
		Type: xdr.LedgerEntryTypeTrustline,
		Post: &xdr.LedgerEntry{
			Data: xdr.LedgerEntryData{
				Type: xdr.LedgerEntryTypeTrustline,
				TrustLine: &xdr.TrustLineEntry{
					AccountId: accountID,
					Asset: xdr.TrustLineAsset{
						Type:            xdr.AssetTypeAssetTypePoolShare,
						LiquidityPoolId: &poolID,
					},
					Balance: 1000,
					Limit:   2000,
					Ext:     xdr.TrustLineEntryExt{V: 0},
				},
			},
		},
	}
}

// makeContractInstanceChange builds an ingest.Change for a ContractData entry with
// ScvLedgerKeyContractInstance key and a WASM executable (non-SAC).
func makeContractInstanceChange(contractHash [32]byte, wasmHash xdr.Hash) ingest.Change {
	return ingest.Change{
		Type: xdr.LedgerEntryTypeContractData,
		Post: &xdr.LedgerEntry{
			Data: xdr.LedgerEntryData{
				Type: xdr.LedgerEntryTypeContractData,
				ContractData: &xdr.ContractDataEntry{
					Contract: xdr.ScAddress{
						Type:       xdr.ScAddressTypeScAddressTypeContract,
						ContractId: (*xdr.ContractId)(&contractHash),
					},
					Key:        xdr.ScVal{Type: xdr.ScValTypeScvLedgerKeyContractInstance},
					Durability: xdr.ContractDataDurabilityPersistent,
					Val: xdr.ScVal{
						Type: xdr.ScValTypeScvContractInstance,
						Instance: &xdr.ScContractInstance{
							Executable: xdr.ContractExecutable{
								Type:     xdr.ContractExecutableTypeContractExecutableWasm,
								WasmHash: &wasmHash,
							},
						},
					},
				},
			},
		},
	}
}

// makeContractBalanceChange builds an ingest.Change for a ContractData entry with
// a Balance key (non-SAC). The holder is an account G-address.
func makeContractBalanceChange(contractHash [32]byte, holderAddress string) ingest.Change {
	return ingest.Change{
		Type: xdr.LedgerEntryTypeContractData,
		Post: &xdr.LedgerEntry{
			Data: xdr.LedgerEntryData{
				Type: xdr.LedgerEntryTypeContractData,
				ContractData: &xdr.ContractDataEntry{
					Contract: xdr.ScAddress{
						Type:       xdr.ScAddressTypeScAddressTypeContract,
						ContractId: (*xdr.ContractId)(&contractHash),
					},
					Key: xdr.ScVal{
						Type: xdr.ScValTypeScvVec,
						Vec: ptrToScVec([]xdr.ScVal{
							{Type: xdr.ScValTypeScvSymbol, Sym: ptrToScSymbol("Balance")},
							{
								Type: xdr.ScValTypeScvAddress,
								Address: &xdr.ScAddress{
									Type:      xdr.ScAddressTypeScAddressTypeAccount,
									AccountId: ptrToAccountID(holderAddress),
								},
							},
						}),
					},
					Durability: xdr.ContractDataDurabilityPersistent,
					Val:        makeBalanceMapVal(1000, true, false),
				},
			},
		},
	}
}

func boolPtr(b bool) *bool {
	return &b
}

func ptrToScMap(m xdr.ScMap) **xdr.ScMap {
	ptr := &m
	return &ptr
}

// makeBalanceMapVal creates a ScVal map with amount, authorized, and clawback fields.
func makeBalanceMapVal(amountLo xdr.Uint64, authorized, clawback bool) xdr.ScVal {
	m := xdr.ScMap{
		{
			Key: xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: ptrToScSymbol("amount")},
			Val: xdr.ScVal{
				Type: xdr.ScValTypeScvI128,
				I128: &xdr.Int128Parts{
					Hi: 0,
					Lo: amountLo,
				},
			},
		},
		{
			Key: xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: ptrToScSymbol("authorized")},
			Val: xdr.ScVal{Type: xdr.ScValTypeScvBool, B: boolPtr(authorized)},
		},
		{
			Key: xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: ptrToScSymbol("clawback")},
			Val: xdr.ScVal{Type: xdr.ScValTypeScvBool, B: boolPtr(clawback)},
		},
	}
	return xdr.ScVal{
		Type: xdr.ScValTypeScvMap,
		Map:  ptrToScMap(m),
	}
}

func TestTokenProcessor_ProcessEntry(t *testing.T) {
	ctx := context.Background()

	t.Run("account_entry", func(t *testing.T) {
		tp := newTestTokenProcessor()
		address := "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"
		balance := xdr.Int64(100_000_000) // 10 XLM
		numSubEntries := xdr.Uint32(3)
		buyingLiab := xdr.Int64(5_000_000)
		sellingLiab := xdr.Int64(2_000_000)

		change := makeAccountChangeWithBalance(address, balance, numSubEntries, buyingLiab, sellingLiab)
		err := tp.ProcessEntry(ctx, change)
		require.NoError(t, err)

		// minimumBalance = (MinimumBaseReserveCount + numSubEntries + numSponsoring - numSponsored) * BaseReserveStroops + sellingLiab
		// With v1 ext (no v2): numSponsoring=0, numSponsored=0
		// = (2 + 3 + 0 - 0) * 5_000_000 + 2_000_000 = 27_000_000
		expectedMinBalance := int64(processors.MinimumBaseReserveCount+3)*processors.BaseReserveStroops + int64(sellingLiab)

		require.Len(t, tp.batch.nativeBalances, 1)
		nb := tp.batch.nativeBalances[0]
		assert.Equal(t, address, nb.AccountAddress)
		assert.Equal(t, int64(balance), nb.Balance)
		assert.Equal(t, expectedMinBalance, nb.MinimumBalance)
		assert.Equal(t, int64(buyingLiab), nb.BuyingLiabilities)
		assert.Equal(t, int64(sellingLiab), nb.SellingLiabilities)
		assert.Equal(t, uint32(100), nb.LedgerNumber)
		assert.Equal(t, 1, tp.entries)
		assert.Equal(t, 1, tp.accountCount)
		assert.Empty(t, tp.batch.trustlineBalances)
		assert.Empty(t, tp.batch.sacBalances)
	})

	t.Run("trustline_entry", func(t *testing.T) {
		tp := newTestTokenProcessor()
		address := "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"
		issuer := "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN"
		assetCode := "USDC"

		change := makeTrustlineChange(address, assetCode, issuer, 5_000_000, 100_000_000)
		err := tp.ProcessEntry(ctx, change)
		require.NoError(t, err)

		require.Len(t, tp.batch.trustlineBalances, 1)
		tb := tp.batch.trustlineBalances[0]
		assert.Equal(t, address, tb.AccountAddress)
		assert.Equal(t, wbdata.DeterministicAssetID(assetCode, issuer), tb.AssetID)
		assert.Equal(t, int64(5_000_000), tb.Balance)
		assert.Equal(t, int64(100_000_000), tb.Limit)
		assert.Equal(t, int64(100), tb.BuyingLiabilities)
		assert.Equal(t, int64(200), tb.SellingLiabilities)
		assert.Equal(t, uint32(xdr.TrustLineFlagsAuthorizedFlag), tb.Flags)
		assert.Equal(t, uint32(100), tb.LedgerNumber)

		// Assert asset is recorded in uniqueAssets
		assetID := wbdata.DeterministicAssetID(assetCode, issuer)
		require.Contains(t, tp.data.uniqueAssets, assetID)
		assert.Equal(t, assetCode, tp.data.uniqueAssets[assetID].Code)
		assert.Equal(t, issuer, tp.data.uniqueAssets[assetID].Issuer)

		assert.Equal(t, 1, tp.entries)
		assert.Equal(t, 1, tp.trustlineCount)
		assert.Empty(t, tp.batch.nativeBalances)
	})

	t.Run("trustline_pool_share_skipped", func(t *testing.T) {
		tp := newTestTokenProcessor()
		address := "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"

		change := makePoolShareTrustlineChange(address)
		err := tp.ProcessEntry(ctx, change)
		require.NoError(t, err)

		assert.Empty(t, tp.batch.trustlineBalances)
		assert.Empty(t, tp.batch.nativeBalances)
		assert.Empty(t, tp.batch.sacBalances)
		assert.Equal(t, 0, tp.entries)
		assert.Equal(t, 0, tp.trustlineCount)
	})

	t.Run("contract_instance_non_sac", func(t *testing.T) {
		tp := newTestTokenProcessor()
		contractHash := [32]byte{0xAA, 0xBB, 0xCC}
		wasmHash := xdr.Hash{0x11, 0x22, 0x33}

		change := makeContractInstanceChange(contractHash, wasmHash)
		err := tp.ProcessEntry(ctx, change)
		require.NoError(t, err)

		contractAddr := strkey.MustEncode(strkey.VersionByteContract, contractHash[:])
		contractUUID := wbdata.DeterministicContractID(contractAddr)

		// Should store the contract as Unknown type
		require.Contains(t, tp.data.uniqueContractTokens, contractUUID)
		contract := tp.data.uniqueContractTokens[contractUUID]
		assert.Equal(t, contractAddr, contract.ContractID)
		assert.Equal(t, string(types.ContractTypeUnknown), contract.Type)

		// Should track the contract ID by WASM hash
		require.Contains(t, tp.data.contractIDsByWasmHash, wasmHash)
		assert.Equal(t, []string{contractAddr}, tp.data.contractIDsByWasmHash[wasmHash])

		// entries incremented twice: once for instance, once for wasm hash tracking
		assert.Equal(t, 2, tp.entries)
	})

	t.Run("contract_balance_non_sac", func(t *testing.T) {
		tp := newTestTokenProcessor()
		contractHash := [32]byte{0xDD, 0xEE, 0xFF}
		holderAddress := "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"

		change := makeContractBalanceChange(contractHash, holderAddress)
		err := tp.ProcessEntry(ctx, change)
		require.NoError(t, err)

		contractAddr := strkey.MustEncode(strkey.VersionByteContract, contractHash[:])
		contractUUID := wbdata.DeterministicContractID(contractAddr)

		// Should track holder -> contract UUID mapping
		require.Contains(t, tp.data.contractTokensByHolderAddress, holderAddress)
		assert.Equal(t, []uuid.UUID{contractUUID}, tp.data.contractTokensByHolderAddress[holderAddress])

		assert.Equal(t, 1, tp.entries)
		// No SAC balance should be added since ContractBalanceFromContractData returns false
		// for arbitrary contracts (only returns true for actual SAC contract IDs)
		assert.Empty(t, tp.batch.sacBalances)
	})

	t.Run("unhandled_entry_type_ignored", func(t *testing.T) {
		tp := newTestTokenProcessor()

		change := ingest.Change{
			Type: xdr.LedgerEntryTypeOffer,
			Post: &xdr.LedgerEntry{
				Data: xdr.LedgerEntryData{
					Type: xdr.LedgerEntryTypeOffer,
					Offer: &xdr.OfferEntry{
						SellerId: xdr.MustAddress("GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"),
						OfferId:  1,
					},
				},
			},
		}
		err := tp.ProcessEntry(ctx, change)
		require.NoError(t, err)

		assert.Empty(t, tp.batch.nativeBalances)
		assert.Empty(t, tp.batch.trustlineBalances)
		assert.Empty(t, tp.batch.sacBalances)
		assert.Equal(t, 0, tp.entries)
		assert.Equal(t, 0, tp.accountCount)
		assert.Equal(t, 0, tp.trustlineCount)
	})
}
