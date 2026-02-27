// Tests for account token caching service using PostgreSQL.
package services

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	wbdata "github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/indexer"
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

func boolPtr(b bool) *bool {
	return &b
}

func ptrToScMap(m xdr.ScMap) **xdr.ScMap {
	ptr := &m
	return &ptr
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

		assetID := wbdata.DeterministicAssetID("USDC", "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN")
		err = db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return trustlineAssetModel.BatchInsert(ctx, dbTx, []wbdata.TrustlineAsset{
				{ID: assetID, Code: "USDC", Issuer: "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN"},
			})
		})
		require.NoError(t, err)

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
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM trustline_assets`)
		require.NoError(t, err)
	}

	t.Run("empty changes returns no error", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		defer mockMetricsService.AssertExpectations(t)

		trustlineBalanceModel := &wbdata.TrustlineBalanceModel{DB: dbConnectionPool, MetricsService: mockMetricsService}
		nativeBalanceModel := &wbdata.NativeBalanceModel{DB: dbConnectionPool, MetricsService: mockMetricsService}
		sacBalanceModel := &wbdata.SACBalanceModel{DB: dbConnectionPool, MetricsService: mockMetricsService}

		service := NewTokenIngestionService(TokenIngestionServiceConfig{
			TrustlineBalanceModel: trustlineBalanceModel,
			NativeBalanceModel:    nativeBalanceModel,
			SACBalanceModel:       sacBalanceModel,
			NetworkPassphrase:     "Test SDF Network ; September 2015",
		})

		err = db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return service.ProcessTokenChanges(ctx, dbTx, map[indexer.TrustlineChangeKey]types.TrustlineChange{}, make(map[string]types.AccountChange), make(map[indexer.SACBalanceChangeKey]types.SACBalanceChange))
		})
		assert.NoError(t, err)
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
