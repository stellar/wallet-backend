// Tests for the Account.balances GraphQL field resolver
package resolvers

import (
	"context"
	"errors"
	"testing"

	"github.com/alitto/pond/v2"
	"github.com/google/uuid"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	graphql1 "github.com/stellar/wallet-backend/internal/serve/graphql/generated"
	"github.com/stellar/wallet-backend/internal/services"
)

const (
	testAccountAddress       = "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"
	testContractAddress      = "CAZXRTOKNUQ2JQQF3NCRU7GYMDJNZ2NMQN6IGN4FCT5DWPODMPVEXSND"
	testSACContractAddress   = "CBHBD77PWZ3AXPQVYVDBHDKEMVNOR26UZUZHWCB6QC7J5SETQPRUQAS4"
	testSEP41ContractAddress = "CAZXRTOKNUQ2JQQF3NCRU7GYMDJNZ2NMQN6IGN4FCT5DWPODMPVEXSND"
	testUSDCIssuer           = "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN"
	testEURIssuer            = "GCEODJVUUVYVFD5KT4TOEDTMXQ76OPFOQC2EMYYMLPXQCUVPOB6XRWPQ"
	testNetworkPassphrase    = "Test SDF Network ; September 2015"
)

// Helper to create ScSymbol pointer
func ptrToScSymbol(s string) *xdr.ScSymbol {
	sym := xdr.ScSymbol(s)
	return &sym
}

// Helper to create ScMap pointer
func ptrToScMap(entries []xdr.ScMapEntry) **xdr.ScMap {
	m := xdr.ScMap(entries)
	ptr := &m
	return &ptr
}

// Helper to create SEP-41 contract data
func createSEP41Contract(contractID, name, symbol string, decimals uint32) *data.Contract { //nolint:unparam
	return &data.Contract{
		ContractID: contractID,
		Type:       string(types.ContractTypeSEP41),
		Name:       &name,
		Symbol:     &symbol,
		Decimals:   decimals,
	}
}

// Helper to create i128 ScVal for SEP-41 balance simulation response
func createI128ScVal(amount int64) xdr.ScVal {
	hi := int64(0)
	if amount < 0 {
		hi = -1
	}
	lo := xdr.Uint64(amount)
	return xdr.ScVal{
		Type: xdr.ScValTypeScvI128,
		I128: &xdr.Int128Parts{
			Hi: xdr.Int64(hi),
			Lo: lo,
		},
	}
}

func testParentAccount(address string) *types.Account {
	return &types.Account{StellarAddress: types.AddressBytea(address)}
}

func TestAccountResolver_Balances(t *testing.T) {
	// Success Cases
	t.Run("success - native balance only", func(t *testing.T) {
		ctx := context.Background()
		mockTrustlineBalanceModel := data.NewTrustlineBalanceModelMock(t)
		mockNativeBalanceModel := data.NewNativeBalanceModelMock(t)
		mockAccountContractTokens := data.NewAccountContractTokensModelMock(t)
		mockRPCService := services.NewRPCServiceMock(t)

		// Setup mocks - native balance comes from DB, not RPC
		mockNativeBalanceModel.On("GetByAccount", ctx, testAccountAddress).Return(&data.NativeBalance{AccountAddress: testAccountAddress, Balance: 10000000000}, nil)
		mockTrustlineBalanceModel.On("GetByAccount", ctx, testAccountAddress).Return([]data.TrustlineBalance{}, nil)
		mockAccountContractTokens.On("GetByAccount", ctx, testAccountAddress).Return([]*data.Contract{}, nil)
		mockRPCService.On("NetworkPassphrase").Return(testNetworkPassphrase)
		// No GetLedgerEntries call needed - native balance comes from DB

		resolver := &accountResolver{
			&Resolver{
				balanceReader:              NewBalanceReader(mockTrustlineBalanceModel, mockNativeBalanceModel, data.NewSACBalanceModelMock(t)),
				accountContractTokensModel: mockAccountContractTokens,
				rpcService:                 mockRPCService,
			},
		}

		balances, err := resolver.Balances(ctx, testParentAccount(testAccountAddress))
		require.NoError(t, err)
		require.Len(t, balances, 1)

		// Verify it's a native balance
		for _, balance := range balances {
			switch balance.GetTokenType() {
			case graphql1.TokenTypeNative:
				nativeBalance := balance.(*graphql1.NativeBalance)
				assert.Equal(t, "1000.0000000", nativeBalance.Balance)
				assert.Equal(t, graphql1.TokenTypeNative, nativeBalance.TokenType)
				assert.NotEmpty(t, nativeBalance.TokenID) // Native asset contract ID
			default:
				t.Errorf("unexpected balance type: %v", balance.GetTokenType())
			}
		}
	})

	t.Run("success - account with classic trustlines", func(t *testing.T) {
		ctx := context.Background()
		mockTrustlineBalanceModel := data.NewTrustlineBalanceModelMock(t)
		mockNativeBalanceModel := data.NewNativeBalanceModelMock(t)
		mockAccountContractTokens := data.NewAccountContractTokensModelMock(t)
		mockRPCService := services.NewRPCServiceMock(t)

		// Setup mocks - native and trustlines come from DB, not RPC
		mockNativeBalanceModel.On("GetByAccount", ctx, testAccountAddress).Return(&data.NativeBalance{AccountAddress: testAccountAddress, Balance: 5000000000}, nil) // 500 XLM
		mockTrustlineBalanceModel.On("GetByAccount", ctx, testAccountAddress).
			Return([]data.TrustlineBalance{
				{
					AssetID:            data.DeterministicAssetID("USDC", testUSDCIssuer),
					Code:               "USDC",
					Issuer:             testUSDCIssuer,
					Balance:            1000000000,  // 100 USDC
					Limit:              10000000000, // 1000 USDC
					BuyingLiabilities:  1000000,
					SellingLiabilities: 2000000,
					Flags:              uint32(xdr.TrustLineFlagsAuthorizedFlag),
					LedgerNumber:       12345,
				},
				{
					AssetID:            data.DeterministicAssetID("EUR", testEURIssuer),
					Code:               "EUR",
					Issuer:             testEURIssuer,
					Balance:            5000000000,  // 500 EUR
					Limit:              20000000000, // 2000 EUR
					BuyingLiabilities:  0,
					SellingLiabilities: 0,
					Flags:              uint32(xdr.TrustLineFlagsAuthorizedToMaintainLiabilitiesFlag),
					LedgerNumber:       12345,
				},
			}, nil)
		mockAccountContractTokens.On("GetByAccount", ctx, testAccountAddress).Return([]*data.Contract{}, nil)
		mockRPCService.On("NetworkPassphrase").Return(testNetworkPassphrase)
		// No GetLedgerEntries call needed - native and trustlines come from DB

		resolver := &accountResolver{
			&Resolver{
				balanceReader:              NewBalanceReader(mockTrustlineBalanceModel, mockNativeBalanceModel, data.NewSACBalanceModelMock(t)),
				accountContractTokensModel: mockAccountContractTokens,
				rpcService:                 mockRPCService,
			},
		}

		balances, err := resolver.Balances(ctx, testParentAccount(testAccountAddress))
		require.NoError(t, err)
		require.Len(t, balances, 3) // 1 native + 2 trustlines

		for _, balance := range balances {
			switch balance.GetTokenType() {
			case graphql1.TokenTypeNative:
				nativeBalance := balance.(*graphql1.NativeBalance)
				assert.Equal(t, "500.0000000", nativeBalance.Balance)
			case graphql1.TokenTypeClassic:
				trustlineBalance := balance.(*graphql1.TrustlineBalance)
				switch trustlineBalance.Code {
				case "USDC":
					assert.Equal(t, "100.0000000", trustlineBalance.Balance)
					assert.Equal(t, graphql1.TokenTypeClassic, trustlineBalance.TokenType)
					assert.Equal(t, testUSDCIssuer, trustlineBalance.Issuer)
					assert.Equal(t, "1000.0000000", trustlineBalance.Limit)
					assert.Equal(t, "0.1000000", trustlineBalance.BuyingLiabilities)
					assert.Equal(t, "0.2000000", trustlineBalance.SellingLiabilities)
					assert.True(t, trustlineBalance.IsAuthorized)
					assert.False(t, trustlineBalance.IsAuthorizedToMaintainLiabilities)
				case "EUR":
					assert.Equal(t, "500.0000000", trustlineBalance.Balance)
					assert.Equal(t, testEURIssuer, trustlineBalance.Issuer)
					assert.False(t, trustlineBalance.IsAuthorized)
					assert.True(t, trustlineBalance.IsAuthorizedToMaintainLiabilities)
				default:
					t.Errorf("unexpected trustline code: %s", trustlineBalance.Code)
				}
			default:
				t.Errorf("unexpected balance type: %v", balance.GetTokenType())
			}
		}
	})

	t.Run("success - contract address with SAC balances from DB", func(t *testing.T) {
		ctx := context.Background()
		mockSACBalanceModel := data.NewSACBalanceModelMock(t)
		mockAccountContractTokens := data.NewAccountContractTokensModelMock(t)
		mockRPCService := services.NewRPCServiceMock(t)

		// For contract addresses (C...), SAC balances come from DB
		sacContractID := data.DeterministicContractID(testSACContractAddress)
		mockSACBalanceModel.On("GetByAccount", ctx, testContractAddress).
			Return([]data.SACBalance{
				{
					AccountAddress:    testContractAddress,
					ContractID:        sacContractID,
					TokenID:           testSACContractAddress,
					Balance:           "2500.0000000",
					IsAuthorized:      true,
					IsClawbackEnabled: false,
					LedgerNumber:      1000,
					Code:              "USDC",
					Issuer:            testUSDCIssuer,
					Decimals:          7,
				},
			}, nil)
		mockAccountContractTokens.On("GetByAccount", ctx, testContractAddress).Return([]*data.Contract{}, nil)
		mockRPCService.On("NetworkPassphrase").Return(testNetworkPassphrase)

		resolver := &accountResolver{
			&Resolver{
				balanceReader:              NewBalanceReader(data.NewTrustlineBalanceModelMock(t), data.NewNativeBalanceModelMock(t), mockSACBalanceModel),
				accountContractTokensModel: mockAccountContractTokens,
				rpcService:                 mockRPCService,
			},
		}

		balances, err := resolver.Balances(ctx, testParentAccount(testContractAddress))
		require.NoError(t, err)
		require.Len(t, balances, 1) // 1 SAC

		for _, balance := range balances {
			switch balance.GetTokenType() {
			case graphql1.TokenTypeSac:
				sacBalance := balance.(*graphql1.SACBalance)
				assert.Equal(t, "2500.0000000", sacBalance.Balance)
				assert.Equal(t, graphql1.TokenTypeSac, sacBalance.TokenType)
				assert.Equal(t, testSACContractAddress, sacBalance.TokenID)
				assert.Equal(t, "USDC", sacBalance.Code)
				assert.Equal(t, testUSDCIssuer, sacBalance.Issuer)
				assert.True(t, sacBalance.IsAuthorized)
				assert.False(t, sacBalance.IsClawbackEnabled)
			default:
				t.Errorf("unexpected balance type: %v", balance.GetTokenType())
			}
		}
	})

	t.Run("success - account with SEP-41 contract balances", func(t *testing.T) {
		ctx := context.Background()
		mockTrustlineBalanceModel := data.NewTrustlineBalanceModelMock(t)
		mockNativeBalanceModel := data.NewNativeBalanceModelMock(t)
		mockAccountContractTokens := data.NewAccountContractTokensModelMock(t)
		mockRPCService := services.NewRPCServiceMock(t)
		mockContractMetadataService := services.NewContractMetadataServiceMock(t)

		// Setup mocks - native balance comes from DB
		mockNativeBalanceModel.On("GetByAccount", ctx, testAccountAddress).Return(&data.NativeBalance{AccountAddress: testAccountAddress, Balance: 1000000000}, nil) // 100 XLM
		mockTrustlineBalanceModel.On("GetByAccount", ctx, testAccountAddress).Return([]data.TrustlineBalance{}, nil)
		mockAccountContractTokens.On("GetByAccount", ctx, testAccountAddress).
			Return([]*data.Contract{createSEP41Contract(testSEP41ContractAddress, "MyToken", "MTK", 7)}, nil)
		mockRPCService.On("NetworkPassphrase").Return(testNetworkPassphrase)

		// Mock FetchSingleField for SEP-41 balance call
		mockContractMetadataService.On("FetchSingleField", ctx, testSEP41ContractAddress, "balance", mock.Anything).
			Return(createI128ScVal(50000000000), nil)
		// No GetLedgerEntries call needed - SEP-41 uses FetchSingleField, native comes from DB

		resolver := &accountResolver{
			&Resolver{
				balanceReader:              NewBalanceReader(mockTrustlineBalanceModel, mockNativeBalanceModel, data.NewSACBalanceModelMock(t)),
				accountContractTokensModel: mockAccountContractTokens,
				rpcService:                 mockRPCService,
				contractMetadataService:    mockContractMetadataService,
				pool:                       pond.NewPool(0),
			},
		}

		balances, err := resolver.Balances(ctx, testParentAccount(testAccountAddress))
		require.NoError(t, err)
		require.Len(t, balances, 2) // 1 native + 1 SEP-41

		for _, balance := range balances {
			switch balance.GetTokenType() {
			case graphql1.TokenTypeNative:
				// Native balance verified by count
			case graphql1.TokenTypeSep41:
				sep41Balance := balance.(*graphql1.SEP41Balance)
				assert.Equal(t, "5000.0000000", sep41Balance.Balance)
				assert.Equal(t, graphql1.TokenTypeSep41, sep41Balance.TokenType)
				assert.Equal(t, testSEP41ContractAddress, sep41Balance.TokenID)
				assert.Equal(t, "MyToken", sep41Balance.Name)
				assert.Equal(t, "MTK", sep41Balance.Symbol)
				assert.Equal(t, int32(7), sep41Balance.Decimals)
			default:
				t.Errorf("unexpected balance type: %v", balance.GetTokenType())
			}
		}
	})

	t.Run("success - mixed balances (native + trustlines + SEP-41)", func(t *testing.T) {
		ctx := context.Background()
		mockTrustlineBalanceModel := data.NewTrustlineBalanceModelMock(t)
		mockNativeBalanceModel := data.NewNativeBalanceModelMock(t)
		mockAccountContractTokens := data.NewAccountContractTokensModelMock(t)
		mockRPCService := services.NewRPCServiceMock(t)
		mockContractMetadataService := services.NewContractMetadataServiceMock(t)

		// Setup mocks - native and trustlines come from DB
		// For G-addresses, SAC balances ARE trustlines (same underlying data)
		mockNativeBalanceModel.On("GetByAccount", ctx, testAccountAddress).Return(&data.NativeBalance{AccountAddress: testAccountAddress, Balance: 2000000000}, nil) // 200 XLM
		mockTrustlineBalanceModel.On("GetByAccount", ctx, testAccountAddress).
			Return([]data.TrustlineBalance{
				{
					AssetID:      data.DeterministicAssetID("USDC", testUSDCIssuer),
					Code:         "USDC",
					Issuer:       testUSDCIssuer,
					Balance:      1000000000,
					Limit:        10000000000,
					Flags:        uint32(xdr.TrustLineFlagsAuthorizedFlag),
					LedgerNumber: 12345,
				},
			}, nil)
		// For G-addresses, only SEP-41 contracts are returned (SAC balances are trustlines)
		mockAccountContractTokens.On("GetByAccount", ctx, testAccountAddress).
			Return([]*data.Contract{
				createSEP41Contract(testSEP41ContractAddress, "CustomToken", "CTK", 6),
			}, nil)
		mockRPCService.On("NetworkPassphrase").Return(testNetworkPassphrase)

		// Mock FetchSingleField for SEP-41 balance call
		mockContractMetadataService.On("FetchSingleField", ctx, testSEP41ContractAddress, "balance", mock.Anything).
			Return(createI128ScVal(30000000000), nil)

		resolver := &accountResolver{
			&Resolver{
				balanceReader:              NewBalanceReader(mockTrustlineBalanceModel, mockNativeBalanceModel, data.NewSACBalanceModelMock(t)),
				accountContractTokensModel: mockAccountContractTokens,
				rpcService:                 mockRPCService,
				contractMetadataService:    mockContractMetadataService,
				pool:                       pond.NewPool(0),
			},
		}

		balances, err := resolver.Balances(ctx, testParentAccount(testAccountAddress))
		require.NoError(t, err)
		require.Len(t, balances, 3) // native + trustline + SEP-41

		for _, balance := range balances {
			switch balance.GetTokenType() {
			case graphql1.TokenTypeNative:
				// Native balance verified by count
			case graphql1.TokenTypeClassic:
				// Classic trustline verified by count
			case graphql1.TokenTypeSep41:
				sep41Balance := balance.(*graphql1.SEP41Balance)
				assert.Equal(t, "CustomToken", sep41Balance.Name)
				assert.Equal(t, "CTK", sep41Balance.Symbol)
				assert.Equal(t, int32(6), sep41Balance.Decimals)
			default:
				t.Errorf("unexpected balance type: %v", balance.GetTokenType())
			}
		}
	})

	t.Run("success - contract address with SEP-41 only (no SAC balances)", func(t *testing.T) {
		ctx := context.Background()
		mockSACBalanceModel := data.NewSACBalanceModelMock(t)
		mockAccountContractTokens := data.NewAccountContractTokensModelMock(t)
		mockRPCService := services.NewRPCServiceMock(t)
		mockContractMetadataService := services.NewContractMetadataServiceMock(t)

		// For contract addresses (C...), SAC balances come from DB (empty in this test)
		mockSACBalanceModel.On("GetByAccount", ctx, testContractAddress).Return([]data.SACBalance{}, nil)
		// SEP-41 contracts come from account_contract_tokens
		mockAccountContractTokens.On("GetByAccount", ctx, testContractAddress).
			Return([]*data.Contract{createSEP41Contract(testSEP41ContractAddress, "Token", "TKN", 7)}, nil)
		mockRPCService.On("NetworkPassphrase").Return(testNetworkPassphrase)

		// Mock FetchSingleField for SEP-41 balance call
		mockContractMetadataService.On("FetchSingleField", ctx, testSEP41ContractAddress, "balance", mock.Anything).
			Return(createI128ScVal(10000000000), nil)

		resolver := &accountResolver{
			&Resolver{
				balanceReader:              NewBalanceReader(data.NewTrustlineBalanceModelMock(t), data.NewNativeBalanceModelMock(t), mockSACBalanceModel),
				accountContractTokensModel: mockAccountContractTokens,
				rpcService:                 mockRPCService,
				contractMetadataService:    mockContractMetadataService,
				pool:                       pond.NewPool(0),
			},
		}

		balances, err := resolver.Balances(ctx, testParentAccount(testContractAddress))
		require.NoError(t, err)
		require.Len(t, balances, 1)

		for _, balance := range balances {
			switch balance.GetTokenType() {
			case graphql1.TokenTypeSep41:
				sep41Balance := balance.(*graphql1.SEP41Balance)
				assert.Equal(t, "1000.0000000", sep41Balance.Balance)
				assert.Equal(t, "Token", sep41Balance.Name)
				assert.Equal(t, "TKN", sep41Balance.Symbol)
				assert.Equal(t, int32(7), sep41Balance.Decimals)
			default:
				t.Errorf("unexpected balance type: %v", balance.GetTokenType())
			}
		}
	})

	t.Run("success - trustline with V0 extension (no liabilities)", func(t *testing.T) {
		ctx := context.Background()
		mockTrustlineBalanceModel := data.NewTrustlineBalanceModelMock(t)
		mockNativeBalanceModel := data.NewNativeBalanceModelMock(t)
		mockAccountContractTokens := data.NewAccountContractTokensModelMock(t)
		mockRPCService := services.NewRPCServiceMock(t)

		// Native and trustlines come from DB (V0 extension - no liabilities)
		mockNativeBalanceModel.On("GetByAccount", ctx, testAccountAddress).Return(&data.NativeBalance{AccountAddress: testAccountAddress, Balance: 1000000000}, nil) // 100 XLM
		mockTrustlineBalanceModel.On("GetByAccount", ctx, testAccountAddress).
			Return([]data.TrustlineBalance{
				{
					AssetID:            data.DeterministicAssetID("USDC", testUSDCIssuer),
					Code:               "USDC",
					Issuer:             testUSDCIssuer,
					Balance:            1000000000,
					Limit:              10000000000,
					Flags:              uint32(xdr.TrustLineFlagsAuthorizedFlag),
					BuyingLiabilities:  0,
					SellingLiabilities: 0,
					LedgerNumber:       12345,
				},
			}, nil)
		mockAccountContractTokens.On("GetByAccount", ctx, testAccountAddress).Return([]*data.Contract{}, nil)
		mockRPCService.On("NetworkPassphrase").Return(testNetworkPassphrase)
		// No GetLedgerEntries call needed - native and trustlines come from DB

		resolver := &accountResolver{
			&Resolver{
				balanceReader:              NewBalanceReader(mockTrustlineBalanceModel, mockNativeBalanceModel, data.NewSACBalanceModelMock(t)),
				accountContractTokensModel: mockAccountContractTokens,
				rpcService:                 mockRPCService,
			},
		}

		balances, err := resolver.Balances(ctx, testParentAccount(testAccountAddress))
		require.NoError(t, err)

		for _, balance := range balances {
			switch balance.GetTokenType() {
			case graphql1.TokenTypeNative:
				// Native balance verified by count
			case graphql1.TokenTypeClassic:
				trustlineBalance := balance.(*graphql1.TrustlineBalance)
				assert.Equal(t, "0.0000000", trustlineBalance.BuyingLiabilities)
				assert.Equal(t, "0.0000000", trustlineBalance.SellingLiabilities)
			default:
				t.Errorf("unexpected balance type: %v", balance.GetTokenType())
			}
		}
	})

	// Error Cases

	t.Run("error - GetTrustlineBalances fails", func(t *testing.T) {
		ctx := context.Background()
		mockTrustlineBalanceModel := data.NewTrustlineBalanceModelMock(t)
		mockNativeBalanceModel := data.NewNativeBalanceModelMock(t)
		mockNativeBalanceModel.On("GetByAccount", mock.Anything, mock.Anything).Return(&data.NativeBalance{AccountAddress: testAccountAddress, Balance: 10000000000}, nil).Maybe()
		mockRPCService := services.NewRPCServiceMock(t)

		mockRPCService.On("NetworkPassphrase").Return(testNetworkPassphrase)
		mockTrustlineBalanceModel.On("GetByAccount", ctx, testAccountAddress).
			Return([]data.TrustlineBalance{}, errors.New("database query failed"))

		resolver := &accountResolver{
			&Resolver{
				balanceReader: NewBalanceReader(mockTrustlineBalanceModel, mockNativeBalanceModel, data.NewSACBalanceModelMock(t)),
				rpcService:    mockRPCService,
			},
		}

		balances, err := resolver.Balances(ctx, testParentAccount(testAccountAddress))
		require.Error(t, err)
		assert.Contains(t, err.Error(), ErrMsgBalancesFetchFailed)
		assert.Nil(t, balances)
	})

	t.Run("error - GetAccountContracts fails", func(t *testing.T) {
		ctx := context.Background()
		mockTrustlineBalanceModel := data.NewTrustlineBalanceModelMock(t)
		mockNativeBalanceModel := data.NewNativeBalanceModelMock(t)
		mockNativeBalanceModel.On("GetByAccount", mock.Anything, mock.Anything).Return(&data.NativeBalance{AccountAddress: testAccountAddress, Balance: 10000000000}, nil).Maybe()
		mockAccountContractTokens := data.NewAccountContractTokensModelMock(t)
		mockRPCService := services.NewRPCServiceMock(t)

		mockRPCService.On("NetworkPassphrase").Return(testNetworkPassphrase)
		mockTrustlineBalanceModel.On("GetByAccount", ctx, testAccountAddress).
			Return([]data.TrustlineBalance{}, nil)
		mockAccountContractTokens.On("GetByAccount", ctx, testAccountAddress).
			Return([]*data.Contract{}, errors.New("database query failed"))

		resolver := &accountResolver{
			&Resolver{
				balanceReader:              NewBalanceReader(mockTrustlineBalanceModel, mockNativeBalanceModel, data.NewSACBalanceModelMock(t)),
				accountContractTokensModel: mockAccountContractTokens,
				rpcService:                 mockRPCService,
			},
		}

		balances, err := resolver.Balances(ctx, testParentAccount(testAccountAddress))
		require.Error(t, err)
		assert.Contains(t, err.Error(), ErrMsgBalancesFetchFailed)
		assert.Nil(t, balances)
	})

	t.Run("error - GetSACBalances DB fails for contract address", func(t *testing.T) {
		ctx := context.Background()
		mockSACBalanceModel := data.NewSACBalanceModelMock(t)
		mockRPCService := services.NewRPCServiceMock(t)

		// For contract addresses (C...), SAC balances come from DB - mock DB error
		mockSACBalanceModel.On("GetByAccount", ctx, testContractAddress).
			Return(nil, errors.New("database connection failed"))
		mockRPCService.On("NetworkPassphrase").Return(testNetworkPassphrase)

		resolver := &accountResolver{
			&Resolver{
				balanceReader: NewBalanceReader(data.NewTrustlineBalanceModelMock(t), data.NewNativeBalanceModelMock(t), mockSACBalanceModel),
				rpcService:    mockRPCService,
			},
		}

		balances, err := resolver.Balances(ctx, testParentAccount(testContractAddress))
		require.Error(t, err)
		assert.Contains(t, err.Error(), ErrMsgBalancesFetchFailed)
		assert.Nil(t, balances)
	})

	t.Run("error - invalid trustline issuer causes error", func(t *testing.T) {
		ctx := context.Background()
		mockTrustlineBalanceModel := data.NewTrustlineBalanceModelMock(t)
		mockNativeBalanceModel := data.NewNativeBalanceModelMock(t)
		mockNativeBalanceModel.On("GetByAccount", mock.Anything, mock.Anything).Return(&data.NativeBalance{AccountAddress: testAccountAddress, Balance: 10000000000}, nil).Maybe()
		mockRPCService := services.NewRPCServiceMock(t)

		// Return trustline with invalid issuer that will fail ContractID computation
		mockTrustlineBalanceModel.On("GetByAccount", ctx, testAccountAddress).
			Return([]data.TrustlineBalance{
				{
					AssetID: uuid.New(),
					Code:    ":",
					Issuer:  "@:::", // Invalid issuer address
				},
			}, nil)
		mockRPCService.On("NetworkPassphrase").Return(testNetworkPassphrase)

		resolver := &accountResolver{
			&Resolver{
				balanceReader: NewBalanceReader(mockTrustlineBalanceModel, mockNativeBalanceModel, data.NewSACBalanceModelMock(t)),
				rpcService:    mockRPCService,
			},
		}

		balances, err := resolver.Balances(ctx, testParentAccount(testAccountAddress))
		require.Error(t, err)
		assert.Contains(t, err.Error(), ErrMsgBalancesFetchFailed)
		assert.Nil(t, balances)
	})

	// Note: Tests for "XDR decoding fails" and "SAC missing amount field" were removed
	// because SAC balances for G-addresses now come from trustlines, and SAC balances
	// for C-addresses come from the database with pre-validated data.

	t.Run("error - SEP-41 wrong type (map instead of i128)", func(t *testing.T) {
		ctx := context.Background()
		mockTrustlineBalanceModel := data.NewTrustlineBalanceModelMock(t)
		mockNativeBalanceModel := data.NewNativeBalanceModelMock(t)
		mockAccountContractTokens := data.NewAccountContractTokensModelMock(t)
		mockRPCService := services.NewRPCServiceMock(t)
		mockContractMetadataService := services.NewContractMetadataServiceMock(t)

		// Setup mocks - native balance comes from DB
		mockNativeBalanceModel.On("GetByAccount", ctx, testAccountAddress).Return(&data.NativeBalance{AccountAddress: testAccountAddress, Balance: 1000000000}, nil)
		mockTrustlineBalanceModel.On("GetByAccount", ctx, testAccountAddress).Return([]data.TrustlineBalance{}, nil)
		mockAccountContractTokens.On("GetByAccount", ctx, testAccountAddress).
			Return([]*data.Contract{createSEP41Contract(testContractAddress, "Test", "TST", 7)}, nil)
		mockRPCService.On("NetworkPassphrase").Return(testNetworkPassphrase)

		// Mock FetchSingleField returning wrong type (map instead of i128) - use SAC map structure
		authorizedSym := ptrToScSymbol("authorized")
		clawbackSym := ptrToScSymbol("clawback")
		authorizedBool := true
		clawbackBool := false
		wrongTypeScVal := xdr.ScVal{
			Type: xdr.ScValTypeScvMap,
			Map: ptrToScMap([]xdr.ScMapEntry{
				{
					Key: xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: authorizedSym},
					Val: xdr.ScVal{Type: xdr.ScValTypeScvBool, B: &authorizedBool},
				},
				{
					Key: xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: clawbackSym},
					Val: xdr.ScVal{Type: xdr.ScValTypeScvBool, B: &clawbackBool},
				},
			}),
		}
		mockContractMetadataService.On("FetchSingleField", ctx, testContractAddress, "balance", mock.Anything).
			Return(wrongTypeScVal, nil)
		// No GetLedgerEntries call needed - SEP-41 uses FetchSingleField, native comes from DB

		resolver := &accountResolver{
			&Resolver{
				balanceReader:              NewBalanceReader(mockTrustlineBalanceModel, mockNativeBalanceModel, data.NewSACBalanceModelMock(t)),
				accountContractTokensModel: mockAccountContractTokens,
				rpcService:                 mockRPCService,
				contractMetadataService:    mockContractMetadataService,
				pool:                       pond.NewPool(0),
			},
		}

		balances, err := resolver.Balances(ctx, testParentAccount(testAccountAddress))
		require.Error(t, err)
		assert.Contains(t, err.Error(), ErrMsgBalancesFetchFailed)
		assert.Nil(t, balances)
	})

	// Edge Cases

	t.Run("edge - unknown contract type skipped", func(t *testing.T) {
		ctx := context.Background()
		mockTrustlineBalanceModel := data.NewTrustlineBalanceModelMock(t)
		mockNativeBalanceModel := data.NewNativeBalanceModelMock(t)
		mockAccountContractTokens := data.NewAccountContractTokensModelMock(t)
		mockRPCService := services.NewRPCServiceMock(t)

		// Setup mocks - native balance comes from DB
		mockNativeBalanceModel.On("GetByAccount", ctx, testAccountAddress).Return(&data.NativeBalance{AccountAddress: testAccountAddress, Balance: 1000000000}, nil) // 100 XLM
		mockTrustlineBalanceModel.On("GetByAccount", ctx, testAccountAddress).Return([]data.TrustlineBalance{}, nil)
		// GetByAccount now returns full Contract objects directly
		unknownContract := &data.Contract{
			ContractID: testContractAddress,
			Type:       string(types.ContractTypeUnknown),
		}
		mockAccountContractTokens.On("GetByAccount", ctx, testAccountAddress).
			Return([]*data.Contract{unknownContract}, nil)
		mockRPCService.On("NetworkPassphrase").Return(testNetworkPassphrase)
		// No GetLedgerEntries call needed - native comes from DB, unknown contracts are skipped

		resolver := &accountResolver{
			&Resolver{
				balanceReader:              NewBalanceReader(mockTrustlineBalanceModel, mockNativeBalanceModel, data.NewSACBalanceModelMock(t)),
				accountContractTokensModel: mockAccountContractTokens,
				rpcService:                 mockRPCService,
			},
		}

		balances, err := resolver.Balances(ctx, testParentAccount(testAccountAddress))
		require.NoError(t, err)
		// Should only have native balance, contract balance skipped
		require.Len(t, balances, 1)
		for _, balance := range balances {
			switch balance.GetTokenType() {
			case graphql1.TokenTypeNative:
				assert.IsType(t, &graphql1.NativeBalance{}, balance)
			default:
				t.Errorf("unexpected balance type: %v", balance.GetTokenType())
			}
		}
	})

	t.Run("edge - trustline authorization flags combinations", func(t *testing.T) {
		ctx := context.Background()
		mockTrustlineBalanceModel := data.NewTrustlineBalanceModelMock(t)
		mockNativeBalanceModel := data.NewNativeBalanceModelMock(t)
		mockAccountContractTokens := data.NewAccountContractTokensModelMock(t)
		mockRPCService := services.NewRPCServiceMock(t)

		// Setup mocks - native and trustlines come from DB
		mockNativeBalanceModel.On("GetByAccount", ctx, testAccountAddress).Return(&data.NativeBalance{AccountAddress: testAccountAddress, Balance: 1000000000}, nil) // 100 XLM
		mockTrustlineBalanceModel.On("GetByAccount", ctx, testAccountAddress).
			Return([]data.TrustlineBalance{
				{
					AssetID:      data.DeterministicAssetID("USDC", testUSDCIssuer),
					Code:         "USDC",
					Issuer:       testUSDCIssuer,
					Balance:      1000000000,
					Limit:        10000000000,
					Flags:        uint32(xdr.TrustLineFlagsAuthorizedFlag | xdr.TrustLineFlagsAuthorizedToMaintainLiabilitiesFlag),
					LedgerNumber: 12345,
				},
				{
					AssetID:      data.DeterministicAssetID("EUR", testEURIssuer),
					Code:         "EUR",
					Issuer:       testEURIssuer,
					Balance:      1000000000,
					Limit:        10000000000,
					Flags:        0, // No flags set
					LedgerNumber: 12345,
				},
			}, nil)
		mockAccountContractTokens.On("GetByAccount", ctx, testAccountAddress).Return([]*data.Contract{}, nil)
		mockRPCService.On("NetworkPassphrase").Return(testNetworkPassphrase)
		// No GetLedgerEntries call needed - native and trustlines come from DB

		resolver := &accountResolver{
			&Resolver{
				balanceReader:              NewBalanceReader(mockTrustlineBalanceModel, mockNativeBalanceModel, data.NewSACBalanceModelMock(t)),
				accountContractTokensModel: mockAccountContractTokens,
				rpcService:                 mockRPCService,
			},
		}

		balances, err := resolver.Balances(ctx, testParentAccount(testAccountAddress))
		require.NoError(t, err)
		require.Len(t, balances, 3)

		for _, balance := range balances {
			switch balance.GetTokenType() {
			case graphql1.TokenTypeNative:
				// Native balance verified by count
			case graphql1.TokenTypeClassic:
				trustlineBalance := balance.(*graphql1.TrustlineBalance)
				switch trustlineBalance.Code {
				case "USDC":
					// USDC - both flags set
					assert.True(t, trustlineBalance.IsAuthorized)
					assert.True(t, trustlineBalance.IsAuthorizedToMaintainLiabilities)
				case "EUR":
					// EUR - no flags set
					assert.False(t, trustlineBalance.IsAuthorized)
					assert.False(t, trustlineBalance.IsAuthorizedToMaintainLiabilities)
				default:
					t.Errorf("unexpected trustline code: %s", trustlineBalance.Code)
				}
			default:
				t.Errorf("unexpected balance type: %v", balance.GetTokenType())
			}
		}
	})
}
