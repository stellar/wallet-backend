// Tests for the balancesByAccountAddress GraphQL query resolver
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
	graphql1 "github.com/stellar/wallet-backend/internal/serve/graphql/generated"
	"github.com/stellar/wallet-backend/internal/services"
)

const (
	testAccountAddress     = "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"
	testContractAddress    = "CAZXRTOKNUQ2JQQF3NCRU7GYMDJNZ2NMQN6IGN4FCT5DWPODMPVEXSND"
	testSACContractAddress = "CBHBD77PWZ3AXPQVYVDBHDKEMVNOR26UZUZHWCB6QC7J5SETQPRUQAS4"
	testUSDCIssuer         = "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN"
	testEURIssuer          = "GCEODJVUUVYVFD5KT4TOEDTMXQ76OPFOQC2EMYYMLPXQCUVPOB6XRWPQ"
	testNetworkPassphrase  = "Test SDF Network ; September 2015"
)

func TestQueryResolver_BalancesByAccountAddress(t *testing.T) {
	// Success Cases
	t.Run("success - native balance only", func(t *testing.T) {
		ctx := context.Background()
		mockTrustlineBalanceModel := data.NewTrustlineBalanceModelMock(t)
		mockNativeBalanceModel := data.NewNativeBalanceModelMock(t)
		mockRPCService := services.NewRPCServiceMock(t)

		// Setup mocks - native balance comes from DB, not RPC
		mockNativeBalanceModel.On("GetByAccount", ctx, testAccountAddress).Return(&data.NativeBalance{AccountAddress: testAccountAddress, Balance: 10000000000}, nil)
		mockTrustlineBalanceModel.On("GetByAccount", ctx, testAccountAddress).Return([]data.TrustlineBalance{}, nil)
		mockRPCService.On("NetworkPassphrase").Return(testNetworkPassphrase)

		resolver := &queryResolver{
			&Resolver{
				balanceReader: NewBalanceReader(mockTrustlineBalanceModel, mockNativeBalanceModel, data.NewSACBalanceModelMock(t)),
				rpcService:    mockRPCService,
			},
		}

		balances, err := resolver.BalancesByAccountAddress(ctx, testAccountAddress)
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
		mockRPCService.On("NetworkPassphrase").Return(testNetworkPassphrase)

		resolver := &queryResolver{
			&Resolver{
				balanceReader: NewBalanceReader(mockTrustlineBalanceModel, mockNativeBalanceModel, data.NewSACBalanceModelMock(t)),
				rpcService:    mockRPCService,
			},
		}

		balances, err := resolver.BalancesByAccountAddress(ctx, testAccountAddress)
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
		mockRPCService.On("NetworkPassphrase").Return(testNetworkPassphrase)

		resolver := &queryResolver{
			&Resolver{
				balanceReader: NewBalanceReader(data.NewTrustlineBalanceModelMock(t), data.NewNativeBalanceModelMock(t), mockSACBalanceModel),
				rpcService:    mockRPCService,
			},
		}

		balances, err := resolver.BalancesByAccountAddress(ctx, testContractAddress)
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

	t.Run("success - trustline with V0 extension (no liabilities)", func(t *testing.T) {
		ctx := context.Background()
		mockTrustlineBalanceModel := data.NewTrustlineBalanceModelMock(t)
		mockNativeBalanceModel := data.NewNativeBalanceModelMock(t)

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

		mockRPCService.On("NetworkPassphrase").Return(testNetworkPassphrase)
		// No GetLedgerEntries call needed - native and trustlines come from DB

		resolver := &queryResolver{
			&Resolver{
				balanceReader: NewBalanceReader(mockTrustlineBalanceModel, mockNativeBalanceModel, data.NewSACBalanceModelMock(t)),
				rpcService:    mockRPCService,
			},
		}

		balances, err := resolver.BalancesByAccountAddress(ctx, testAccountAddress)
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

	t.Run("error - empty address", func(t *testing.T) {
		ctx := context.Background()
		resolver := &queryResolver{&Resolver{}}

		balances, err := resolver.BalancesByAccountAddress(ctx, "")
		assert.Error(t, err)
		assert.Nil(t, balances)
	})

	t.Run("error - GetTrustlineBalances fails", func(t *testing.T) {
		ctx := context.Background()
		mockTrustlineBalanceModel := data.NewTrustlineBalanceModelMock(t)
		mockNativeBalanceModel := data.NewNativeBalanceModelMock(t)
		mockNativeBalanceModel.On("GetByAccount", mock.Anything, mock.Anything).Return(&data.NativeBalance{AccountAddress: testAccountAddress, Balance: 10000000000}, nil).Maybe()
		mockRPCService := services.NewRPCServiceMock(t)

		mockRPCService.On("NetworkPassphrase").Return(testNetworkPassphrase)
		mockTrustlineBalanceModel.On("GetByAccount", ctx, testAccountAddress).
			Return([]data.TrustlineBalance{}, errors.New("database query failed"))

		resolver := &queryResolver{
			&Resolver{
				balanceReader: NewBalanceReader(mockTrustlineBalanceModel, mockNativeBalanceModel, data.NewSACBalanceModelMock(t)),
				rpcService:    mockRPCService,
			},
		}

		balances, err := resolver.BalancesByAccountAddress(ctx, testAccountAddress)
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

		resolver := &queryResolver{
			&Resolver{
				balanceReader: NewBalanceReader(data.NewTrustlineBalanceModelMock(t), data.NewNativeBalanceModelMock(t), mockSACBalanceModel),
				rpcService:    mockRPCService,
			},
		}

		balances, err := resolver.BalancesByAccountAddress(ctx, testContractAddress)
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

		resolver := &queryResolver{
			&Resolver{
				balanceReader: NewBalanceReader(mockTrustlineBalanceModel, mockNativeBalanceModel, data.NewSACBalanceModelMock(t)),
				rpcService:    mockRPCService,
			},
		}

		balances, err := resolver.BalancesByAccountAddress(ctx, testAccountAddress)
		require.Error(t, err)
		assert.Contains(t, err.Error(), ErrMsgBalancesFetchFailed)
		assert.Nil(t, balances)
	})

	// Edge Cases

	t.Run("edge - trustline authorization flags combinations", func(t *testing.T) {
		ctx := context.Background()
		mockTrustlineBalanceModel := data.NewTrustlineBalanceModelMock(t)
		mockNativeBalanceModel := data.NewNativeBalanceModelMock(t)

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

		mockRPCService.On("NetworkPassphrase").Return(testNetworkPassphrase)
		// No GetLedgerEntries call needed - native and trustlines come from DB

		resolver := &queryResolver{
			&Resolver{
				balanceReader: NewBalanceReader(mockTrustlineBalanceModel, mockNativeBalanceModel, data.NewSACBalanceModelMock(t)),
				rpcService:    mockRPCService,
			},
		}

		balances, err := resolver.BalancesByAccountAddress(ctx, testAccountAddress)
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

// Second test account for multi-account tests
const testAccountAddress2 = "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN"

func TestQueryResolver_BalancesByAccountAddresses(t *testing.T) {
	t.Run("success - single account", func(t *testing.T) {
		ctx := context.Background()
		mockTrustlineBalanceModel := data.NewTrustlineBalanceModelMock(t)
		mockNativeBalanceModel := data.NewNativeBalanceModelMock(t)

		mockRPCService := services.NewRPCServiceMock(t)
		mockContract := data.NewContractModelMock(t)

		// Native balance from DB (1000 XLM = 10000000000 stroops)
		mockNativeBalanceModel.On("GetByAccount", ctx, testAccountAddress).Return(&data.NativeBalance{AccountAddress: testAccountAddress, Balance: 10000000000}, nil)
		mockTrustlineBalanceModel.On("GetByAccount", ctx, testAccountAddress).Return([]data.TrustlineBalance{}, nil)

		mockRPCService.On("NetworkPassphrase").Return(testNetworkPassphrase)
		// No RPC GetLedgerEntries call needed since no SAC contracts

		resolver := &queryResolver{
			&Resolver{
				models: &data.Models{
					Contract: mockContract,
				},
				balanceReader: NewBalanceReader(mockTrustlineBalanceModel, mockNativeBalanceModel, data.NewSACBalanceModelMock(t)),
				rpcService:    mockRPCService,
				pool:          pond.NewPool(0),
				config:        ResolverConfig{MaxAccountsPerBalancesQuery: 10, MaxWorkerPoolSize: 10},
			},
		}

		results, err := resolver.BalancesByAccountAddresses(ctx, []string{testAccountAddress})
		require.NoError(t, err)
		require.Len(t, results, 1)

		assert.Equal(t, testAccountAddress, results[0].Address)
		assert.Nil(t, results[0].Error)
		require.Len(t, results[0].Balances, 1)

		nativeBalance, ok := results[0].Balances[0].(*graphql1.NativeBalance)
		require.True(t, ok)
		assert.Equal(t, "1000.0000000", nativeBalance.Balance)
		assert.Equal(t, graphql1.TokenTypeNative, nativeBalance.TokenType)
	})

	t.Run("success - multiple accounts with mixed balances", func(t *testing.T) {
		ctx := context.Background()
		mockTrustlineBalanceModel := data.NewTrustlineBalanceModelMock(t)
		mockNativeBalanceModel := data.NewNativeBalanceModelMock(t)

		mockRPCService := services.NewRPCServiceMock(t)

		// Setup for account 1 (native from DB + trustline from DB)
		mockNativeBalanceModel.On("GetByAccount", ctx, testAccountAddress).Return(&data.NativeBalance{AccountAddress: testAccountAddress, Balance: 5000000000}, nil)
		mockTrustlineBalanceModel.On("GetByAccount", ctx, testAccountAddress).
			Return([]data.TrustlineBalance{{
				AssetID:      data.DeterministicAssetID("USDC", testUSDCIssuer),
				Code:         "USDC",
				Issuer:       testUSDCIssuer,
				Balance:      1000000000,
				Limit:        10000000000,
				Flags:        uint32(xdr.TrustLineFlagsAuthorizedFlag),
				LedgerNumber: 12345,
			}}, nil)

		// Setup for account 2 (native from DB only)
		mockNativeBalanceModel.On("GetByAccount", ctx, testAccountAddress2).Return(&data.NativeBalance{AccountAddress: testAccountAddress2, Balance: 10000000000}, nil)
		mockTrustlineBalanceModel.On("GetByAccount", ctx, testAccountAddress2).Return([]data.TrustlineBalance{}, nil)

		mockRPCService.On("NetworkPassphrase").Return(testNetworkPassphrase)
		// No RPC GetLedgerEntries call needed since no SAC contracts

		resolver := &queryResolver{
			&Resolver{
				balanceReader: NewBalanceReader(mockTrustlineBalanceModel, mockNativeBalanceModel, data.NewSACBalanceModelMock(t)),
				rpcService:    mockRPCService,
				pool:          pond.NewPool(0),
				config:        ResolverConfig{MaxAccountsPerBalancesQuery: 10, MaxWorkerPoolSize: 10},
			},
		}

		results, err := resolver.BalancesByAccountAddresses(ctx, []string{testAccountAddress, testAccountAddress2})
		require.NoError(t, err)
		require.Len(t, results, 2)

		// Verify account 1
		assert.Equal(t, testAccountAddress, results[0].Address)
		assert.Nil(t, results[0].Error)
		require.Len(t, results[0].Balances, 2) // native + trustline

		// Verify account 2
		assert.Equal(t, testAccountAddress2, results[1].Address)
		assert.Nil(t, results[1].Error)
		require.Len(t, results[1].Balances, 1) // native only
	})

	t.Run("success - partial failure with per-account error", func(t *testing.T) {
		ctx := context.Background()
		mockTrustlineBalanceModel := data.NewTrustlineBalanceModelMock(t)
		mockNativeBalanceModel := data.NewNativeBalanceModelMock(t)

		mockRPCService := services.NewRPCServiceMock(t)
		mockContract := data.NewContractModelMock(t)

		// Account 1 succeeds - native balance from DB
		mockNativeBalanceModel.On("GetByAccount", ctx, testAccountAddress).Return(&data.NativeBalance{AccountAddress: testAccountAddress, Balance: 5000000000}, nil)
		mockTrustlineBalanceModel.On("GetByAccount", ctx, testAccountAddress).Return([]data.TrustlineBalance{}, nil)

		// Account 2 succeeds on native balance from DB, but fails on trustlines
		mockNativeBalanceModel.On("GetByAccount", ctx, testAccountAddress2).Return(&data.NativeBalance{AccountAddress: testAccountAddress2, Balance: 5000000000}, nil)
		mockTrustlineBalanceModel.On("GetByAccount", ctx, testAccountAddress2).
			Return([]data.TrustlineBalance{}, errors.New("db connection failed"))

		mockRPCService.On("NetworkPassphrase").Return(testNetworkPassphrase)
		// No RPC GetLedgerEntries call needed since no SAC contracts

		resolver := &queryResolver{
			&Resolver{
				models: &data.Models{
					Contract: mockContract,
				},
				balanceReader: NewBalanceReader(mockTrustlineBalanceModel, mockNativeBalanceModel, data.NewSACBalanceModelMock(t)),
				rpcService:    mockRPCService,
				pool:          pond.NewPool(0),
				config:        ResolverConfig{MaxAccountsPerBalancesQuery: 10, MaxWorkerPoolSize: 10},
			},
		}

		results, err := resolver.BalancesByAccountAddresses(ctx, []string{testAccountAddress, testAccountAddress2})
		require.NoError(t, err) // Overall call succeeds
		require.Len(t, results, 2)

		// Account 1 succeeded
		assert.Equal(t, testAccountAddress, results[0].Address)
		assert.Nil(t, results[0].Error)
		require.Len(t, results[0].Balances, 1)

		// Account 2 has error
		assert.Equal(t, testAccountAddress2, results[1].Address)
		assert.NotNil(t, results[1].Error)
		assert.Contains(t, *results[1].Error, "getting trustline balances")
		assert.Empty(t, results[1].Balances)
	})

	t.Run("success - deduplication of addresses", func(t *testing.T) {
		ctx := context.Background()
		mockTrustlineBalanceModel := data.NewTrustlineBalanceModelMock(t)
		mockNativeBalanceModel := data.NewNativeBalanceModelMock(t)

		mockRPCService := services.NewRPCServiceMock(t)
		mockContract := data.NewContractModelMock(t)

		// Only called once due to deduplication - native balance from DB
		mockNativeBalanceModel.On("GetByAccount", ctx, testAccountAddress).Return(&data.NativeBalance{AccountAddress: testAccountAddress, Balance: 10000000000}, nil).Once()
		mockTrustlineBalanceModel.On("GetByAccount", ctx, testAccountAddress).Return([]data.TrustlineBalance{}, nil).Once()

		mockRPCService.On("NetworkPassphrase").Return(testNetworkPassphrase)
		// No RPC GetLedgerEntries call needed since no SAC contracts

		resolver := &queryResolver{
			&Resolver{
				models: &data.Models{
					Contract: mockContract,
				},
				balanceReader: NewBalanceReader(mockTrustlineBalanceModel, mockNativeBalanceModel, data.NewSACBalanceModelMock(t)),
				rpcService:    mockRPCService,
				pool:          pond.NewPool(0),
				config:        ResolverConfig{MaxAccountsPerBalancesQuery: 10, MaxWorkerPoolSize: 10},
			},
		}

		// Pass same address twice
		results, err := resolver.BalancesByAccountAddresses(ctx, []string{testAccountAddress, testAccountAddress})
		require.NoError(t, err)
		require.Len(t, results, 1) // Deduplicated to single result

		assert.Equal(t, testAccountAddress, results[0].Address)
		assert.Nil(t, results[0].Error)
	})

	t.Run("error - exceeds max addresses", func(t *testing.T) {
		ctx := context.Background()
		resolver := &queryResolver{
			&Resolver{
				pool:   pond.NewPool(10),
				config: ResolverConfig{MaxAccountsPerBalancesQuery: 20, MaxWorkerPoolSize: 10},
			},
		}

		// Create 21 addresses
		addresses := make([]string, 21)
		for i := range addresses {
			addresses[i] = testAccountAddress
		}

		results, err := resolver.BalancesByAccountAddresses(ctx, addresses)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "maximum 20 addresses allowed")
		assert.Nil(t, results)
	})

	t.Run("error - empty addresses array", func(t *testing.T) {
		ctx := context.Background()
		resolver := &queryResolver{
			&Resolver{
				pool:   pond.NewPool(10),
				config: ResolverConfig{MaxAccountsPerBalancesQuery: 10, MaxWorkerPoolSize: 10},
			},
		}

		results, err := resolver.BalancesByAccountAddresses(ctx, []string{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "addresses array cannot be empty")
		assert.Nil(t, results)
	})

	t.Run("error - native balance DB failure affects account", func(t *testing.T) {
		ctx := context.Background()
		mockTrustlineBalanceModel := data.NewTrustlineBalanceModelMock(t)
		mockNativeBalanceModel := data.NewNativeBalanceModelMock(t)

		mockRPCService := services.NewRPCServiceMock(t)

		// Native balance DB fails for both accounts
		mockNativeBalanceModel.On("GetByAccount", ctx, testAccountAddress).Return(nil, errors.New("DB unavailable"))
		mockNativeBalanceModel.On("GetByAccount", ctx, testAccountAddress2).Return(nil, errors.New("DB unavailable"))
		mockRPCService.On("NetworkPassphrase").Return(testNetworkPassphrase)

		resolver := &queryResolver{
			&Resolver{
				balanceReader: NewBalanceReader(mockTrustlineBalanceModel, mockNativeBalanceModel, data.NewSACBalanceModelMock(t)),
				rpcService:    mockRPCService,
				pool:          pond.NewPool(0),
				config:        ResolverConfig{MaxAccountsPerBalancesQuery: 10, MaxWorkerPoolSize: 10},
			},
		}

		results, err := resolver.BalancesByAccountAddresses(ctx, []string{testAccountAddress, testAccountAddress2})
		require.NoError(t, err) // Overall call succeeds, but each account has error
		require.Len(t, results, 2)

		// Both accounts have errors due to native balance failure
		assert.NotNil(t, results[0].Error)
		assert.Contains(t, *results[0].Error, "getting native balance")
		assert.NotNil(t, results[1].Error)
		assert.Contains(t, *results[1].Error, "getting native balance")
	})
}
