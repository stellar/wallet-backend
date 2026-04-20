// Tests for the Account.balances GraphQL field resolver.
package resolvers

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/vektah/gqlparser/v2/gqlerror"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	graphql1 "github.com/stellar/wallet-backend/internal/serve/graphql/generated"
	"github.com/stellar/wallet-backend/internal/services"
)

const (
	testAccountAddress        = "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"
	testContractAddress       = "CAZXRTOKNUQ2JQQF3NCRU7GYMDJNZ2NMQN6IGN4FCT5DWPODMPVEXSND"
	testSACContractAddress    = "CBHBD77PWZ3AXPQVYVDBHDKEMVNOR26UZUZHWCB6QC7J5SETQPRUQAS4"
	testSEP41ContractAddress  = "CAZXRTOKNUQ2JQQF3NCRU7GYMDJNZ2NMQN6IGN4FCT5DWPODMPVEXSND"
	testSEP41ContractAddress2 = "CBZXRTOKNUQ2JQQF3NCRU7GYMDJNZ2NMQN6IGN4FCT5DWPODMPVEXSNA"
	testUSDCIssuer            = "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN"
	testNetworkPassphrase     = "Test SDF Network ; September 2015"
)

func createSEP41Contract(contractID, name, symbol string, decimals uint32) *data.Contract {
	return &data.Contract{
		ID:         data.DeterministicContractID(contractID),
		ContractID: contractID,
		Type:       string(types.ContractTypeSEP41),
		Name:       &name,
		Symbol:     &symbol,
		Decimals:   decimals,
	}
}

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

func int32Ptr(v int32) *int32 {
	return &v
}

func limitMatcher(expected int32) interface{} {
	return mock.MatchedBy(func(limit *int32) bool {
		return limit != nil && *limit == expected
	})
}

func cursorUUIDMatcher(expected uuid.UUID) interface{} {
	return mock.MatchedBy(func(cursor *uuid.UUID) bool {
		return cursor != nil && *cursor == expected
	})
}

func flattenBalanceNodes(conn *graphql1.BalanceConnection) []graphql1.Balance {
	if conn == nil {
		return nil
	}

	nodes := make([]graphql1.Balance, 0, len(conn.Edges))
	for _, edge := range conn.Edges {
		nodes = append(nodes, edge.Node)
	}
	return nodes
}

func TestAccountResolver_Balances(t *testing.T) {
	t.Run("empty account returns empty connection", func(t *testing.T) {
		ctx := context.Background()
		mockTrustlineBalanceModel := data.NewTrustlineBalanceModelMock(t)
		mockNativeBalanceModel := data.NewNativeBalanceModelMock(t)
		mockAccountContractTokens := data.NewAccountContractTokensModelMock(t)
		mockRPCService := services.NewRPCServiceMock(t)

		mockNativeBalanceModel.On("GetByAccount", ctx, testAccountAddress).Return(nil, nil)
		mockTrustlineBalanceModel.On("GetByAccount", ctx, testAccountAddress, limitMatcher(51), (*uuid.UUID)(nil), data.ASC).
			Return([]data.TrustlineBalance{}, nil)
		mockAccountContractTokens.On("GetSEP41ByAccount", ctx, testAccountAddress, limitMatcher(51), (*uuid.UUID)(nil), data.ASC).
			Return([]*data.Contract{}, nil)
		mockRPCService.On("NetworkPassphrase").Return(testNetworkPassphrase)

		resolver := &accountResolver{
			&Resolver{
				balanceReader:              NewBalanceReader(mockTrustlineBalanceModel, mockNativeBalanceModel, data.NewSACBalanceModelMock(t)),
				accountContractTokensModel: mockAccountContractTokens,
				rpcService:                 mockRPCService,
			},
		}

		conn, err := resolver.Balances(ctx, testParentAccount(testAccountAddress), nil, nil, nil, nil)
		require.NoError(t, err)
		require.NotNil(t, conn)
		assert.Empty(t, conn.Edges)
		assert.False(t, conn.PageInfo.HasNextPage)
		assert.False(t, conn.PageInfo.HasPreviousPage)
		assert.Nil(t, conn.PageInfo.StartCursor)
		assert.Nil(t, conn.PageInfo.EndCursor)
	})

	t.Run("mixed balances return connection in canonical source order", func(t *testing.T) {
		ctx := context.Background()
		mockTrustlineBalanceModel := data.NewTrustlineBalanceModelMock(t)
		mockNativeBalanceModel := data.NewNativeBalanceModelMock(t)
		mockAccountContractTokens := data.NewAccountContractTokensModelMock(t)
		mockRPCService := services.NewRPCServiceMock(t)
		mockContractMetadataService := services.NewContractMetadataServiceMock(t)

		trustlineID := data.DeterministicAssetID("USDC", testUSDCIssuer)
		sep41Contract := createSEP41Contract(testSEP41ContractAddress, "CustomToken", "CTK", 6)

		mockNativeBalanceModel.On("GetByAccount", ctx, testAccountAddress).
			Return(&data.NativeBalance{AccountID: types.AddressBytea(testAccountAddress), Balance: 2000000000}, nil)
		mockTrustlineBalanceModel.On("GetByAccount", ctx, testAccountAddress, limitMatcher(50), (*uuid.UUID)(nil), data.ASC).
			Return([]data.TrustlineBalance{{
				AssetID:      trustlineID,
				Code:         "USDC",
				Issuer:       testUSDCIssuer,
				Balance:      1000000000,
				Limit:        10000000000,
				Flags:        uint32(xdr.TrustLineFlagsAuthorizedFlag),
				LedgerNumber: 12345,
			}}, nil)
		mockAccountContractTokens.On("GetSEP41ByAccount", ctx, testAccountAddress, limitMatcher(49), (*uuid.UUID)(nil), data.ASC).
			Return([]*data.Contract{sep41Contract}, nil)
		mockRPCService.On("NetworkPassphrase").Return(testNetworkPassphrase)
		mockContractMetadataService.On("FetchSingleField", ctx, testSEP41ContractAddress, "balance", mock.Anything).
			Return(createI128ScVal(30000000000), nil)

		resolver := &accountResolver{
			&Resolver{
				balanceReader:              NewBalanceReader(mockTrustlineBalanceModel, mockNativeBalanceModel, data.NewSACBalanceModelMock(t)),
				accountContractTokensModel: mockAccountContractTokens,
				rpcService:                 mockRPCService,
				contractMetadataService:    mockContractMetadataService,
			},
		}

		conn, err := resolver.Balances(ctx, testParentAccount(testAccountAddress), nil, nil, nil, nil)
		require.NoError(t, err)
		require.Len(t, conn.Edges, 3)
		assert.False(t, conn.PageInfo.HasNextPage)
		assert.False(t, conn.PageInfo.HasPreviousPage)

		nodes := flattenBalanceNodes(conn)
		require.IsType(t, &graphql1.NativeBalance{}, nodes[0])
		require.IsType(t, &graphql1.TrustlineBalance{}, nodes[1])
		require.IsType(t, &graphql1.SEP41Balance{}, nodes[2])

		native := nodes[0].(*graphql1.NativeBalance)
		assert.Equal(t, "200.0000000", native.Balance)

		trustline := nodes[1].(*graphql1.TrustlineBalance)
		assert.Equal(t, "USDC", trustline.Code)
		assert.Equal(t, "100.0000000", trustline.Balance)

		sep41 := nodes[2].(*graphql1.SEP41Balance)
		assert.Equal(t, "CustomToken", sep41.Name)
		assert.Equal(t, "CTK", sep41.Symbol)
	})

	t.Run("contract address returns SAC and SEP41 balances", func(t *testing.T) {
		ctx := context.Background()
		mockSACBalanceModel := data.NewSACBalanceModelMock(t)
		mockAccountContractTokens := data.NewAccountContractTokensModelMock(t)
		mockRPCService := services.NewRPCServiceMock(t)
		mockContractMetadataService := services.NewContractMetadataServiceMock(t)

		sacContractID := data.DeterministicContractID(testSACContractAddress)
		sep41Contract := createSEP41Contract(testSEP41ContractAddress, "Token", "TKN", 7)

		mockSACBalanceModel.On("GetByAccount", ctx, testContractAddress, limitMatcher(51), (*uuid.UUID)(nil), data.ASC).
			Return([]data.SACBalance{{
				AccountID:         types.AddressBytea(testContractAddress),
				ContractID:        sacContractID,
				TokenID:           testSACContractAddress,
				Balance:           "2500.0000000",
				IsAuthorized:      true,
				IsClawbackEnabled: false,
				LedgerNumber:      1000,
				Code:              "USDC",
				Issuer:            testUSDCIssuer,
				Decimals:          7,
			}}, nil)
		mockAccountContractTokens.On("GetSEP41ByAccount", ctx, testContractAddress, limitMatcher(50), (*uuid.UUID)(nil), data.ASC).
			Return([]*data.Contract{sep41Contract}, nil)
		mockRPCService.On("NetworkPassphrase").Return(testNetworkPassphrase)
		mockContractMetadataService.On("FetchSingleField", ctx, testSEP41ContractAddress, "balance", mock.Anything).
			Return(createI128ScVal(10000000000), nil)

		resolver := &accountResolver{
			&Resolver{
				balanceReader:              NewBalanceReader(data.NewTrustlineBalanceModelMock(t), data.NewNativeBalanceModelMock(t), mockSACBalanceModel),
				accountContractTokensModel: mockAccountContractTokens,
				rpcService:                 mockRPCService,
				contractMetadataService:    mockContractMetadataService,
			},
		}

		conn, err := resolver.Balances(ctx, testParentAccount(testContractAddress), nil, nil, nil, nil)
		require.NoError(t, err)
		require.Len(t, conn.Edges, 2)

		nodes := flattenBalanceNodes(conn)
		require.IsType(t, &graphql1.SACBalance{}, nodes[0])
		require.IsType(t, &graphql1.SEP41Balance{}, nodes[1])
	})

	t.Run("forward pagination crosses sources and only fetches returned SEP41 pages", func(t *testing.T) {
		ctx := context.Background()
		mockTrustlineBalanceModel := data.NewTrustlineBalanceModelMock(t)
		mockNativeBalanceModel := data.NewNativeBalanceModelMock(t)
		mockAccountContractTokens := data.NewAccountContractTokensModelMock(t)
		mockRPCService := services.NewRPCServiceMock(t)
		mockContractMetadataService := services.NewContractMetadataServiceMock(t)

		trustlineID := data.DeterministicAssetID("USDC", testUSDCIssuer)
		sep41Contract1 := createSEP41Contract(testSEP41ContractAddress, "TokenOne", "ONE", 7)
		sep41Contract2 := createSEP41Contract(testSEP41ContractAddress2, "TokenTwo", "TWO", 7)

		mockNativeBalanceModel.On("GetByAccount", ctx, testAccountAddress).
			Return(&data.NativeBalance{AccountID: types.AddressBytea(testAccountAddress), Balance: 1000000000}, nil).
			Once()
		mockTrustlineBalanceModel.On("GetByAccount", ctx, testAccountAddress, limitMatcher(2), (*uuid.UUID)(nil), data.ASC).
			Return([]data.TrustlineBalance{{
				AssetID:      trustlineID,
				Code:         "USDC",
				Issuer:       testUSDCIssuer,
				Balance:      500000000,
				Limit:        10000000000,
				Flags:        uint32(xdr.TrustLineFlagsAuthorizedFlag),
				LedgerNumber: 12345,
			}}, nil).
			Once()
		mockAccountContractTokens.On("GetSEP41ByAccount", ctx, testAccountAddress, limitMatcher(1), (*uuid.UUID)(nil), data.ASC).
			Return([]*data.Contract{sep41Contract1}, nil).
			Once()

		mockTrustlineBalanceModel.On("GetByAccount", ctx, testAccountAddress, limitMatcher(3), cursorUUIDMatcher(trustlineID), data.ASC).
			Return([]data.TrustlineBalance{}, nil).
			Once()
		mockAccountContractTokens.On("GetSEP41ByAccount", ctx, testAccountAddress, limitMatcher(3), (*uuid.UUID)(nil), data.ASC).
			Return([]*data.Contract{sep41Contract1, sep41Contract2}, nil).
			Once()
		mockRPCService.On("NetworkPassphrase").Return(testNetworkPassphrase)
		mockContractMetadataService.On("FetchSingleField", ctx, testSEP41ContractAddress, "balance", mock.Anything).
			Return(createI128ScVal(10000000000), nil).
			Once()
		mockContractMetadataService.On("FetchSingleField", ctx, testSEP41ContractAddress2, "balance", mock.Anything).
			Return(createI128ScVal(20000000000), nil).
			Once()

		resolver := &accountResolver{
			&Resolver{
				balanceReader:              NewBalanceReader(mockTrustlineBalanceModel, mockNativeBalanceModel, data.NewSACBalanceModelMock(t)),
				accountContractTokensModel: mockAccountContractTokens,
				rpcService:                 mockRPCService,
				contractMetadataService:    mockContractMetadataService,
			},
		}

		firstPage, err := resolver.Balances(ctx, testParentAccount(testAccountAddress), int32Ptr(2), nil, nil, nil)
		require.NoError(t, err)
		require.Len(t, firstPage.Edges, 2)
		assert.True(t, firstPage.PageInfo.HasNextPage)
		assert.False(t, firstPage.PageInfo.HasPreviousPage)

		firstNodes := flattenBalanceNodes(firstPage)
		require.IsType(t, &graphql1.NativeBalance{}, firstNodes[0])
		require.IsType(t, &graphql1.TrustlineBalance{}, firstNodes[1])

		secondPage, err := resolver.Balances(ctx, testParentAccount(testAccountAddress), int32Ptr(2), firstPage.PageInfo.EndCursor, nil, nil)
		require.NoError(t, err)
		require.Len(t, secondPage.Edges, 2)
		assert.False(t, secondPage.PageInfo.HasNextPage)
		assert.True(t, secondPage.PageInfo.HasPreviousPage)

		secondNodes := flattenBalanceNodes(secondPage)
		require.IsType(t, &graphql1.SEP41Balance{}, secondNodes[0])
		require.IsType(t, &graphql1.SEP41Balance{}, secondNodes[1])
	})

	t.Run("backward pagination returns the last balances in canonical order", func(t *testing.T) {
		ctx := context.Background()
		mockTrustlineBalanceModel := data.NewTrustlineBalanceModelMock(t)
		mockNativeBalanceModel := data.NewNativeBalanceModelMock(t)
		mockAccountContractTokens := data.NewAccountContractTokensModelMock(t)
		mockRPCService := services.NewRPCServiceMock(t)
		mockContractMetadataService := services.NewContractMetadataServiceMock(t)

		trustlineID := data.DeterministicAssetID("USDC", testUSDCIssuer)
		sep41Contract := createSEP41Contract(testSEP41ContractAddress, "Token", "TKN", 7)

		mockAccountContractTokens.On("GetSEP41ByAccount", ctx, testAccountAddress, limitMatcher(3), (*uuid.UUID)(nil), data.DESC).
			Return([]*data.Contract{sep41Contract}, nil)
		mockTrustlineBalanceModel.On("GetByAccount", ctx, testAccountAddress, limitMatcher(2), (*uuid.UUID)(nil), data.DESC).
			Return([]data.TrustlineBalance{{
				AssetID:      trustlineID,
				Code:         "USDC",
				Issuer:       testUSDCIssuer,
				Balance:      500000000,
				Limit:        10000000000,
				Flags:        uint32(xdr.TrustLineFlagsAuthorizedFlag),
				LedgerNumber: 12345,
			}}, nil)
		mockNativeBalanceModel.On("GetByAccount", ctx, testAccountAddress).
			Return(&data.NativeBalance{AccountID: types.AddressBytea(testAccountAddress), Balance: 1000000000}, nil)
		mockRPCService.On("NetworkPassphrase").Return(testNetworkPassphrase)
		mockContractMetadataService.On("FetchSingleField", ctx, testSEP41ContractAddress, "balance", mock.Anything).
			Return(createI128ScVal(10000000000), nil)

		resolver := &accountResolver{
			&Resolver{
				balanceReader:              NewBalanceReader(mockTrustlineBalanceModel, mockNativeBalanceModel, data.NewSACBalanceModelMock(t)),
				accountContractTokensModel: mockAccountContractTokens,
				rpcService:                 mockRPCService,
				contractMetadataService:    mockContractMetadataService,
			},
		}

		conn, err := resolver.Balances(ctx, testParentAccount(testAccountAddress), nil, nil, int32Ptr(2), nil)
		require.NoError(t, err)
		require.Len(t, conn.Edges, 2)
		assert.False(t, conn.PageInfo.HasNextPage)
		assert.True(t, conn.PageInfo.HasPreviousPage)

		nodes := flattenBalanceNodes(conn)
		require.IsType(t, &graphql1.TrustlineBalance{}, nodes[0])
		require.IsType(t, &graphql1.SEP41Balance{}, nodes[1])
	})

	t.Run("rejects page sizes above the balance max", func(t *testing.T) {
		ctx := context.Background()
		resolver := &accountResolver{&Resolver{}}

		conn, err := resolver.Balances(ctx, testParentAccount(testAccountAddress), int32Ptr(101), nil, nil, nil)
		require.Error(t, err)
		assert.Nil(t, conn)

		var gqlErr *gqlerror.Error
		require.True(t, errors.As(err, &gqlErr))
		assert.Equal(t, "BAD_USER_INPUT", gqlErr.Extensions["code"])
		assert.Contains(t, gqlErr.Message, "first must be less than or equal to 100")
	})

	t.Run("returns internal error when trustline pagination fails", func(t *testing.T) {
		ctx := context.Background()
		mockTrustlineBalanceModel := data.NewTrustlineBalanceModelMock(t)
		mockNativeBalanceModel := data.NewNativeBalanceModelMock(t)
		mockRPCService := services.NewRPCServiceMock(t)

		mockNativeBalanceModel.On("GetByAccount", ctx, testAccountAddress).
			Return(&data.NativeBalance{AccountID: types.AddressBytea(testAccountAddress), Balance: 1000000000}, nil)
		mockTrustlineBalanceModel.On("GetByAccount", ctx, testAccountAddress, limitMatcher(50), (*uuid.UUID)(nil), data.ASC).
			Return(nil, errors.New("database query failed"))
		mockRPCService.On("NetworkPassphrase").Return(testNetworkPassphrase)

		resolver := &accountResolver{
			&Resolver{
				balanceReader: NewBalanceReader(mockTrustlineBalanceModel, mockNativeBalanceModel, data.NewSACBalanceModelMock(t)),
				rpcService:    mockRPCService,
			},
		}

		conn, err := resolver.Balances(ctx, testParentAccount(testAccountAddress), nil, nil, nil, nil)
		require.Error(t, err)
		assert.Nil(t, conn)

		var gqlErr *gqlerror.Error
		require.True(t, errors.As(err, &gqlErr))
		assert.Equal(t, "INTERNAL_ERROR", gqlErr.Extensions["code"])
		assert.Equal(t, ErrMsgBalancesFetchFailed, gqlErr.Message)
	})
}
