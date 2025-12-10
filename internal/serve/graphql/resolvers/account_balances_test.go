// Tests for the balancesByAccountAddress GraphQL query resolver
package resolvers

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"testing"

	"github.com/alitto/pond/v2"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/entities"
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

// Helper to decode contract ID from strkey to Hash
func contractIDToHash(contractID string) xdr.Hash {
	// Decode the contract address (C... format)
	// Use strkey.Decode with VersionByteContract
	decoded, err := strkey.Decode(strkey.VersionByteContract, contractID)
	if err != nil {
		panic(fmt.Sprintf("invalid contract ID: %v", err))
	}
	// Convert the bytes to Hash
	var hash xdr.Hash
	copy(hash[:], decoded)
	return hash
}

// Helper to create ScVec pointer
func ptrToScVec(vals []xdr.ScVal) **xdr.ScVec {
	vec := xdr.ScVec(vals)
	ptr := &vec
	return &ptr
}

// Helper to create ScMap pointer
func ptrToScMap(entries []xdr.ScMapEntry) **xdr.ScMap {
	m := xdr.ScMap(entries)
	ptr := &m
	return &ptr
}

// encodeLedgerEntryDataToBase64 encodes ledger entry data to base64 string
func encodeLedgerEntryDataToBase64(data xdr.LedgerEntryData) string {
	var buf bytes.Buffer
	_, err := xdr.Marshal(&buf, data)
	if err != nil {
		panic(fmt.Sprintf("failed to marshal XDR: %v", err))
	}
	return base64.StdEncoding.EncodeToString(buf.Bytes())
}

// createAccountLedgerEntry creates a base64 encoded account ledger entry with native balance
func createAccountLedgerEntry(address string, balance int64) entities.LedgerEntryResult { //nolint:unparam
	accountID := xdr.MustAddress(address)
	accountEntry := xdr.AccountEntry{
		AccountId:     accountID,
		Balance:       xdr.Int64(balance),
		SeqNum:        xdr.SequenceNumber(1),
		NumSubEntries: 0,
		Thresholds:    xdr.Thresholds{0, 0, 0, 0},
	}

	ledgerEntryData := xdr.LedgerEntryData{
		Type:    xdr.LedgerEntryTypeAccount,
		Account: &accountEntry,
	}

	return entities.LedgerEntryResult{
		DataXDR:            encodeLedgerEntryDataToBase64(ledgerEntryData),
		LastModifiedLedger: 1000,
	}
}

// createTrustlineLedgerEntry creates a base64 encoded trustline ledger entry
func createTrustlineLedgerEntry(accountAddress, assetCode, assetIssuer string, balance, limit int64, flags uint32, buyingLiabilities, sellingLiabilities int64) entities.LedgerEntryResult { //nolint:unparam
	accountID := xdr.MustAddress(accountAddress)
	asset := xdr.MustNewCreditAsset(assetCode, assetIssuer)
	trustlineAsset := asset.ToTrustLineAsset()

	trustlineEntry := xdr.TrustLineEntry{
		AccountId: accountID,
		Asset:     trustlineAsset,
		Balance:   xdr.Int64(balance),
		Limit:     xdr.Int64(limit),
		Flags:     xdr.Uint32(flags),
	}

	// Add V1 extension with liabilities if provided
	if buyingLiabilities > 0 || sellingLiabilities > 0 {
		trustlineEntry.Ext = xdr.TrustLineEntryExt{
			V: 1,
			V1: &xdr.TrustLineEntryV1{
				Liabilities: xdr.Liabilities{
					Buying:  xdr.Int64(buyingLiabilities),
					Selling: xdr.Int64(sellingLiabilities),
				},
			},
		}
	}

	ledgerEntryData := xdr.LedgerEntryData{
		Type:      xdr.LedgerEntryTypeTrustline,
		TrustLine: &trustlineEntry,
	}

	return entities.LedgerEntryResult{
		DataXDR:            encodeLedgerEntryDataToBase64(ledgerEntryData),
		LastModifiedLedger: 1000,
	}
}

// createSACContractDataEntry creates a SAC balance entry with authorization fields
func createSACContractDataEntry(contractID, holderAddress string, amount int64, authorized, clawback bool) entities.LedgerEntryResult {
	// Decode contract ID from strkey
	contractHash := contractIDToHash(contractID)

	// Create balance key [Symbol("Balance"), Address(holder)]
	holderAccountID := xdr.MustAddress(holderAddress)
	balanceSymbol := ptrToScSymbol("Balance")
	balanceKey := xdr.ScVal{
		Type: xdr.ScValTypeScvVec,
		Vec: ptrToScVec([]xdr.ScVal{
			{
				Type: xdr.ScValTypeScvSymbol,
				Sym:  balanceSymbol,
			},
			{
				Type: xdr.ScValTypeScvAddress,
				Address: &xdr.ScAddress{
					Type:      xdr.ScAddressTypeScAddressTypeAccount,
					AccountId: &holderAccountID,
				},
			},
		}),
	}

	// Create SAC balance value as map with amount, authorized, clawback
	hi := int64(0)
	if amount < 0 {
		hi = -1
	}
	lo := xdr.Uint64(amount)

	amountSym := ptrToScSymbol("amount")
	authorizedSym := ptrToScSymbol("authorized")
	clawbackSym := ptrToScSymbol("clawback")

	balanceValue := xdr.ScVal{
		Type: xdr.ScValTypeScvMap,
		Map: ptrToScMap([]xdr.ScMapEntry{
			{
				Key: xdr.ScVal{
					Type: xdr.ScValTypeScvSymbol,
					Sym:  amountSym,
				},
				Val: xdr.ScVal{
					Type: xdr.ScValTypeScvI128,
					I128: &xdr.Int128Parts{
						Hi: xdr.Int64(hi),
						Lo: lo,
					},
				},
			},
			{
				Key: xdr.ScVal{
					Type: xdr.ScValTypeScvSymbol,
					Sym:  authorizedSym,
				},
				Val: xdr.ScVal{
					Type: xdr.ScValTypeScvBool,
					B:    &authorized,
				},
			},
			{
				Key: xdr.ScVal{
					Type: xdr.ScValTypeScvSymbol,
					Sym:  clawbackSym,
				},
				Val: xdr.ScVal{
					Type: xdr.ScValTypeScvBool,
					B:    &clawback,
				},
			},
		}),
	}

	contractIDXdr := xdr.ContractId(contractHash)
	contractDataEntry := xdr.ContractDataEntry{
		Contract: xdr.ScAddress{
			Type:       xdr.ScAddressTypeScAddressTypeContract,
			ContractId: &contractIDXdr,
		},
		Key:        balanceKey,
		Durability: xdr.ContractDataDurabilityPersistent,
		Val:        balanceValue,
	}

	ledgerEntryData := xdr.LedgerEntryData{
		Type:         xdr.LedgerEntryTypeContractData,
		ContractData: &contractDataEntry,
	}

	return entities.LedgerEntryResult{
		DataXDR:            encodeLedgerEntryDataToBase64(ledgerEntryData),
		LastModifiedLedger: 1000,
	}
}

// createSEP41ContractDataEntry creates a SEP-41 balance entry (direct i128)
func createSEP41ContractDataEntry(contractID, holderAddress string, isHolderContract bool, amount int64) entities.LedgerEntryResult { //nolint:unparam
	// Decode contract ID from strkey
	contractHash := contractIDToHash(contractID)

	// Create balance key [Symbol("Balance"), Address(holder)]
	var holderScAddress xdr.ScAddress
	if isHolderContract {
		holderContractHash := contractIDToHash(holderAddress)
		holderContractID := xdr.ContractId(holderContractHash)
		holderScAddress = xdr.ScAddress{
			Type:       xdr.ScAddressTypeScAddressTypeContract,
			ContractId: &holderContractID,
		}
	} else {
		holderAccountID := xdr.MustAddress(holderAddress)
		holderScAddress = xdr.ScAddress{
			Type:      xdr.ScAddressTypeScAddressTypeAccount,
			AccountId: &holderAccountID,
		}
	}
	balanceSymbol := ptrToScSymbol("Balance")
	balanceKey := xdr.ScVal{
		Type: xdr.ScValTypeScvVec,
		Vec: ptrToScVec([]xdr.ScVal{
			{
				Type: xdr.ScValTypeScvSymbol,
				Sym:  balanceSymbol,
			},
			{
				Type:    xdr.ScValTypeScvAddress,
				Address: &holderScAddress,
			},
		}),
	}

	// Create SEP-41 balance value as direct i128
	hi := int64(0)
	if amount < 0 {
		hi = -1
	}
	lo := xdr.Uint64(amount)

	balanceValue := xdr.ScVal{
		Type: xdr.ScValTypeScvI128,
		I128: &xdr.Int128Parts{
			Hi: xdr.Int64(hi),
			Lo: lo,
		},
	}

	contractIDXdr := xdr.ContractId(contractHash)
	contractDataEntry := xdr.ContractDataEntry{
		Contract: xdr.ScAddress{
			Type:       xdr.ScAddressTypeScAddressTypeContract,
			ContractId: &contractIDXdr,
		},
		Key:        balanceKey,
		Durability: xdr.ContractDataDurabilityPersistent,
		Val:        balanceValue,
	}

	ledgerEntryData := xdr.LedgerEntryData{
		Type:         xdr.LedgerEntryTypeContractData,
		ContractData: &contractDataEntry,
	}

	return entities.LedgerEntryResult{
		DataXDR:            encodeLedgerEntryDataToBase64(ledgerEntryData),
		LastModifiedLedger: 1000,
	}
}

// Helper to create SAC contract data
func createSACContract(contractID, code, issuer string) *data.Contract {
	return &data.Contract{
		ID:     contractID,
		Type:   string(types.ContractTypeSAC),
		Code:   &code,
		Issuer: &issuer,
	}
}

// Helper to create SEP-41 contract data
func createSEP41Contract(contractID, name, symbol string, decimals uint32) *data.Contract { //nolint:unparam
	return &data.Contract{
		ID:       contractID,
		Type:     string(types.ContractTypeSEP41),
		Name:     &name,
		Symbol:   &symbol,
		Decimals: decimals,
	}
}

func TestQueryResolver_BalancesByAccountAddress(t *testing.T) {
	// Success Cases
	t.Run("success - native balance only", func(t *testing.T) {
		ctx := context.Background()
		mockAccountTokenService := services.NewAccountTokenServiceMock(t)
		mockRPCService := services.NewRPCServiceMock(t)
		mockContract := data.NewContractModelMock(t)

		// Setup mocks
		mockAccountTokenService.On("GetAccountTrustlines", ctx, testAccountAddress).Return([]string{}, nil)
		mockAccountTokenService.On("GetAccountContracts", ctx, testAccountAddress).Return([]string{}, nil)
		mockContract.On("BatchGetByIDs", ctx, []string{}).Return([]*data.Contract{}, nil)
		mockRPCService.On("NetworkPassphrase").Return(testNetworkPassphrase)

		// Create native balance ledger entry
		accountEntry := createAccountLedgerEntry(testAccountAddress, 10000000000) // 1000 XLM
		mockRPCService.On("GetLedgerEntries", mock.MatchedBy(func(keys []string) bool {
			return len(keys) == 1 // Only account key
		})).Return(entities.RPCGetLedgerEntriesResult{
			Entries: []entities.LedgerEntryResult{accountEntry},
		}, nil)

		resolver := &queryResolver{
			&Resolver{
				models: &data.Models{
					Contract: mockContract,
				},
				accountTokenService: mockAccountTokenService,
				rpcService:          mockRPCService,
			},
		}

		balances, err := resolver.BalancesByAccountAddress(ctx, testAccountAddress)
		require.NoError(t, err)
		require.Len(t, balances, 1)

		// Verify it's a native balance
		nativeBalance, ok := balances[0].(*graphql1.NativeBalance)
		require.True(t, ok)
		assert.Equal(t, "1000.0000000", nativeBalance.Balance)
		assert.Equal(t, graphql1.TokenTypeNative, nativeBalance.TokenType)
		assert.NotEmpty(t, nativeBalance.TokenID) // Native asset contract ID
	})

	t.Run("success - account with classic trustlines", func(t *testing.T) {
		ctx := context.Background()
		mockAccountTokenService := services.NewAccountTokenServiceMock(t)
		mockRPCService := services.NewRPCServiceMock(t)
		mockContract := data.NewContractModelMock(t)

		// Setup mocks
		mockAccountTokenService.On("GetAccountTrustlines", ctx, testAccountAddress).
			Return([]string{"USDC:" + testUSDCIssuer, "EUR:" + testEURIssuer}, nil)
		mockAccountTokenService.On("GetAccountContracts", ctx, testAccountAddress).Return([]string{}, nil)
		mockContract.On("BatchGetByIDs", ctx, []string{}).Return([]*data.Contract{}, nil)
		mockRPCService.On("NetworkPassphrase").Return(testNetworkPassphrase)

		// Create ledger entries
		accountEntry := createAccountLedgerEntry(testAccountAddress, 5000000000) // 500 XLM
		usdcTrustline := createTrustlineLedgerEntry(
			testAccountAddress, "USDC", testUSDCIssuer,
			1000000000,  // balance: 100 USDC
			10000000000, // limit: 1000 USDC
			uint32(xdr.TrustLineFlagsAuthorizedFlag),
			1000000, // buying liabilities
			2000000, // selling liabilities
		)
		eurTrustline := createTrustlineLedgerEntry(
			testAccountAddress, "EUR", testEURIssuer,
			5000000000,  // balance: 500 EUR
			20000000000, // limit: 2000 EUR
			uint32(xdr.TrustLineFlagsAuthorizedToMaintainLiabilitiesFlag),
			0, 0, // no liabilities
		)

		mockRPCService.On("GetLedgerEntries", mock.MatchedBy(func(keys []string) bool {
			return len(keys) == 3 // account + 2 trustlines
		})).Return(entities.RPCGetLedgerEntriesResult{
			Entries: []entities.LedgerEntryResult{accountEntry, usdcTrustline, eurTrustline},
		}, nil)

		resolver := &queryResolver{
			&Resolver{
				models: &data.Models{
					Contract: mockContract,
				},
				accountTokenService: mockAccountTokenService,
				rpcService:          mockRPCService,
			},
		}

		balances, err := resolver.BalancesByAccountAddress(ctx, testAccountAddress)
		require.NoError(t, err)
		require.Len(t, balances, 3) // 1 native + 2 trustlines

		// Verify native balance
		nativeBalance, ok := balances[0].(*graphql1.NativeBalance)
		require.True(t, ok)
		assert.Equal(t, "500.0000000", nativeBalance.Balance)

		// Verify USDC trustline
		usdcBalance, ok := balances[1].(*graphql1.TrustlineBalance)
		require.True(t, ok)
		assert.Equal(t, "100.0000000", usdcBalance.Balance)
		assert.Equal(t, graphql1.TokenTypeClassic, usdcBalance.TokenType)
		assert.Equal(t, "USDC", usdcBalance.Code)
		assert.Equal(t, testUSDCIssuer, usdcBalance.Issuer)
		assert.Equal(t, "1000.0000000", usdcBalance.Limit)
		assert.Equal(t, "0.1000000", usdcBalance.BuyingLiabilities)
		assert.Equal(t, "0.2000000", usdcBalance.SellingLiabilities)
		assert.True(t, usdcBalance.IsAuthorized)
		assert.False(t, usdcBalance.IsAuthorizedToMaintainLiabilities)

		// Verify EUR trustline
		eurBalance, ok := balances[2].(*graphql1.TrustlineBalance)
		require.True(t, ok)
		assert.Equal(t, "500.0000000", eurBalance.Balance)
		assert.Equal(t, "EUR", eurBalance.Code)
		assert.Equal(t, testEURIssuer, eurBalance.Issuer)
		assert.False(t, eurBalance.IsAuthorized)
		assert.True(t, eurBalance.IsAuthorizedToMaintainLiabilities)
	})

	t.Run("success - account with SAC contract balances", func(t *testing.T) {
		ctx := context.Background()
		mockAccountTokenService := services.NewAccountTokenServiceMock(t)
		mockRPCService := services.NewRPCServiceMock(t)
		mockContract := data.NewContractModelMock(t)

		// Setup mocks
		mockAccountTokenService.On("GetAccountTrustlines", ctx, testAccountAddress).Return([]string{}, nil)
		mockAccountTokenService.On("GetAccountContracts", ctx, testAccountAddress).
			Return([]string{testSACContractAddress}, nil)
		mockContract.On("BatchGetByIDs", ctx, []string{testSACContractAddress}).
			Return([]*data.Contract{createSACContract(testSACContractAddress, "USDC", testUSDCIssuer)}, nil)
		mockRPCService.On("NetworkPassphrase").Return(testNetworkPassphrase)

		// Create ledger entries
		accountEntry := createAccountLedgerEntry(testAccountAddress, 1000000000)
		sacEntry := createSACContractDataEntry(testSACContractAddress, testAccountAddress, 25000000000, true, false)

		mockRPCService.On("GetLedgerEntries", mock.MatchedBy(func(keys []string) bool {
			return len(keys) == 2 // account + 1 contract
		})).Return(entities.RPCGetLedgerEntriesResult{
			Entries: []entities.LedgerEntryResult{accountEntry, sacEntry},
		}, nil)

		resolver := &queryResolver{
			&Resolver{
				models: &data.Models{
					Contract: mockContract,
				},
				accountTokenService: mockAccountTokenService,
				rpcService:          mockRPCService,
			},
		}

		balances, err := resolver.BalancesByAccountAddress(ctx, testAccountAddress)
		require.NoError(t, err)
		require.Len(t, balances, 2) // 1 native + 1 SAC

		// Verify SAC balance
		sacBalance, ok := balances[1].(*graphql1.SACBalance)
		require.True(t, ok)
		assert.Equal(t, "2500.0000000", sacBalance.Balance)
		assert.Equal(t, graphql1.TokenTypeSac, sacBalance.TokenType)
		assert.Equal(t, testSACContractAddress, sacBalance.TokenID)
		assert.Equal(t, "USDC", sacBalance.Code)
		assert.Equal(t, testUSDCIssuer, sacBalance.Issuer)
		assert.True(t, sacBalance.IsAuthorized)
		assert.False(t, sacBalance.IsClawbackEnabled)
	})

	t.Run("success - account with SEP-41 contract balances", func(t *testing.T) {
		ctx := context.Background()
		mockAccountTokenService := services.NewAccountTokenServiceMock(t)
		mockRPCService := services.NewRPCServiceMock(t)
		mockContract := data.NewContractModelMock(t)

		// Setup mocks
		mockAccountTokenService.On("GetAccountTrustlines", ctx, testAccountAddress).Return([]string{}, nil)
		mockAccountTokenService.On("GetAccountContracts", ctx, testAccountAddress).
			Return([]string{testSEP41ContractAddress}, nil)
		mockContract.On("BatchGetByIDs", ctx, []string{testSEP41ContractAddress}).
			Return([]*data.Contract{createSEP41Contract(testSEP41ContractAddress, "MyToken", "MTK", 7)}, nil)
		mockRPCService.On("NetworkPassphrase").Return(testNetworkPassphrase)

		// Create ledger entries
		accountEntry := createAccountLedgerEntry(testAccountAddress, 1000000000)
		sep41Entry := createSEP41ContractDataEntry(testSEP41ContractAddress, testAccountAddress, false, 50000000000)

		mockRPCService.On("GetLedgerEntries", mock.MatchedBy(func(keys []string) bool {
			return len(keys) == 2 // account + 1 contract
		})).Return(entities.RPCGetLedgerEntriesResult{
			Entries: []entities.LedgerEntryResult{accountEntry, sep41Entry},
		}, nil)

		resolver := &queryResolver{
			&Resolver{
				models: &data.Models{
					Contract: mockContract,
				},
				accountTokenService: mockAccountTokenService,
				rpcService:          mockRPCService,
			},
		}

		balances, err := resolver.BalancesByAccountAddress(ctx, testAccountAddress)
		require.NoError(t, err)
		require.Len(t, balances, 2) // 1 native + 1 SEP-41

		// Verify SEP-41 balance
		sep41Balance, ok := balances[1].(*graphql1.SEP41Balance)
		require.True(t, ok)
		assert.Equal(t, "5000.0000000", sep41Balance.Balance)
		assert.Equal(t, graphql1.TokenTypeSep41, sep41Balance.TokenType)
		assert.Equal(t, testSEP41ContractAddress, sep41Balance.TokenID)
		assert.Equal(t, "MyToken", sep41Balance.Name)
		assert.Equal(t, "MTK", sep41Balance.Symbol)
		assert.Equal(t, int32(7), sep41Balance.Decimals)
	})

	t.Run("success - mixed balances (native + trustlines + SAC + SEP-41)", func(t *testing.T) {
		ctx := context.Background()
		mockAccountTokenService := services.NewAccountTokenServiceMock(t)
		mockRPCService := services.NewRPCServiceMock(t)
		mockContract := data.NewContractModelMock(t)

		// Setup mocks
		mockAccountTokenService.On("GetAccountTrustlines", ctx, testAccountAddress).
			Return([]string{"USDC:" + testUSDCIssuer}, nil)
		mockAccountTokenService.On("GetAccountContracts", ctx, testAccountAddress).
			Return([]string{testSACContractAddress, testSEP41ContractAddress}, nil)
		mockContract.On("BatchGetByIDs", ctx, []string{testSACContractAddress, testSEP41ContractAddress}).
			Return([]*data.Contract{
				createSACContract(testSACContractAddress, "EURC", testEURIssuer),
				createSEP41Contract(testSEP41ContractAddress, "CustomToken", "CTK", 6),
			}, nil)
		mockRPCService.On("NetworkPassphrase").Return(testNetworkPassphrase)

		// Create all ledger entries
		accountEntry := createAccountLedgerEntry(testAccountAddress, 2000000000)
		usdcTrustline := createTrustlineLedgerEntry(testAccountAddress, "USDC", testUSDCIssuer, 1000000000, 10000000000, uint32(xdr.TrustLineFlagsAuthorizedFlag), 0, 0)
		sacEntry := createSACContractDataEntry(testSACContractAddress, testAccountAddress, 15000000000, true, true)
		sep41Entry := createSEP41ContractDataEntry(testSEP41ContractAddress, testAccountAddress, false, 30000000000)

		mockRPCService.On("GetLedgerEntries", mock.MatchedBy(func(keys []string) bool {
			return len(keys) == 4 // account + trustline + 2 contracts
		})).Return(entities.RPCGetLedgerEntriesResult{
			Entries: []entities.LedgerEntryResult{accountEntry, usdcTrustline, sacEntry, sep41Entry},
		}, nil)

		resolver := &queryResolver{
			&Resolver{
				models: &data.Models{
					Contract: mockContract,
				},
				accountTokenService: mockAccountTokenService,
				rpcService:          mockRPCService,
			},
		}

		balances, err := resolver.BalancesByAccountAddress(ctx, testAccountAddress)
		require.NoError(t, err)
		require.Len(t, balances, 4)

		// Verify all balance types are present
		assert.IsType(t, &graphql1.NativeBalance{}, balances[0])
		assert.IsType(t, &graphql1.TrustlineBalance{}, balances[1])
		assert.IsType(t, &graphql1.SACBalance{}, balances[2])
		assert.IsType(t, &graphql1.SEP41Balance{}, balances[3])

		// Verify SAC balance details
		sacBalance := balances[2].(*graphql1.SACBalance)
		assert.Equal(t, "EURC", sacBalance.Code)
		assert.Equal(t, testEURIssuer, sacBalance.Issuer)

		// Verify SEP-41 balance details
		sep41Balance := balances[3].(*graphql1.SEP41Balance)
		assert.Equal(t, "CustomToken", sep41Balance.Name)
		assert.Equal(t, "CTK", sep41Balance.Symbol)
		assert.Equal(t, int32(6), sep41Balance.Decimals)
	})

	t.Run("success - contract address (skips account and trustlines)", func(t *testing.T) {
		ctx := context.Background()
		mockAccountTokenService := services.NewAccountTokenServiceMock(t)
		mockRPCService := services.NewRPCServiceMock(t)
		mockContract := data.NewContractModelMock(t)

		// For contract addresses, GetAccountTrustlines should NOT be called
		// Only GetAccountContracts
		mockAccountTokenService.On("GetAccountContracts", ctx, testSEP41ContractAddress).
			Return([]string{testSEP41ContractAddress}, nil)
		mockContract.On("BatchGetByIDs", ctx, []string{testSEP41ContractAddress}).
			Return([]*data.Contract{createSEP41Contract(testSEP41ContractAddress, "Token", "TKN", 7)}, nil)
		mockRPCService.On("NetworkPassphrase").Return(testNetworkPassphrase)

		sep41Entry := createSEP41ContractDataEntry(testSEP41ContractAddress, testContractAddress, true, 10000000000)

		mockRPCService.On("GetLedgerEntries", mock.MatchedBy(func(keys []string) bool {
			return len(keys) == 1 // Only contract data, no account key
		})).Return(entities.RPCGetLedgerEntriesResult{
			Entries: []entities.LedgerEntryResult{sep41Entry},
		}, nil)

		resolver := &queryResolver{
			&Resolver{
				models: &data.Models{
					Contract: mockContract,
				},
				accountTokenService: mockAccountTokenService,
				rpcService:          mockRPCService,
			},
		}

		balances, err := resolver.BalancesByAccountAddress(ctx, testContractAddress)
		require.NoError(t, err)
		require.Len(t, balances, 1)

		sep41Balance, ok := balances[0].(*graphql1.SEP41Balance)
		require.True(t, ok)
		assert.Equal(t, "1000.0000000", sep41Balance.Balance)
		assert.Equal(t, "Token", sep41Balance.Name)
		assert.Equal(t, "TKN", sep41Balance.Symbol)
		assert.Equal(t, int32(7), sep41Balance.Decimals)
	})

	t.Run("success - trustline with V0 extension (no liabilities)", func(t *testing.T) {
		ctx := context.Background()
		mockAccountTokenService := services.NewAccountTokenServiceMock(t)
		mockRPCService := services.NewRPCServiceMock(t)
		mockContract := data.NewContractModelMock(t)

		mockAccountTokenService.On("GetAccountTrustlines", ctx, testAccountAddress).
			Return([]string{"USDC:" + testUSDCIssuer}, nil)
		mockAccountTokenService.On("GetAccountContracts", ctx, testAccountAddress).Return([]string{}, nil)
		mockContract.On("BatchGetByIDs", ctx, []string{}).Return([]*data.Contract{}, nil)
		mockRPCService.On("NetworkPassphrase").Return(testNetworkPassphrase)

		accountEntry := createAccountLedgerEntry(testAccountAddress, 1000000000)
		// V0 extension (no liabilities - pass 0s)
		usdcTrustline := createTrustlineLedgerEntry(testAccountAddress, "USDC", testUSDCIssuer, 1000000000, 10000000000, uint32(xdr.TrustLineFlagsAuthorizedFlag), 0, 0)

		mockRPCService.On("GetLedgerEntries", mock.Anything).Return(entities.RPCGetLedgerEntriesResult{
			Entries: []entities.LedgerEntryResult{accountEntry, usdcTrustline},
		}, nil)

		resolver := &queryResolver{
			&Resolver{
				models: &data.Models{
					Contract: mockContract,
				},
				accountTokenService: mockAccountTokenService,
				rpcService:          mockRPCService,
			},
		}

		balances, err := resolver.BalancesByAccountAddress(ctx, testAccountAddress)
		require.NoError(t, err)

		trustlineBalance := balances[1].(*graphql1.TrustlineBalance)
		assert.Equal(t, "0.0000000", trustlineBalance.BuyingLiabilities)
		assert.Equal(t, "0.0000000", trustlineBalance.SellingLiabilities)
	})

	// Error Cases

	t.Run("error - empty address", func(t *testing.T) {
		ctx := context.Background()
		resolver := &queryResolver{&Resolver{}}

		balances, err := resolver.BalancesByAccountAddress(ctx, "")
		assert.Error(t, err)
		assert.Nil(t, balances)
	})

	t.Run("error - GetAccountTrustlines fails", func(t *testing.T) {
		ctx := context.Background()
		mockAccountTokenService := services.NewAccountTokenServiceMock(t)

		mockAccountTokenService.On("GetAccountTrustlines", ctx, testAccountAddress).
			Return([]string{}, errors.New("redis connection failed"))

		resolver := &queryResolver{
			&Resolver{
				accountTokenService: mockAccountTokenService,
			},
		}

		balances, err := resolver.BalancesByAccountAddress(ctx, testAccountAddress)
		require.Error(t, err)
		assert.Contains(t, err.Error(), ErrMsgBalancesFetchFailed)
		assert.Nil(t, balances)
	})

	t.Run("error - GetAccountContracts fails", func(t *testing.T) {
		ctx := context.Background()
		mockAccountTokenService := services.NewAccountTokenServiceMock(t)
		mockContract := data.NewContractModelMock(t)

		mockAccountTokenService.On("GetAccountTrustlines", ctx, testAccountAddress).
			Return([]string{}, nil)
		mockAccountTokenService.On("GetAccountContracts", ctx, testAccountAddress).
			Return([]string{}, errors.New("redis connection failed"))

		resolver := &queryResolver{
			&Resolver{
				models: &data.Models{
					Contract: mockContract,
				},
				accountTokenService: mockAccountTokenService,
			},
		}

		balances, err := resolver.BalancesByAccountAddress(ctx, testAccountAddress)
		require.Error(t, err)
		assert.Contains(t, err.Error(), ErrMsgBalancesFetchFailed)
		assert.Nil(t, balances)
	})

	t.Run("error - GetLedgerEntries RPC fails", func(t *testing.T) {
		ctx := context.Background()
		mockAccountTokenService := services.NewAccountTokenServiceMock(t)
		mockRPCService := services.NewRPCServiceMock(t)
		mockContract := data.NewContractModelMock(t)

		mockAccountTokenService.On("GetAccountTrustlines", ctx, testAccountAddress).Return([]string{}, nil)
		mockAccountTokenService.On("GetAccountContracts", ctx, testAccountAddress).Return([]string{}, nil)
		mockContract.On("BatchGetByIDs", ctx, []string{}).Return([]*data.Contract{}, nil)
		mockRPCService.On("GetLedgerEntries", mock.Anything).
			Return(entities.RPCGetLedgerEntriesResult{}, errors.New("RPC node unavailable"))

		resolver := &queryResolver{
			&Resolver{
				models: &data.Models{
					Contract: mockContract,
				},
				accountTokenService: mockAccountTokenService,
				rpcService:          mockRPCService,
			},
		}

		balances, err := resolver.BalancesByAccountAddress(ctx, testAccountAddress)
		require.Error(t, err)
		assert.Contains(t, err.Error(), ErrMsgRPCUnavailable)
		assert.Nil(t, balances)
	})

	t.Run("error - invalid trustline format", func(t *testing.T) {
		ctx := context.Background()
		mockAccountTokenService := services.NewAccountTokenServiceMock(t)
		mockRPCService := services.NewRPCServiceMock(t)
		mockContract := data.NewContractModelMock(t)

		// Return invalid trustline format (missing colon)
		mockAccountTokenService.On("GetAccountTrustlines", ctx, testAccountAddress).
			Return([]string{"INVALID_FORMAT"}, nil)

		resolver := &queryResolver{
			&Resolver{
				models: &data.Models{
					Contract: mockContract,
				},
				accountTokenService: mockAccountTokenService,
				rpcService:          mockRPCService,
			},
		}

		balances, err := resolver.BalancesByAccountAddress(ctx, testAccountAddress)
		require.Error(t, err)
		assert.Contains(t, err.Error(), ErrMsgBalancesFetchFailed)
		assert.Nil(t, balances)
	})

	t.Run("error - GetContractType fails", func(t *testing.T) {
		ctx := context.Background()
		mockAccountTokenService := services.NewAccountTokenServiceMock(t)
		mockRPCService := services.NewRPCServiceMock(t)
		mockContract := data.NewContractModelMock(t)

		mockAccountTokenService.On("GetAccountTrustlines", ctx, testAccountAddress).Return([]string{}, nil)
		mockAccountTokenService.On("GetAccountContracts", ctx, testAccountAddress).
			Return([]string{testContractAddress}, nil)
		mockContract.On("BatchGetByIDs", ctx, []string{testContractAddress}).
			Return([]*data.Contract{}, errors.New("failed to get contracts"))

		resolver := &queryResolver{
			&Resolver{
				models: &data.Models{
					Contract: mockContract,
				},
				accountTokenService: mockAccountTokenService,
				rpcService:          mockRPCService,
			},
		}

		balances, err := resolver.BalancesByAccountAddress(ctx, testAccountAddress)
		require.Error(t, err)
		assert.Contains(t, err.Error(), ErrMsgBalancesFetchFailed)
		assert.Nil(t, balances)
	})

	t.Run("error - XDR decoding fails", func(t *testing.T) {
		ctx := context.Background()
		mockAccountTokenService := services.NewAccountTokenServiceMock(t)
		mockRPCService := services.NewRPCServiceMock(t)
		mockContract := data.NewContractModelMock(t)

		mockAccountTokenService.On("GetAccountTrustlines", ctx, testAccountAddress).Return([]string{}, nil)
		mockAccountTokenService.On("GetAccountContracts", ctx, testAccountAddress).Return([]string{}, nil)
		mockContract.On("BatchGetByIDs", ctx, []string{}).Return([]*data.Contract{}, nil)
		mockRPCService.On("NetworkPassphrase").Return(testNetworkPassphrase)

		// Return invalid XDR
		mockRPCService.On("GetLedgerEntries", mock.Anything).Return(entities.RPCGetLedgerEntriesResult{
			Entries: []entities.LedgerEntryResult{
				{
					DataXDR:            "invalid_base64_xdr",
					LastModifiedLedger: 1000,
				},
			},
		}, nil)

		resolver := &queryResolver{
			&Resolver{
				models: &data.Models{
					Contract: mockContract,
				},
				accountTokenService: mockAccountTokenService,
				rpcService:          mockRPCService,
			},
		}

		balances, err := resolver.BalancesByAccountAddress(ctx, testAccountAddress)
		require.Error(t, err)
		assert.Contains(t, err.Error(), ErrMsgBalancesFetchFailed)
		assert.Nil(t, balances)
	})

	t.Run("error - SAC missing amount field", func(t *testing.T) {
		ctx := context.Background()
		mockAccountTokenService := services.NewAccountTokenServiceMock(t)
		mockRPCService := services.NewRPCServiceMock(t)
		mockContract := data.NewContractModelMock(t)

		mockAccountTokenService.On("GetAccountTrustlines", ctx, testAccountAddress).Return([]string{}, nil)
		mockAccountTokenService.On("GetAccountContracts", ctx, testAccountAddress).
			Return([]string{testContractAddress}, nil)
		mockContract.On("BatchGetByIDs", ctx, []string{testContractAddress}).
			Return([]*data.Contract{createSACContract(testContractAddress, "TEST", testUSDCIssuer)}, nil)
		mockRPCService.On("NetworkPassphrase").Return(testNetworkPassphrase)

		// Create SAC entry with missing "amount" field - need to manually create malformed XDR
		contractHash := contractIDToHash(testContractAddress)
		holderAccountID := xdr.MustAddress(testAccountAddress)

		balanceSymbol := ptrToScSymbol("Balance")
		balanceKey := xdr.ScVal{
			Type: xdr.ScValTypeScvVec,
			Vec: ptrToScVec([]xdr.ScVal{
				{Type: xdr.ScValTypeScvSymbol, Sym: balanceSymbol},
				{Type: xdr.ScValTypeScvAddress, Address: &xdr.ScAddress{Type: xdr.ScAddressTypeScAddressTypeAccount, AccountId: &holderAccountID}},
			}),
		}

		// SAC map without "amount" field
		authorizedSym := ptrToScSymbol("authorized")
		clawbackSym := ptrToScSymbol("clawback")
		authorizedBool := true
		clawbackBool := false

		balanceValue := xdr.ScVal{
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

		contractID := xdr.ContractId(contractHash)
		contractDataEntry := xdr.ContractDataEntry{
			Contract:   xdr.ScAddress{Type: xdr.ScAddressTypeScAddressTypeContract, ContractId: &contractID},
			Key:        balanceKey,
			Durability: xdr.ContractDataDurabilityPersistent,
			Val:        balanceValue,
		}

		ledgerEntryData := xdr.LedgerEntryData{
			Type:         xdr.LedgerEntryTypeContractData,
			ContractData: &contractDataEntry,
		}

		malformedEntry := entities.LedgerEntryResult{
			DataXDR:            encodeLedgerEntryDataToBase64(ledgerEntryData),
			LastModifiedLedger: 1000,
		}

		accountEntry := createAccountLedgerEntry(testAccountAddress, 1000000000)

		mockRPCService.On("GetLedgerEntries", mock.Anything).Return(entities.RPCGetLedgerEntriesResult{
			Entries: []entities.LedgerEntryResult{accountEntry, malformedEntry},
		}, nil)

		resolver := &queryResolver{
			&Resolver{
				models: &data.Models{
					Contract: mockContract,
				},
				accountTokenService: mockAccountTokenService,
				rpcService:          mockRPCService,
			},
		}

		balances, err := resolver.BalancesByAccountAddress(ctx, testAccountAddress)
		require.Error(t, err)
		assert.Contains(t, err.Error(), ErrMsgBalancesFetchFailed)
		assert.Nil(t, balances)
	})

	t.Run("error - SEP-41 wrong type (map instead of i128)", func(t *testing.T) {
		ctx := context.Background()
		mockAccountTokenService := services.NewAccountTokenServiceMock(t)
		mockRPCService := services.NewRPCServiceMock(t)
		mockContract := data.NewContractModelMock(t)

		mockAccountTokenService.On("GetAccountTrustlines", ctx, testAccountAddress).Return([]string{}, nil)
		mockAccountTokenService.On("GetAccountContracts", ctx, testAccountAddress).
			Return([]string{testContractAddress}, nil)
		mockContract.On("BatchGetByIDs", ctx, []string{testContractAddress}).
			Return([]*data.Contract{createSEP41Contract(testContractAddress, "Test", "TST", 7)}, nil)
		mockRPCService.On("NetworkPassphrase").Return(testNetworkPassphrase)

		// Create SEP-41 entry with wrong type (use SAC structure instead of i128)
		wrongTypeEntry := createSACContractDataEntry(testContractAddress, testAccountAddress, 10000000000, true, false)

		accountEntry := createAccountLedgerEntry(testAccountAddress, 1000000000)

		mockRPCService.On("GetLedgerEntries", mock.Anything).Return(entities.RPCGetLedgerEntriesResult{
			Entries: []entities.LedgerEntryResult{accountEntry, wrongTypeEntry},
		}, nil)

		resolver := &queryResolver{
			&Resolver{
				models: &data.Models{
					Contract: mockContract,
				},
				accountTokenService: mockAccountTokenService,
				rpcService:          mockRPCService,
			},
		}

		balances, err := resolver.BalancesByAccountAddress(ctx, testAccountAddress)
		require.Error(t, err)
		assert.Contains(t, err.Error(), ErrMsgBalancesFetchFailed)
		assert.Nil(t, balances)
	})

	// Edge Cases

	t.Run("edge - unknown contract type skipped", func(t *testing.T) {
		ctx := context.Background()
		mockAccountTokenService := services.NewAccountTokenServiceMock(t)
		mockRPCService := services.NewRPCServiceMock(t)
		mockContract := data.NewContractModelMock(t)

		mockAccountTokenService.On("GetAccountTrustlines", ctx, testAccountAddress).Return([]string{}, nil)
		mockAccountTokenService.On("GetAccountContracts", ctx, testAccountAddress).
			Return([]string{testContractAddress}, nil)
		// Return contract with unknown/empty type
		unknownContract := &data.Contract{
			ID:   testContractAddress,
			Type: string(types.ContractTypeUnknown),
		}
		mockContract.On("BatchGetByIDs", ctx, []string{testContractAddress}).
			Return([]*data.Contract{unknownContract}, nil)
		mockRPCService.On("NetworkPassphrase").Return(testNetworkPassphrase)

		accountEntry := createAccountLedgerEntry(testAccountAddress, 1000000000)
		contractEntry := createSEP41ContractDataEntry(testContractAddress, testAccountAddress, false, 10000000000)

		mockRPCService.On("GetLedgerEntries", mock.Anything).Return(entities.RPCGetLedgerEntriesResult{
			Entries: []entities.LedgerEntryResult{accountEntry, contractEntry},
		}, nil)

		resolver := &queryResolver{
			&Resolver{
				models: &data.Models{
					Contract: mockContract,
				},
				accountTokenService: mockAccountTokenService,
				rpcService:          mockRPCService,
			},
		}

		balances, err := resolver.BalancesByAccountAddress(ctx, testAccountAddress)
		require.NoError(t, err)
		// Should only have native balance, contract balance skipped
		require.Len(t, balances, 1)
		assert.IsType(t, &graphql1.NativeBalance{}, balances[0])
	})

	t.Run("edge - trustline authorization flags combinations", func(t *testing.T) {
		ctx := context.Background()
		mockAccountTokenService := services.NewAccountTokenServiceMock(t)
		mockRPCService := services.NewRPCServiceMock(t)
		mockContract := data.NewContractModelMock(t)

		mockAccountTokenService.On("GetAccountTrustlines", ctx, testAccountAddress).
			Return([]string{"USDC:" + testUSDCIssuer, "EUR:" + testEURIssuer}, nil)
		mockAccountTokenService.On("GetAccountContracts", ctx, testAccountAddress).Return([]string{}, nil)
		mockContract.On("BatchGetByIDs", ctx, []string{}).Return([]*data.Contract{}, nil)
		mockRPCService.On("NetworkPassphrase").Return(testNetworkPassphrase)

		accountEntry := createAccountLedgerEntry(testAccountAddress, 1000000000)
		// Both flags set
		usdcTrustline := createTrustlineLedgerEntry(
			testAccountAddress, "USDC", testUSDCIssuer, 1000000000, 10000000000,
			uint32(xdr.TrustLineFlagsAuthorizedFlag|xdr.TrustLineFlagsAuthorizedToMaintainLiabilitiesFlag),
			0, 0,
		)
		// No flags set
		eurTrustline := createTrustlineLedgerEntry(
			testAccountAddress, "EUR", testEURIssuer, 1000000000, 10000000000, 0, 0, 0,
		)

		mockRPCService.On("GetLedgerEntries", mock.Anything).Return(entities.RPCGetLedgerEntriesResult{
			Entries: []entities.LedgerEntryResult{accountEntry, usdcTrustline, eurTrustline},
		}, nil)

		resolver := &queryResolver{
			&Resolver{
				models: &data.Models{
					Contract: mockContract,
				},
				accountTokenService: mockAccountTokenService,
				rpcService:          mockRPCService,
			},
		}

		balances, err := resolver.BalancesByAccountAddress(ctx, testAccountAddress)
		require.NoError(t, err)
		require.Len(t, balances, 3)

		// USDC - both flags set
		usdcBalance := balances[1].(*graphql1.TrustlineBalance)
		assert.True(t, usdcBalance.IsAuthorized)
		assert.True(t, usdcBalance.IsAuthorizedToMaintainLiabilities)

		// EUR - no flags set
		eurBalance := balances[2].(*graphql1.TrustlineBalance)
		assert.False(t, eurBalance.IsAuthorized)
		assert.False(t, eurBalance.IsAuthorizedToMaintainLiabilities)
	})
}

// Second test account for multi-account tests
const testAccountAddress2 = "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN"

func TestQueryResolver_BalancesByAccountAddresses(t *testing.T) {
	t.Run("success - single account", func(t *testing.T) {
		ctx := context.Background()
		mockAccountTokenService := services.NewAccountTokenServiceMock(t)
		mockRPCService := services.NewRPCServiceMock(t)
		mockContract := data.NewContractModelMock(t)

		mockAccountTokenService.On("GetAccountTrustlines", ctx, testAccountAddress).Return([]string{}, nil)
		mockAccountTokenService.On("GetAccountContracts", ctx, testAccountAddress).Return([]string{}, nil)
		// BatchGetByIDs is not called when contractIDs is empty
		mockRPCService.On("NetworkPassphrase").Return(testNetworkPassphrase)

		accountEntry := createAccountLedgerEntry(testAccountAddress, 10000000000) // 1000 XLM
		mockRPCService.On("GetLedgerEntries", mock.MatchedBy(func(keys []string) bool {
			return len(keys) == 1
		})).Return(entities.RPCGetLedgerEntriesResult{
			Entries: []entities.LedgerEntryResult{accountEntry},
		}, nil)

		resolver := &queryResolver{
			&Resolver{
				models: &data.Models{
					Contract: mockContract,
				},
				accountTokenService: mockAccountTokenService,
				rpcService:          mockRPCService,
				pool:                pond.NewPool(0),
				config:              ResolverConfig{MaxAccountsPerBalancesQuery: 10},
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
		mockAccountTokenService := services.NewAccountTokenServiceMock(t)
		mockRPCService := services.NewRPCServiceMock(t)
		mockContract := data.NewContractModelMock(t)

		// Setup for account 1 (native + trustline)
		mockAccountTokenService.On("GetAccountTrustlines", ctx, testAccountAddress).
			Return([]string{"USDC:" + testUSDCIssuer}, nil)
		mockAccountTokenService.On("GetAccountContracts", ctx, testAccountAddress).Return([]string{}, nil)

		// Setup for account 2 (native only)
		mockAccountTokenService.On("GetAccountTrustlines", ctx, testAccountAddress2).Return([]string{}, nil)
		mockAccountTokenService.On("GetAccountContracts", ctx, testAccountAddress2).Return([]string{}, nil)

		// Note: BatchGetByIDs is not called when contractIDs is empty (len == 0)
		mockRPCService.On("NetworkPassphrase").Return(testNetworkPassphrase)

		// Ledger entries
		account1Entry := createAccountLedgerEntry(testAccountAddress, 5000000000)   // 500 XLM
		account2Entry := createAccountLedgerEntry(testAccountAddress2, 10000000000) // 1000 XLM
		usdcTrustline := createTrustlineLedgerEntry(
			testAccountAddress, "USDC", testUSDCIssuer,
			1000000000, 10000000000, uint32(xdr.TrustLineFlagsAuthorizedFlag), 0, 0,
		)

		mockRPCService.On("GetLedgerEntries", mock.MatchedBy(func(keys []string) bool {
			return len(keys) == 3 // 2 accounts + 1 trustline
		})).Return(entities.RPCGetLedgerEntriesResult{
			Entries: []entities.LedgerEntryResult{account1Entry, usdcTrustline, account2Entry},
		}, nil)

		resolver := &queryResolver{
			&Resolver{
				models: &data.Models{
					Contract: mockContract,
				},
				accountTokenService: mockAccountTokenService,
				rpcService:          mockRPCService,
				pool:                pond.NewPool(0),
				config:              ResolverConfig{MaxAccountsPerBalancesQuery: 10},
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
		mockAccountTokenService := services.NewAccountTokenServiceMock(t)
		mockRPCService := services.NewRPCServiceMock(t)
		mockContract := data.NewContractModelMock(t)

		// Account 1 succeeds
		mockAccountTokenService.On("GetAccountTrustlines", ctx, testAccountAddress).Return([]string{}, nil)
		mockAccountTokenService.On("GetAccountContracts", ctx, testAccountAddress).Return([]string{}, nil)

		// Account 2 fails on trustlines
		mockAccountTokenService.On("GetAccountTrustlines", ctx, testAccountAddress2).
			Return([]string{}, errors.New("redis connection failed"))

		// Note: BatchGetByIDs is not called when contractIDs is empty (len == 0)
		mockRPCService.On("NetworkPassphrase").Return(testNetworkPassphrase)

		accountEntry := createAccountLedgerEntry(testAccountAddress, 5000000000)
		mockRPCService.On("GetLedgerEntries", mock.Anything).Return(entities.RPCGetLedgerEntriesResult{
			Entries: []entities.LedgerEntryResult{accountEntry},
		}, nil)

		resolver := &queryResolver{
			&Resolver{
				models: &data.Models{
					Contract: mockContract,
				},
				accountTokenService: mockAccountTokenService,
				rpcService:          mockRPCService,
				pool:                pond.NewPool(0),
				config:              ResolverConfig{MaxAccountsPerBalancesQuery: 10},
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
		assert.Contains(t, *results[1].Error, "getting trustlines")
		assert.Empty(t, results[1].Balances)
	})

	t.Run("success - deduplication of addresses", func(t *testing.T) {
		ctx := context.Background()
		mockAccountTokenService := services.NewAccountTokenServiceMock(t)
		mockRPCService := services.NewRPCServiceMock(t)
		mockContract := data.NewContractModelMock(t)

		// Only called once due to deduplication
		mockAccountTokenService.On("GetAccountTrustlines", ctx, testAccountAddress).Return([]string{}, nil).Once()
		mockAccountTokenService.On("GetAccountContracts", ctx, testAccountAddress).Return([]string{}, nil).Once()
		// Note: BatchGetByIDs is not called when contractIDs is empty (len == 0)
		mockRPCService.On("NetworkPassphrase").Return(testNetworkPassphrase)

		accountEntry := createAccountLedgerEntry(testAccountAddress, 10000000000)
		mockRPCService.On("GetLedgerEntries", mock.Anything).Return(entities.RPCGetLedgerEntriesResult{
			Entries: []entities.LedgerEntryResult{accountEntry},
		}, nil)

		resolver := &queryResolver{
			&Resolver{
				models: &data.Models{
					Contract: mockContract,
				},
				accountTokenService: mockAccountTokenService,
				rpcService:          mockRPCService,
				pool:                pond.NewPool(0),
				config:              ResolverConfig{MaxAccountsPerBalancesQuery: 10},
			},
		}

		// Pass same address twice
		results, err := resolver.BalancesByAccountAddresses(ctx, []string{testAccountAddress, testAccountAddress})
		require.NoError(t, err)
		require.Len(t, results, 1) // Deduplicated to single result

		assert.Equal(t, testAccountAddress, results[0].Address)
		assert.Nil(t, results[0].Error)
	})

	t.Run("success - contract address (skips native and trustlines)", func(t *testing.T) {
		ctx := context.Background()
		mockAccountTokenService := services.NewAccountTokenServiceMock(t)
		mockRPCService := services.NewRPCServiceMock(t)
		mockContract := data.NewContractModelMock(t)

		// GetAccountTrustlines should NOT be called for contract address
		mockAccountTokenService.On("GetAccountContracts", ctx, testSEP41ContractAddress).
			Return([]string{testSEP41ContractAddress}, nil)
		mockContract.On("BatchGetByIDs", ctx, []string{testSEP41ContractAddress}).
			Return([]*data.Contract{createSEP41Contract(testSEP41ContractAddress, "Token", "TKN", 7)}, nil)
		mockRPCService.On("NetworkPassphrase").Return(testNetworkPassphrase)

		sep41Entry := createSEP41ContractDataEntry(testSEP41ContractAddress, testSEP41ContractAddress, true, 10000000000)
		mockRPCService.On("GetLedgerEntries", mock.MatchedBy(func(keys []string) bool {
			return len(keys) == 1 // Only contract data, no account key
		})).Return(entities.RPCGetLedgerEntriesResult{
			Entries: []entities.LedgerEntryResult{sep41Entry},
		}, nil)

		resolver := &queryResolver{
			&Resolver{
				models: &data.Models{
					Contract: mockContract,
				},
				accountTokenService: mockAccountTokenService,
				rpcService:          mockRPCService,
				pool:                pond.NewPool(0),
				config:              ResolverConfig{MaxAccountsPerBalancesQuery: 10},
			},
		}

		results, err := resolver.BalancesByAccountAddresses(ctx, []string{testSEP41ContractAddress})
		require.NoError(t, err)
		require.Len(t, results, 1)

		assert.Equal(t, testSEP41ContractAddress, results[0].Address)
		assert.Nil(t, results[0].Error)
		require.Len(t, results[0].Balances, 1)

		sep41Balance, ok := results[0].Balances[0].(*graphql1.SEP41Balance)
		require.True(t, ok)
		assert.Equal(t, "1000.0000000", sep41Balance.Balance)
	})

	t.Run("error - exceeds max addresses", func(t *testing.T) {
		ctx := context.Background()
		resolver := &queryResolver{
			&Resolver{
				pool:   pond.NewPool(0),
				config: ResolverConfig{MaxAccountsPerBalancesQuery: 20},
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
				pool:   pond.NewPool(0),
				config: ResolverConfig{MaxAccountsPerBalancesQuery: 10},
			},
		}

		results, err := resolver.BalancesByAccountAddresses(ctx, []string{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "addresses array cannot be empty")
		assert.Nil(t, results)
	})

	t.Run("error - RPC failure affects all accounts", func(t *testing.T) {
		ctx := context.Background()
		mockAccountTokenService := services.NewAccountTokenServiceMock(t)
		mockRPCService := services.NewRPCServiceMock(t)
		mockContract := data.NewContractModelMock(t)

		mockAccountTokenService.On("GetAccountTrustlines", ctx, testAccountAddress).Return([]string{}, nil)
		mockAccountTokenService.On("GetAccountContracts", ctx, testAccountAddress).Return([]string{}, nil)
		mockAccountTokenService.On("GetAccountTrustlines", ctx, testAccountAddress2).Return([]string{}, nil)
		mockAccountTokenService.On("GetAccountContracts", ctx, testAccountAddress2).Return([]string{}, nil)
		// Note: BatchGetByIDs is not called when contractIDs is empty (len == 0)

		// RPC fails
		mockRPCService.On("GetLedgerEntries", mock.Anything).
			Return(entities.RPCGetLedgerEntriesResult{}, errors.New("RPC node unavailable"))

		resolver := &queryResolver{
			&Resolver{
				models: &data.Models{
					Contract: mockContract,
				},
				accountTokenService: mockAccountTokenService,
				rpcService:          mockRPCService,
				pool:                pond.NewPool(0),
				config:              ResolverConfig{MaxAccountsPerBalancesQuery: 10},
			},
		}

		results, err := resolver.BalancesByAccountAddresses(ctx, []string{testAccountAddress, testAccountAddress2})
		require.Error(t, err) // Overall call succeeds, but each account has error
		assert.Contains(t, err.Error(), "failed to fetch ledger entries from RPC")
		require.Len(t, results, 0)
	})

	t.Run("success - mixed account and contract addresses", func(t *testing.T) {
		ctx := context.Background()
		mockAccountTokenService := services.NewAccountTokenServiceMock(t)
		mockRPCService := services.NewRPCServiceMock(t)
		mockContract := data.NewContractModelMock(t)

		// Account address (no contracts, so BatchGetByIDs is not called for this account)
		mockAccountTokenService.On("GetAccountTrustlines", ctx, testAccountAddress).Return([]string{}, nil)
		mockAccountTokenService.On("GetAccountContracts", ctx, testAccountAddress).Return([]string{}, nil)

		// Contract address (has contracts, so BatchGetByIDs IS called)
		mockAccountTokenService.On("GetAccountContracts", ctx, testSEP41ContractAddress).
			Return([]string{testSEP41ContractAddress}, nil)
		// Note: BatchGetByIDs is not called when contractIDs is empty (len == 0) for the account address
		mockContract.On("BatchGetByIDs", ctx, []string{testSEP41ContractAddress}).
			Return([]*data.Contract{createSEP41Contract(testSEP41ContractAddress, "Token", "TKN", 7)}, nil)
		mockRPCService.On("NetworkPassphrase").Return(testNetworkPassphrase)

		accountEntry := createAccountLedgerEntry(testAccountAddress, 5000000000)
		sep41Entry := createSEP41ContractDataEntry(testSEP41ContractAddress, testSEP41ContractAddress, true, 10000000000)

		mockRPCService.On("GetLedgerEntries", mock.Anything).Return(entities.RPCGetLedgerEntriesResult{
			Entries: []entities.LedgerEntryResult{accountEntry, sep41Entry},
		}, nil)

		resolver := &queryResolver{
			&Resolver{
				models: &data.Models{
					Contract: mockContract,
				},
				accountTokenService: mockAccountTokenService,
				rpcService:          mockRPCService,
				pool:                pond.NewPool(0),
				config:              ResolverConfig{MaxAccountsPerBalancesQuery: 10},
			},
		}

		results, err := resolver.BalancesByAccountAddresses(ctx, []string{testAccountAddress, testSEP41ContractAddress})
		require.NoError(t, err)
		require.Len(t, results, 2)

		// Account has native balance
		assert.Equal(t, testAccountAddress, results[0].Address)
		assert.Nil(t, results[0].Error)
		require.Len(t, results[0].Balances, 1)
		assert.IsType(t, &graphql1.NativeBalance{}, results[0].Balances[0])

		// Contract has SEP-41 balance
		assert.Equal(t, testSEP41ContractAddress, results[1].Address)
		assert.Nil(t, results[1].Error)
		require.Len(t, results[1].Balances, 1)
		assert.IsType(t, &graphql1.SEP41Balance{}, results[1].Balances[0])
	})
}
