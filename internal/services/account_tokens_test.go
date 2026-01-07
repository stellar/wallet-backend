package services

import (
	"context"
	"io"
	"strconv"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/ingest/sac"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	wbdata "github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/store"
)

// Helper function to encode asset IDs for test setup
func testEncodeAssetIDs(ids []int64) string {
	return string(encodeAssetIDs(ids))
}

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

// setupTestRedis creates a miniredis instance for testing
func setupTestRedis(t *testing.T) (*miniredis.Miniredis, *store.RedisStore) {
	mr := miniredis.RunT(t)
	// miniredis Port() returns a string, convert to int
	port, err := strconv.Atoi(mr.Port())
	require.NoError(t, err)
	redisStore := store.NewRedisStore(mr.Host(), port, "")
	return mr, redisStore
}

func TestExtractHolderAddress(t *testing.T) {
	service := &accountTokenService{
		trustlinesPrefix: trustlinesKeyPrefix,
		contractsPrefix:  contractsKeyPrefix,
	}

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
			name:       "valid CODE:ISSUER format",
			asset:      "USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN",
			wantCode:   "USDC",
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
			name:       "multiple colons - splits on first",
			asset:      "USD:C:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN",
			wantCode:   "USD",
			wantIssuer: "C:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN",
			wantErr:    false,
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

func TestEncodeDecodeAssetIDs(t *testing.T) {
	t.Run("empty slice returns nil", func(t *testing.T) {
		encoded := encodeAssetIDs([]int64{})
		assert.Nil(t, encoded)

		decoded := decodeAssetIDs(nil)
		assert.Nil(t, decoded)

		decoded = decodeAssetIDs([]byte{})
		assert.Nil(t, decoded)
	})

	t.Run("single ID roundtrip", func(t *testing.T) {
		ids := []int64{42}
		encoded := encodeAssetIDs(ids)
		decoded := decodeAssetIDs(encoded)
		assert.Equal(t, ids, decoded)
	})

	t.Run("multiple IDs roundtrip", func(t *testing.T) {
		ids := []int64{1, 2, 3, 100, 1000, 163006, 253529}
		encoded := encodeAssetIDs(ids)
		decoded := decodeAssetIDs(encoded)
		assert.Equal(t, ids, decoded)
	})

	t.Run("large IDs roundtrip", func(t *testing.T) {
		ids := []int64{1, 127, 128, 16383, 16384, 2097151, 2097152, 268435455}
		encoded := encodeAssetIDs(ids)
		decoded := decodeAssetIDs(encoded)
		assert.Equal(t, ids, decoded)
	})

	t.Run("varint is smaller than ASCII for typical IDs", func(t *testing.T) {
		// Typical trustline IDs from production
		ids := []int64{163006, 153698, 22755, 197674, 162872}
		encoded := encodeAssetIDs(ids)

		// ASCII would be "163006,153698,22755,197674,162872" = 34 bytes
		asciiLen := len("163006,153698,22755,197674,162872")
		assert.Less(t, len(encoded), asciiLen, "varint should be smaller than ASCII")
	})
}

func TestBuildTrustlineKey(t *testing.T) {
	service := &accountTokenService{
		trustlinesPrefix: trustlinesKeyPrefix,
	}

	t.Run("same account always maps to same bucket", func(t *testing.T) {
		account := "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"
		key1 := service.buildTrustlineKey(account)
		key2 := service.buildTrustlineKey(account)
		assert.Equal(t, key1, key2)
	})

	t.Run("key format is trustlines prefix plus bucket number", func(t *testing.T) {
		account := "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"
		key := service.buildTrustlineKey(account)
		assert.True(t, len(key) > len(trustlinesKeyPrefix))
		assert.Equal(t, trustlinesKeyPrefix, key[:len(trustlinesKeyPrefix)])
	})

	t.Run("different accounts can map to different buckets", func(t *testing.T) {
		// Test with multiple accounts - at least some should hash to different buckets
		accounts := []string{
			"GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N",
			"GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN",
			"GBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
			"GCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC",
		}
		keys := make(map[string]bool)
		for _, acc := range accounts {
			keys[service.buildTrustlineKey(acc)] = true
		}
		// With 4 accounts, we expect at least 2 different bucket keys
		assert.GreaterOrEqual(t, len(keys), 2, "expected different accounts to potentially hash to different buckets")
	})
}

func TestGetAccountTrustlines(t *testing.T) {
	ctx := context.Background()

	t.Run("empty account address returns error", func(t *testing.T) {
		mr, redisStore := setupTestRedis(t)
		defer mr.Close()

		service := &accountTokenService{
			redisStore:       redisStore,
			trustlinesPrefix: trustlinesKeyPrefix,
			contractsPrefix:  contractsKeyPrefix,
		}

		got, err := service.GetAccountTrustlines(ctx, "")
		assert.Error(t, err)
		assert.Nil(t, got)
	})

	t.Run("account with no trustlines returns empty", func(t *testing.T) {
		mr, redisStore := setupTestRedis(t)
		defer mr.Close()

		service := &accountTokenService{
			redisStore:       redisStore,
			trustlinesPrefix: trustlinesKeyPrefix,
			contractsPrefix:  contractsKeyPrefix,
		}

		got, err := service.GetAccountTrustlines(ctx, "GBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB")
		assert.NoError(t, err)
		assert.Nil(t, got)
	})

	t.Run("account with single trustline", func(t *testing.T) {
		mr, redisStore := setupTestRedis(t)
		defer mr.Close()

		mockAssetModel := wbdata.NewTrustlineAssetModelMock(t)
		service := &accountTokenService{
			redisStore:          redisStore,
			trustlineAssetModel: mockAssetModel,
			trustlinesPrefix:    trustlinesKeyPrefix,
			contractsPrefix:     contractsKeyPrefix,
		}

		accountAddress := "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"
		key := service.buildTrustlineKey(accountAddress)

		// Insert trustline ID in varint binary format
		mr.HSet(key, accountAddress, testEncodeAssetIDs([]int64{1}))

		// Mock BatchGetByIDs to return the asset
		mockAssetModel.On("BatchGetByIDs", ctx, []int64{1}).Return([]*wbdata.TrustlineAsset{
			{ID: 1, Code: "USDC", Issuer: "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN"},
		}, nil)

		got, err := service.GetAccountTrustlines(ctx, accountAddress)
		assert.NoError(t, err)
		assert.Equal(t, []*wbdata.TrustlineAsset{{ID: 1, Code: "USDC", Issuer: "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN"}}, got)
	})

	t.Run("account with multiple trustlines", func(t *testing.T) {
		mr, redisStore := setupTestRedis(t)
		defer mr.Close()

		mockAssetModel := wbdata.NewTrustlineAssetModelMock(t)
		service := &accountTokenService{
			redisStore:          redisStore,
			trustlineAssetModel: mockAssetModel,
			trustlinesPrefix:    trustlinesKeyPrefix,
			contractsPrefix:     contractsKeyPrefix,
		}

		accountAddress := "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"
		key := service.buildTrustlineKey(accountAddress)

		// Insert multiple trustline IDs in varint binary format
		mr.HSet(key, accountAddress, testEncodeAssetIDs([]int64{1, 2, 3}))

		// Mock BatchGetByIDs to return multiple assets
		mockAssetModel.On("BatchGetByIDs", ctx, []int64{1, 2, 3}).Return([]*wbdata.TrustlineAsset{
			{ID: 1, Code: "USDC", Issuer: "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN"},
			{ID: 2, Code: "EUROC", Issuer: "GA7FCCMTTSUIC37PODEL6EOOSPDRILP6OQI5FWCWDDVDBLJV72W6RINZ"},
			{ID: 3, Code: "BTC", Issuer: "GBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"},
		}, nil)

		got, err := service.GetAccountTrustlines(ctx, accountAddress)
		assert.NoError(t, err)
		assert.Len(t, got, 3)
		assert.Contains(t, got, &wbdata.TrustlineAsset{ID: 1, Code: "USDC", Issuer: "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN"})
		assert.Contains(t, got, &wbdata.TrustlineAsset{ID: 2, Code: "EUROC", Issuer: "GA7FCCMTTSUIC37PODEL6EOOSPDRILP6OQI5FWCWDDVDBLJV72W6RINZ"})
		assert.Contains(t, got, &wbdata.TrustlineAsset{ID: 3, Code: "BTC", Issuer: "GBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"})
	})

	t.Run("handles BatchGetByIDs error", func(t *testing.T) {
		mr, redisStore := setupTestRedis(t)
		defer mr.Close()

		mockAssetModel := wbdata.NewTrustlineAssetModelMock(t)
		service := &accountTokenService{
			redisStore:          redisStore,
			trustlineAssetModel: mockAssetModel,
			trustlinesPrefix:    trustlinesKeyPrefix,
			contractsPrefix:     contractsKeyPrefix,
		}

		accountAddress := "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"
		key := service.buildTrustlineKey(accountAddress)

		// Insert trustline ID in varint binary format
		mr.HSet(key, accountAddress, testEncodeAssetIDs([]int64{1}))

		// Mock returns error
		mockAssetModel.On("BatchGetByIDs", ctx, []int64{1}).Return(nil, assert.AnError)

		got, err := service.GetAccountTrustlines(ctx, accountAddress)
		assert.Error(t, err)
		assert.Nil(t, got)
		assert.Contains(t, err.Error(), "resolving asset IDs")
	})
}

func TestGetAccountContracts(t *testing.T) {
	ctx := context.Background()

	t.Run("empty account address returns error", func(t *testing.T) {
		mr, redisStore := setupTestRedis(t)
		defer mr.Close()

		service := &accountTokenService{
			redisStore:       redisStore,
			trustlinesPrefix: trustlinesKeyPrefix,
			contractsPrefix:  contractsKeyPrefix,
		}

		got, err := service.GetAccountContracts(ctx, "")
		assert.Error(t, err)
		assert.Nil(t, got)
	})

	t.Run("account with contracts", func(t *testing.T) {
		mr, redisStore := setupTestRedis(t)
		defer mr.Close()

		service := &accountTokenService{
			redisStore:       redisStore,
			trustlinesPrefix: trustlinesKeyPrefix,
			contractsPrefix:  contractsKeyPrefix,
		}

		// Insert contracts directly into Redis
		key := contractsKeyPrefix + "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"
		_, err := mr.SetAdd(key, "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4", "CBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB")
		require.NoError(t, err)

		got, err := service.GetAccountContracts(ctx, "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N")
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{"CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4", "CBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"}, got)
	})

	t.Run("account with no contracts returns empty", func(t *testing.T) {
		mr, redisStore := setupTestRedis(t)
		defer mr.Close()

		service := &accountTokenService{
			redisStore:       redisStore,
			trustlinesPrefix: trustlinesKeyPrefix,
			contractsPrefix:  contractsKeyPrefix,
		}

		got, err := service.GetAccountContracts(ctx, "GBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB")
		assert.NoError(t, err)
		assert.Nil(t, got)
	})
}

func TestCollectAccountTokensFromCheckpoint(t *testing.T) {
	ctx := context.Background()
	service := &accountTokenService{
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
		// TrustlinesByAccountAddress contains asset strings in "CODE:ISSUER" format
		expectedAsset := "USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN"
		assert.Contains(t, cpData.TrustlinesByAccountAddress["GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"], expectedAsset)
		assert.Empty(t, cpData.ContractsByHolderAddress)
		assert.Empty(t, cpData.ContractTypesByContractID)
		assert.Empty(t, cpData.ContractIDsByWasmHash)
		assert.Empty(t, cpData.ContractTypesByWasmHash)
	})

	t.Run("reads contract balance entries", func(t *testing.T) {
		// Create a contract balance entry with ScVec key type
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
		holderContracts := cpData.ContractsByHolderAddress["GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"]
		assert.Equal(t, 1, len(holderContracts))
		// Contract address should start with C
		for _, contractAddr := range holderContracts {
			assert.Equal(t, "C", string(contractAddr[0]))
		}
		assert.Empty(t, cpData.ContractTypesByContractID)
		assert.Empty(t, cpData.ContractIDsByWasmHash)
		assert.Empty(t, cpData.ContractTypesByWasmHash)
	})

	t.Run("reads contract code entries", func(t *testing.T) {
		// Create a CONTRACT_CODE ledger entry
		wasmHash := xdr.Hash{100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131}
		wasmCode := []byte{0x00, 0x61, 0x73, 0x6D, 0x01, 0x00, 0x00, 0x00} // Minimal WASM header

		contractCodeEntry := xdr.LedgerEntry{
			Data: xdr.LedgerEntryData{
				Type: xdr.LedgerEntryTypeContractCode,
				ContractCode: &xdr.ContractCodeEntry{
					Hash: wasmHash,
					Code: wasmCode,
				},
			},
		}

		changes := []ingest.Change{
			{
				Type: xdr.LedgerEntryTypeContractCode,
				Post: &contractCodeEntry,
			},
		}

		reader := &mockChangeReader{changes: changes}
		cpData, err := service.collectAccountTokensFromCheckpoint(ctx, reader)

		require.NoError(t, err)
		assert.Empty(t, cpData.TrustlinesByAccountAddress)
		assert.Empty(t, cpData.ContractsByHolderAddress)
		assert.Empty(t, cpData.ContractTypesByContractID)
		assert.Empty(t, cpData.ContractIDsByWasmHash)
		// ContractTypesByWasmHash should contain the validated contract type
		assert.Len(t, cpData.ContractTypesByWasmHash, 1)
		assert.Equal(t, types.ContractTypeUnknown, cpData.ContractTypesByWasmHash[wasmHash])
	})

	t.Run("reads valid SAC contract instance entries", func(t *testing.T) {
		// Create a valid SAC (Stellar Asset Contract) using the sac.AssetToContractData helper
		issuer := "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN"
		asset := xdr.MustNewCreditAsset("USDC", issuer)

		// Get the proper contract ID for this asset
		contractID, err := asset.ContractID(service.networkPassphrase)
		require.NoError(t, err)

		// Create valid SAC contract data with proper storage structure
		contractData, err := sac.AssetToContractData(false, "USDC", issuer, contractID)
		require.NoError(t, err)

		// Wrap in a LedgerEntry
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

		// Valid SAC should be immediately classified as ContractTypeSAC
		assert.Len(t, cpData.ContractTypesByContractID, 1)

		// Get the contract address from the contract ID
		contractAddress, err := strkey.Encode(strkey.VersionByteContract, contractID[:])
		require.NoError(t, err)

		// Verify the SAC was properly detected and classified
		assert.Contains(t, cpData.ContractTypesByContractID, contractAddress)
		assert.Equal(t, types.ContractTypeSAC, cpData.ContractTypesByContractID[contractAddress])

		// SAC contracts should NOT be added to contractsByWasm (they don't need WASM validation)
		assert.Empty(t, cpData.ContractIDsByWasmHash)
		assert.Empty(t, cpData.ContractTypesByWasmHash)
	})

	t.Run("reads non-SAC WASM contract instance entries", func(t *testing.T) {
		// Create a WASM-based contract instance (non-SAC)
		contractHash := xdr.Hash{10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160, 170, 180, 190, 200, 210, 220, 230, 240, 250, 26, 27, 28, 29, 30, 31, 32}
		contractID := xdr.ContractId(contractHash)

		// Create a WASM hash for the contract bytecode
		wasmHash := xdr.Hash{5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90, 95, 100, 105, 110, 115, 120, 125, 130, 135, 140, 145, 150, 155, 160}

		// Create a WASM contract instance
		contractInstance := xdr.ScContractInstance{
			Executable: xdr.ContractExecutable{
				Type:     xdr.ContractExecutableTypeContractExecutableWasm,
				WasmHash: &wasmHash,
			},
			Storage: nil,
		}

		instanceKey := xdr.ScVal{
			Type: xdr.ScValTypeScvLedgerKeyContractInstance,
		}

		contractDataEntry := xdr.LedgerEntry{
			Data: xdr.LedgerEntryData{
				Type: xdr.LedgerEntryTypeContractData,
				ContractData: &xdr.ContractDataEntry{
					Contract: xdr.ScAddress{
						Type:       xdr.ScAddressTypeScAddressTypeContract,
						ContractId: &contractID,
					},
					Key:        instanceKey,
					Durability: xdr.ContractDataDurabilityPersistent,
					Val: xdr.ScVal{
						Type:     xdr.ScValTypeScvContractInstance,
						Instance: &contractInstance,
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
		assert.Empty(t, cpData.ContractsByHolderAddress)
		// Non-SAC contracts should NOT be classified yet (happens in enrichContractTypes)
		assert.Empty(t, cpData.ContractTypesByContractID)
		// Non-SAC contracts should be added to contractsByWasm for validation
		assert.Len(t, cpData.ContractIDsByWasmHash, 1)
		contractAddresses, found := cpData.ContractIDsByWasmHash[wasmHash]
		require.True(t, found, "WASM hash should be in map")
		assert.Len(t, contractAddresses, 1)

		// Get the contract address from the contract ID
		contractAddress, err := strkey.Encode(strkey.VersionByteContract, contractID[:])
		require.NoError(t, err)
		assert.Equal(t, contractAddress, contractAddresses[0])
		assert.Empty(t, cpData.ContractTypesByWasmHash)
	})

	t.Run("skips contract instances with nil WASM hash", func(t *testing.T) {
		// Create a WASM-based contract instance but with nil WASM hash
		// This is an edge case that should be handled gracefully
		contractHash := xdr.Hash{11, 22, 33, 44, 55, 66, 77, 88, 99, 111, 122, 133, 144, 155, 166, 177, 188, 199, 210, 221, 232, 243, 254, 9, 18, 27, 36, 45, 54, 63, 72, 81}
		contractID := xdr.ContractId(contractHash)

		// Create a WASM contract instance with nil WASM hash
		contractInstance := xdr.ScContractInstance{
			Executable: xdr.ContractExecutable{
				Type:     xdr.ContractExecutableTypeContractExecutableWasm,
				WasmHash: nil, // nil hash - should be skipped
			},
			Storage: nil,
		}

		instanceKey := xdr.ScVal{
			Type: xdr.ScValTypeScvLedgerKeyContractInstance,
		}

		contractDataEntry := xdr.LedgerEntry{
			Data: xdr.LedgerEntryData{
				Type: xdr.LedgerEntryTypeContractData,
				ContractData: &xdr.ContractDataEntry{
					Contract: xdr.ScAddress{
						Type:       xdr.ScAddressTypeScAddressTypeContract,
						ContractId: &contractID,
					},
					Key:        instanceKey,
					Durability: xdr.ContractDataDurabilityPersistent,
					Val: xdr.ScVal{
						Type:     xdr.ScValTypeScvContractInstance,
						Instance: &contractInstance,
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

		// Should not error, just skip the entry
		require.NoError(t, err)
		assert.Empty(t, cpData.TrustlinesByAccountAddress)
		assert.Empty(t, cpData.ContractsByHolderAddress)
		assert.Empty(t, cpData.ContractTypesByContractID)
		// Contract with nil WASM hash should NOT be added to contractsByWasm
		assert.Empty(t, cpData.ContractIDsByWasmHash)
		assert.Empty(t, cpData.ContractTypesByWasmHash)
	})

	t.Run("handles mixed SAC and WASM contract instances", func(t *testing.T) {
		// Test processing both a valid SAC and a WASM contract in the same batch
		// This verifies that both types are processed correctly without interfering with each other

		// Create a valid SAC
		sacIssuer := "GA7FCCMTTSUIC37PODEL6EOOSPDRILP6OQI5FWCWDDVDBLJV72W6RINZ"
		sacAsset := xdr.MustNewCreditAsset("EUROC", sacIssuer)
		sacContractID, err := sacAsset.ContractID(service.networkPassphrase)
		require.NoError(t, err)

		sacContractData, err := sac.AssetToContractData(false, "EUROC", sacIssuer, sacContractID)
		require.NoError(t, err)

		sacEntry := xdr.LedgerEntry{
			Data: sacContractData,
		}

		// Create a WASM contract
		wasmContractHash := xdr.Hash{200, 201, 202, 203, 204, 205, 206, 207, 208, 209, 210, 211, 212, 213, 214, 215, 216, 217, 218, 219, 220, 221, 222, 223, 224, 225, 226, 227, 228, 229, 230, 231}
		wasmID := xdr.ContractId(wasmContractHash)
		wasmBytecodeHash := xdr.Hash{50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81}
		wasmInstance := xdr.ScContractInstance{
			Executable: xdr.ContractExecutable{
				Type:     xdr.ContractExecutableTypeContractExecutableWasm,
				WasmHash: &wasmBytecodeHash,
			},
			Storage: nil,
		}

		instanceKey := xdr.ScVal{
			Type: xdr.ScValTypeScvLedgerKeyContractInstance,
		}

		wasmEntry := xdr.LedgerEntry{
			Data: xdr.LedgerEntryData{
				Type: xdr.LedgerEntryTypeContractData,
				ContractData: &xdr.ContractDataEntry{
					Contract: xdr.ScAddress{
						Type:       xdr.ScAddressTypeScAddressTypeContract,
						ContractId: &wasmID,
					},
					Key:        instanceKey,
					Durability: xdr.ContractDataDurabilityPersistent,
					Val: xdr.ScVal{
						Type:     xdr.ScValTypeScvContractInstance,
						Instance: &wasmInstance,
					},
				},
			},
		}

		changes := []ingest.Change{
			{
				Type: xdr.LedgerEntryTypeContractData,
				Post: &sacEntry,
			},
			{
				Type: xdr.LedgerEntryTypeContractData,
				Post: &wasmEntry,
			},
		}

		reader := &mockChangeReader{changes: changes}
		cpData, err := service.collectAccountTokensFromCheckpoint(ctx, reader)

		require.NoError(t, err)
		assert.Empty(t, cpData.TrustlinesByAccountAddress)
		assert.Empty(t, cpData.ContractsByHolderAddress)

		// SAC should be in contractTypes
		assert.Len(t, cpData.ContractTypesByContractID, 1)
		sacAddress, err := strkey.Encode(strkey.VersionByteContract, sacContractID[:])
		require.NoError(t, err)
		assert.Contains(t, cpData.ContractTypesByContractID, sacAddress)
		assert.Equal(t, types.ContractTypeSAC, cpData.ContractTypesByContractID[sacAddress])

		// WASM contract should be in contractsByWasm
		assert.Len(t, cpData.ContractIDsByWasmHash, 1)
		wasmAddresses, found := cpData.ContractIDsByWasmHash[wasmBytecodeHash]
		require.True(t, found, "WASM hash should be in map")
		assert.Len(t, wasmAddresses, 1)
		wasmAddress, err := strkey.Encode(strkey.VersionByteContract, wasmID[:])
		require.NoError(t, err)
		assert.Equal(t, wasmAddress, wasmAddresses[0])

		// Ensure they're different contracts
		assert.NotEqual(t, sacAddress, wasmAddress)
		assert.Empty(t, cpData.ContractTypesByWasmHash)
	})

	t.Run("handles context cancellation", func(t *testing.T) {
		cancelledCtx, cancel := context.WithCancel(ctx)
		cancel() // Cancel immediately

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

	t.Run("handles EOF correctly", func(t *testing.T) {
		reader := &mockChangeReader{
			changes: []ingest.Change{}, // Empty, will return EOF immediately
		}

		cpData, err := service.collectAccountTokensFromCheckpoint(ctx, reader)

		require.NoError(t, err)
		assert.Empty(t, cpData.TrustlinesByAccountAddress)
		assert.Empty(t, cpData.ContractsByHolderAddress)
		assert.Empty(t, cpData.ContractTypesByContractID)
		assert.Empty(t, cpData.ContractIDsByWasmHash)
		assert.Empty(t, cpData.ContractTypesByWasmHash)
	})
}

func TestProcessTokenChanges(t *testing.T) {
	ctx := context.Background()

	t.Run("empty changes returns no error", func(t *testing.T) {
		mr, redisStore := setupTestRedis(t)
		defer mr.Close()

		service := &accountTokenService{
			redisStore:       redisStore,
			trustlinesPrefix: trustlinesKeyPrefix,
			contractsPrefix:  contractsKeyPrefix,
		}

		err := service.ProcessTokenChanges(ctx, []types.TrustlineChange{}, []types.ContractChange{})
		assert.NoError(t, err)
	})

	t.Run("add contract stores full address directly", func(t *testing.T) {
		mr, redisStore := setupTestRedis(t)
		defer mr.Close()

		service := &accountTokenService{
			redisStore:       redisStore,
			trustlinesPrefix: trustlinesKeyPrefix,
			contractsPrefix:  contractsKeyPrefix,
		}

		// No trustline changes, so BatchGetOrInsert is not called (early return for empty assets)
		err := service.ProcessTokenChanges(ctx, []types.TrustlineChange{}, []types.ContractChange{
			{
				AccountID:    "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N",
				ContractID:   "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4",
				ContractType: types.ContractTypeSAC,
			},
		})
		assert.NoError(t, err)

		// Verify contract is stored directly
		contracts, err := service.GetAccountContracts(ctx, "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N")
		assert.NoError(t, err)
		assert.Contains(t, contracts, "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4")

		// Also verify in raw Redis (no ID conversion)
		members, err := mr.SMembers(contractsKeyPrefix + "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N")
		assert.NoError(t, err)
		assert.Contains(t, members, "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4")
	})

	t.Run("skip empty contract ID", func(t *testing.T) {
		mr, redisStore := setupTestRedis(t)
		defer mr.Close()

		service := &accountTokenService{
			redisStore:       redisStore,
			trustlinesPrefix: trustlinesKeyPrefix,
			contractsPrefix:  contractsKeyPrefix,
		}

		// No trustline changes, so BatchGetOrInsert is not called (early return for empty assets)
		err := service.ProcessTokenChanges(ctx, []types.TrustlineChange{}, []types.ContractChange{
			{
				AccountID:    "GAFOZCZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N",
				ContractID:   "",
				ContractType: types.ContractTypeSAC,
			},
		})
		assert.NoError(t, err)

		// No key should be created for empty contract ID
		key := contractsKeyPrefix + "GAFOZCZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"
		assert.False(t, mr.Exists(key))
	})

	t.Run("skip empty asset in trustline", func(t *testing.T) {
		mr, redisStore := setupTestRedis(t)
		defer mr.Close()

		service := &accountTokenService{
			redisStore:       redisStore,
			trustlinesPrefix: trustlinesKeyPrefix,
			contractsPrefix:  contractsKeyPrefix,
		}

		// Empty asset is skipped during parsing, so BatchGetOrInsert is not called (early return for empty assets)
		err := service.ProcessTokenChanges(ctx, []types.TrustlineChange{
			{
				AccountID: "GADOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N",
				Asset:     "",
				Operation: types.TrustlineOpAdd,
			},
		}, []types.ContractChange{})
		assert.NoError(t, err)

		// No key should be created for empty asset
		key := trustlinesKeyPrefix + "GADOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"
		assert.False(t, mr.Exists(key))
	})

	t.Run("add trustline to new account", func(t *testing.T) {
		mr, redisStore := setupTestRedis(t)
		defer mr.Close()

		mockAssetModel := wbdata.NewTrustlineAssetModelMock(t)
		service := &accountTokenService{
			redisStore:          redisStore,
			trustlineAssetModel: mockAssetModel,
			trustlinesPrefix:    trustlinesKeyPrefix,
			contractsPrefix:     contractsKeyPrefix,
		}

		accountAddress := "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"

		// Mock BatchGetOrInsert to return asset ID
		mockAssetModel.On("BatchGetOrInsert", ctx, []wbdata.TrustlineAsset{
			{Code: "USDC", Issuer: "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN"},
		}).Return(map[string]int64{"USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN": 1}, nil)

		err := service.ProcessTokenChanges(ctx, []types.TrustlineChange{
			{
				AccountID:   accountAddress,
				Asset:       "USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN",
				Operation:   types.TrustlineOpAdd,
				OperationID: 1,
			},
		}, []types.ContractChange{})
		assert.NoError(t, err)

		// Verify trustline is stored in Redis in varint format
		key := service.buildTrustlineKey(accountAddress)
		val := mr.HGet(key, accountAddress)
		decodedIDs := decodeAssetIDs([]byte(val))
		assert.Equal(t, []int64{1}, decodedIDs)
	})

	t.Run("add trustline to existing account", func(t *testing.T) {
		mr, redisStore := setupTestRedis(t)
		defer mr.Close()

		mockAssetModel := wbdata.NewTrustlineAssetModelMock(t)
		service := &accountTokenService{
			redisStore:          redisStore,
			trustlineAssetModel: mockAssetModel,
			trustlinesPrefix:    trustlinesKeyPrefix,
			contractsPrefix:     contractsKeyPrefix,
		}

		accountAddress := "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"
		key := service.buildTrustlineKey(accountAddress)

		// Pre-populate with existing trustline in varint format
		mr.HSet(key, accountAddress, testEncodeAssetIDs([]int64{1}))

		// Mock BatchGetOrInsert for the new asset
		mockAssetModel.On("BatchGetOrInsert", ctx, []wbdata.TrustlineAsset{
			{Code: "EUROC", Issuer: "GA7FCCMTTSUIC37PODEL6EOOSPDRILP6OQI5FWCWDDVDBLJV72W6RINZ"},
		}).Return(map[string]int64{"EUROC:GA7FCCMTTSUIC37PODEL6EOOSPDRILP6OQI5FWCWDDVDBLJV72W6RINZ": 2}, nil)

		err := service.ProcessTokenChanges(ctx, []types.TrustlineChange{
			{
				AccountID:   accountAddress,
				Asset:       "EUROC:GA7FCCMTTSUIC37PODEL6EOOSPDRILP6OQI5FWCWDDVDBLJV72W6RINZ",
				Operation:   types.TrustlineOpAdd,
				OperationID: 1,
			},
		}, []types.ContractChange{})
		assert.NoError(t, err)

		// Verify both trustlines exist by decoding the varint data
		val := mr.HGet(key, accountAddress)
		decodedIDs := decodeAssetIDs([]byte(val))
		assert.Len(t, decodedIDs, 2)
		assert.Contains(t, decodedIDs, int64(1))
		assert.Contains(t, decodedIDs, int64(2))
	})

	t.Run("remove trustline from account with multiple", func(t *testing.T) {
		mr, redisStore := setupTestRedis(t)
		defer mr.Close()

		mockAssetModel := wbdata.NewTrustlineAssetModelMock(t)
		service := &accountTokenService{
			redisStore:          redisStore,
			trustlineAssetModel: mockAssetModel,
			trustlinesPrefix:    trustlinesKeyPrefix,
			contractsPrefix:     contractsKeyPrefix,
		}

		accountAddress := "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"
		key := service.buildTrustlineKey(accountAddress)

		// Pre-populate with multiple trustlines in varint format
		mr.HSet(key, accountAddress, testEncodeAssetIDs([]int64{1, 2}))

		// Mock BatchGetOrInsert for the asset being removed
		mockAssetModel.On("BatchGetOrInsert", ctx, []wbdata.TrustlineAsset{
			{Code: "USDC", Issuer: "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN"},
		}).Return(map[string]int64{"USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN": 1}, nil)

		err := service.ProcessTokenChanges(ctx, []types.TrustlineChange{
			{
				AccountID:   accountAddress,
				Asset:       "USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN",
				Operation:   types.TrustlineOpRemove,
				OperationID: 1,
			},
		}, []types.ContractChange{})
		assert.NoError(t, err)

		// Verify only ID 2 remains
		val := mr.HGet(key, accountAddress)
		decodedIDs := decodeAssetIDs([]byte(val))
		assert.Equal(t, []int64{2}, decodedIDs)
	})

	t.Run("remove last trustline deletes field", func(t *testing.T) {
		mr, redisStore := setupTestRedis(t)
		defer mr.Close()

		mockAssetModel := wbdata.NewTrustlineAssetModelMock(t)
		service := &accountTokenService{
			redisStore:          redisStore,
			trustlineAssetModel: mockAssetModel,
			trustlinesPrefix:    trustlinesKeyPrefix,
			contractsPrefix:     contractsKeyPrefix,
		}

		accountAddress := "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"
		key := service.buildTrustlineKey(accountAddress)

		// Pre-populate with single trustline in varint format
		mr.HSet(key, accountAddress, testEncodeAssetIDs([]int64{1}))

		// Mock BatchGetOrInsert for the asset being removed
		mockAssetModel.On("BatchGetOrInsert", ctx, []wbdata.TrustlineAsset{
			{Code: "USDC", Issuer: "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN"},
		}).Return(map[string]int64{"USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN": 1}, nil)

		err := service.ProcessTokenChanges(ctx, []types.TrustlineChange{
			{
				AccountID:   accountAddress,
				Asset:       "USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN",
				Operation:   types.TrustlineOpRemove,
				OperationID: 1,
			},
		}, []types.ContractChange{})
		assert.NoError(t, err)

		// Verify the field was deleted (HGet returns empty string for non-existent field)
		val := mr.HGet(key, accountAddress)
		assert.Empty(t, val)
	})

	t.Run("multiple changes sorted by operation ID", func(t *testing.T) {
		mr, redisStore := setupTestRedis(t)
		defer mr.Close()

		mockAssetModel := wbdata.NewTrustlineAssetModelMock(t)
		service := &accountTokenService{
			redisStore:          redisStore,
			trustlineAssetModel: mockAssetModel,
			trustlinesPrefix:    trustlinesKeyPrefix,
			contractsPrefix:     contractsKeyPrefix,
		}

		accountAddress := "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"

		// Mock BatchGetOrInsert - same asset appears twice so it's deduplicated
		mockAssetModel.On("BatchGetOrInsert", ctx, []wbdata.TrustlineAsset{
			{Code: "USDC", Issuer: "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN"},
		}).Return(map[string]int64{"USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN": 1}, nil)

		// Send changes out of order - remove then add (by opID)
		// Op 2: Remove, Op 1: Add - should be processed as Add first, then Remove
		err := service.ProcessTokenChanges(ctx, []types.TrustlineChange{
			{
				AccountID:   accountAddress,
				Asset:       "USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN",
				Operation:   types.TrustlineOpRemove,
				OperationID: 2,
			},
			{
				AccountID:   accountAddress,
				Asset:       "USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN",
				Operation:   types.TrustlineOpAdd,
				OperationID: 1,
			},
		}, []types.ContractChange{})
		assert.NoError(t, err)

		// After Add (op 1) then Remove (op 2), no trustlines should remain
		key := service.buildTrustlineKey(accountAddress)
		val := mr.HGet(key, accountAddress)
		assert.Empty(t, val)
	})

	t.Run("handles BatchGetOrInsert error", func(t *testing.T) {
		mr, redisStore := setupTestRedis(t)
		defer mr.Close()

		mockAssetModel := wbdata.NewTrustlineAssetModelMock(t)
		service := &accountTokenService{
			redisStore:          redisStore,
			trustlineAssetModel: mockAssetModel,
			trustlinesPrefix:    trustlinesKeyPrefix,
			contractsPrefix:     contractsKeyPrefix,
		}

		// Mock BatchGetOrInsert to return error
		mockAssetModel.On("BatchGetOrInsert", ctx, []wbdata.TrustlineAsset{
			{Code: "USDC", Issuer: "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN"},
		}).Return(nil, assert.AnError)

		err := service.ProcessTokenChanges(ctx, []types.TrustlineChange{
			{
				AccountID:   "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N",
				Asset:       "USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN",
				Operation:   types.TrustlineOpAdd,
				OperationID: 1,
			},
		}, []types.ContractChange{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "getting or creating trustline asset IDs")
	})

	t.Run("handles invalid asset format", func(t *testing.T) {
		mr, redisStore := setupTestRedis(t)
		defer mr.Close()

		service := &accountTokenService{
			redisStore:       redisStore,
			trustlinesPrefix: trustlinesKeyPrefix,
			contractsPrefix:  contractsKeyPrefix,
		}

		accountAddress := "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"

		// Invalid asset is skipped during parsing, so BatchGetOrInsert is not called (early return for empty assets)
		// Asset without colon should be skipped (not cause error)
		err := service.ProcessTokenChanges(ctx, []types.TrustlineChange{
			{
				AccountID:   accountAddress,
				Asset:       "INVALIDASSET", // No colon
				Operation:   types.TrustlineOpAdd,
				OperationID: 1,
			},
		}, []types.ContractChange{})
		assert.NoError(t, err)

		// No trustline should be stored
		key := service.buildTrustlineKey(accountAddress)
		val := mr.HGet(key, accountAddress)
		assert.Empty(t, val)
	})
}
