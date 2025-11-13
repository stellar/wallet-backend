package services

import (
	"context"
	"io"
	"strconv"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/ingest/sac"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/store"
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
		trustlinesPrefix:   trustlinesKeyPrefix,
		contractsPrefix:    contractsKeyPrefix,
		contractTypePrefix: contractTypePrefix,
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

func TestExtractContractID(t *testing.T) {
	service := &accountTokenService{
		trustlinesPrefix:   trustlinesKeyPrefix,
		contractsPrefix:    contractsKeyPrefix,
		contractTypePrefix: contractTypePrefix,
	}

	hash := xdr.Hash{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}
	contractID := xdr.ContractId(hash)
	contractAddress, err := strkey.Encode(strkey.VersionByteContract, contractID[:])
	require.NoError(t, err)

	tests := []struct {
		name    string
		entry   xdr.ContractDataEntry
		want    string
		wantErr bool
	}{
		{
			name: "valid contract ID",
			entry: xdr.ContractDataEntry{
				Contract: xdr.ScAddress{
					Type:       xdr.ScAddressTypeScAddressTypeContract,
					ContractId: &contractID,
				},
			},
			want:    contractAddress,
			wantErr: false,
		},
		{
			name: "wrong address type - account instead of contract",
			entry: xdr.ContractDataEntry{
				Contract: xdr.ScAddress{
					Type:      xdr.ScAddressTypeScAddressTypeAccount,
					AccountId: ptrToAccountID("GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"),
				},
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "nil contract ID",
			entry: xdr.ContractDataEntry{
				Contract: xdr.ScAddress{
					Type:       xdr.ScAddressTypeScAddressTypeContract,
					ContractId: nil,
				},
			},
			want:    "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := service.extractContractID(tt.entry)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Empty(t, got)
			} else {
				assert.NoError(t, err)
				assert.NotEmpty(t, got)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestGetAccountTrustlines(t *testing.T) {
	ctx := context.Background()
	mr, redisStore := setupTestRedis(t)
	defer mr.Close()

	service := &accountTokenService{
		redisStore:         redisStore,
		trustlinesPrefix:   trustlinesKeyPrefix,
		contractsPrefix:    contractsKeyPrefix,
		contractTypePrefix: contractTypePrefix,
	}

	tests := []struct {
		name           string
		accountAddress string
		setupData      func()
		want           []string
		wantErr        bool
	}{
		{
			name:           "empty account address",
			accountAddress: "",
			setupData:      func() {},
			want:           nil,
			wantErr:        true,
		},
		{
			name:           "account with trustlines",
			accountAddress: "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N",
			setupData: func() {
				key := trustlinesKeyPrefix + "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"
				//nolint:errcheck // test setup
				_, _ = mr.SetAdd(key, "USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN")
				//nolint:errcheck // test setup
				_, _ = mr.SetAdd(key, "EUROC:GA7FCCMTTSUIC37PODEL6EOOSPDRILP6OQI5FWCWDDVDBLJV72W6RINZ")
			},
			want:    []string{"USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN", "EUROC:GA7FCCMTTSUIC37PODEL6EOOSPDRILP6OQI5FWCWDDVDBLJV72W6RINZ"},
			wantErr: false,
		},
		{
			name:           "account with no trustlines",
			accountAddress: "GBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
			setupData:      func() {},
			want:           []string{},
			wantErr:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mr.FlushAll()
			tt.setupData()

			got, err := service.GetAccountTrustlines(ctx, tt.accountAddress)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.ElementsMatch(t, tt.want, got)
			}
		})
	}
}

func TestGetAccountContracts(t *testing.T) {
	ctx := context.Background()
	mr, redisStore := setupTestRedis(t)
	defer mr.Close()

	service := &accountTokenService{
		redisStore:         redisStore,
		trustlinesPrefix:   trustlinesKeyPrefix,
		contractsPrefix:    contractsKeyPrefix,
		contractTypePrefix: contractTypePrefix,
	}

	tests := []struct {
		name           string
		accountAddress string
		setupData      func()
		want           []string
		wantErr        bool
	}{
		{
			name:           "empty account address",
			accountAddress: "",
			setupData:      func() {},
			want:           nil,
			wantErr:        true,
		},
		{
			name:           "account with contracts",
			accountAddress: "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N",
			setupData: func() {
				key := contractsKeyPrefix + "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"
				//nolint:errcheck // test setup
				_, _ = mr.SetAdd(key, "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4")
				//nolint:errcheck // test setup
				_, _ = mr.SetAdd(key, "CBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB")
			},
			want:    []string{"CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4", "CBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"},
			wantErr: false,
		},
		{
			name:           "account with no contracts",
			accountAddress: "GBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
			setupData:      func() {},
			want:           []string{},
			wantErr:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mr.FlushAll()
			tt.setupData()

			got, err := service.GetAccountContracts(ctx, tt.accountAddress)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.ElementsMatch(t, tt.want, got)
			}
		})
	}
}

func TestGetContractType(t *testing.T) {
	ctx := context.Background()
	mr, redisStore := setupTestRedis(t)
	defer mr.Close()

	service := &accountTokenService{
		redisStore:         redisStore,
		trustlinesPrefix:   trustlinesKeyPrefix,
		contractsPrefix:    contractsKeyPrefix,
		contractTypePrefix: contractTypePrefix,
	}

	tests := []struct {
		name       string
		contractID string
		setupData  func()
		want       types.ContractType
		wantErr    bool
	}{
		{
			name:       "empty contract ID",
			contractID: "",
			setupData:  func() {},
			want:       types.ContractTypeUnknown,
			wantErr:    true,
		},
		{
			name:       "SAC contract type",
			contractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4",
			setupData: func() {
				key := contractTypePrefix + "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4"
				err := mr.Set(key, string(types.ContractTypeSAC))
				require.NoError(t, err)
			},
			want:    types.ContractTypeSAC,
			wantErr: false,
		},
		{
			name:       "SEP41 contract type",
			contractID: "CBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
			setupData: func() {
				key := contractTypePrefix + "CBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
				err := mr.Set(key, string(types.ContractTypeSEP41))
				require.NoError(t, err)
			},
			want:    types.ContractTypeSEP41,
			wantErr: false,
		},
		{
			name:       "unknown contract type - not in cache",
			contractID: "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC",
			setupData:  func() {},
			want:       types.ContractTypeUnknown,
			wantErr:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mr.FlushAll()
			tt.setupData()

			got, err := service.GetContractType(ctx, tt.contractID)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestCollectAccountTokensFromCheckpoint(t *testing.T) {
	ctx := context.Background()
	service := &accountTokenService{
		networkPassphrase: "Test SDF Network ; September 2015",
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
		trustlines, contracts, contractTypes, contractsByWasm, err := service.collectAccountTokensFromCheckpoint(ctx, reader)

		require.NoError(t, err)
		assert.Len(t, trustlines, 1)
		assert.Contains(t, trustlines, "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N")
		assert.Contains(t, trustlines["GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"], "USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN")
		assert.Empty(t, contracts)
		assert.Empty(t, contractTypes)
		assert.Empty(t, contractsByWasm)
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
		trustlines, contracts, contractTypes, contractsByWasm, err := service.collectAccountTokensFromCheckpoint(ctx, reader)

		require.NoError(t, err)
		assert.Empty(t, trustlines)
		assert.Len(t, contracts, 1)
		assert.Contains(t, contracts, "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N")
		assert.Len(t, contracts["GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"], 1)
		// Contract address should start with C
		assert.Equal(t, "C", string(contracts["GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"][0][0]))
		assert.Empty(t, contractTypes)
		assert.Empty(t, contractsByWasm)
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
		trustlines, contracts, contractTypes, contractsByWasm, err := service.collectAccountTokensFromCheckpoint(ctx, reader)

		require.NoError(t, err)
		assert.Empty(t, trustlines)
		assert.Empty(t, contracts)

		// Valid SAC should be immediately classified as ContractTypeSAC
		assert.Len(t, contractTypes, 1)

		// Get the contract address from the contract ID
		contractAddress, err := strkey.Encode(strkey.VersionByteContract, contractID[:])
		require.NoError(t, err)

		// Verify the SAC was properly detected and classified
		assert.Contains(t, contractTypes, contractAddress)
		assert.Equal(t, types.ContractTypeSAC, contractTypes[contractAddress])

		// SAC contracts should NOT be added to contractsByWasm (they don't need WASM validation)
		assert.Empty(t, contractsByWasm)
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
		trustlines, contracts, contractTypes, contractsByWasm, err := service.collectAccountTokensFromCheckpoint(ctx, reader)

		require.NoError(t, err)
		assert.Empty(t, trustlines)
		assert.Empty(t, contracts)
		// Non-SAC contracts should NOT be classified yet (happens in enrichContractTypes)
		assert.Empty(t, contractTypes)
		// Non-SAC contracts should be added to contractsByWasm for batch validation
		assert.Len(t, contractsByWasm, 1)
		contractAddresses, found := contractsByWasm[wasmHash]
		require.True(t, found, "WASM hash should be in map")
		assert.Len(t, contractAddresses, 1)

		// Get the contract address from the contract ID
		contractAddress, err := strkey.Encode(strkey.VersionByteContract, contractID[:])
		require.NoError(t, err)
		assert.Equal(t, contractAddress, contractAddresses[0])
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
		trustlines, contracts, contractTypes, contractsByWasm, err := service.collectAccountTokensFromCheckpoint(ctx, reader)

		// Should not error, just skip the entry
		require.NoError(t, err)
		assert.Empty(t, trustlines)
		assert.Empty(t, contracts)
		assert.Empty(t, contractTypes)
		// Contract with nil WASM hash should NOT be added to contractsByWasm
		assert.Empty(t, contractsByWasm)
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
		trustlines, contracts, contractTypes, contractsByWasm, err := service.collectAccountTokensFromCheckpoint(ctx, reader)

		require.NoError(t, err)
		assert.Empty(t, trustlines)
		assert.Empty(t, contracts)

		// SAC should be in contractTypes
		assert.Len(t, contractTypes, 1)
		sacAddress, err := strkey.Encode(strkey.VersionByteContract, sacContractID[:])
		require.NoError(t, err)
		assert.Contains(t, contractTypes, sacAddress)
		assert.Equal(t, types.ContractTypeSAC, contractTypes[sacAddress])

		// WASM contract should be in contractsByWasm
		assert.Len(t, contractsByWasm, 1)
		wasmAddresses, found := contractsByWasm[wasmBytecodeHash]
		require.True(t, found, "WASM hash should be in map")
		assert.Len(t, wasmAddresses, 1)
		wasmAddress, err := strkey.Encode(strkey.VersionByteContract, wasmID[:])
		require.NoError(t, err)
		assert.Equal(t, wasmAddress, wasmAddresses[0])

		// Ensure they're different contracts
		assert.NotEqual(t, sacAddress, wasmAddress)
	})

	t.Run("handles context cancellation", func(t *testing.T) {
		cancelledCtx, cancel := context.WithCancel(ctx)
		cancel() // Cancel immediately

		reader := &mockChangeReader{
			changes: []ingest.Change{},
		}

		_, _, _, _, err := service.collectAccountTokensFromCheckpoint(cancelledCtx, reader)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cancelled")
	})

	t.Run("handles reader errors", func(t *testing.T) {
		reader := &mockChangeReader{
			err: assert.AnError,
		}

		_, _, _, _, err := service.collectAccountTokensFromCheckpoint(ctx, reader)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "reading checkpoint changes")
	})

	t.Run("handles EOF correctly", func(t *testing.T) {
		reader := &mockChangeReader{
			changes: []ingest.Change{}, // Empty, will return EOF immediately
		}

		trustlines, contracts, contractTypes, contractsByWasm, err := service.collectAccountTokensFromCheckpoint(ctx, reader)

		require.NoError(t, err)
		assert.Empty(t, trustlines)
		assert.Empty(t, contracts)
		assert.Empty(t, contractTypes)
		assert.Empty(t, contractsByWasm)
	})
}

func TestProcessTokenChanges(t *testing.T) {
	ctx := context.Background()
	mr, redisStore := setupTestRedis(t)
	defer mr.Close()

	service := &accountTokenService{
		redisStore:         redisStore,
		trustlinesPrefix:   trustlinesKeyPrefix,
		contractsPrefix:    contractsKeyPrefix,
		contractTypePrefix: contractTypePrefix,
	}

	tests := []struct {
		name             string
		trustlineChanges []types.TrustlineChange
		contractChanges  []types.ContractChange
		setupData        func()
		verifyData       func(t *testing.T)
		wantErr          bool
	}{
		{
			name:             "empty changes",
			trustlineChanges: []types.TrustlineChange{},
			contractChanges:  []types.ContractChange{},
			setupData:        func() {},
			verifyData:       func(t *testing.T) {},
			wantErr:          false,
		},
		{
			name: "add trustline",
			trustlineChanges: []types.TrustlineChange{
				{
					AccountID: "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N",
					Asset:     "USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN",
					Operation: types.TrustlineOpAdd,
				},
			},
			contractChanges: []types.ContractChange{},
			setupData:       func() {},
			verifyData: func(t *testing.T) {
				key := trustlinesKeyPrefix + "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"
				members, err := mr.SMembers(key)
				require.NoError(t, err)
				assert.Contains(t, members, "USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN")
			},
			wantErr: false,
		},
		{
			name: "remove trustline",
			trustlineChanges: []types.TrustlineChange{
				{
					AccountID: "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N",
					Asset:     "USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN",
					Operation: types.TrustlineOpRemove,
				},
			},
			contractChanges: []types.ContractChange{},
			setupData: func() {
				key := trustlinesKeyPrefix + "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"
				//nolint:errcheck // test setup
				_, _ = mr.SetAdd(key, "USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN")
			},
			verifyData: func(t *testing.T) {
				key := trustlinesKeyPrefix + "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"
				// Check if key exists before getting members (set may be deleted if empty)
				if mr.Exists(key) {
					members, err := mr.SMembers(key)
					require.NoError(t, err)
					assert.NotContains(t, members, "USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN")
				}
			},
			wantErr: false,
		},
		{
			name:             "add contract",
			trustlineChanges: []types.TrustlineChange{},
			contractChanges: []types.ContractChange{
				{
					AccountID:    "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N",
					ContractID:   "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4",
					ContractType: types.ContractTypeSAC,
				},
			},
			setupData: func() {},
			verifyData: func(t *testing.T) {
				contractKey := contractsKeyPrefix + "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"
				members, err := mr.SMembers(contractKey)
				require.NoError(t, err)
				assert.Contains(t, members, "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4")

				typeKey := contractTypePrefix + "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4"
				contractType, err := mr.Get(typeKey)
				assert.NoError(t, err)
				assert.Equal(t, string(types.ContractTypeSAC), contractType)
			},
			wantErr: false,
		},
		{
			name: "skip empty asset in trustline",
			trustlineChanges: []types.TrustlineChange{
				{
					AccountID: "GADOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N",
					Asset:     "",
					Operation: types.TrustlineOpAdd,
				},
			},
			contractChanges: []types.ContractChange{},
			setupData:       func() {},
			verifyData: func(t *testing.T) {
				// No key should be created for empty asset
				key := trustlinesKeyPrefix + "GADOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"
				assert.False(t, mr.Exists(key))
			},
			wantErr: false,
		},
		{
			name:             "skip empty contract ID",
			trustlineChanges: []types.TrustlineChange{},
			contractChanges: []types.ContractChange{
				{
					AccountID:    "GAFOZCZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N",
					ContractID:   "",
					ContractType: types.ContractTypeSAC,
				},
			},
			setupData: func() {},
			verifyData: func(t *testing.T) {
				// No key should be created for empty contract ID
				key := contractsKeyPrefix + "GAFOZCZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"
				assert.False(t, mr.Exists(key))
			},
			wantErr: false,
		},
		{
			name: "mixed operations",
			trustlineChanges: []types.TrustlineChange{
				{
					AccountID: "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N",
					Asset:     "USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN",
					Operation: types.TrustlineOpAdd,
				},
			},
			contractChanges: []types.ContractChange{
				{
					AccountID:    "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N",
					ContractID:   "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4",
					ContractType: types.ContractTypeSEP41,
				},
			},
			setupData: func() {},
			verifyData: func(t *testing.T) {
				trustlineKey := trustlinesKeyPrefix + "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"
				trustlines, err := mr.SMembers(trustlineKey)
				require.NoError(t, err)
				assert.Contains(t, trustlines, "USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN")

				contractKey := contractsKeyPrefix + "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"
				contracts, err := mr.SMembers(contractKey)
				require.NoError(t, err)
				assert.Contains(t, contracts, "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4")

				typeKey := contractTypePrefix + "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4"
				contractType, err := mr.Get(typeKey)
				assert.NoError(t, err)
				assert.Equal(t, string(types.ContractTypeSEP41), contractType)
			},
			wantErr: false,
		},
		{
			name: "invalid trustline operation type",
			trustlineChanges: []types.TrustlineChange{
				{
					AccountID: "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N",
					Asset:     "USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN",
					Operation: "INVALID_OP",
				},
			},
			contractChanges: []types.ContractChange{},
			setupData:       func() {},
			verifyData:      func(t *testing.T) {},
			wantErr:         true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mr.FlushAll()
			tt.setupData()

			err := service.ProcessTokenChanges(ctx, tt.trustlineChanges, tt.contractChanges)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				tt.verifyData(t)
			}
		})
	}
}

func TestStoreAccountTokensInRedis(t *testing.T) {
	ctx := context.Background()
	mr, redisStore := setupTestRedis(t)
	defer mr.Close()

	service := &accountTokenService{
		redisStore:         redisStore,
		trustlinesPrefix:   trustlinesKeyPrefix,
		contractsPrefix:    contractsKeyPrefix,
		contractTypePrefix: contractTypePrefix,
	}

	t.Run("stores trustlines in Redis", func(t *testing.T) {
		mr.FlushAll()

		trustlines := map[string][]string{
			"account1": {"USDC:issuer1", "EUROC:issuer2"},
			"account2": {"XLM:native"},
		}
		contracts := make(map[string][]string)
		contractTypes := make(map[string]types.ContractType)

		err := service.storeAccountTokensInRedis(ctx, trustlines, contracts, contractTypes)
		require.NoError(t, err)

		// Verify trustlines were stored
		members1, err := mr.SMembers(trustlinesKeyPrefix + "account1")
		require.NoError(t, err)
		assert.Contains(t, members1, "USDC:issuer1")
		assert.Contains(t, members1, "EUROC:issuer2")

		members2, err := mr.SMembers(trustlinesKeyPrefix + "account2")
		require.NoError(t, err)
		assert.Contains(t, members2, "XLM:native")
	})

	t.Run("stores contracts in Redis", func(t *testing.T) {
		mr.FlushAll()

		trustlines := make(map[string][]string)
		contracts := map[string][]string{
			"account1": {"contract1", "contract2"},
			"account2": {"contract3"},
		}
		contractTypes := make(map[string]types.ContractType)

		err := service.storeAccountTokensInRedis(ctx, trustlines, contracts, contractTypes)
		require.NoError(t, err)

		// Verify contracts were stored
		members1, err := mr.SMembers(contractsKeyPrefix + "account1")
		require.NoError(t, err)
		assert.Contains(t, members1, "contract1")
		assert.Contains(t, members1, "contract2")

		members2, err := mr.SMembers(contractsKeyPrefix + "account2")
		require.NoError(t, err)
		assert.Contains(t, members2, "contract3")
	})

	t.Run("stores contract types in Redis", func(t *testing.T) {
		mr.FlushAll()

		trustlines := make(map[string][]string)
		contracts := make(map[string][]string)
		contractTypes := map[string]types.ContractType{
			"contract1": types.ContractTypeSAC,
			"contract2": types.ContractTypeSEP41,
		}

		err := service.storeAccountTokensInRedis(ctx, trustlines, contracts, contractTypes)
		require.NoError(t, err)

		// Verify contract types were stored
		type1, err := mr.Get(contractTypePrefix + "contract1")
		assert.NoError(t, err)
		assert.Equal(t, string(types.ContractTypeSAC), type1)

		type2, err := mr.Get(contractTypePrefix + "contract2")
		assert.NoError(t, err)
		assert.Equal(t, string(types.ContractTypeSEP41), type2)
	})

	t.Run("stores all types together", func(t *testing.T) {
		mr.FlushAll()

		trustlines := map[string][]string{
			"account1": {"USDC:issuer1"},
		}
		contracts := map[string][]string{
			"account2": {"contract1"},
		}
		contractTypes := map[string]types.ContractType{
			"contract1": types.ContractTypeSAC,
		}

		err := service.storeAccountTokensInRedis(ctx, trustlines, contracts, contractTypes)
		require.NoError(t, err)

		// Verify all were stored
		trustlineMembers, err := mr.SMembers(trustlinesKeyPrefix + "account1")
		require.NoError(t, err)
		assert.Contains(t, trustlineMembers, "USDC:issuer1")

		contractMembers, err := mr.SMembers(contractsKeyPrefix + "account2")
		require.NoError(t, err)
		assert.Contains(t, contractMembers, "contract1")

		contractType, err := mr.Get(contractTypePrefix + "contract1")
		assert.NoError(t, err)
		assert.Equal(t, string(types.ContractTypeSAC), contractType)
	})
}
