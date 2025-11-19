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

func TestGetAccountTrustlines(t *testing.T) {
	ctx := context.Background()
	mr, redisStore := setupTestRedis(t)
	defer mr.Close()

	service := &accountTokenService{
		redisStore:       redisStore,
		trustlinesPrefix: trustlinesKeyPrefix,
		contractsPrefix:  contractsKeyPrefix,
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
		redisStore:       redisStore,
		trustlinesPrefix: trustlinesKeyPrefix,
		contractsPrefix:  contractsKeyPrefix,
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
		cpData, err := service.collectAccountTokensFromCheckpoint(ctx, reader)

		require.NoError(t, err)
		assert.Len(t, cpData.Trustlines, 1)
		assert.Contains(t, cpData.Trustlines, "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N")
		assert.Contains(t, cpData.Trustlines["GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"], "USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN")
		assert.Empty(t, cpData.Contracts)
		assert.Empty(t, cpData.ContractTypesByContractID)
		assert.Empty(t, cpData.ContractIDsByWasmHash)
		assert.Empty(t, cpData.ContractCodesByWasmHash)
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
		assert.Empty(t, cpData.Trustlines)
		assert.Len(t, cpData.Contracts, 1)
		assert.Contains(t, cpData.Contracts, "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N")
		assert.Len(t, cpData.Contracts["GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"], 1)
		// Contract address should start with C
		assert.Equal(t, "C", string(cpData.Contracts["GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"][0][0]))
		assert.Empty(t, cpData.ContractTypesByContractID)
		assert.Empty(t, cpData.ContractIDsByWasmHash)
		assert.Empty(t, cpData.ContractCodesByWasmHash)
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
		assert.Empty(t, cpData.Trustlines)
		assert.Empty(t, cpData.Contracts)
		assert.Empty(t, cpData.ContractTypesByContractID)
		assert.Empty(t, cpData.ContractIDsByWasmHash)

		// CONTRACT_CODE entries should be collected
		assert.Len(t, cpData.ContractCodesByWasmHash, 1)
		storedCode, found := cpData.ContractCodesByWasmHash[wasmHash]
		require.True(t, found, "WASM hash should be in contractCodesByWasmHash map")
		assert.Equal(t, wasmCode, storedCode)
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
		assert.Empty(t, cpData.Trustlines)
		assert.Empty(t, cpData.Contracts)

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
		assert.Empty(t, cpData.ContractCodesByWasmHash)
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
		assert.Empty(t, cpData.Trustlines)
		assert.Empty(t, cpData.Contracts)
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
		assert.Empty(t, cpData.ContractCodesByWasmHash)
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
		assert.Empty(t, cpData.Trustlines)
		assert.Empty(t, cpData.Contracts)
		assert.Empty(t, cpData.ContractTypesByContractID)
		// Contract with nil WASM hash should NOT be added to contractsByWasm
		assert.Empty(t, cpData.ContractIDsByWasmHash)
		assert.Empty(t, cpData.ContractCodesByWasmHash)
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
		assert.Empty(t, cpData.Trustlines)
		assert.Empty(t, cpData.Contracts)

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
		assert.Empty(t, cpData.ContractCodesByWasmHash)
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
		assert.Empty(t, cpData.Trustlines)
		assert.Empty(t, cpData.Contracts)
		assert.Empty(t, cpData.ContractTypesByContractID)
		assert.Empty(t, cpData.ContractIDsByWasmHash)
		assert.Empty(t, cpData.ContractCodesByWasmHash)
	})
}

func TestAddTrustlines(t *testing.T) {
	ctx := context.Background()
	mr, redisStore := setupTestRedis(t)
	defer mr.Close()

	service := &accountTokenService{
		redisStore:       redisStore,
		trustlinesPrefix: trustlinesKeyPrefix,
		contractsPrefix:  contractsKeyPrefix,
	}

	tests := []struct {
		name           string
		accountAddress string
		assets         []string
		wantErr        bool
		setupData      func()
		verify         func(t *testing.T)
	}{
		{
			name:           "empty account address",
			accountAddress: "",
			assets:         []string{"USDC:issuer1"},
			wantErr:        true,
			setupData:      func() {},
			verify:         func(t *testing.T) {},
		},
		{
			name:           "empty assets list - no-op",
			accountAddress: "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N",
			assets:         []string{},
			wantErr:        false,
			setupData:      func() {},
			verify: func(t *testing.T) {
				// No verification needed - the no-op behavior is confirmed by no error
			},
		},
		{
			name:           "successfully add single trustline",
			accountAddress: "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N",
			assets:         []string{"USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN"},
			wantErr:        false,
			setupData:      func() {},
			verify: func(t *testing.T) {
				members, err := mr.SMembers(trustlinesKeyPrefix + "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N")
				require.NoError(t, err)
				assert.Len(t, members, 1)
				assert.Contains(t, members, "USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN")
			},
		},
		{
			name:           "successfully add multiple trustlines",
			accountAddress: "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N",
			assets: []string{
				"USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN",
				"EUROC:GA7FCCMTTSUIC37PODEL6EOOSPDRILP6OQI5FWCWDDVDBLJV72W6RINZ",
			},
			wantErr:   false,
			setupData: func() {},
			verify: func(t *testing.T) {
				members, err := mr.SMembers(trustlinesKeyPrefix + "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N")
				require.NoError(t, err)
				assert.Len(t, members, 2)
				assert.Contains(t, members, "USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN")
				assert.Contains(t, members, "EUROC:GA7FCCMTTSUIC37PODEL6EOOSPDRILP6OQI5FWCWDDVDBLJV72W6RINZ")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mr.FlushAll()
			tt.setupData()

			err := service.AddTrustlines(ctx, tt.accountAddress, tt.assets)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				tt.verify(t)
			}
		})
	}
}

func TestAddContracts(t *testing.T) {
	ctx := context.Background()
	mr, redisStore := setupTestRedis(t)
	defer mr.Close()

	service := &accountTokenService{
		redisStore:       redisStore,
		trustlinesPrefix: trustlinesKeyPrefix,
		contractsPrefix:  contractsKeyPrefix,
	}

	tests := []struct {
		name           string
		accountAddress string
		contractIDs    []string
		wantErr        bool
		setupData      func()
		verify         func(t *testing.T)
	}{
		{
			name:           "empty account address",
			accountAddress: "",
			contractIDs:    []string{"CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4"},
			wantErr:        true,
			setupData:      func() {},
			verify:         func(t *testing.T) {},
		},
		{
			name:           "empty contracts list - no-op",
			accountAddress: "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N",
			contractIDs:    []string{},
			wantErr:        false,
			setupData:      func() {},
			verify: func(t *testing.T) {
				// No verification needed - the no-op behavior is confirmed by no error
			},
		},
		{
			name:           "successfully add single contract",
			accountAddress: "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N",
			contractIDs:    []string{"CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4"},
			wantErr:        false,
			setupData:      func() {},
			verify: func(t *testing.T) {
				members, err := mr.SMembers(contractsKeyPrefix + "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N")
				require.NoError(t, err)
				assert.Len(t, members, 1)
				assert.Contains(t, members, "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4")
			},
		},
		{
			name:           "successfully add multiple contracts",
			accountAddress: "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N",
			contractIDs: []string{
				"CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4",
				"CBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
			},
			wantErr:   false,
			setupData: func() {},
			verify: func(t *testing.T) {
				members, err := mr.SMembers(contractsKeyPrefix + "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N")
				require.NoError(t, err)
				assert.Len(t, members, 2)
				assert.Contains(t, members, "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4")
				assert.Contains(t, members, "CBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mr.FlushAll()
			tt.setupData()

			err := service.AddContracts(ctx, tt.accountAddress, tt.contractIDs)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				tt.verify(t)
			}
		})
	}
}

func TestStoreAccountTokensInRedis(t *testing.T) {
	ctx := context.Background()
	mr, redisStore := setupTestRedis(t)
	defer mr.Close()

	service := &accountTokenService{
		redisStore:       redisStore,
		trustlinesPrefix: trustlinesKeyPrefix,
		contractsPrefix:  contractsKeyPrefix,
	}

	t.Run("stores trustlines in Redis", func(t *testing.T) {
		mr.FlushAll()

		trustlines := map[string][]string{
			"account1": {"USDC:issuer1", "EUROC:issuer2"},
			"account2": {"XLM:native"},
		}
		contracts := make(map[string][]string)

		err := service.storeAccountTokensInRedis(ctx, trustlines, contracts)
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

		err := service.storeAccountTokensInRedis(ctx, trustlines, contracts)
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
}

func TestProcessTokenChanges(t *testing.T) {
	ctx := context.Background()
	mr, redisStore := setupTestRedis(t)
	defer mr.Close()

	service := &accountTokenService{
		redisStore:         redisStore,
		trustlinesPrefix:   trustlinesKeyPrefix,
		contractsPrefix:    contractsKeyPrefix,
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
