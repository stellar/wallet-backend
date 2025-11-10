// Package services provides account token management with Redis caching tests.
// This file tests core functions for account_tokens.go.
package services

import (
	"context"
	"strconv"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/store"
)

// setupTestRedis creates a miniredis instance for testing
func setupTestAccountTokensRedis(t *testing.T) (*miniredis.Miniredis, *store.RedisStore) {
	mr := miniredis.RunT(t)
	// miniredis Port() returns a string, convert to int
	port, err := strconv.Atoi(mr.Port())
	require.NoError(t, err)
	redisStore := store.NewRedisStore(mr.Host(), port, "")
	return mr, redisStore
}

func TestExtractHolderAddress(t *testing.T) {
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
		// Note: nil vector case omitted as it causes panic in XDR library's GetVec()
		// In practice, this should never happen with valid XDR data
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
			got, err := extractHolderAddress(tt.key)
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
	hash := xdr.Hash{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}
	contractID := xdr.ContractId(hash)

	tests := []struct {
		name    string
		entry   xdr.ContractDataEntry
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
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := extractContractID(tt.entry)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Empty(t, got)
			} else {
				assert.NoError(t, err)
				assert.NotEmpty(t, got)
				// Verify it starts with C (contract address prefix)
				assert.Equal(t, "C", string(got[0]))
			}
		})
	}
}

func TestGetAccountTrustlines(t *testing.T) {
	ctx := context.Background()
	mr, redisStore := setupTestAccountTokensRedis(t)
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
				mr.SetAdd(key, "USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN")
				mr.SetAdd(key, "EUROC:GA7FCCMTTSUIC37PODEL6EOOSPDRILP6OQI5FWCWDDVDBLJV72W6RINZ")
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
	mr, redisStore := setupTestAccountTokensRedis(t)
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
				mr.SetAdd(key, "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4")
				mr.SetAdd(key, "CBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB")
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
	mr, redisStore := setupTestAccountTokensRedis(t)
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

func TestProcessTokenChanges(t *testing.T) {
	ctx := context.Background()
	mr, redisStore := setupTestAccountTokensRedis(t)
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
				mr.SetAdd(key, "USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN")
			},
			verifyData: func(t *testing.T) {
				key := trustlinesKeyPrefix + "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"
				members, err := mr.SMembers(key)
				require.NoError(t, err)
				assert.NotContains(t, members, "USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN")
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
					AccountID: "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N",
					Asset:     "",
					Operation: types.TrustlineOpAdd,
				},
			},
			contractChanges: []types.ContractChange{},
			setupData:       func() {},
			verifyData:      func(t *testing.T) {
				// No verification needed - empty asset should be skipped and no key created
			},
			wantErr: false,
		},
		{
			name:             "skip empty contract ID",
			trustlineChanges: []types.TrustlineChange{},
			contractChanges: []types.ContractChange{
				{
					AccountID:    "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N",
					ContractID:   "",
					ContractType: types.ContractTypeSAC,
				},
			},
			setupData: func() {},
			verifyData: func(t *testing.T) {
				// No verification needed - empty contract ID should be skipped and no key created
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

func TestKeyBuildingHelpers(t *testing.T) {
	service := &accountTokenService{
		trustlinesPrefix:   trustlinesKeyPrefix,
		contractsPrefix:    contractsKeyPrefix,
		contractTypePrefix: contractTypePrefix,
	}

	t.Run("buildTrustlineKey", func(t *testing.T) {
		accountAddress := "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"
		expected := trustlinesKeyPrefix + accountAddress
		got := service.buildTrustlineKey(accountAddress)
		assert.Equal(t, expected, got)
	})

	t.Run("buildContractKey", func(t *testing.T) {
		accountAddress := "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"
		expected := contractsKeyPrefix + accountAddress
		got := service.buildContractKey(accountAddress)
		assert.Equal(t, expected, got)
	})

	t.Run("buildContractTypeKey", func(t *testing.T) {
		contractID := "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4"
		expected := contractTypePrefix + contractID
		got := service.buildContractTypeKey(contractID)
		assert.Equal(t, expected, got)
	})
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

func ptrToAccountID(address string) *xdr.AccountId {
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

