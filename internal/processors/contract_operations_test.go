package processors

import (
	"testing"
	"time"

	set "github.com/deckarep/golang-set/v2"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/network"
	operation_processor "github.com/stellar/go/processors/operation"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/utils"
)

func Test_calculateContractID(t *testing.T) {
	networkPassphrase := network.TestNetworkPassphrase
	salt := xdr.Uint256{195, 179, 60, 131, 211, 25, 160, 131, 45, 151, 203, 11, 11, 116, 166, 232, 51, 92, 179, 76, 220, 111, 96, 246, 72, 68, 195, 127, 194, 19, 147, 252}

	rawAddress, err := strkey.Decode(strkey.VersionByteAccountID, "GBWAH7AOBZYAYLT76Z7MQDDRRJCCERRVRSCJ4GAEGV2S5W474ZLEOH4U")
	require.NoError(t, err)
	var uint256Val xdr.Uint256
	copy(uint256Val[:], rawAddress)
	fromAddress := xdr.ScAddress{
		Type: xdr.ScAddressTypeScAddressTypeAccount,
		AccountId: utils.PointOf(xdr.AccountId{
			Type:    xdr.PublicKeyTypePublicKeyTypeEd25519,
			Ed25519: &uint256Val,
		}),
	}

	contractID, err := calculateContractID(networkPassphrase, xdr.ContractIdPreimageFromAddress{
		Address: fromAddress,
		Salt:    salt,
	})
	require.NoError(t, err)
	require.Equal(t, "CANZKJUEZM22DO2XLJP4ARZAJFG7GJVBIEXJ7T4F2GAIAV4D4RMXMDVD", contractID)

	const (
		accountID4  = "GBWAH7AOBZYAYLT76Z7MQDDRRJCCERRVRSCJ4GAEGV2S5W474ZLEOH4U"
		contractID3 = "CANZKJUEZM22DO2XLJP4ARZAJFG7GJVBIEXJ7T4F2GAIAV4D4RMXMDVD"
	)
	preimage := xdr.ContractIdPreimage{
		Type: xdr.ContractIdPreimageTypeContractIdPreimageFromAddress,
		FromAddress: &xdr.ContractIdPreimageFromAddress{ // <--- contractID3
			Address: makeScAddress(accountID4),
			Salt:    xdr.Uint256{195, 179, 60, 131, 211, 25, 160, 131, 45, 151, 203, 11, 11, 116, 166, 232, 51, 92, 179, 76, 220, 111, 96, 246, 72, 68, 195, 127, 194, 19, 147, 252},
		},
	}
	contractID, err = calculateContractID(networkPassphrase, preimage.MustFromAddress())
	require.NoError(t, err)
	require.Equal(t, contractID3, contractID)
}

func Test_scAddressesForScVal(t *testing.T) {
	// GDYH62HW5R57ZFCJE77Q32YVUANQPK2A4663BWFVKAIMINNWVV6QEI5P
	accountID1 := xdr.MustAddress("GDYH62HW5R57ZFCJE77Q32YVUANQPK2A4663BWFVKAIMINNWVV6QEI5P")
	scAddressAccount1 := xdr.ScAddress{
		Type:      xdr.ScAddressTypeScAddressTypeAccount,
		AccountId: &accountID1,
	}

	// GBWAH7AOBZYAYLT76Z7MQDDRRJCCERRVRSCJ4GAEGV2S5W474ZLEOH4U
	accountID2 := xdr.MustAddress("GBWAH7AOBZYAYLT76Z7MQDDRRJCCERRVRSCJ4GAEGV2S5W474ZLEOH4U")
	scAddressAccount2 := xdr.ScAddress{
		Type:      xdr.ScAddressTypeScAddressTypeAccount,
		AccountId: &accountID2,
	}
	// GBWAH7AOBZYAYLT76Z7MQDDRRJCCERRVRSCJ4GAEGV2S5W474ZLEOH4U re-encoded as a C-account
	accountID2Bytes := strkey.MustDecode(strkey.VersionByteAccountID, accountID2.Address())
	scAddressContract2AsAccountID := xdr.ScAddress{
		Type:       xdr.ScAddressTypeScAddressTypeContract,
		ContractId: utils.PointOf(xdr.Hash(accountID2Bytes)),
	}

	// CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC
	decodedContractID, err := strkey.Decode(strkey.VersionByteContract, "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC")
	require.NoError(t, err)
	contractID1 := xdr.Hash(decodedContractID)
	scAddressContract1 := xdr.ScAddress{
		Type:       xdr.ScAddressTypeScAddressTypeContract,
		ContractId: &contractID1,
	}
	// CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC re-encoded as a G-account
	contractID1AsAccountID, err := strkey.Encode(strkey.VersionByteAccountID, contractID1[:])
	require.NoError(t, err)
	scAddressContract1AsAccountID := xdr.ScAddress{
		Type:      xdr.ScAddressTypeScAddressTypeAccount,
		AccountId: utils.PointOf(xdr.MustAddress(contractID1AsAccountID)),
	}

	// CDSMYK7ADPT32KBXPXWSOWMBANDDFG76IVB4HWHOE2SA3DPAKXA4C6ZR
	decodedContractID, err = strkey.Decode(strkey.VersionByteContract, "CDSMYK7ADPT32KBXPXWSOWMBANDDFG76IVB4HWHOE2SA3DPAKXA4C6ZR")
	require.NoError(t, err)
	contractID2 := xdr.Hash(decodedContractID)
	scAddressContract2 := xdr.ScAddress{
		Type:       xdr.ScAddressTypeScAddressTypeContract,
		ContractId: &contractID2,
	}

	testCases := []struct {
		name          string
		scVal         xdr.ScVal
		wantAddresses set.Set[xdr.ScAddress]
	}{
		{
			name:          "🟡unsupported_scv_type",
			scVal:         xdr.ScVal{Type: xdr.ScValTypeScvI32, I32: utils.PointOf(xdr.Int32(1))},
			wantAddresses: set.NewSet[xdr.ScAddress](),
		},
		{
			name: "🟢scv_address",
			scVal: xdr.ScVal{
				Type:    xdr.ScValTypeScvAddress,
				Address: &scAddressAccount1,
			},
			wantAddresses: set.NewSet(scAddressAccount1),
		},
		{
			name: "🟢scv_vec_with_addresses",
			scVal: func() xdr.ScVal {
				vec := xdr.ScVec{
					xdr.ScVal{Type: xdr.ScValTypeScvAddress, Address: &scAddressAccount1},
					xdr.ScVal{Type: xdr.ScValTypeScvAddress, Address: &scAddressAccount2},
					xdr.ScVal{Type: xdr.ScValTypeScvAddress, Address: &scAddressContract1},
					xdr.ScVal{Type: xdr.ScValTypeScvAddress, Address: &scAddressContract2},
				}
				vecPtr := &vec
				return xdr.ScVal{Type: xdr.ScValTypeScvVec, Vec: &vecPtr}
			}(),
			wantAddresses: set.NewSet(scAddressAccount1, scAddressAccount2, scAddressContract1, scAddressContract2),
		},
		{
			name: "🟢scv_map_with_addresses",
			scVal: func() xdr.ScVal {
				scMap := utils.PointOf(xdr.ScMap{
					xdr.ScMapEntry{
						Key: xdr.ScVal{Type: xdr.ScValTypeScvAddress, Address: &scAddressAccount1},
						Val: xdr.ScVal{Type: xdr.ScValTypeScvAddress, Address: &scAddressContract1},
					},
				})
				return xdr.ScVal{Type: xdr.ScValTypeScvMap, Map: &scMap}
			}(),
			wantAddresses: set.NewSet(scAddressAccount1, scAddressContract1),
		},
		{
			name: "🟢scv_bytes_as_contract_id",
			scVal: func() xdr.ScVal {
				scb := xdr.ScBytes(contractID1[:])
				return xdr.ScVal{Type: xdr.ScValTypeScvBytes, Bytes: &scb}
			}(),
			wantAddresses: set.NewSet(scAddressContract1, scAddressContract1AsAccountID),
		},
		{
			name: "🟢scv_bytes_as_account_id",
			scVal: func() xdr.ScVal {
				decoded, err := strkey.Decode(strkey.VersionByteAccountID, accountID2.Address())
				require.NoError(t, err)
				scb := xdr.ScBytes(decoded)
				return xdr.ScVal{Type: xdr.ScValTypeScvBytes, Bytes: &scb}
			}(),
			wantAddresses: set.NewSet(scAddressAccount2, scAddressContract2AsAccountID),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := scAddressesForScVal(tc.scVal)
			assert.Equal(t, tc.wantAddresses.Cardinality(), result.Cardinality())
			assert.ElementsMatch(t, tc.wantAddresses.ToSlice(), result.ToSlice())
		})
	}
}

func Test_participantsForScVal(t *testing.T) {
	// GDYH62HW5R57ZFCJE77Q32YVUANQPK2A4663BWFVKAIMINNWVV6QEI5P
	accountID1 := xdr.MustAddress("GDYH62HW5R57ZFCJE77Q32YVUANQPK2A4663BWFVKAIMINNWVV6QEI5P")
	scAddressAccount1 := xdr.ScAddress{
		Type:      xdr.ScAddressTypeScAddressTypeAccount,
		AccountId: &accountID1,
	}

	// CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC
	decodedContractID, err := strkey.Decode(strkey.VersionByteContract, "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC")
	require.NoError(t, err)
	contractID1 := xdr.Hash(decodedContractID)
	scAddressContract1 := xdr.ScAddress{
		Type:       xdr.ScAddressTypeScAddressTypeContract,
		ContractId: &contractID1,
	}

	testCases := []struct {
		name          string
		scVal         xdr.ScVal
		wantAddresses set.Set[string]
	}{
		{
			name:          "🟡unsupported_scv_type",
			scVal:         xdr.ScVal{Type: xdr.ScValTypeScvI32, I32: utils.PointOf(xdr.Int32(1))},
			wantAddresses: set.NewSet[string](),
		},
		{
			name: "🟢scv_address",
			scVal: xdr.ScVal{
				Type:    xdr.ScValTypeScvAddress,
				Address: &scAddressAccount1,
			},
			wantAddresses: set.NewSet("GDYH62HW5R57ZFCJE77Q32YVUANQPK2A4663BWFVKAIMINNWVV6QEI5P"),
		},
		{
			name: "🟢scv_map_with_address_and_vector",
			scVal: func() xdr.ScVal {
				vec := &xdr.ScVec{
					xdr.ScVal{Type: xdr.ScValTypeScvAddress, Address: &scAddressAccount1},
				}
				scMap := utils.PointOf(xdr.ScMap{
					xdr.ScMapEntry{
						Key: xdr.ScVal{Type: xdr.ScValTypeScvAddress, Address: &scAddressContract1},
						Val: xdr.ScVal{Type: xdr.ScValTypeScvVec, Vec: utils.PointOf(vec)},
					},
				})
				return xdr.ScVal{Type: xdr.ScValTypeScvMap, Map: &scMap}
			}(),
			wantAddresses: set.NewSet("GDYH62HW5R57ZFCJE77Q32YVUANQPK2A4663BWFVKAIMINNWVV6QEI5P", "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := participantsForScVal(tc.scVal)
			require.NoError(t, err)
			assert.Equal(t, tc.wantAddresses.Cardinality(), result.Cardinality())
			assert.ElementsMatch(t, tc.wantAddresses.ToSlice(), result.ToSlice())
		})
	}
}

// func Test_participantsForAuthEntries(t *testing.T) {
// 	// GDYH62HW5R57ZFCJE77Q32YVUANQPK2A4663BWFVKAIMINNWVV6QEI5P
// 	accountID1 := xdr.MustAddress("GDYH62HW5R57ZFCJE77Q32YVUANQPK2A4663BWFVKAIMINNWVV6QEI5P")
// 	scAddressAccount1 := xdr.ScAddress{
// 		Type:      xdr.ScAddressTypeScAddressTypeAccount,
// 		AccountId: &accountID1,
// 	}

// 	// CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC
// 	decodedContractID, err := strkey.Decode(strkey.VersionByteContract, "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC")
// 	require.NoError(t, err)
// 	contractID1 := xdr.Hash(decodedContractID)
// 	scAddressContract1 := xdr.ScAddress{
// 		Type:       xdr.ScAddressTypeScAddressTypeContract,
// 		ContractId: &contractID1,
// 	}

// 	// GBWAH7AOBZYAYLT76Z7MQDDRRJCCERRVRSCJ4GAEGV2S5W474ZLEOH4U
// 	accountID2 := xdr.MustAddress("GBWAH7AOBZYAYLT76Z7MQDDRRJCCERRVRSCJ4GAEGV2S5W474ZLEOH4U")
// 	scAddressAccount2 := xdr.ScAddress{
// 		Type:      xdr.ScAddressTypeScAddressTypeAccount,
// 		AccountId: &accountID2,
// 	}

// 	testCases := []struct {
// 		name            string
// 		authEntries     []xdr.SorobanAuthorizationEntry
// 		expected        []string
// 		wantErrContains string
// 	}{
// 		{
// 			name:        "🟢empty_auth_entries",
// 			authEntries: []xdr.SorobanAuthorizationEntry{},
// 			expected:    []string{},
// 		},
// 		{
// 			name: "🟢single_account_auth_entry",
// 			authEntries: []xdr.SorobanAuthorizationEntry{
// 				{
// 					Credentials: xdr.SorobanCredentials{
// 						Type: xdr.SorobanCredentialsTypeSorobanCredentialsAddress,
// 						Address: utils.PointOf(xdr.SorobanAddressCredentials{
// 							Address: scAddressAccount1,
// 						}),
// 					},
// 				},
// 			},
// 			expected: []string{"GDYH62HW5R57ZFCJE77Q32YVUANQPK2A4663BWFVKAIMINNWVV6QEI5P"},
// 		},
// 		{
// 			name: "🟢single_contract_auth_entry",
// 			authEntries: []xdr.SorobanAuthorizationEntry{
// 				{
// 					Credentials: xdr.SorobanCredentials{
// 						Type: xdr.SorobanCredentialsTypeSorobanCredentialsAddress,
// 						Address: utils.PointOf(xdr.SorobanAddressCredentials{
// 							Address: scAddressContract1,
// 						}),
// 					},
// 				},
// 			},
// 			expected: []string{"CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"},
// 		},
// 		{
// 			name: "🟢multiple_auth_entries",
// 			authEntries: []xdr.SorobanAuthorizationEntry{
// 				{
// 					Credentials: xdr.SorobanCredentials{
// 						Type: xdr.SorobanCredentialsTypeSorobanCredentialsAddress,
// 						Address: utils.PointOf(xdr.SorobanAddressCredentials{
// 							Address: scAddressAccount1,
// 						}),
// 					},
// 				},
// 				{
// 					Credentials: xdr.SorobanCredentials{
// 						Type: xdr.SorobanCredentialsTypeSorobanCredentialsAddress,
// 						Address: utils.PointOf(xdr.SorobanAddressCredentials{
// 							Address: scAddressContract1,
// 						}),
// 					},
// 				},
// 				{
// 					Credentials: xdr.SorobanCredentials{
// 						Type: xdr.SorobanCredentialsTypeSorobanCredentialsAddress,
// 						Address: utils.PointOf(xdr.SorobanAddressCredentials{
// 							Address: scAddressAccount2,
// 						}),
// 					},
// 				},
// 			},
// 			expected: []string{
// 				"GDYH62HW5R57ZFCJE77Q32YVUANQPK2A4663BWFVKAIMINNWVV6QEI5P",
// 				"CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC",
// 				"GBWAH7AOBZYAYLT76Z7MQDDRRJCCERRVRSCJ4GAEGV2S5W474ZLEOH4U",
// 			},
// 		},
// 		{
// 			name: "🟡unsupported_credentials_type_should_be_ignored",
// 			authEntries: []xdr.SorobanAuthorizationEntry{
// 				{
// 					Credentials: xdr.SorobanCredentials{
// 						Type: xdr.SorobanCredentialsTypeSorobanCredentialsSourceAccount,
// 						Address: utils.PointOf(xdr.SorobanAddressCredentials{
// 							Address: scAddressContract1,
// 						}),
// 					},
// 				},
// 				{
// 					Credentials: xdr.SorobanCredentials{
// 						Type: xdr.SorobanCredentialsTypeSorobanCredentialsAddress,
// 						Address: utils.PointOf(xdr.SorobanAddressCredentials{
// 							Address: scAddressAccount2,
// 						}),
// 					},
// 				},
// 			},
// 			expected: []string{"GBWAH7AOBZYAYLT76Z7MQDDRRJCCERRVRSCJ4GAEGV2S5W474ZLEOH4U"},
// 		},
// 		{
// 			name: "🟢duplicate_addresses_should_be_deduplicated",
// 			authEntries: []xdr.SorobanAuthorizationEntry{
// 				{
// 					Credentials: xdr.SorobanCredentials{
// 						Type: xdr.SorobanCredentialsTypeSorobanCredentialsAddress,
// 						Address: utils.PointOf(xdr.SorobanAddressCredentials{
// 							Address: scAddressAccount1,
// 						}),
// 					},
// 				},
// 				{
// 					Credentials: xdr.SorobanCredentials{
// 						Type: xdr.SorobanCredentialsTypeSorobanCredentialsAddress,
// 						Address: utils.PointOf(xdr.SorobanAddressCredentials{ // Duplicate
// 							Address: scAddressAccount1,
// 						}),
// 					},
// 				},
// 			},
// 			expected: []string{"GDYH62HW5R57ZFCJE77Q32YVUANQPK2A4663BWFVKAIMINNWVV6QEI5P"},
// 		},
// 	}

// 	for _, tc := range testCases {
// 		t.Run(tc.name, func(t *testing.T) {
// 			participants, err := participantsForAuthEntries(tc.authEntries)

// 			if tc.wantErrContains != "" {
// 				assert.Error(t, err)
// 				assert.ErrorContains(t, err, tc.wantErrContains)
// 				assert.Empty(t, participants)
// 			} else {
// 				assert.NoError(t, err)
// 				assert.Equal(t, len(tc.expected), participants.Cardinality())
// 				for _, expectedParticipant := range tc.expected {
// 					assert.True(t, participants.Contains(expectedParticipant))
// 				}
// 			}
// 		})
// 	}
// }

func makeScAddress(accountID string) xdr.ScAddress {
	return xdr.ScAddress{
		Type:      xdr.ScAddressTypeScAddressTypeAccount,
		AccountId: utils.PointOf(xdr.MustAddress(accountID)),
	}
}

func makeScContract(contractID string) xdr.ScAddress {
	decoded := strkey.MustDecode(strkey.VersionByteContract, contractID)
	return xdr.ScAddress{
		Type:       xdr.ScAddressTypeScAddressTypeContract,
		ContractId: utils.PointOf(xdr.Hash(decoded)),
	}
}

func Test_participantsForSorobanOp(t *testing.T) {
	// Test addresses
	const (
		accountID1      = "GAGWN4445WLODCXT7RUZXJLQK5XWX4GICXDOAAZZGK2N3BR67RIIVWJ7"
		accountID2      = "GBKV7KN5K2CJA7TC5AUQNI76JBXHLMQSHT426JEAR3TPVKNSMKMG4RZN"
		accountID3      = "GCTNXY3EZFV2BL4CWHIRSBJVBEYFXANMIDJEVITS66YXOQEF3PL7LHXQ"
		accountID4      = "GBWAH7AOBZYAYLT76Z7MQDDRRJCCERRVRSCJ4GAEGV2S5W474ZLEOH4U"
		contractID1     = "CBN2MBW4AFEHXMLE5ADTAWFOQKEHBYTVO62AZ7DTQONACYE26VFPHKVA"
		contractID2     = "CCSZ54OHAF6BBBFVKHGA6WFWNQLEBXBVO3JYY4BPRYQTXOYJ7LI3QE4D"
		contractID3     = "CANZKJUEZM22DO2XLJP4ARZAJFG7GJVBIEXJ7T4F2GAIAV4D4RMXMDVD"
		contractID4     = "CAXR4FCMM4RTFCOHZ3EOFEOQHDHMBLSZQBXFTX2OWHDQWO5IFCFF6Z3K"
		xlmSACContracID = "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"
	)

	// Custom types for test cases
	type BaseOpConfig struct {
		opType               xdr.OperationType
		feeBumpSourceAccount string
		txSourceAccount      string
		opSourceAccount      string
	}

	type FootprintOpConfig struct {
		readOnlyAddresses  []xdr.ScAddress
		readWriteAddresses []xdr.ScAddress
	}

	type InvokeHostOpCreateContractConfig struct {
		contractType    xdr.HostFunctionType
		preimageType    xdr.ContractIdPreimageType
		constructorArgs []xdr.ScAddress
		authAccounts    []xdr.ScAddress
	}

	type InvokeHostOpInvokeContractConfig struct {
		contractAddress xdr.ScAddress
		argAddresses    []xdr.ScAddress
		authAccounts    []xdr.ScAddress
	}

	type TestOpConfig struct {
		Base                   BaseOpConfig
		FootprintOpConfig      FootprintOpConfig
		CreateContractOpConfig InvokeHostOpCreateContractConfig
		InvokeContractOpConfig InvokeHostOpInvokeContractConfig
	}

	// Helpers:
	makeAuthEntries := func(sorobanAuthFn xdr.SorobanAuthorizedFunction, authAccounts []xdr.ScAddress) []xdr.SorobanAuthorizationEntry {
		authEntries := []xdr.SorobanAuthorizationEntry{}
		for _, authAccount := range authAccounts {
			authEntries = append(authEntries, xdr.SorobanAuthorizationEntry{
				Credentials: xdr.SorobanCredentials{
					Type:    xdr.SorobanCredentialsTypeSorobanCredentialsAddress,
					Address: utils.PointOf(xdr.SorobanAddressCredentials{Address: authAccount}),
				},
				RootInvocation: xdr.SorobanAuthorizedInvocation{
					Function:       sorobanAuthFn,
					SubInvocations: nil,
				},
			})
		}

		return authEntries
	}

	// Fixtures functions
	makeBasicSorobanOp := func(base BaseOpConfig) operation_processor.TransactionOperationWrapper {
		var opSourceAccount *xdr.MuxedAccount
		if base.opSourceAccount != "" {
			opSourceAccount = utils.PointOf(xdr.MustMuxedAddress(base.opSourceAccount))
		}

		return operation_processor.TransactionOperationWrapper{
			Network:      network.TestNetworkPassphrase,
			LedgerClosed: time.Now(),
			Operation: xdr.Operation{
				Body:          xdr.OperationBody{Type: base.opType},
				SourceAccount: opSourceAccount,
			},
			Transaction: ingest.LedgerTransaction{
				Envelope: xdr.TransactionEnvelope{
					Type: xdr.EnvelopeTypeEnvelopeTypeTx,
					V1: &xdr.TransactionV1Envelope{
						Tx: xdr.Transaction{
							SourceAccount: xdr.MustMuxedAddress(base.txSourceAccount),
							Ext: xdr.TransactionExt{
								V:           int32(1),
								SorobanData: &xdr.SorobanTransactionData{},
							},
						},
					},
				},
			},
		}
	}

	makeFootprintOp := func(base BaseOpConfig, footprint FootprintOpConfig) operation_processor.TransactionOperationWrapper {
		op := makeBasicSorobanOp(base)

		buildLedgerKeys := func(addresses []xdr.ScAddress) []xdr.LedgerKey {
			ledgerKeys := []xdr.LedgerKey{}
			for _, address := range addresses {
				ledgerKey := xdr.LedgerKey{}
				switch address.Type {
				case xdr.ScAddressTypeScAddressTypeAccount:
					ledgerKey.Type = xdr.LedgerEntryTypeAccount
					ledgerKey.Account = &xdr.LedgerKeyAccount{AccountId: *address.AccountId}

				case xdr.ScAddressTypeScAddressTypeContract:
					ledgerKey.Type = xdr.LedgerEntryTypeContractData
					ledgerKey.ContractData = &xdr.LedgerKeyContractData{Contract: address}
				}
				ledgerKeys = append(ledgerKeys, ledgerKey)
			}
			return ledgerKeys
		}
		op.Transaction.Envelope.V1.Tx.Ext.SorobanData.Resources.Footprint.ReadOnly = buildLedgerKeys(footprint.readOnlyAddresses)
		op.Transaction.Envelope.V1.Tx.Ext.SorobanData.Resources.Footprint.ReadWrite = buildLedgerKeys(footprint.readWriteAddresses)

		return op
	}

	makeCreateContractOp := func(base BaseOpConfig, createContractOpConfig InvokeHostOpCreateContractConfig) operation_processor.TransactionOperationWrapper {
		op := makeBasicSorobanOp(base)
		deployerAccount := base.txSourceAccount
		if base.opSourceAccount != "" {
			deployerAccount = base.opSourceAccount
		}

		var preimage xdr.ContractIdPreimage
		switch createContractOpConfig.preimageType {
		case xdr.ContractIdPreimageTypeContractIdPreimageFromAsset:
			preimage = xdr.ContractIdPreimage{
				Type:      xdr.ContractIdPreimageTypeContractIdPreimageFromAsset,
				FromAsset: &xdr.Asset{Type: xdr.AssetTypeAssetTypeNative},
			}

		case xdr.ContractIdPreimageTypeContractIdPreimageFromAddress:
			rawAddress, err := strkey.Decode(strkey.VersionByteAccountID, deployerAccount)
			require.NoError(t, err)
			var uint256Val xdr.Uint256
			copy(uint256Val[:], rawAddress)

			preimage = xdr.ContractIdPreimage{
				FromAddress: &xdr.ContractIdPreimageFromAddress{
					Address: xdr.ScAddress{
						Type: xdr.ScAddressTypeScAddressTypeAccount,
						AccountId: utils.PointOf(xdr.AccountId{
							Type:    xdr.PublicKeyTypePublicKeyTypeEd25519,
							Ed25519: &uint256Val,
						}),
					},
					Salt: xdr.Uint256{195, 179, 60, 131, 211, 25, 160, 131, 45, 151, 203, 11, 11, 116, 166, 232, 51, 92, 179, 76, 220, 111, 96, 246, 72, 68, 195, 127, 194, 19, 147, 252},
				},
			}
		}

		constructorArgs := []xdr.ScVal{}
		for _, arg := range createContractOpConfig.constructorArgs {
			switch arg.Type {
			case xdr.ScAddressTypeScAddressTypeAccount:
				constructorArgs = append(constructorArgs, xdr.ScVal{Type: xdr.ScValTypeScvAddress, Address: &arg})
			case xdr.ScAddressTypeScAddressTypeContract:
				constructorArgs = append(constructorArgs, xdr.ScVal{Type: xdr.ScValTypeScvAddress, Address: &arg})
			default:
				require.Fail(t, "unsupported/unimplemented constructor arg type")
			}
		}

		sorobanAuthFn := xdr.SorobanAuthorizedFunction{Type: xdr.SorobanAuthorizedFunctionTypeSorobanAuthorizedFunctionTypeCreateContractHostFn}
		hostFn := xdr.HostFunction{Type: createContractOpConfig.contractType}
		switch createContractOpConfig.contractType {
		case xdr.HostFunctionTypeHostFunctionTypeCreateContract:
			hostFn.CreateContract = &xdr.CreateContractArgs{ContractIdPreimage: preimage}
			sorobanAuthFn.CreateContractHostFn = hostFn.CreateContract
		case xdr.HostFunctionTypeHostFunctionTypeCreateContractV2:
			hostFn.CreateContractV2 = &xdr.CreateContractArgsV2{ContractIdPreimage: preimage, ConstructorArgs: constructorArgs}
			sorobanAuthFn.CreateContractV2HostFn = hostFn.CreateContractV2
		default:
			require.Fail(t, "unsupported/unimplemented contract type")
		}

		op.Operation.Body.InvokeHostFunctionOp = &xdr.InvokeHostFunctionOp{
			HostFunction: hostFn,
			Auth:         makeAuthEntries(sorobanAuthFn, createContractOpConfig.authAccounts),
		}

		return op
	}

	makeInvokeContractOp := func(base BaseOpConfig, invokeContractOpConfig InvokeHostOpInvokeContractConfig) operation_processor.TransactionOperationWrapper {
		op := makeBasicSorobanOp(base)

		var args xdr.ScVec
		for _, arg := range invokeContractOpConfig.argAddresses {
			args = append(args, xdr.ScVal{Type: xdr.ScValTypeScvAddress, Address: &arg})
		}

		invokeContractArgs := xdr.InvokeContractArgs{
			ContractAddress: invokeContractOpConfig.contractAddress,
			FunctionName:    xdr.ScSymbol("authorized_fn"),
			Args:            args,
		}

		sorobanAuthFn := xdr.SorobanAuthorizedFunction{
			Type:       xdr.SorobanAuthorizedFunctionTypeSorobanAuthorizedFunctionTypeContractFn,
			ContractFn: &invokeContractArgs,
		}

		op.Operation.Body.InvokeHostFunctionOp = &xdr.InvokeHostFunctionOp{
			HostFunction: xdr.HostFunction{
				Type:           xdr.HostFunctionTypeHostFunctionTypeInvokeContract,
				InvokeContract: &invokeContractArgs,
			},
			Auth: makeAuthEntries(sorobanAuthFn, invokeContractOpConfig.authAccounts),
		}

		return op
	}

	makeUploadWasmOp := func(base BaseOpConfig) operation_processor.TransactionOperationWrapper {
		op := makeBasicSorobanOp(base)
		op.Operation.Body.InvokeHostFunctionOp = &xdr.InvokeHostFunctionOp{
			HostFunction: xdr.HostFunction{
				Type: xdr.HostFunctionTypeHostFunctionTypeUploadContractWasm,
				Wasm: &[]byte{1, 2, 3, 4, 5},
			},
		}
		return op
	}

	makeNonSorobanOp := func(base BaseOpConfig) operation_processor.TransactionOperationWrapper {
		op := makeBasicSorobanOp(base)
		op.Transaction.Envelope.V1.Tx.Ext.V = 0
		op.Transaction.Envelope.V1.Tx.Ext.SorobanData = nil
		return op
	}

	makeFeeBumpOp := func(feeBumpSourceAccount string, baseOp operation_processor.TransactionOperationWrapper) operation_processor.TransactionOperationWrapper {
		op := baseOp
		op.Transaction.Envelope.V1 = &xdr.TransactionV1Envelope{}
		op.Transaction.Envelope.Type = xdr.EnvelopeTypeEnvelopeTypeTxFeeBump
		op.Transaction.Envelope.FeeBump = &xdr.FeeBumpTransactionEnvelope{
			Tx: xdr.FeeBumpTransaction{
				FeeSource: xdr.MustMuxedAddress(feeBumpSourceAccount),
				InnerTx: xdr.FeeBumpTransactionInnerTx{
					Type: baseOp.Transaction.Envelope.Type,
					V1:   baseOp.Transaction.Envelope.V1,
				},
			},
		}
		return op
	}

	// includeSubinvocations will add subinvocations to any existing SorobanAuthorizationEntry where the following
	// accounts are present: [xlmSACContracID, contractID1, contractID2, contractID3, contractID4, accountID2, accountID3, accountID4].
	includeSubinvocations := func(baseOp operation_processor.TransactionOperationWrapper) operation_processor.TransactionOperationWrapper {
		op := baseOp

		subInvocations := []xdr.SorobanAuthorizedInvocation{
			{
				Function: xdr.SorobanAuthorizedFunction{
					Type: xdr.SorobanAuthorizedFunctionTypeSorobanAuthorizedFunctionTypeCreateContractHostFn,
					CreateContractHostFn: &xdr.CreateContractArgs{ContractIdPreimage: xdr.ContractIdPreimage{
						Type:      xdr.ContractIdPreimageTypeContractIdPreimageFromAsset,
						FromAsset: &xdr.Asset{Type: xdr.AssetTypeAssetTypeNative}, // <--- xlmSACContracID
					}},
				},
				SubInvocations: []xdr.SorobanAuthorizedInvocation{
					{
						Function: xdr.SorobanAuthorizedFunction{
							Type: xdr.SorobanAuthorizedFunctionTypeSorobanAuthorizedFunctionTypeContractFn,
							ContractFn: &xdr.InvokeContractArgs{
								ContractAddress: makeScContract(contractID1), // <--- contractID1
								FunctionName:    xdr.ScSymbol("sub_fn"),
								Args:            xdr.ScVec{xdr.ScVal{Type: xdr.ScValTypeScvAddress, Address: utils.PointOf(makeScContract(contractID2))}}, // <--- contractID2
							},
						},
						SubInvocations: nil,
					},
					{
						Function: xdr.SorobanAuthorizedFunction{
							Type: xdr.SorobanAuthorizedFunctionTypeSorobanAuthorizedFunctionTypeCreateContractV2HostFn,
							CreateContractV2HostFn: &xdr.CreateContractArgsV2{
								ConstructorArgs: []xdr.ScVal{
									{Type: xdr.ScValTypeScvAddress, Address: utils.PointOf(makeScAddress(accountID3))}, // <--- accountID3
								},
								ContractIdPreimage: xdr.ContractIdPreimage{
									Type: xdr.ContractIdPreimageTypeContractIdPreimageFromAddress,
									FromAddress: &xdr.ContractIdPreimageFromAddress{ // <--- contractID3
										Address: makeScAddress(accountID4),
										Salt:    xdr.Uint256{195, 179, 60, 131, 211, 25, 160, 131, 45, 151, 203, 11, 11, 116, 166, 232, 51, 92, 179, 76, 220, 111, 96, 246, 72, 68, 195, 127, 194, 19, 147, 252},
									},
								},
							},
						},
						SubInvocations: nil,
					},
				},
			},
			{
				Function: xdr.SorobanAuthorizedFunction{
					Type: xdr.SorobanAuthorizedFunctionTypeSorobanAuthorizedFunctionTypeContractFn,
					ContractFn: &xdr.InvokeContractArgs{
						ContractAddress: makeScContract(contractID4),
						FunctionName:    xdr.ScSymbol("sub_fn"),
						Args:            xdr.ScVec{xdr.ScVal{Type: xdr.ScValTypeScvAddress, Address: utils.PointOf(makeScAddress(accountID2))}}, // <--- accountID2
					},
				},
				SubInvocations: nil,
			},
		}

		// Add subinvocations to existing auth entries
		if op.Operation.Body.InvokeHostFunctionOp != nil && len(op.Operation.Body.InvokeHostFunctionOp.Auth) > 0 {
			for i := range op.Operation.Body.InvokeHostFunctionOp.Auth {
				op.Operation.Body.MustInvokeHostFunctionOp().Auth[i].RootInvocation.SubInvocations = subInvocations
			}
		}

		return op
	}

	makeOp := func(config TestOpConfig) operation_processor.TransactionOperationWrapper {
		var op operation_processor.TransactionOperationWrapper

		switch config.Base.opType {
		case xdr.OperationTypeExtendFootprintTtl, xdr.OperationTypeRestoreFootprint:
			op = makeFootprintOp(config.Base, config.FootprintOpConfig)
		case xdr.OperationTypeInvokeHostFunction:
			switch config.CreateContractOpConfig.contractType {
			case xdr.HostFunctionTypeHostFunctionTypeCreateContract, xdr.HostFunctionTypeHostFunctionTypeCreateContractV2:
				op = makeCreateContractOp(config.Base, config.CreateContractOpConfig)
			case xdr.HostFunctionTypeHostFunctionTypeUploadContractWasm:
				op = makeUploadWasmOp(config.Base)
			case xdr.HostFunctionTypeHostFunctionTypeInvokeContract:
				op = makeInvokeContractOp(config.Base, config.InvokeContractOpConfig)
			default:
				require.Fail(t, "unsupported/unimplemented contract type")
			}
		default:
			op = makeNonSorobanOp(config.Base)
		}

		if config.Base.feeBumpSourceAccount != "" {
			op = makeFeeBumpOp(config.Base.feeBumpSourceAccount, op)
		}

		return op
	}

	// Test cases
	testCases := []struct {
		name             string
		op               operation_processor.TransactionOperationWrapper
		wantParticipants set.Set[string]
		wantErrContains  string
	}{
		{
			name: "🔴non_soroban_operation",
			op: makeOp(TestOpConfig{
				Base: BaseOpConfig{
					opType:          xdr.OperationTypePayment,
					txSourceAccount: accountID1,
				},
			}),
			wantErrContains: ErrNotSorobanOperation.Error(),
		},
		{
			name: "🟢ExtendFootprintTtl/ReadOnly/tx/tx.SourceAccount",
			op: makeOp(TestOpConfig{
				Base: BaseOpConfig{
					opType:          xdr.OperationTypeExtendFootprintTtl,
					txSourceAccount: accountID1,
				},
				FootprintOpConfig: FootprintOpConfig{
					readOnlyAddresses: []xdr.ScAddress{makeScContract(contractID1)},
				},
			}),
			wantParticipants: set.NewSet(accountID1, contractID1),
		},
		{
			name: "🟢ExtendFootprintTtl/ReadOnly&ReadWrite/tx/op.SourceAccount",
			op: makeOp(TestOpConfig{
				Base: BaseOpConfig{
					opType:          xdr.OperationTypeExtendFootprintTtl,
					txSourceAccount: accountID1,
					opSourceAccount: accountID2,
				},
				FootprintOpConfig: FootprintOpConfig{
					readOnlyAddresses:  []xdr.ScAddress{makeScContract(contractID1), makeScAddress(accountID3)},
					readWriteAddresses: []xdr.ScAddress{makeScContract(contractID2)},
				},
			}),
			wantParticipants: set.NewSet(accountID2, accountID3, contractID1, contractID2),
		},
		{
			name: "🟢RestoreFootprint/ReadWrite/fee_bump_tx/tx.SourceAccount",
			op: makeOp(TestOpConfig{
				Base: BaseOpConfig{
					opType:               xdr.OperationTypeRestoreFootprint,
					feeBumpSourceAccount: accountID3,
					txSourceAccount:      accountID1,
				},
				FootprintOpConfig: FootprintOpConfig{
					readWriteAddresses: []xdr.ScAddress{makeScContract(contractID1)},
				},
			}),
			wantParticipants: set.NewSet(accountID1, contractID1),
		},
		{
			name: "🟢InvokeHost/CreateContract/fromAddress/tx/tx.SourceAccount",
			op: makeOp(TestOpConfig{
				Base: BaseOpConfig{
					opType:          xdr.OperationTypeInvokeHostFunction,
					txSourceAccount: accountID2,
				},
				CreateContractOpConfig: InvokeHostOpCreateContractConfig{
					contractType: xdr.HostFunctionTypeHostFunctionTypeCreateContract,
					preimageType: xdr.ContractIdPreimageTypeContractIdPreimageFromAddress,
				},
			}),
			wantParticipants: set.NewSet(accountID2, "CBFECUDH6TY6GHBBJS2ASEAXCL2KMBGF46E7A2F42SWMICKG2VDFVPED"),
		},
		{
			name: "🟢InvokeHost/CreateContractV2/fromAddress/tx/tx.SourceAccount/args",
			op: makeOp(TestOpConfig{
				Base: BaseOpConfig{
					opType:          xdr.OperationTypeInvokeHostFunction,
					txSourceAccount: accountID2,
				},
				CreateContractOpConfig: InvokeHostOpCreateContractConfig{
					contractType: xdr.HostFunctionTypeHostFunctionTypeCreateContractV2,
					preimageType: xdr.ContractIdPreimageTypeContractIdPreimageFromAddress,
					constructorArgs: []xdr.ScAddress{
						makeScAddress(accountID1),
						makeScContract(contractID1),
					},
				},
			}),
			wantParticipants: set.NewSet(accountID2, "CBFECUDH6TY6GHBBJS2ASEAXCL2KMBGF46E7A2F42SWMICKG2VDFVPED", accountID1, contractID1),
		},
		{
			name: "🟢InvokeHost/CreateContractV2/fromAsset/tx/tx.SourceAccount",
			op: makeOp(TestOpConfig{
				Base: BaseOpConfig{
					opType:          xdr.OperationTypeInvokeHostFunction,
					txSourceAccount: accountID3,
				},
				CreateContractOpConfig: InvokeHostOpCreateContractConfig{
					contractType: xdr.HostFunctionTypeHostFunctionTypeCreateContractV2,
					preimageType: xdr.ContractIdPreimageTypeContractIdPreimageFromAsset,
				},
			}),
			wantParticipants: set.NewSet(accountID3, xlmSACContracID),
		},
		{
			name: "🟢InvokeHost/InvokeContract/auth/args/tx/tx.SourceAccount",
			op: makeOp(TestOpConfig{
				Base: BaseOpConfig{
					opType:          xdr.OperationTypeInvokeHostFunction,
					txSourceAccount: accountID3,
				},
				InvokeContractOpConfig: InvokeHostOpInvokeContractConfig{
					contractAddress: makeScContract(contractID1),
					argAddresses:    []xdr.ScAddress{makeScAddress(accountID1), makeScAddress(accountID2)},
					authAccounts:    []xdr.ScAddress{makeScContract(xlmSACContracID)},
				},
			}),
			wantParticipants: set.NewSet(accountID3, contractID1, accountID1, xlmSACContracID, accountID2),
		},
		{
			name: "🟢UploadWasm/tx/op.SourceAccount",
			op: makeOp(TestOpConfig{
				Base: BaseOpConfig{
					opType:          xdr.OperationTypeInvokeHostFunction,
					txSourceAccount: accountID1,
					opSourceAccount: accountID2,
				},
				CreateContractOpConfig: InvokeHostOpCreateContractConfig{
					contractType: xdr.HostFunctionTypeHostFunctionTypeUploadContractWasm,
				},
			}),
			wantParticipants: set.NewSet(accountID2),
		},
		{
			name: "🟢InvokeHost/CreateContract/fromAddress/tx/tx.SourceAccount/subinvocations",
			op: includeSubinvocations(makeOp(TestOpConfig{
				Base: BaseOpConfig{
					opType:          xdr.OperationTypeInvokeHostFunction,
					txSourceAccount: accountID2,
				},
				CreateContractOpConfig: InvokeHostOpCreateContractConfig{
					contractType: xdr.HostFunctionTypeHostFunctionTypeCreateContract,
					preimageType: xdr.ContractIdPreimageTypeContractIdPreimageFromAddress,
					authAccounts: []xdr.ScAddress{makeScAddress(accountID2)},
				},
			})),
			// xlmSACContracID, contractID1, contractID2, contractID3, contractID4, accountID2, accountID3, accountID4
			wantParticipants: set.NewSet(accountID2, "CBFECUDH6TY6GHBBJS2ASEAXCL2KMBGF46E7A2F42SWMICKG2VDFVPED", xlmSACContracID, contractID1, contractID2, contractID3, contractID4, accountID3, accountID4),
		},
		// {
		// 	name: "🟢InvokeHost/CreateContractV2/fromAddress/tx/tx.SourceAccount/subinvocations",
		// 	op: includeSubinvocations(makeOp(TestOpConfig{
		// 		Base: BaseOpConfig{
		// 			opType:          xdr.OperationTypeInvokeHostFunction,
		// 			txSourceAccount: accountID2,
		// 		},
		// 		CreateContractOpConfig: InvokeHostOpCreateContractConfig{
		// 			contractType: xdr.HostFunctionTypeHostFunctionTypeCreateContractV2,
		// 			preimageType: xdr.ContractIdPreimageTypeContractIdPreimageFromAddress,
		// 			authAccounts: []xdr.ScAddress{makeScAddress(accountID2)},
		// 		},
		// 	})),
		// 	// xlmSACContracID, contractID1, contractID2, contractID3, contractID4, accountID2, accountID3, accountID4
		// 	wantParticipants: set.NewSet(accountID2, "CBFECUDH6TY6GHBBJS2ASEAXCL2KMBGF46E7A2F42SWMICKG2VDFVPED", xlmSACContracID, contractID1, contractID2, contractID3, contractID4, accountID3, accountID4),
		// },
		{
			name: "🟢InvokeHost/InvokeContract/auth/args/tx/tx.SourceAccount/subinvocations",
			op: includeSubinvocations(makeOp(TestOpConfig{
				Base: BaseOpConfig{
					opType:          xdr.OperationTypeInvokeHostFunction,
					txSourceAccount: accountID2,
				},
				InvokeContractOpConfig: InvokeHostOpInvokeContractConfig{
					contractAddress: makeScContract(contractID1),
					authAccounts:    []xdr.ScAddress{makeScAddress(accountID1)},
				},
			})),
			wantParticipants: set.NewSet(
				accountID1, accountID2, contractID1,
				xlmSACContracID, contractID1, contractID2, contractID3, contractID4, accountID3, accountID4,
			),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			participants, err := participantsForSorobanOp(tc.op)

			if tc.wantErrContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrContains)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.wantParticipants, participants)
			}
		})
	}
}
