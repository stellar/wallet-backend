package processors

import (
	"testing"

	set "github.com/deckarep/golang-set/v2"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/network"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/utils"
)

func Test_metaXDR(t *testing.T) {
	// transfer(from: GDYH62HW5R57ZFCJE77Q32YVUANQPK2A4663BWFVKAIMINNWVV6QEI5P, to: CDSMYK7ADPT32KBXPXWSOWMBANDDFG76IVB4HWHOE2SA3DPAKXA4C6ZR, asset: CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC)/ hash=45593823f5229546618668ed22c8c4e8d3f74a6138f609fa9aa317ad217e64e3
	const ledgerCloseMetaXDR = "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"

	var lcm xdr.LedgerCloseMeta
	err := xdr.SafeUnmarshalBase64(ledgerCloseMetaXDR, &lcm)
	require.NoError(t, err)

	ledgerTxReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(network.TestNetworkPassphrase, lcm)
	require.NoError(t, err)
	ingestTx, err := ledgerTxReader.Read()
	require.NoError(t, err)

	processor := NewParticipantsProcessor(network.TestNetworkPassphrase)
	gotParticipants, err := processor.GetOperationsParticipants(ingestTx)
	require.NoError(t, err)
	for opID, opParticipants := range gotParticipants {
		for _, participant := range opParticipants.Participants.ToSlice() {
			opType := opParticipants.Operation.Body.Type
			t.Logf("opID: %d, opType: %s, participant: %s", opID, opType.String(), participant)
		}
		contractOpParticipants, err := GetContractOpParticipants(opParticipants.Operation, ingestTx)
		require.NoError(t, err)
		t.Logf("contractOpParticipants: %s", contractOpParticipants)
		wantParticipants := []string{"GDYH62HW5R57ZFCJE77Q32YVUANQPK2A4663BWFVKAIMINNWVV6QEI5P", "CDSMYK7ADPT32KBXPXWSOWMBANDDFG76IVB4HWHOE2SA3DPAKXA4C6ZR", "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"}
		require.ElementsMatch(t, wantParticipants, contractOpParticipants)
	}
}

func Test_getScValScvAddresses(t *testing.T) {
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
			result := getScValScvAddresses(tc.scVal)
			assert.Equal(t, tc.wantAddresses.Cardinality(), result.Cardinality())
			assert.ElementsMatch(t, tc.wantAddresses.ToSlice(), result.ToSlice())
		})
	}
}

func Test_getScValParticipants(t *testing.T) {
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
			result, err := getScValParticipants(tc.scVal)
			require.NoError(t, err)
			assert.Equal(t, tc.wantAddresses.Cardinality(), result.Cardinality())
			assert.ElementsMatch(t, tc.wantAddresses.ToSlice(), result.ToSlice())
		})
	}
}

func Test_getAuthEntryParticipants(t *testing.T) {
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

	// GBWAH7AOBZYAYLT76Z7MQDDRRJCCERRVRSCJ4GAEGV2S5W474ZLEOH4U
	accountID2 := xdr.MustAddress("GBWAH7AOBZYAYLT76Z7MQDDRRJCCERRVRSCJ4GAEGV2S5W474ZLEOH4U")
	scAddressAccount2 := xdr.ScAddress{
		Type:      xdr.ScAddressTypeScAddressTypeAccount,
		AccountId: &accountID2,
	}

	testCases := []struct {
		name            string
		authEntries     []xdr.SorobanAuthorizationEntry
		expected        []string
		wantErrContains string
	}{
		{
			name:        "🟢empty_auth_entries",
			authEntries: []xdr.SorobanAuthorizationEntry{},
			expected:    []string{},
		},
		{
			name: "🟢single_account_auth_entry",
			authEntries: []xdr.SorobanAuthorizationEntry{
				{
					Credentials: xdr.SorobanCredentials{
						Type: xdr.SorobanCredentialsTypeSorobanCredentialsAddress,
						Address: utils.PointOf(xdr.SorobanAddressCredentials{
							Address: scAddressAccount1,
						}),
					},
				},
			},
			expected: []string{"GDYH62HW5R57ZFCJE77Q32YVUANQPK2A4663BWFVKAIMINNWVV6QEI5P"},
		},
		{
			name: "🟢single_contract_auth_entry",
			authEntries: []xdr.SorobanAuthorizationEntry{
				{
					Credentials: xdr.SorobanCredentials{
						Type: xdr.SorobanCredentialsTypeSorobanCredentialsAddress,
						Address: utils.PointOf(xdr.SorobanAddressCredentials{
							Address: scAddressContract1,
						}),
					},
				},
			},
			expected: []string{"CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"},
		},
		{
			name: "🟢multiple_auth_entries",
			authEntries: []xdr.SorobanAuthorizationEntry{
				{
					Credentials: xdr.SorobanCredentials{
						Type: xdr.SorobanCredentialsTypeSorobanCredentialsAddress,
						Address: utils.PointOf(xdr.SorobanAddressCredentials{
							Address: scAddressAccount1,
						}),
					},
				},
				{
					Credentials: xdr.SorobanCredentials{
						Type: xdr.SorobanCredentialsTypeSorobanCredentialsAddress,
						Address: utils.PointOf(xdr.SorobanAddressCredentials{
							Address: scAddressContract1,
						}),
					},
				},
				{
					Credentials: xdr.SorobanCredentials{
						Type: xdr.SorobanCredentialsTypeSorobanCredentialsAddress,
						Address: utils.PointOf(xdr.SorobanAddressCredentials{
							Address: scAddressAccount2,
						}),
					},
				},
			},
			expected: []string{
				"GDYH62HW5R57ZFCJE77Q32YVUANQPK2A4663BWFVKAIMINNWVV6QEI5P",
				"CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC",
				"GBWAH7AOBZYAYLT76Z7MQDDRRJCCERRVRSCJ4GAEGV2S5W474ZLEOH4U",
			},
		},
		{
			name: "🟡unsupported_credentials_type_should_be_ignored",
			authEntries: []xdr.SorobanAuthorizationEntry{
				{
					Credentials: xdr.SorobanCredentials{
						Type: xdr.SorobanCredentialsTypeSorobanCredentialsSourceAccount,
						Address: utils.PointOf(xdr.SorobanAddressCredentials{
							Address: scAddressContract1,
						}),
					},
				},
				{
					Credentials: xdr.SorobanCredentials{
						Type: xdr.SorobanCredentialsTypeSorobanCredentialsAddress,
						Address: utils.PointOf(xdr.SorobanAddressCredentials{
							Address: scAddressAccount2,
						}),
					},
				},
			},
			expected: []string{"GBWAH7AOBZYAYLT76Z7MQDDRRJCCERRVRSCJ4GAEGV2S5W474ZLEOH4U"},
		},
		{
			name: "🟢duplicate_addresses_should_be_deduplicated",
			authEntries: []xdr.SorobanAuthorizationEntry{
				{
					Credentials: xdr.SorobanCredentials{
						Type: xdr.SorobanCredentialsTypeSorobanCredentialsAddress,
						Address: utils.PointOf(xdr.SorobanAddressCredentials{
							Address: scAddressAccount1,
						}),
					},
				},
				{
					Credentials: xdr.SorobanCredentials{
						Type: xdr.SorobanCredentialsTypeSorobanCredentialsAddress,
						Address: utils.PointOf(xdr.SorobanAddressCredentials{ // Duplicate
							Address: scAddressAccount1,
						}),
					},
				},
			},
			expected: []string{"GDYH62HW5R57ZFCJE77Q32YVUANQPK2A4663BWFVKAIMINNWVV6QEI5P"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			participants, err := getAuthEntryParticipants(tc.authEntries)

			if tc.wantErrContains != "" {
				assert.Error(t, err)
				assert.ErrorContains(t, err, tc.wantErrContains)
				assert.Empty(t, participants)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, len(tc.expected), participants.Cardinality())
				for _, expectedParticipant := range tc.expected {
					assert.True(t, participants.Contains(expectedParticipant))
				}
			}
		})
	}
}

func Test_getContractID(t *testing.T) {
	testCases := []struct {
		name              string
		networkPassphrase string
		opXDRStr          string
		wantContractID    string
	}{
		{
			name:              "🟢INVOKE_HOST_FUNCTION/CreateContractV2",
			opXDRStr:          "AAAAAAAAABgAAAADAAAAAAAAAAAAAAAAbAP8Dg5wDC5/9n7IDHGKRCJGNYyEnhgENXUu25/mVkfDszyD0xmggy2XywsLdKboM1yzTNxvYPZIRMN/whOT/AAAAABZPFPHak6AnPiuLp9sXpV+KSwSVllBH+VkL93LXY2EqAAAAAEAAAAQAAAAAQAAAAUAAAAPAAAAB0VkMjU1MTkAAAAADQAAACCx6kbMzSf2zoYoDWEaBAMVOOg4C9qyQjO+Rktf48yb8gAAABAAAAABAAAAAQAAAAEAAAAQAAAAAQAAAAEAAAABAAAAEAAAAAEAAAABAAAADwAAAApQZXJzaXN0ZW50AAAAAAABAAAAAAAAAAIAAAAAAAAAAAAAAABsA/wODnAMLn/2fsgMcYpEIkY1jISeGAQ1dS7bn+ZWR8OzPIPTGaCDLZfLCwt0pugzXLNM3G9g9khEw3/CE5P8AAAAAFk8U8dqToCc+K4un2xelX4pLBJWWUEf5WQv3ctdjYSoAAAAAQAAABAAAAABAAAABQAAAA8AAAAHRWQyNTUxOQAAAAANAAAAILHqRszNJ/bOhigNYRoEAxU46DgL2rJCM75GS1/jzJvyAAAAEAAAAAEAAAABAAAAAQAAABAAAAABAAAAAQAAAAEAAAAQAAAAAQAAAAEAAAAPAAAAClBlcnNpc3RlbnQAAAAAAAA=",
			networkPassphrase: network.TestNetworkPassphrase,
			wantContractID:    "CANZKJUEZM22DO2XLJP4ARZAJFG7GJVBIEXJ7T4F2GAIAV4D4RMXMDVD",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var op xdr.Operation
			err := xdr.SafeUnmarshalBase64(opXDRStr, &op)
			require.NoError(t, err)

			gotContractID, err := getContractID(tc.networkPassphrase, op)
			require.NoError(t, err)

			assert.Equal(t, tc.wantContractID, gotContractID)
		})
	}
}

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
}
