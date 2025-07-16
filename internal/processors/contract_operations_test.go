package processors

import (
	"fmt"
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

	contractID, err := calculateContractID(networkPassphrase, xdr.ContractIdPreimageFromAddress{
		Address: makeScAddress("GBWAH7AOBZYAYLT76Z7MQDDRRJCCERRVRSCJ4GAEGV2S5W474ZLEOH4U"),
		Salt:    salt,
	})
	require.NoError(t, err)
	require.Equal(t, "CANZKJUEZM22DO2XLJP4ARZAJFG7GJVBIEXJ7T4F2GAIAV4D4RMXMDVD", contractID)
}

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
		ContractId: utils.PointOf(xdr.ContractId(decoded)),
	}
}

// makeFeeBumpOp updates the envelope type to a fee bump envelope and sets the fee source account.
func makeFeeBumpOp(feeBumpSourceAccount string, baseOp operation_processor.TransactionOperationWrapper) operation_processor.TransactionOperationWrapper {
	op := baseOp
	op.Transaction.Envelope.V0 = nil
	op.Transaction.Envelope.V1 = nil
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

func Test_participantsForSorobanOp_nonSorobanOp(t *testing.T) {
	const txSourceAccount = "GAGWN4445WLODCXT7RUZXJLQK5XWX4GICXDOAAZZGK2N3BR67RIIVWJ7"

	nonSorobanOp := func() operation_processor.TransactionOperationWrapper {
		return operation_processor.TransactionOperationWrapper{
			Network:      network.TestNetworkPassphrase,
			LedgerClosed: time.Now(),
			Operation: xdr.Operation{
				Body: xdr.OperationBody{Type: xdr.OperationTypePayment},
			},
			Transaction: ingest.LedgerTransaction{
				Envelope: xdr.TransactionEnvelope{
					Type: xdr.EnvelopeTypeEnvelopeTypeTx,
					V1: &xdr.TransactionV1Envelope{
						Tx: xdr.Transaction{
							SourceAccount: xdr.MustMuxedAddress(txSourceAccount),
							Ext: xdr.TransactionExt{
								V:           0,
								SorobanData: nil,
							},
						},
					},
				},
			},
		}
	}

	// Test cases
	testCases := []struct {
		name            string
		op              operation_processor.TransactionOperationWrapper
		wantErrContains string
	}{
		{
			name:            "🔴non_soroban_operation",
			op:              nonSorobanOp(),
			wantErrContains: ErrNotSorobanOperation.Error(),
		},
		{
			name:            "🔴feeBump(non_soroban_operation)",
			op:              makeFeeBumpOp(txSourceAccount, nonSorobanOp()),
			wantErrContains: ErrNotSorobanOperation.Error(),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			participants, err := participantsForSorobanOp(tc.op)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErrContains)
			assert.Empty(t, participants)
		})
	}
}

func Test_participantsForSorobanOp_footprintOps(t *testing.T) {
	const (
		txSourceAccount = "GAGWN4445WLODCXT7RUZXJLQK5XWX4GICXDOAAZZGK2N3BR67RIIVWJ7"
		opSourceAccount = "GBKV7KN5K2CJA7TC5AUQNI76JBXHLMQSHT426JEAR3TPVKNSMKMG4RZN"
		accountID1      = "GCTNXY3EZFV2BL4CWHIRSBJVBEYFXANMIDJEVITS66YXOQEF3PL7LHXQ"
		contractID1     = "CBN2MBW4AFEHXMLE5ADTAWFOQKEHBYTVO62AZ7DTQONACYE26VFPHKVA"
		contractID2     = "CCSZ54OHAF6BBBFVKHGA6WFWNQLEBXBVO3JYY4BPRYQTXOYJ7LI3QE4D"
	)

	basicSorobanOp := func() operation_processor.TransactionOperationWrapper {
		return operation_processor.TransactionOperationWrapper{
			Network:      network.TestNetworkPassphrase,
			LedgerClosed: time.Now(),
			Operation:    xdr.Operation{},
			Transaction: ingest.LedgerTransaction{
				Envelope: xdr.TransactionEnvelope{
					Type: xdr.EnvelopeTypeEnvelopeTypeTx,
					V1: &xdr.TransactionV1Envelope{
						Tx: xdr.Transaction{
							SourceAccount: xdr.MustMuxedAddress(txSourceAccount),
							Ext: xdr.TransactionExt{
								V:           1,
								SorobanData: &xdr.SorobanTransactionData{},
							},
						},
					},
				},
			},
		}
	}

	makeContractDataLedgerKey := func(contractID string) xdr.LedgerKey {
		return xdr.LedgerKey{
			Type: xdr.LedgerEntryTypeContractData,
			ContractData: &xdr.LedgerKeyContractData{
				Contract: makeScContract(contractID),
			},
		}
	}

	makeAccountLedgerKey := func(accountID string) xdr.LedgerKey {
		return xdr.LedgerKey{
			Type: xdr.LedgerEntryTypeAccount,
			Account: &xdr.LedgerKeyAccount{
				AccountId: xdr.MustAddress(accountID),
			},
		}
	}

	type TestCase struct {
		name             string
		op               operation_processor.TransactionOperationWrapper
		wantParticipants set.Set[string]
	}

	testCases := []TestCase{}
	for _, feeBump := range []bool{false, true} {
		for _, opType := range []xdr.OperationType{xdr.OperationTypeExtendFootprintTtl, xdr.OperationTypeRestoreFootprint} {
			prefix := opType.String()
			if feeBump {
				prefix = fmt.Sprintf("fee_bump(%s)", prefix)
			}

			testCases = append(testCases,
				TestCase{
					name: fmt.Sprintf("🟢%s/ReadOnly/tx.SourceAccount", prefix),
					op: func() operation_processor.TransactionOperationWrapper {
						op := basicSorobanOp()
						op.Operation.Body.Type = opType
						op.Transaction.Envelope.V1.Tx.Ext.SorobanData.Resources.Footprint.ReadOnly = []xdr.LedgerKey{
							makeContractDataLedgerKey(contractID1),
						}
						if feeBump {
							op = makeFeeBumpOp(txSourceAccount, op)
						}
						return op
					}(),
					wantParticipants: set.NewSet(txSourceAccount, contractID1),
				},
				TestCase{
					name: fmt.Sprintf("🟢%s/ReadOnly/op.SourceAccount", prefix),
					op: func() operation_processor.TransactionOperationWrapper {
						op := basicSorobanOp()
						op.Operation.Body.Type = opType
						op.Operation.SourceAccount = utils.PointOf(xdr.MustMuxedAddress(opSourceAccount))
						op.Transaction.Envelope.V1.Tx.Ext.SorobanData.Resources.Footprint.ReadOnly = []xdr.LedgerKey{
							makeContractDataLedgerKey(contractID1),
						}
						if feeBump {
							op = makeFeeBumpOp(txSourceAccount, op)
						}
						return op
					}(),
					wantParticipants: set.NewSet(opSourceAccount, contractID1),
				},
				TestCase{
					name: fmt.Sprintf("🟢%s/ReadOnly&ReadWrite/tx.SourceAccount", prefix),
					op: func() operation_processor.TransactionOperationWrapper {
						op := basicSorobanOp()
						op.Operation.Body.Type = opType
						op.Transaction.Envelope.V1.Tx.Ext.SorobanData.Resources.Footprint.ReadOnly = []xdr.LedgerKey{
							makeContractDataLedgerKey(contractID1),
							makeAccountLedgerKey(accountID1),
						}
						op.Transaction.Envelope.V1.Tx.Ext.SorobanData.Resources.Footprint.ReadWrite = []xdr.LedgerKey{
							makeContractDataLedgerKey(contractID2),
						}
						if feeBump {
							op = makeFeeBumpOp(txSourceAccount, op)
						}
						return op
					}(),
					wantParticipants: set.NewSet(txSourceAccount, contractID1, accountID1, contractID2),
				},
			)
		}
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			participants, err := participantsForSorobanOp(tc.op)

			require.NoError(t, err)
			assert.Equal(t, tc.wantParticipants, participants)
		})
	}
}

func Test_participantsForSorobanOp_invokeHostFunction_uploadWasm(t *testing.T) {
	const (
		txSourceAccount = "GAGWN4445WLODCXT7RUZXJLQK5XWX4GICXDOAAZZGK2N3BR67RIIVWJ7"
		opSourceAccount = "GBKV7KN5K2CJA7TC5AUQNI76JBXHLMQSHT426JEAR3TPVKNSMKMG4RZN"
	)

	uploadWasmOp := func() operation_processor.TransactionOperationWrapper {
		return operation_processor.TransactionOperationWrapper{
			Network:      network.TestNetworkPassphrase,
			LedgerClosed: time.Now(),
			Operation: xdr.Operation{
				Body: xdr.OperationBody{
					Type: xdr.OperationTypeInvokeHostFunction,
					InvokeHostFunctionOp: &xdr.InvokeHostFunctionOp{
						HostFunction: xdr.HostFunction{
							Type: xdr.HostFunctionTypeHostFunctionTypeUploadContractWasm,
							Wasm: &[]byte{1, 2, 3, 4, 5},
						},
					},
				},
			},
			Transaction: ingest.LedgerTransaction{
				Envelope: xdr.TransactionEnvelope{
					Type: xdr.EnvelopeTypeEnvelopeTypeTx,
					V1: &xdr.TransactionV1Envelope{
						Tx: xdr.Transaction{
							SourceAccount: xdr.MustMuxedAddress(txSourceAccount),
							Ext: xdr.TransactionExt{
								V:           1,
								SorobanData: &xdr.SorobanTransactionData{},
							},
						},
					},
				},
			},
		}
	}

	testCases := []struct {
		name             string
		op               operation_processor.TransactionOperationWrapper
		wantParticipants set.Set[string]
	}{
		{
			name:             "🟢upload_wasm/tx.SourceAccount",
			op:               uploadWasmOp(),
			wantParticipants: set.NewSet(txSourceAccount),
		},
		{
			name: "🟢upload_wasm/tx.SourceAccount",
			op: func() operation_processor.TransactionOperationWrapper {
				op := uploadWasmOp()
				op.Operation.SourceAccount = utils.PointOf(xdr.MustMuxedAddress(opSourceAccount))
				return op
			}(),
			wantParticipants: set.NewSet(opSourceAccount),
		},
		{
			name:             "🟢feeBump(upload_wasm)/tx.SourceAccount",
			op:               makeFeeBumpOp(txSourceAccount, uploadWasmOp()),
			wantParticipants: set.NewSet(txSourceAccount),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			participants, err := participantsForSorobanOp(tc.op)
			require.NoError(t, err)
			assert.Equal(t, tc.wantParticipants, participants)
		})
	}
}

// makeAuthEntries receives a []xdr.ScAddress and returns a []xdr.SorobanAuthorizationEntry. It also populates the
// SorobanAuthorizedFunction's type and function fields based on the operation provided.
func makeAuthEntries(t *testing.T, op *operation_processor.TransactionOperationWrapper, authAccounts ...xdr.ScAddress) []xdr.SorobanAuthorizationEntry {
	t.Helper()
	sorobanAuthFn := xdr.SorobanAuthorizedFunction{}
	switch op.Operation.Body.InvokeHostFunctionOp.HostFunction.Type {
	case xdr.HostFunctionTypeHostFunctionTypeCreateContract:
		sorobanAuthFn.Type = xdr.SorobanAuthorizedFunctionTypeSorobanAuthorizedFunctionTypeCreateContractHostFn
		sorobanAuthFn.CreateContractHostFn = op.Operation.Body.InvokeHostFunctionOp.HostFunction.CreateContract
	case xdr.HostFunctionTypeHostFunctionTypeCreateContractV2:
		sorobanAuthFn.Type = xdr.SorobanAuthorizedFunctionTypeSorobanAuthorizedFunctionTypeCreateContractV2HostFn
		sorobanAuthFn.CreateContractV2HostFn = op.Operation.Body.InvokeHostFunctionOp.HostFunction.CreateContractV2
	case xdr.HostFunctionTypeHostFunctionTypeInvokeContract:
		sorobanAuthFn.Type = xdr.SorobanAuthorizedFunctionTypeSorobanAuthorizedFunctionTypeContractFn
		sorobanAuthFn.ContractFn = op.Operation.Body.InvokeHostFunctionOp.HostFunction.InvokeContract
	default:
		require.Fail(t, "unsupported host function type", "host function type: %s", op.Operation.Body.InvokeHostFunctionOp.HostFunction.Type)
	}

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

// Test addresses used in the subInvocations:
const (
	deployerAccountID  = "GAGWN4445WLODCXT7RUZXJLQK5XWX4GICXDOAAZZGK2N3BR67RIIVWJ7"
	deployedContractID = "CCUWLGAV43F52A2ZYHRWIWNCNMSZBGWEUTWRKEX5SHJXK74GFSZFGPZY"
	accountID1         = "GCTNXY3EZFV2BL4CWHIRSBJVBEYFXANMIDJEVITS66YXOQEF3PL7LHXQ"
	accountID2         = "GBKV7KN5K2CJA7TC5AUQNI76JBXHLMQSHT426JEAR3TPVKNSMKMG4RZN"
	contractID1        = "CBN2MBW4AFEHXMLE5ADTAWFOQKEHBYTVO62AZ7DTQONACYE26VFPHKVA"
	contractID2        = "CCSZ54OHAF6BBBFVKHGA6WFWNQLEBXBVO3JYY4BPRYQTXOYJ7LI3QE4D"
	contractID3        = "CAXR4FCMM4RTFCOHZ3EOFEOQHDHMBLSZQBXFTX2OWHDQWO5IFCFF6Z3K"
	xlmSACID           = "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"
)

// includeSubInvocations will add subInvocations to any existing SorobanAuthorizationEntry. After adding the subinvocations,
// the following addresses are expected to be present:
// [deployerAccountID, deployedContractID, contractID1, contractID3, xlmSACID]
func includeSubInvocations(baseOp operation_processor.TransactionOperationWrapper) operation_processor.TransactionOperationWrapper {
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
							Args:            xdr.ScVec{xdr.ScVal{Type: xdr.ScValTypeScvAddress, Address: utils.PointOf(makeScContract(contractID2))}}, // <--- contractID2 (args addresses are not returned)
						},
					},
					SubInvocations: nil,
				},
				{
					Function: xdr.SorobanAuthorizedFunction{
						Type: xdr.SorobanAuthorizedFunctionTypeSorobanAuthorizedFunctionTypeCreateContractV2HostFn,
						CreateContractV2HostFn: &xdr.CreateContractArgsV2{
							ConstructorArgs: []xdr.ScVal{
								{Type: xdr.ScValTypeScvAddress, Address: utils.PointOf(makeScAddress(accountID1))}, // <--- accountID1 (args addresses are not returned)
							},
							ContractIdPreimage: xdr.ContractIdPreimage{
								Type: xdr.ContractIdPreimageTypeContractIdPreimageFromAddress,
								FromAddress: &xdr.ContractIdPreimageFromAddress{ // <--- deployedContractID
									Address: makeScAddress(deployerAccountID), //   <--- deployerAccountID
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
					ContractAddress: makeScContract(contractID3),
					FunctionName:    xdr.ScSymbol("sub_fn"),
					Args:            xdr.ScVec{xdr.ScVal{Type: xdr.ScValTypeScvAddress, Address: utils.PointOf(makeScAddress(accountID2))}}, // <--- accountID2 (args addresses are not returned)
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

func Test_participantsForSorobanOp_invokeHostFunction_createContract(t *testing.T) {
	const (
		txSourceAccount       = "GAUE24B36YYY3CXTXNFE3IFXU6EE4NUOS5L744IWGTNXVXZAXFGMP6CC"
		opSourceAccount       = "GBZURSTQQRSU3XB66CHJ3SH2ZWLG663V5SWM6HF3FL72BOMYHDT4QTUF"
		fromSourceAccount     = "GCQIH6MRLCJREVE76LVTKKEZXRIT6KSX7KU65HPDDBYFKFYHIYSJE57R"
		authSignerAccount     = "GDG2KKXC62BINMUZNBTLG235323N6BOIR33JBF4ELTOUKUG5BDE6HJZT"
		usdcSACContractID     = "CBIELTK6YBZJU5UP2WWQEUCYKLPU6AUNZ2BQ4WWFEIE3USCIHMXQDAMA"
		constructorAccountID  = "GAHPYWLK6YRN7CVYZOO4H3VDRZ7PVF5UJGLZCSPAEIKJE2XSWF5LAGER"
		constructorContractID = "CDNVQW44C3HALYNVQ4SOBXY5EWYTGVYXX6JPESOLQDABJI5FC5LTRRUE"
	)
	usdcAsset := xdr.Asset{
		Type: xdr.AssetTypeAssetTypeCreditAlphanum4,
		AlphaNum4: &xdr.AlphaNum4{
			AssetCode: [4]byte{'U', 'S', 'D', 'C'},
			Issuer:    xdr.MustAddress("GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5"),
		},
	}
	salt := xdr.Uint256{195, 179, 60, 131, 211, 25, 160, 131, 45, 151, 203, 11, 11, 116, 166, 232, 51, 92, 179, 76, 220, 111, 96, 246, 72, 68, 195, 127, 194, 19, 147, 252}

	basicSorobanOp := func() operation_processor.TransactionOperationWrapper {
		return operation_processor.TransactionOperationWrapper{
			Network:      network.TestNetworkPassphrase,
			LedgerClosed: time.Now(),
			Operation: xdr.Operation{
				Body: xdr.OperationBody{
					Type: xdr.OperationTypeInvokeHostFunction,
					InvokeHostFunctionOp: &xdr.InvokeHostFunctionOp{
						HostFunction: xdr.HostFunction{},
						Auth:         []xdr.SorobanAuthorizationEntry{},
					},
				},
			},
			Transaction: ingest.LedgerTransaction{
				Envelope: xdr.TransactionEnvelope{
					Type: xdr.EnvelopeTypeEnvelopeTypeTx,
					V1: &xdr.TransactionV1Envelope{
						Tx: xdr.Transaction{
							SourceAccount: xdr.MustMuxedAddress(txSourceAccount),
							Ext: xdr.TransactionExt{
								V:           1,
								SorobanData: &xdr.SorobanTransactionData{},
							},
						},
					},
				},
			},
		}
	}

	setFromAddress := func(op *operation_processor.TransactionOperationWrapper, hostFnType xdr.HostFunctionType, fromSourceAccount string) {
		op.Operation.Body.InvokeHostFunctionOp.HostFunction.Type = hostFnType
		preimage := xdr.ContractIdPreimage{
			Type: xdr.ContractIdPreimageTypeContractIdPreimageFromAddress,
			FromAddress: &xdr.ContractIdPreimageFromAddress{
				Address: makeScAddress(fromSourceAccount),
				Salt:    salt,
			},
		}

		switch hostFnType {
		case xdr.HostFunctionTypeHostFunctionTypeCreateContract:
			op.Operation.Body.InvokeHostFunctionOp.HostFunction.CreateContract = &xdr.CreateContractArgs{
				ContractIdPreimage: preimage,
			}
		case xdr.HostFunctionTypeHostFunctionTypeCreateContractV2:
			op.Operation.Body.InvokeHostFunctionOp.HostFunction.CreateContractV2 = &xdr.CreateContractArgsV2{
				ContractIdPreimage: preimage,
			}
		default:
			require.Fail(t, "unsupported host function type", "host function type: %s", hostFnType)
		}
	}

	setFromAsset := func(op *operation_processor.TransactionOperationWrapper, hostFnType xdr.HostFunctionType, asset xdr.Asset) {
		op.Operation.Body.InvokeHostFunctionOp.HostFunction.Type = hostFnType
		preimage := xdr.ContractIdPreimage{
			Type:      xdr.ContractIdPreimageTypeContractIdPreimageFromAsset,
			FromAsset: &asset,
		}

		switch hostFnType {
		case xdr.HostFunctionTypeHostFunctionTypeCreateContract:
			op.Operation.Body.InvokeHostFunctionOp.HostFunction.CreateContract = &xdr.CreateContractArgs{
				ContractIdPreimage: preimage,
			}
		case xdr.HostFunctionTypeHostFunctionTypeCreateContractV2:
			op.Operation.Body.InvokeHostFunctionOp.HostFunction.CreateContractV2 = &xdr.CreateContractArgsV2{
				ContractIdPreimage: preimage,
			}
		default:
			require.Fail(t, "unsupported host function type", "host function type: %s", hostFnType)
		}
	}

	type TestCase struct {
		name             string
		op               operation_processor.TransactionOperationWrapper
		wantParticipants set.Set[string]
	}

	testCases := []TestCase{}
	for _, withSubinvocations := range []bool{false, true} {
		for _, feeBump := range []bool{false, true} {
			for _, hostFnType := range []xdr.HostFunctionType{xdr.HostFunctionTypeHostFunctionTypeCreateContract, xdr.HostFunctionTypeHostFunctionTypeCreateContractV2} {
				prefix := hostFnType.String()
				subInvocationsParticipants := set.NewSet[string]()
				if withSubinvocations {
					prefix = fmt.Sprintf("%s,withSubinvocations🔄", prefix)
					subInvocationsParticipants = set.NewSet(deployerAccountID, contractID1, deployedContractID, contractID3, xlmSACID, authSignerAccount)
				}
				if feeBump {
					prefix = fmt.Sprintf("feeBump(%s)", prefix)
				}
				testCases = append(testCases,
					TestCase{
						name: fmt.Sprintf("🟢%s/FromAddress/tx.SourceAccount", prefix),
						op: func() operation_processor.TransactionOperationWrapper {
							op := basicSorobanOp()
							setFromAddress(&op, hostFnType, fromSourceAccount)
							if withSubinvocations {
								op.Operation.Body.InvokeHostFunctionOp.Auth = makeAuthEntries(t, &op, makeScAddress(authSignerAccount))
								op = includeSubInvocations(op)
							}
							if feeBump {
								op = makeFeeBumpOp(txSourceAccount, op)
							}
							return op
						}(),
						wantParticipants: set.NewSet(txSourceAccount, fromSourceAccount, "CA7UGIYR2H63C2ETN2VE4WDQ6YX5XNEWNWC2DP7A64B2ZR7VJJWF3SBF").Union(subInvocationsParticipants),
					},
					TestCase{
						name: fmt.Sprintf("🟢%s/FromAddress/op.SourceAccount", prefix),
						op: func() operation_processor.TransactionOperationWrapper {
							op := basicSorobanOp()
							op.Operation.SourceAccount = utils.PointOf(xdr.MustMuxedAddress(opSourceAccount))
							setFromAddress(&op, hostFnType, fromSourceAccount)
							if withSubinvocations {
								op.Operation.Body.InvokeHostFunctionOp.Auth = makeAuthEntries(t, &op, makeScAddress(authSignerAccount))
								op = includeSubInvocations(op)
							}
							if feeBump {
								op = makeFeeBumpOp(txSourceAccount, op)
							}
							return op
						}(),
						wantParticipants: set.NewSet(opSourceAccount, fromSourceAccount, "CA7UGIYR2H63C2ETN2VE4WDQ6YX5XNEWNWC2DP7A64B2ZR7VJJWF3SBF").Union(subInvocationsParticipants),
					},
					TestCase{
						name: fmt.Sprintf("🟢%s/FromAsset/tx.SourceAccount", prefix),
						op: func() operation_processor.TransactionOperationWrapper {
							op := basicSorobanOp()
							setFromAsset(&op, hostFnType, usdcAsset)
							if withSubinvocations {
								op.Operation.Body.InvokeHostFunctionOp.Auth = makeAuthEntries(t, &op, makeScAddress(authSignerAccount))
								op = includeSubInvocations(op)
							}
							if feeBump {
								op = makeFeeBumpOp(txSourceAccount, op)
							}
							return op
						}(),
						wantParticipants: set.NewSet(txSourceAccount, usdcSACContractID).Union(subInvocationsParticipants),
					},
				)
			}
		}
	}
	testCases = append(testCases, TestCase{
		name: fmt.Sprintf("🟢%s.ConstructorArgs/FromAccount/op.SourceAccount", xdr.HostFunctionTypeHostFunctionTypeCreateContractV2),
		op: func() operation_processor.TransactionOperationWrapper {
			op := basicSorobanOp()
			setFromAddress(&op, xdr.HostFunctionTypeHostFunctionTypeCreateContractV2, fromSourceAccount)
			op.Operation.Body.InvokeHostFunctionOp.HostFunction.CreateContractV2.ConstructorArgs = []xdr.ScVal{ // <--- args addresses are not returned
				{Type: xdr.ScValTypeScvAddress, Address: utils.PointOf(makeScAddress(constructorAccountID))},
				{Type: xdr.ScValTypeScvAddress, Address: utils.PointOf(makeScContract(constructorContractID))},
			}
			return op
		}(),
		wantParticipants: set.NewSet(
			txSourceAccount, fromSourceAccount, "CA7UGIYR2H63C2ETN2VE4WDQ6YX5XNEWNWC2DP7A64B2ZR7VJJWF3SBF",
		),
	})

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			participants, err := participantsForSorobanOp(tc.op)

			require.NoError(t, err)
			assert.Equal(t, tc.wantParticipants, participants)
		})
	}
}

func Test_participantsForSorobanOp_invokeHostFunction_invokeContract(t *testing.T) {
	const (
		txSourceAccount   = "GAUE24B36YYY3CXTXNFE3IFXU6EE4NUOS5L744IWGTNXVXZAXFGMP6CC"
		opSourceAccount   = "GBZURSTQQRSU3XB66CHJ3SH2ZWLG663V5SWM6HF3FL72BOMYHDT4QTUF"
		argAccountID1     = "GCQIH6MRLCJREVE76LVTKKEZXRIT6KSX7KU65HPDDBYFKFYHIYSJE57R"
		argAccountID2     = "GDG2KKXC62BINMUZNBTLG235323N6BOIR33JBF4ELTOUKUG5BDE6HJZT"
		argContractID1    = "CBIELTK6YBZJU5UP2WWQEUCYKLPU6AUNZ2BQ4WWFEIE3USCIHMXQDAMA"
		argContractID2    = "CDNVQW44C3HALYNVQ4SOBXY5EWYTGVYXX6JPESOLQDABJI5FC5LTRRUE"
		authSignerAccount = "GDG2KKXC62BINMUZNBTLG235323N6BOIR33JBF4ELTOUKUG5BDE6HJZT"
		invokedContractID = "CBL6KD2LFMLAUKFFWNNXWOXFN73GAXLEA4WMJRLQ5L76DMYTM3KWQVJN"
	)

	makeInvokeContractOp := func(argAddresses ...xdr.ScAddress) operation_processor.TransactionOperationWrapper {
		return operation_processor.TransactionOperationWrapper{
			Network:      network.TestNetworkPassphrase,
			LedgerClosed: time.Now(),
			Operation: xdr.Operation{
				Body: xdr.OperationBody{
					Type: xdr.OperationTypeInvokeHostFunction,
					InvokeHostFunctionOp: &xdr.InvokeHostFunctionOp{
						HostFunction: xdr.HostFunction{
							Type: xdr.HostFunctionTypeHostFunctionTypeInvokeContract,

							InvokeContract: &xdr.InvokeContractArgs{
								ContractAddress: makeScContract(invokedContractID),
								FunctionName:    xdr.ScSymbol("authorized_fn"),
								Args: func() []xdr.ScVal {
									args := make([]xdr.ScVal, len(argAddresses))
									for i, argAddress := range argAddresses {
										args[i] = xdr.ScVal{Type: xdr.ScValTypeScvAddress, Address: utils.PointOf(argAddress)}
									}
									return args
								}(),
							},
						},
						Auth: []xdr.SorobanAuthorizationEntry{},
					},
				},
			},
			Transaction: ingest.LedgerTransaction{
				Envelope: xdr.TransactionEnvelope{
					Type: xdr.EnvelopeTypeEnvelopeTypeTx,
					V1: &xdr.TransactionV1Envelope{
						Tx: xdr.Transaction{
							SourceAccount: xdr.MustMuxedAddress(txSourceAccount),
							Ext: xdr.TransactionExt{
								V:           1,
								SorobanData: &xdr.SorobanTransactionData{},
							},
						},
					},
				},
			},
		}
	}

	type TestCase struct {
		name             string
		op               operation_processor.TransactionOperationWrapper
		wantParticipants set.Set[string]
	}

	testCases := []TestCase{}
	for _, withSubinvocations := range []bool{false, true} {
		for _, feeBump := range []bool{false, true} {
			prefix := ""
			subInvocationsParticipants := set.NewSet[string]()
			if withSubinvocations {
				prefix = "🔄WithSubinvocations🔄"
				subInvocationsParticipants = set.NewSet(deployerAccountID, contractID1, deployedContractID, contractID3, xlmSACID, authSignerAccount)
			}
			if feeBump {
				prefix = fmt.Sprintf("feeBump(%s)", prefix)
			}
			testCases = append(testCases,
				TestCase{
					name: fmt.Sprintf("🟢%s/tx.SourceAccount", prefix),
					op: func() operation_processor.TransactionOperationWrapper {
						op := makeInvokeContractOp(makeScAddress(argAccountID1), makeScAddress(argAccountID2))
						if withSubinvocations {
							op.Operation.Body.InvokeHostFunctionOp.Auth = makeAuthEntries(t, &op, makeScAddress(authSignerAccount))
							op = includeSubInvocations(op)
						}
						if feeBump {
							op = makeFeeBumpOp(txSourceAccount, op)
						}
						return op
					}(),
					wantParticipants: set.NewSet(txSourceAccount, invokedContractID).Union(subInvocationsParticipants),
				},
				TestCase{
					name: fmt.Sprintf("🟢%s/op.SourceAccount", prefix),
					op: func() operation_processor.TransactionOperationWrapper {
						op := makeInvokeContractOp(makeScContract(argContractID1), makeScContract(argContractID2))
						op.Operation.SourceAccount = utils.PointOf(xdr.MustMuxedAddress(opSourceAccount))
						if withSubinvocations {
							op.Operation.Body.InvokeHostFunctionOp.Auth = makeAuthEntries(t, &op, makeScAddress(authSignerAccount))
							op = includeSubInvocations(op)
						}
						if feeBump {
							op = makeFeeBumpOp(txSourceAccount, op)
						}
						return op
					}(),
					wantParticipants: set.NewSet(opSourceAccount, invokedContractID).Union(subInvocationsParticipants),
				},
			)
		}
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			participants, err := participantsForSorobanOp(tc.op)

			require.NoError(t, err)
			assert.Equal(t, tc.wantParticipants, participants)
		})
	}
}
