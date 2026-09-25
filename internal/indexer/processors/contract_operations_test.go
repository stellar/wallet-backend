package processors

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	set "github.com/deckarep/golang-set/v2"
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/utils"
)

func Test_calculateContractID(t *testing.T) {
	salt := xdr.Uint256{195, 179, 60, 131, 211, 25, 160, 131, 45, 151, 203, 11, 11, 116, 166, 232, 51, 92, 179, 76, 220, 111, 96, 246, 72, 68, 195, 127, 194, 19, 147, 252}

	contractID, err := calculateContractID(network.TestNetworkPassphrase, xdr.ContractIdPreimageFromAddress{
		Address: makeScAddress("GBWAH7AOBZYAYLT76Z7MQDDRRJCCERRVRSCJ4GAEGV2S5W474ZLEOH4U"),
		Salt:    salt,
	})
	require.NoError(t, err)
	require.Equal(t, "CANZKJUEZM22DO2XLJP4ARZAJFG7GJVBIEXJ7T4F2GAIAV4D4RMXMDVD", contractID)
}

func Test_participantsForSorobanOp_nonSorobanOp(t *testing.T) {
	const txSourceAccount = "GAGWN4445WLODCXT7RUZXJLQK5XWX4GICXDOAAZZGK2N3BR67RIIVWJ7"

	nonSorobanOp := func() *TransactionOperationWrapper {
		return &TransactionOperationWrapper{
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
		op              *TransactionOperationWrapper
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

	basicSorobanOp := func() *TransactionOperationWrapper {
		return &TransactionOperationWrapper{
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
		op               *TransactionOperationWrapper
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
					op: func() *TransactionOperationWrapper {
						op := basicSorobanOp()
						op.Operation.Body.Type = opType
						op.Transaction.Envelope.V1.Tx.Ext.SorobanData.Resources.Footprint.ReadOnly = []xdr.LedgerKey{
							makeContractDataLedgerKey(contractID1), // <--- footprint is not returned
						}
						if feeBump {
							op = makeFeeBumpOp(txSourceAccount, op)
						}
						return op
					}(),
					wantParticipants: set.NewThreadUnsafeSet(txSourceAccount),
				},
				TestCase{
					name: fmt.Sprintf("🟢%s/ReadOnly/op.SourceAccount", prefix),
					op: func() *TransactionOperationWrapper {
						op := basicSorobanOp()
						op.Operation.Body.Type = opType
						op.Operation.SourceAccount = utils.PointOf(xdr.MustMuxedAddress(opSourceAccount))
						op.Transaction.Envelope.V1.Tx.Ext.SorobanData.Resources.Footprint.ReadOnly = []xdr.LedgerKey{
							makeContractDataLedgerKey(contractID1), // <--- footprint is not returned
						}
						if feeBump {
							op = makeFeeBumpOp(txSourceAccount, op)
						}
						return op
					}(),
					wantParticipants: set.NewThreadUnsafeSet(opSourceAccount),
				},
				TestCase{
					name: fmt.Sprintf("🟢%s/ReadOnly&ReadWrite/tx.SourceAccount", prefix),
					op: func() *TransactionOperationWrapper {
						op := basicSorobanOp()
						op.Operation.Body.Type = opType
						op.Transaction.Envelope.V1.Tx.Ext.SorobanData.Resources.Footprint.ReadOnly = []xdr.LedgerKey{
							makeContractDataLedgerKey(contractID1), // <--- footprint is not returned
							makeAccountLedgerKey(accountID1),       // <--- footprint is not returned
						}
						op.Transaction.Envelope.V1.Tx.Ext.SorobanData.Resources.Footprint.ReadWrite = []xdr.LedgerKey{
							makeContractDataLedgerKey(contractID2), // <--- footprint is not returned
						}
						if feeBump {
							op = makeFeeBumpOp(txSourceAccount, op)
						}
						return op
					}(),
					wantParticipants: set.NewThreadUnsafeSet(txSourceAccount),
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
	const opSourceAccount = "GBKV7KN5K2CJA7TC5AUQNI76JBXHLMQSHT426JEAR3TPVKNSMKMG4RZN"

	uploadWasmOp := func() *TransactionOperationWrapper {
		op := makeBasicSorobanOp()
		op.Operation = xdr.Operation{
			Body: xdr.OperationBody{
				Type: xdr.OperationTypeInvokeHostFunction,
				InvokeHostFunctionOp: &xdr.InvokeHostFunctionOp{
					HostFunction: xdr.HostFunction{
						Type: xdr.HostFunctionTypeHostFunctionTypeUploadContractWasm,
						Wasm: &[]byte{1, 2, 3, 4, 5},
					},
				},
			},
		}
		return op
	}

	testCases := []struct {
		name             string
		op               *TransactionOperationWrapper
		wantParticipants set.Set[string]
	}{
		{
			name:             "🟢upload_wasm/tx.SourceAccount",
			op:               uploadWasmOp(),
			wantParticipants: set.NewThreadUnsafeSet(txSourceAccount),
		},
		{
			name: "🟢upload_wasm/tx.SourceAccount",
			op: func() *TransactionOperationWrapper {
				op := uploadWasmOp()
				op.Operation.SourceAccount = utils.PointOf(xdr.MustMuxedAddress(opSourceAccount))
				return op
			}(),
			wantParticipants: set.NewThreadUnsafeSet(opSourceAccount),
		},
		{
			name:             "🟢feeBump(upload_wasm)/tx.SourceAccount",
			op:               makeFeeBumpOp(txSourceAccount, uploadWasmOp()),
			wantParticipants: set.NewThreadUnsafeSet(txSourceAccount),
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
func makeAuthEntries(t *testing.T, op *TransactionOperationWrapper, authAccounts ...xdr.ScAddress) []xdr.SorobanAuthorizationEntry {
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
)

// includeSubInvocations will add subInvocations to any existing SorobanAuthorizationEntry. After adding the subinvocations,
// the tree names these addresses, none of which becomes a participant:
// deployerAccountID, deployedContractID, contractID1, contractID3 and the XLM SAC.
func includeSubInvocations(op *TransactionOperationWrapper) {
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
}

func Test_participantsForSorobanOp_invokeHostFunction_createContract(t *testing.T) {
	const (
		opSourceAccount       = "GBZURSTQQRSU3XB66CHJ3SH2ZWLG663V5SWM6HF3FL72BOMYHDT4QTUF"
		fromSourceAccount     = "GCQIH6MRLCJREVE76LVTKKEZXRIT6KSX7KU65HPDDBYFKFYHIYSJE57R"
		constructorAccountID  = "GAHPYWLK6YRN7CVYZOO4H3VDRZ7PVF5UJGLZCSPAEIKJE2XSWF5LAGER"
		constructorContractID = "CDNVQW44C3HALYNVQ4SOBXY5EWYTGVYXX6JPESOLQDABJI5FC5LTRRUE"
	)

	type TestCase struct {
		name             string
		op               *TransactionOperationWrapper
		wantParticipants set.Set[string]
	}

	testCases := []TestCase{}
	for _, feeBump := range []bool{false, true} {
		for _, hostFnType := range []xdr.HostFunctionType{xdr.HostFunctionTypeHostFunctionTypeCreateContract, xdr.HostFunctionTypeHostFunctionTypeCreateContractV2} {
			prefix := strings.ReplaceAll(hostFnType.String(), "HostFunctionTypeHostFunctionType", "")
			if feeBump {
				prefix = fmt.Sprintf("feeBump(%s)", prefix)
			}
			testCases = append(testCases,
				TestCase{
					name: fmt.Sprintf("🟢%s/FromAddress/tx.SourceAccount", prefix),
					op: func() *TransactionOperationWrapper {
						op := makeBasicSorobanOp()
						setFromAddress(op, hostFnType, fromSourceAccount)
						if feeBump {
							op = makeFeeBumpOp(txSourceAccount, op)
						}
						return op
					}(),
					wantParticipants: set.NewThreadUnsafeSet(txSourceAccount),
				},
				TestCase{
					name: fmt.Sprintf("🟢%s/FromAddress/op.SourceAccount", prefix),
					op: func() *TransactionOperationWrapper {
						op := makeBasicSorobanOp()
						op.Operation.SourceAccount = utils.PointOf(xdr.MustMuxedAddress(opSourceAccount))
						setFromAddress(op, hostFnType, fromSourceAccount)
						if feeBump {
							op = makeFeeBumpOp(txSourceAccount, op)
						}
						return op
					}(),
					wantParticipants: set.NewThreadUnsafeSet(opSourceAccount),
				},
				TestCase{
					name: fmt.Sprintf("🟢%s/FromAsset/tx.SourceAccount", prefix),
					op: func() *TransactionOperationWrapper {
						op := makeBasicSorobanOp()
						setFromAsset(op, hostFnType, usdcAssetTestnet)
						if feeBump {
							op = makeFeeBumpOp(txSourceAccount, op)
						}
						return op
					}(),
					wantParticipants: set.NewThreadUnsafeSet(txSourceAccount),
				},
			)
		}
	}
	testCases = append(testCases, TestCase{
		name: fmt.Sprintf("🟢%s.ConstructorArgs/FromAccount/op.SourceAccount", xdr.HostFunctionTypeHostFunctionTypeCreateContractV2),
		op: func() *TransactionOperationWrapper {
			op := makeBasicSorobanOp()
			setFromAddress(op, xdr.HostFunctionTypeHostFunctionTypeCreateContractV2, fromSourceAccount)
			op.Operation.Body.InvokeHostFunctionOp.HostFunction.CreateContractV2.ConstructorArgs = []xdr.ScVal{ // <--- args addresses are not returned
				{Type: xdr.ScValTypeScvAddress, Address: utils.PointOf(makeScAddress(constructorAccountID))},
				{Type: xdr.ScValTypeScvAddress, Address: utils.PointOf(makeScContract(constructorContractID))},
			}
			return op
		}(),
		wantParticipants: set.NewThreadUnsafeSet(txSourceAccount),
	})

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			participants, err := participantsForSorobanOp(tc.op)

			require.NoError(t, err)
			assert.Equal(t, tc.wantParticipants, participants)
		})
	}
}

// invokedContractID is the contract makeInvokeContractOp invokes.
const invokedContractID = "CBL6KD2LFMLAUKFFWNNXWOXFN73GAXLEA4WMJRLQ5L76DMYTM3KWQVJN"

// makeInvokeContractOp builds an InvokeContract operation on invokedContractID whose
// arguments are the given addresses. Argument addresses are never participants.
func makeInvokeContractOp(argAddresses ...xdr.ScAddress) *TransactionOperationWrapper {
	op := makeBasicSorobanOp()
	op.Operation = xdr.Operation{
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
	}
	return op
}

func Test_participantsForSorobanOp_invokeHostFunction_invokeContract(t *testing.T) {
	const (
		opSourceAccount = "GBZURSTQQRSU3XB66CHJ3SH2ZWLG663V5SWM6HF3FL72BOMYHDT4QTUF"
		argAccountID1   = "GCQIH6MRLCJREVE76LVTKKEZXRIT6KSX7KU65HPDDBYFKFYHIYSJE57R"
		argAccountID2   = "GDG2KKXC62BINMUZNBTLG235323N6BOIR33JBF4ELTOUKUG5BDE6HJZT"
		argContractID1  = "CBIELTK6YBZJU5UP2WWQEUCYKLPU6AUNZ2BQ4WWFEIE3USCIHMXQDAMA"
		argContractID2  = "CDNVQW44C3HALYNVQ4SOBXY5EWYTGVYXX6JPESOLQDABJI5FC5LTRRUE"
	)

	type TestCase struct {
		name             string
		op               *TransactionOperationWrapper
		wantParticipants set.Set[string]
	}

	testCases := []TestCase{}
	for _, feeBump := range []bool{false, true} {
		prefix := ""
		if feeBump {
			prefix = "feeBump"
		}
		testCases = append(testCases,
			TestCase{
				name: fmt.Sprintf("🟢%s/tx.SourceAccount", prefix),
				op: func() *TransactionOperationWrapper {
					op := makeInvokeContractOp(makeScAddress(argAccountID1), makeScAddress(argAccountID2))
					if feeBump {
						op = makeFeeBumpOp(txSourceAccount, op)
					}
					return op
				}(),
				wantParticipants: set.NewThreadUnsafeSet(txSourceAccount),
			},
			TestCase{
				name: fmt.Sprintf("🟢%s/op.SourceAccount", prefix),
				op: func() *TransactionOperationWrapper {
					op := makeInvokeContractOp(makeScContract(argContractID1), makeScContract(argContractID2))
					op.Operation.SourceAccount = utils.PointOf(xdr.MustMuxedAddress(opSourceAccount))
					if feeBump {
						op = makeFeeBumpOp(txSourceAccount, op)
					}
					return op
				}(),
				wantParticipants: set.NewThreadUnsafeSet(opSourceAccount),
			},
		)
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			participants, err := participantsForSorobanOp(tc.op)

			require.NoError(t, err)
			assert.Equal(t, tc.wantParticipants, participants)
		})
	}
}

func Test_participantsForSorobanOp_muxedSource(t *testing.T) {
	const invokedContractID = "CBL6KD2LFMLAUKFFWNNXWOXFN73GAXLEA4WMJRLQ5L76DMYTM3KWQVJN"
	muxedSource := makeMuxedAccount(txSourceAccount, 42)

	createContractBody := func(hostFnType xdr.HostFunctionType) xdr.OperationBody {
		op := makeBasicSorobanOp()
		setFromAddress(op, hostFnType, txSourceAccount)
		return op.Operation.Body
	}

	bodies := []struct {
		name string
		body xdr.OperationBody
	}{
		{
			name: "createContractV1",
			body: createContractBody(xdr.HostFunctionTypeHostFunctionTypeCreateContract),
		},
		{
			name: "createContractV2",
			body: createContractBody(xdr.HostFunctionTypeHostFunctionTypeCreateContractV2),
		},
		{
			name: "invokeContract",
			body: xdr.OperationBody{
				Type: xdr.OperationTypeInvokeHostFunction,
				InvokeHostFunctionOp: &xdr.InvokeHostFunctionOp{
					HostFunction: xdr.HostFunction{
						Type: xdr.HostFunctionTypeHostFunctionTypeInvokeContract,
						InvokeContract: &xdr.InvokeContractArgs{
							ContractAddress: makeScContract(invokedContractID),
							FunctionName:    xdr.ScSymbol("fn"),
						},
					},
				},
			},
		},
		{
			name: "extendFootprintTtl",
			body: xdr.OperationBody{Type: xdr.OperationTypeExtendFootprintTtl, ExtendFootprintTtlOp: &xdr.ExtendFootprintTtlOp{}},
		},
		{
			name: "restoreFootprint",
			body: xdr.OperationBody{Type: xdr.OperationTypeRestoreFootprint, RestoreFootprintOp: &xdr.RestoreFootprintOp{}},
		},
	}
	sources := []struct {
		name  string
		apply func(op *TransactionOperationWrapper)
	}{
		{
			name:  "muxedOpSource",
			apply: func(op *TransactionOperationWrapper) { op.Operation.SourceAccount = &muxedSource },
		},
		{
			name:  "muxedTxSource",
			apply: func(op *TransactionOperationWrapper) { op.Transaction.Envelope.V1.Tx.SourceAccount = muxedSource },
		},
	}

	for _, b := range bodies {
		for _, s := range sources {
			t.Run(b.name+"/"+s.name, func(t *testing.T) {
				op := makeBasicSorobanOp()
				op.Operation = xdr.Operation{Body: b.body}
				s.apply(op)
				require.Equal(t, muxedSource.Address(), op.SourceAccount().Address(), "the op source must resolve to the muxed account")

				participants, err := participantsForSorobanOp(op)
				require.NoError(t, err)
				assert.Equal(t, set.NewThreadUnsafeSet(txSourceAccount), participants)

				requireNoEncodedKeyCollision(t, participants)
			})
		}
	}
}

// Test_participantsForSorobanOp_authorizersFromNonceEntries: an address that authorised the
// invocation is a participant because the host wrote its nonce entry, whether it is an
// account (G) or a custom-account contract (C).
func Test_participantsForSorobanOp_authorizersFromNonceEntries(t *testing.T) {
	op := makeInvokeContractOp()
	setOperationMeta(op, xdr.LedgerEntryChanges{
		nonceEntryCreated(makeScAddress(accountID1), 1),
		nonceEntryCreated(makeScContract(contractID1), 2),
	}, nil)

	participants, err := participantsForSorobanOp(op)
	require.NoError(t, err)
	assert.Equal(t, set.NewThreadUnsafeSet(txSourceAccount, accountID1, contractID1), participants)
}

// Test_participantsForSorobanOp_ignoresEventEmitters: a contract that emitted an event did
// not authorise the operation, so it is not a participant.
func Test_participantsForSorobanOp_ignoresEventEmitters(t *testing.T) {
	op := makeInvokeContractOp()
	setOperationMeta(op, nil, []xdr.ContractEvent{contractEventFrom(invokedContractID), contractEventFrom(contractID2)})

	participants, err := participantsForSorobanOp(op)
	require.NoError(t, err)
	assert.Equal(t, set.NewThreadUnsafeSet(txSourceAccount), participants)
}

// Test_participantsForSorobanOp_ignoresDeclaredAuthTree is the regression test for
// forged participant attribution. The submitter declares an Address-credential entry
// naming a victim account (unsigned, unmatched, so no nonce entry) and a SOURCE_ACCOUNT
// entry whose invocation tree names contracts and a deployer. None of them executed, so
// none may become a participant.
func Test_participantsForSorobanOp_ignoresDeclaredAuthTree(t *testing.T) {
	const (
		victimAccount = "GAAZI4TCR3TY5OJHCTJC2A4QSY6CJWJH5IAJTGKIN2ER7LBNVKOCCWN7"
	)

	op := makeInvokeContractOp()
	// One unmatched Address-credential entry naming the victim, one SOURCE_ACCOUNT entry.
	op.Operation.Body.InvokeHostFunctionOp.Auth = append(
		makeAuthEntries(t, op, makeScAddress(victimAccount)),
		xdr.SorobanAuthorizationEntry{
			Credentials:    xdr.SorobanCredentials{Type: xdr.SorobanCredentialsTypeSorobanCredentialsSourceAccount},
			RootInvocation: xdr.SorobanAuthorizedInvocation{Function: xdr.SorobanAuthorizedFunction{Type: xdr.SorobanAuthorizedFunctionTypeSorobanAuthorizedFunctionTypeContractFn, ContractFn: &xdr.InvokeContractArgs{ContractAddress: makeScContract(invokedContractID)}}},
		},
	)
	includeSubInvocations(op) // names deployerAccountID, deployedContractID, contractID1, contractID3 and the XLM SAC
	// Meta records only what executed: the invoked contract emitted one event.
	setOperationMeta(op, nil, []xdr.ContractEvent{contractEventFrom(invokedContractID)})

	participants, err := participantsForSorobanOp(op)
	require.NoError(t, err)
	assert.Equal(t, set.NewThreadUnsafeSet(txSourceAccount), participants)
}

// Test_participantsForSorobanOp_realTestnetMeta replays testnet transaction
// 803030e1931f4297c88098089f440349c48b9940d847ef1b3afb668d540a0e11 (ledger 4679347,
// protocol 28, TransactionMeta V4). One Address-credential auth entry was matched and
// authenticated, so the host wrote its nonce entry under the invoked contract, a custom
// account. A second contract ran and emitted an event but authorised nothing, so it is
// not a participant.
func Test_participantsForSorobanOp_realTestnetMeta(t *testing.T) {
	const (
		source         = "GBKDL6ZOWLBYFFMWP5NL22HDQ7SR4WCZ5PY3LVOYLKNQJ5BF3WGJAZSS"
		invoked        = "CBBRS7XLNIGUYYFUEFOL5KGYH4QHE7LVT2YWOARN3VGGQCDVIWNAU4EJ" // the authorizer, via its nonce entry
		nestedContract = "CABXBYJNZ7IUW4G3D6BND5YCAQF3ASSDMDAOKQQ63UYFSO7WUU2TIP5G" // emitted an event only
	)
	readB64 := func(name string) string {
		b, err := os.ReadFile(filepath.Join("testdata", name))
		require.NoError(t, err)
		return string(b)
	}
	var envelope xdr.TransactionEnvelope
	require.NoError(t, xdr.SafeUnmarshalBase64(readB64("testnet_4679347_803030e1_envelope.b64"), &envelope))
	var meta xdr.TransactionMeta
	require.NoError(t, xdr.SafeUnmarshalBase64(readB64("testnet_4679347_803030e1_meta.b64"), &meta))
	require.EqualValues(t, 4, meta.V)

	op := &TransactionOperationWrapper{
		Index:          0,
		Network:        network.TestNetworkPassphrase,
		LedgerSequence: 4679347,
		LedgerClosed:   closeTime,
		Operation:      envelope.Operations()[0],
		Transaction: ingest.LedgerTransaction{
			Index:      1,
			Envelope:   envelope,
			Result:     xdr.TransactionResultPair{Result: xdr.TransactionResult{Result: xdr.TransactionResultResult{Code: xdr.TransactionResultCodeTxSuccess, Results: &[]xdr.OperationResult{}}}},
			UnsafeMeta: meta,
			Ledger:     xdr.LedgerCloseMeta{V: 1, V1: &xdr.LedgerCloseMetaV1{LedgerHeader: xdr.LedgerHeaderHistoryEntry{Header: xdr.LedgerHeader{LedgerSeq: 4679347}}}},
		},
	}

	participants, err := participantsForSorobanOp(op)
	require.NoError(t, err)
	assert.Equal(t, set.NewThreadUnsafeSet(source, invoked), participants)
	assert.NotContains(t, participants.ToSlice(), nestedContract)
}
