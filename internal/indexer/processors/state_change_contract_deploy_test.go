package processors

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/network"
	operation_processor "github.com/stellar/go/processors/operation"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/utils"
)

func Test_StateChangeContractDeployProcessor_Process_invalidOpType(t *testing.T) {
	proc := NewStateChangeContractDeployProcessor(network.TestNetworkPassphrase)

	op := operation_processor.TransactionOperationWrapper{
		Operation: xdr.Operation{Body: xdr.OperationBody{Type: xdr.OperationTypePayment}},
	}
	changes, err := proc.Process(op)
	assert.ErrorIs(t, err, ErrInvalidOpType)
	assert.Nil(t, changes)
}

func Test_StateChangeContractDeployProcessor_createContract_stateChanges(t *testing.T) {
	const (
		txSourceAccount       = "GAUE24B36YYY3CXTXNFE3IFXU6EE4NUOS5L744IWGTNXVXZAXFGMP6CC"
		opSourceAccount       = "GBZURSTQQRSU3XB66CHJ3SH2ZWLG663V5SWM6HF3FL72BOMYHDT4QTUF"
		fromSourceAccount     = "GCQIH6MRLCJREVE76LVTKKEZXRIT6KSX7KU65HPDDBYFKFYHIYSJE57R"
		authSignerAccount     = "GDG2KKXC62BINMUZNBTLG235323N6BOIR33JBF4ELTOUKUG5BDE6HJZT"
		usdcSACContractID     = "CBIELTK6YBZJU5UP2WWQEUCYKLPU6AUNZ2BQ4WWFEIE3USCIHMXQDAMA"
		constructorAccountID  = "GAHPYWLK6YRN7CVYZOO4H3VDRZ7PVF5UJGLZCSPAEIKJE2XSWF5LAGER"
		constructorContractID = "CDNVQW44C3HALYNVQ4SOBXY5EWYTGVYXX6JPESOLQDABJI5FC5LTRRUE"
	)
	usdcXdrAsset := xdr.Asset{
		Type: xdr.AssetTypeAssetTypeCreditAlphanum4,
		AlphaNum4: &xdr.AlphaNum4{
			AssetCode: [4]byte{'U', 'S', 'D', 'C'},
			Issuer:    xdr.MustAddress("GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5"),
		},
	}
	salt := xdr.Uint256{195, 179, 60, 131, 211, 25, 160, 131, 45, 151, 203, 11, 11, 116, 166, 232, 51, 92, 179, 76, 220, 111, 96, 246, 72, 68, 195, 127, 194, 19, 147, 252}

	closeTime := time.Now()
	basicSorobanOp := func() operation_processor.TransactionOperationWrapper {
		return operation_processor.TransactionOperationWrapper{
			Network:        network.TestNetworkPassphrase,
			LedgerClosed:   closeTime,
			LedgerSequence: 12345,
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
				Hash: xdr.Hash{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20},
				Ledger: xdr.LedgerCloseMeta{
					V: 1,
					V1: &xdr.LedgerCloseMetaV1{
						LedgerHeader: xdr.LedgerHeaderHistoryEntry{
							Header: xdr.LedgerHeader{
								LedgerSeq: 12345,
								ScpValue:  xdr.StellarValue{CloseTime: xdr.TimePoint(closeTime.Unix())},
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
		wantStateChanges []types.StateChange
	}

	testCases := []TestCase{}

	stateChangeBuilder := NewStateChangeBuilder(12345, closeTime.Unix(), "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20").
		WithOperationID(53021371269121).
		WithReason(types.StateChangeReasonDeploy).
		WithCategory(types.StateChangeCategoryContract)

	for _, withSubinvocations := range []bool{false, true} {
		for _, feeBump := range []bool{false, true} {
			for _, hostFnType := range []xdr.HostFunctionType{xdr.HostFunctionTypeHostFunctionTypeCreateContract, xdr.HostFunctionTypeHostFunctionTypeCreateContractV2} {
				prefix := strings.ReplaceAll(hostFnType.String(), "HostFunctionTypeHostFunctionType", "")
				subInvocationsStateChanges := []types.StateChange{}
				if withSubinvocations {
					prefix = fmt.Sprintf("%s,withSubinvocations🔄", prefix)
					subInvocationsStateChanges = []types.StateChange{
						stateChangeBuilder.Clone().
							WithDeployer(deployerAccountID).
							WithAccount(deployedContractID).
							Build(),
					}
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
						wantStateChanges: append(subInvocationsStateChanges,
							stateChangeBuilder.Clone().
								WithDeployer(fromSourceAccount).
								WithAccount("CA7UGIYR2H63C2ETN2VE4WDQ6YX5XNEWNWC2DP7A64B2ZR7VJJWF3SBF").
								Build(),
						),
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
						wantStateChanges: append(subInvocationsStateChanges,
							stateChangeBuilder.Clone().
								WithDeployer(fromSourceAccount).
								WithAccount("CA7UGIYR2H63C2ETN2VE4WDQ6YX5XNEWNWC2DP7A64B2ZR7VJJWF3SBF").
								Build(),
						),
					},
					TestCase{
						name: fmt.Sprintf("🟢%s/FromAsset/tx.SourceAccount", prefix),
						op: func() operation_processor.TransactionOperationWrapper {
							op := basicSorobanOp()
							setFromAsset(&op, hostFnType, usdcXdrAsset)
							if withSubinvocations {
								op.Operation.Body.InvokeHostFunctionOp.Auth = makeAuthEntries(t, &op, makeScAddress(authSignerAccount))
								op = includeSubInvocations(op)
							}
							if feeBump {
								op = makeFeeBumpOp(txSourceAccount, op)
							}
							return op
						}(),
						wantStateChanges: subInvocationsStateChanges,
					},
				)
			}
		}
	}

	proc := NewStateChangeContractDeployProcessor(network.TestNetworkPassphrase)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stateChanges, err := proc.Process(tc.op)

			require.NoError(t, err)
			assertStateChangesElementsMatch(t, tc.wantStateChanges, stateChanges)
		})
	}
}

func assertStateChangeEqual(t *testing.T, want types.StateChange, got types.StateChange) {
	gotV2 := got
	gotV2.IngestedAt = want.IngestedAt
	assert.Equal(t, want, gotV2)
}

func assertStateChangesElementsMatch(t *testing.T, want []types.StateChange, got []types.StateChange) {
	if len(want) != len(got) {
		assert.Fail(t, "state changes length mismatch", "want %d, got %d", len(want), len(got))
	}

	wantMap := make(map[string]types.StateChange)
	for _, w := range want {
		wantMap[w.ID] = w
	}

	for _, g := range got {
		if _, ok := wantMap[g.ID]; !ok {
			assert.Fail(t, "state change not found", "state change id: %s", g.ID)
		}

		assertStateChangeEqual(t, wantMap[g.ID], g)
	}
}
