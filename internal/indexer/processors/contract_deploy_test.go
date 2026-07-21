package processors

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/utils"
)

func Test_ContractDeployProcessor_Process_invalidOpType(t *testing.T) {
	ctx := context.Background()
	proc := NewContractDeployProcessor(network.TestNetworkPassphrase, nil)

	op := &TransactionOperationWrapper{
		Operation: xdr.Operation{Body: xdr.OperationBody{Type: xdr.OperationTypePayment}},
	}
	changes, err := proc.ProcessOperation(ctx, op)
	assert.ErrorIs(t, err, ErrInvalidOpType)
	assert.Nil(t, changes)
}

func Test_ContractDeployProcessor_Process_createContract(t *testing.T) {
	const (
		opSourceAccount   = "GBZURSTQQRSU3XB66CHJ3SH2ZWLG663V5SWM6HF3FL72BOMYHDT4QTUF"
		fromSourceAccount = "GCQIH6MRLCJREVE76LVTKKEZXRIT6KSX7KU65HPDDBYFKFYHIYSJE57R"
		authSignerAccount = "GDG2KKXC62BINMUZNBTLG235323N6BOIR33JBF4ELTOUKUG5BDE6HJZT"
	)

	ctx := context.Background()

	builder := NewStateChangeBuilder(12345, closeTime.Unix(), 53021371269120, nil).
		WithOperationID(53021371269121).
		WithReason(types.StateChangeReasonCreate).
		WithCategory(types.StateChangeCategoryAccount)

	type TestCase struct {
		name             string
		op               *TransactionOperationWrapper
		wantStateChanges []types.StateChange
	}
	testCases := []TestCase{}

	// We test with/without subinvocations, which can contain nested invocations to this or another contract.
	for _, withSubinvocations := range []bool{false, true} {
		// We test with/without fee bump, to ensure the processor handles both cases.
		for _, feeBump := range []bool{false, true} {
			for _, hostFnType := range []xdr.HostFunctionType{xdr.HostFunctionTypeHostFunctionTypeCreateContract, xdr.HostFunctionTypeHostFunctionTypeCreateContractV2} {
				prefix := strings.ReplaceAll(hostFnType.String(), "HostFunctionTypeHostFunctionType", "")

				subInvocationsStateChanges := []types.StateChange{}
				if withSubinvocations {
					prefix = fmt.Sprintf("%s,withSubinvocations🔄", prefix)
					subInvocationsStateChanges = []types.StateChange{
						builder.Clone().
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
						op: func() *TransactionOperationWrapper {
							op := makeBasicSorobanOp()
							setFromAddress(op, hostFnType, fromSourceAccount)
							if withSubinvocations {
								op.Operation.Body.InvokeHostFunctionOp.Auth = makeAuthEntries(t, op, makeScAddress(authSignerAccount))
								includeSubInvocations(op)
							}
							if feeBump {
								op = makeFeeBumpOp(txSourceAccount, op)
							}
							return op
						}(),
						wantStateChanges: append(subInvocationsStateChanges,
							builder.Clone().
								WithDeployer(fromSourceAccount).
								WithAccount("CA7UGIYR2H63C2ETN2VE4WDQ6YX5XNEWNWC2DP7A64B2ZR7VJJWF3SBF").
								Build(),
						),
					},
					TestCase{
						name: fmt.Sprintf("🟢%s/FromAddress/op.SourceAccount", prefix),
						op: func() *TransactionOperationWrapper {
							op := makeBasicSorobanOp()
							op.Operation.SourceAccount = utils.PointOf(xdr.MustMuxedAddress(opSourceAccount))
							setFromAddress(op, hostFnType, fromSourceAccount)
							if withSubinvocations {
								op.Operation.Body.InvokeHostFunctionOp.Auth = makeAuthEntries(t, op, makeScAddress(authSignerAccount))
								includeSubInvocations(op)
							}
							if feeBump {
								op = makeFeeBumpOp(txSourceAccount, op)
							}
							return op
						}(),
						wantStateChanges: append(subInvocationsStateChanges,
							builder.Clone().
								WithDeployer(fromSourceAccount).
								WithAccount("CA7UGIYR2H63C2ETN2VE4WDQ6YX5XNEWNWC2DP7A64B2ZR7VJJWF3SBF").
								Build(),
						),
					},
					TestCase{
						name: fmt.Sprintf("🟢%s/FromAsset/tx.SourceAccount", prefix),
						op: func() *TransactionOperationWrapper {
							op := makeBasicSorobanOp()
							setFromAsset(op, hostFnType, usdcAssetTestnet)
							if withSubinvocations {
								op.Operation.Body.InvokeHostFunctionOp.Auth = makeAuthEntries(t, op, makeScAddress(authSignerAccount))
								includeSubInvocations(op)
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

	proc := NewContractDeployProcessor(network.TestNetworkPassphrase, nil)
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stateChanges, err := proc.ProcessOperation(ctx, tc.op)

			require.NoError(t, err)
			assertStateChangesElementsMatch(t, tc.wantStateChanges, stateChanges)
		})
	}
}

// Test_ContractDeployProcessor_Process_multipleContractsDeterministicOrder pins the emission
// order for an operation that deploys several contracts across its root host function and its
// auth invocation tree. AssignStateChangeOrdinals (internal/indexer/types/types.go) derives each
// state change's ID from its slice position within the operation, so re-ingesting the same ledger
// must yield the same slice order every time. This test asserts the exact order (not
// ElementsMatch) and also exercises dedup: the auth tree redeclares the same contract creation
// that a sibling subinvocation already produced, and it must not be emitted twice.
func Test_ContractDeployProcessor_Process_multipleContractsDeterministicOrder(t *testing.T) {
	const (
		rootDeployer = "GCQIH6MRLCJREVE76LVTKKEZXRIT6KSX7KU65HPDDBYFKFYHIYSJE57R"
		subDeployerB = "GDG2KKXC62BINMUZNBTLG235323N6BOIR33JBF4ELTOUKUG5BDE6HJZT"
		subDeployerC = "GAGWN4445WLODCXT7RUZXJLQK5XWX4GICXDOAAZZGK2N3BR67RIIVWJ7"

		contractA = "CA7UGIYR2H63C2ETN2VE4WDQ6YX5XNEWNWC2DP7A64B2ZR7VJJWF3SBF" // rootDeployer + TestSalt, root host function
	)
	saltB := xdr.Uint256{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}
	saltC := xdr.Uint256{2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2}

	preimageB := xdr.ContractIdPreimageFromAddress{Address: makeScAddress(subDeployerB), Salt: saltB}
	preimageC := xdr.ContractIdPreimageFromAddress{Address: makeScAddress(subDeployerC), Salt: saltC}

	contractB, err := calculateContractID(network.TestNetworkPassphrase, preimageB)
	require.NoError(t, err)
	contractC, err := calculateContractID(network.TestNetworkPassphrase, preimageC)
	require.NoError(t, err)

	ctx := context.Background()

	op := makeBasicSorobanOp()
	setFromAddress(op, xdr.HostFunctionTypeHostFunctionTypeCreateContract, rootDeployer)
	op.Operation.Body.InvokeHostFunctionOp.Auth = []xdr.SorobanAuthorizationEntry{
		{
			RootInvocation: xdr.SorobanAuthorizedInvocation{
				Function: xdr.SorobanAuthorizedFunction{
					Type: xdr.SorobanAuthorizedFunctionTypeSorobanAuthorizedFunctionTypeContractFn,
				},
				SubInvocations: []xdr.SorobanAuthorizedInvocation{
					{
						// Deploys contractB, then walks depth-first into its own SubInvocations
						// (deploying contractC) before the loop moves on to the next sibling.
						Function: xdr.SorobanAuthorizedFunction{
							Type:                 xdr.SorobanAuthorizedFunctionTypeSorobanAuthorizedFunctionTypeCreateContractHostFn,
							CreateContractHostFn: &xdr.CreateContractArgs{ContractIdPreimage: xdr.ContractIdPreimage{Type: xdr.ContractIdPreimageTypeContractIdPreimageFromAddress, FromAddress: &preimageB}},
						},
						SubInvocations: []xdr.SorobanAuthorizedInvocation{
							{
								Function: xdr.SorobanAuthorizedFunction{
									Type:                   xdr.SorobanAuthorizedFunctionTypeSorobanAuthorizedFunctionTypeCreateContractV2HostFn,
									CreateContractV2HostFn: &xdr.CreateContractArgsV2{ContractIdPreimage: xdr.ContractIdPreimage{Type: xdr.ContractIdPreimageTypeContractIdPreimageFromAddress, FromAddress: &preimageC}},
								},
							},
						},
					},
					{
						// Redeclares the same contractB creation as the sibling above; must be
						// deduped and not appear a second time in the output.
						Function: xdr.SorobanAuthorizedFunction{
							Type:                 xdr.SorobanAuthorizedFunctionTypeSorobanAuthorizedFunctionTypeCreateContractHostFn,
							CreateContractHostFn: &xdr.CreateContractArgs{ContractIdPreimage: xdr.ContractIdPreimage{Type: xdr.ContractIdPreimageTypeContractIdPreimageFromAddress, FromAddress: &preimageB}},
						},
					},
				},
			},
		},
	}

	proc := NewContractDeployProcessor(network.TestNetworkPassphrase, nil)
	stateChanges, err := proc.ProcessOperation(ctx, op)
	require.NoError(t, err)

	builder := NewStateChangeBuilder(12345, closeTime.Unix(), 53021371269120, nil).
		WithOperationID(53021371269121).
		WithReason(types.StateChangeReasonCreate).
		WithCategory(types.StateChangeCategoryAccount)

	wantOrder := []types.StateChange{
		builder.Clone().WithDeployer(rootDeployer).WithAccount(contractA).Build(),
		builder.Clone().WithDeployer(subDeployerB).WithAccount(contractB).Build(),
		builder.Clone().WithDeployer(subDeployerC).WithAccount(contractC).Build(),
	}

	require.Len(t, stateChanges, len(wantOrder))
	for i := range wantOrder {
		assertStateChangeEqual(t, wantOrder[i], stateChanges[i])
	}
}

func Test_ContractDeployProcessor_Process_invokeContract(t *testing.T) {
	const (
		opSourceAccount   = "GBZURSTQQRSU3XB66CHJ3SH2ZWLG663V5SWM6HF3FL72BOMYHDT4QTUF"
		argAccountID1     = "GCQIH6MRLCJREVE76LVTKKEZXRIT6KSX7KU65HPDDBYFKFYHIYSJE57R"
		argAccountID2     = "GDG2KKXC62BINMUZNBTLG235323N6BOIR33JBF4ELTOUKUG5BDE6HJZT"
		argContractID1    = "CBIELTK6YBZJU5UP2WWQEUCYKLPU6AUNZ2BQ4WWFEIE3USCIHMXQDAMA"
		argContractID2    = "CDNVQW44C3HALYNVQ4SOBXY5EWYTGVYXX6JPESOLQDABJI5FC5LTRRUE"
		authSignerAccount = "GDG2KKXC62BINMUZNBTLG235323N6BOIR33JBF4ELTOUKUG5BDE6HJZT"
		invokedContractID = "CBL6KD2LFMLAUKFFWNNXWOXFN73GAXLEA4WMJRLQ5L76DMYTM3KWQVJN"
	)

	ctx := context.Background()

	makeInvokeContractOp := func(argAddresses ...xdr.ScAddress) *TransactionOperationWrapper {
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

	builder := NewStateChangeBuilder(12345, closeTime.Unix(), 53021371269120, nil).
		WithOperationID(53021371269121).
		WithReason(types.StateChangeReasonCreate).
		WithCategory(types.StateChangeCategoryAccount)

	type TestCase struct {
		name             string
		op               *TransactionOperationWrapper
		wantStateChanges []types.StateChange
	}
	testCases := []TestCase{}

	// We test with/without subinvocations, which can contain nested invocations to this or another contract.
	for _, withSubinvocations := range []bool{false, true} {
		// We test with/without fee bump, to ensure the processor handles both cases.
		for _, feeBump := range []bool{false, true} {
			prefix := ""

			subInvocationsStateChanges := []types.StateChange{}
			if withSubinvocations {
				prefix = "🔄WithSubinvocations🔄"
				subInvocationsStateChanges = []types.StateChange{
					builder.Clone().
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
					name: fmt.Sprintf("🟢%s/tx.SourceAccount", prefix),
					op: func() *TransactionOperationWrapper {
						op := makeInvokeContractOp(makeScAddress(argAccountID1), makeScAddress(argAccountID2))
						if withSubinvocations {
							op.Operation.Body.InvokeHostFunctionOp.Auth = makeAuthEntries(t, op, makeScAddress(authSignerAccount))
							includeSubInvocations(op)
						}
						if feeBump {
							op = makeFeeBumpOp(txSourceAccount, op)
						}
						return op
					}(),
					wantStateChanges: subInvocationsStateChanges,
				},
				TestCase{
					name: fmt.Sprintf("🟢%s/op.SourceAccount", prefix),
					op: func() *TransactionOperationWrapper {
						op := makeInvokeContractOp(makeScContract(argContractID1), makeScContract(argContractID2))
						op.Operation.SourceAccount = utils.PointOf(xdr.MustMuxedAddress(opSourceAccount))
						if withSubinvocations {
							op.Operation.Body.InvokeHostFunctionOp.Auth = makeAuthEntries(t, op, makeScAddress(authSignerAccount))
							includeSubInvocations(op)
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

	proc := NewContractDeployProcessor(network.TestNetworkPassphrase, nil)
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stateChanges, err := proc.ProcessOperation(ctx, tc.op)

			require.NoError(t, err)
			assertStateChangesElementsMatch(t, tc.wantStateChanges, stateChanges)
		})
	}
}
