package processors

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stellar/go/network"
	operation_processor "github.com/stellar/go/processors/operation"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/utils"
)

func Test_ContractDeployProcessor_Process_invalidOpType(t *testing.T) {
	ctx := context.Background()
	proc := NewContractDeployProcessor(network.TestNetworkPassphrase)

	op := &operation_processor.TransactionOperationWrapper{
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

	builder := NewStateChangeBuilder(12345, closeTime.Unix(), "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20", 53021371269120).
		WithOperationID(53021371269121).
		WithReason(types.StateChangeReasonDeploy).
		WithCategory(types.StateChangeCategoryContract)

	type TestCase struct {
		name             string
		op               *operation_processor.TransactionOperationWrapper
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
						op: func() *operation_processor.TransactionOperationWrapper {
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
						op: func() *operation_processor.TransactionOperationWrapper {
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
						op: func() *operation_processor.TransactionOperationWrapper {
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

	proc := NewContractDeployProcessor(network.TestNetworkPassphrase)
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stateChanges, err := proc.ProcessOperation(ctx, tc.op)

			require.NoError(t, err)
			assertStateChangesElementsMatch(t, tc.wantStateChanges, stateChanges)
		})
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

	makeInvokeContractOp := func(argAddresses ...xdr.ScAddress) *operation_processor.TransactionOperationWrapper {
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

	builder := NewStateChangeBuilder(12345, closeTime.Unix(), "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20", 53021371269120).
		WithOperationID(53021371269121).
		WithReason(types.StateChangeReasonDeploy).
		WithCategory(types.StateChangeCategoryContract)

	type TestCase struct {
		name             string
		op               *operation_processor.TransactionOperationWrapper
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
					op: func() *operation_processor.TransactionOperationWrapper {
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
					op: func() *operation_processor.TransactionOperationWrapper {
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

	proc := NewContractDeployProcessor(network.TestNetworkPassphrase)
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stateChanges, err := proc.ProcessOperation(ctx, tc.op)

			require.NoError(t, err)
			assertStateChangesElementsMatch(t, tc.wantStateChanges, stateChanges)
		})
	}
}
