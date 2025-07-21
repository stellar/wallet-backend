package processors

import (
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

func Test_StateChangeContractDeployProcessor_Process_invalidOpType(t *testing.T) {
	proc := NewStateChangeContractDeployProcessor(network.TestNetworkPassphrase)

	op := &operation_processor.TransactionOperationWrapper{
		Operation: xdr.Operation{Body: xdr.OperationBody{Type: xdr.OperationTypePayment}},
	}
	changes, err := proc.Process(op)
	assert.ErrorIs(t, err, ErrInvalidOpType)
	assert.Nil(t, changes)
}

func Test_StateChangeContractDeployProcessor_createContract_stateChanges(t *testing.T) {
	const (
		opSourceAccount   = "GBZURSTQQRSU3XB66CHJ3SH2ZWLG663V5SWM6HF3FL72BOMYHDT4QTUF"
		fromSourceAccount = "GCQIH6MRLCJREVE76LVTKKEZXRIT6KSX7KU65HPDDBYFKFYHIYSJE57R"
		authSignerAccount = "GDG2KKXC62BINMUZNBTLG235323N6BOIR33JBF4ELTOUKUG5BDE6HJZT"
	)

	type TestCase struct {
		name             string
		op               *operation_processor.TransactionOperationWrapper
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
							stateChangeBuilder.Clone().
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
							stateChangeBuilder.Clone().
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

	proc := NewStateChangeContractDeployProcessor(network.TestNetworkPassphrase)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stateChanges, err := proc.Process(tc.op)

			require.NoError(t, err)
			assertStateChangesElementsMatch(t, tc.wantStateChanges, stateChanges)
		})
	}
}
