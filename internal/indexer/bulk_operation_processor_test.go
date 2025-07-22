package indexer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stellar/go/network"
	operation_processor "github.com/stellar/go/processors/operation"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/indexer/processors"
	"github.com/stellar/wallet-backend/internal/indexer/types"
)

func Test_BulkOperationProcessor_ProcessOperation(t *testing.T) {
	ctx := context.Background()
	testOp := &operation_processor.TransactionOperationWrapper{
		Network:      network.TestNetworkPassphrase,
		LedgerClosed: time.Now(),
		Operation: xdr.Operation{
			Body: xdr.OperationBody{Type: xdr.OperationTypePayment},
		},
	}

	type testCase struct {
		name            string
		numProcessors   int
		prepareMocks    func(t *testing.T, processors []OperationStateChangeProcessorInterface)
		wantErrContains string
		wantResult      []types.StateChange
	}

	testCases := []testCase{
		{
			name:          "🟢successful_processing_with_multiple_processors",
			numProcessors: 2,
			prepareMocks: func(t *testing.T, procs []OperationStateChangeProcessorInterface) {
				mock1 := procs[0].(*MockOperationStateChangeProcessor)
				mock2 := procs[1].(*MockOperationStateChangeProcessor)

				stateChanges1 := []types.StateChange{{ID: "sc1"}}
				stateChanges2 := []types.StateChange{{ID: "sc2"}}

				mock1.On("ProcessOperation", ctx, testOp).Return(stateChanges1, nil)
				mock2.On("ProcessOperation", ctx, testOp).Return(stateChanges2, nil)
			},
			wantResult: []types.StateChange{{ID: "sc1"}, {ID: "sc2"}},
		},
		{
			name:          "🟢successful_processing_with_empty_results",
			numProcessors: 2,
			prepareMocks: func(t *testing.T, procs []OperationStateChangeProcessorInterface) {
				mock1 := procs[0].(*MockOperationStateChangeProcessor)
				mock2 := procs[1].(*MockOperationStateChangeProcessor)

				mock1.On("ProcessOperation", ctx, testOp).Return([]types.StateChange{}, nil)
				mock2.On("ProcessOperation", ctx, testOp).Return([]types.StateChange{}, nil)
			},
			wantResult: []types.StateChange{},
		},
		{
			name:          "🟢ignores_err_invalid_op_type_errors",
			numProcessors: 2,
			prepareMocks: func(t *testing.T, procs []OperationStateChangeProcessorInterface) {
				mock1 := procs[0].(*MockOperationStateChangeProcessor)
				mock2 := procs[1].(*MockOperationStateChangeProcessor)

				stateChanges1 := []types.StateChange{{ID: "sc1"}}
				stateChanges2 := []types.StateChange{{ID: "sc2"}}

				mock1.On("ProcessOperation", ctx, testOp).Return(stateChanges1, processors.ErrInvalidOpType)
				mock2.On("ProcessOperation", ctx, testOp).Return(stateChanges2, nil)
			},
			wantResult: []types.StateChange{{ID: "sc2"}},
		},
		{
			name:          "🔴returns_first_error_from_processor_with_proper_wrapping",
			numProcessors: 1,
			prepareMocks: func(t *testing.T, procs []OperationStateChangeProcessorInterface) {
				mock := procs[0].(*MockOperationStateChangeProcessor)

				stateChanges := []types.StateChange{{ID: "sc1"}}
				expectedError := errors.New("processor error")

				mock.On("ProcessOperation", ctx, testOp).Return(stateChanges, expectedError)
			},
			wantErrContains: "processor *indexer.MockOperationStateChangeProcessor failed:",
			wantResult:      nil,
		},
		{
			name:          "🔴returns_first_error_when_multiple_processors_would_fail",
			numProcessors: 1,
			prepareMocks: func(t *testing.T, procs []OperationStateChangeProcessorInterface) {
				mock := procs[0].(*MockOperationStateChangeProcessor)

				stateChanges := []types.StateChange{{ID: "sc1"}}
				resErr := errors.New("first error")

				mock.On("ProcessOperation", ctx, testOp).Return(stateChanges, resErr)
			},
			wantErrContains: "first error",
			wantResult:      nil,
		},
		{
			name:          "🟢deduplicates_state_changes_by_id",
			numProcessors: 2,
			prepareMocks: func(t *testing.T, procs []OperationStateChangeProcessorInterface) {
				mock1 := procs[0].(*MockOperationStateChangeProcessor)
				mock2 := procs[1].(*MockOperationStateChangeProcessor)

				stateChanges1 := []types.StateChange{{ID: "sc1", StateChangeCategory: types.StateChangeCategoryDebit}}
				stateChanges2 := []types.StateChange{{ID: "sc1", StateChangeCategory: types.StateChangeCategoryDebit}}

				mock1.On("ProcessOperation", ctx, testOp).Return(stateChanges1, nil)
				mock2.On("ProcessOperation", ctx, testOp).Return(stateChanges2, nil)
			},
			wantResult: []types.StateChange{{ID: "sc1", StateChangeCategory: types.StateChangeCategoryDebit}},
		},
		{
			name:          "🟢works_with_no_processors",
			numProcessors: 0,
			prepareMocks:  func(t *testing.T, procs []OperationStateChangeProcessorInterface) {},
			wantResult:    []types.StateChange{},
		},
		{
			name:          "🟢works_with_single_processor",
			numProcessors: 1,
			prepareMocks: func(t *testing.T, procs []OperationStateChangeProcessorInterface) {
				mock := procs[0].(*MockOperationStateChangeProcessor)
				stateChanges := []types.StateChange{{ID: "sc1"}}

				mock.On("ProcessOperation", ctx, testOp).Return(stateChanges, nil)
			},
			wantResult: []types.StateChange{{ID: "sc1"}},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			processors := []OperationStateChangeProcessorInterface{}
			for range tc.numProcessors {
				newProcessor := &MockOperationStateChangeProcessor{}
				defer newProcessor.AssertExpectations(t)
				processors = append(processors, newProcessor)
			}
			tc.prepareMocks(t, processors)

			bulkProcessor := NewBulkOperationProcessor(processors...)
			result, err := bulkProcessor.ProcessOperation(ctx, testOp)

			if tc.wantErrContains != "" {
				require.Error(t, err)
				assert.ErrorContains(t, err, tc.wantErrContains)
				assert.Empty(t, result)
			} else {
				require.NoError(t, err)
				assert.ElementsMatch(t, tc.wantResult, result)
			}
		})
	}
}
