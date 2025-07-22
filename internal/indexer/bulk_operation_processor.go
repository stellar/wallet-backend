package indexer

import (
	"context"
	"errors"
	"fmt"

	operation_processor "github.com/stellar/go/processors/operation"

	"github.com/stellar/wallet-backend/internal/indexer/processors"
	"github.com/stellar/wallet-backend/internal/indexer/types"
)

// BulkOperationProcessor combines multiple OperationProcessorInterface instances
// and processes operations through all of them, collecting their results.
type BulkOperationProcessor struct {
	processors []OperationStateChangeProcessorInterface
}

// NewBulkOperationProcessor creates a new bulk processor with the given processors.
func NewBulkOperationProcessor(processors ...OperationStateChangeProcessorInterface) *BulkOperationProcessor {
	return &BulkOperationProcessor{
		processors: processors,
	}
}

// ProcessOperation processes the operation through all child processors and combines their results.
func (b *BulkOperationProcessor) ProcessOperation(ctx context.Context, opWrapper *operation_processor.TransactionOperationWrapper) ([]types.StateChange, error) {
	stateChangesMap := map[string]types.StateChange{}

	for _, processor := range b.processors {
		stateChanges, err := processor.ProcessOperation(ctx, opWrapper)
		if err != nil && !errors.Is(err, processors.ErrInvalidOpType) {
			return nil, fmt.Errorf("processor %T failed: %w", processor, err)
		} else if err != nil {
			continue
		}

		for _, stateChange := range stateChanges {
			stateChangesMap[stateChange.ID] = stateChange
		}
	}

	stateChanges := make([]types.StateChange, 0, len(stateChangesMap))
	for _, stateChange := range stateChangesMap {
		stateChanges = append(stateChanges, stateChange)
	}
	return stateChanges, nil
}
