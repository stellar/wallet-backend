package indexer

import (
	"context"
	"errors"
	"fmt"

	set "github.com/deckarep/golang-set/v2"
	"github.com/stellar/go/ingest"
	operation_processor "github.com/stellar/go/processors/operation"

	"github.com/stellar/wallet-backend/internal/indexer/processors"
	"github.com/stellar/wallet-backend/internal/indexer/types"
)

type IndexerBufferInterface interface {
	PushParticipantTransaction(participant string, transaction types.Transaction)
	PushParticipantOperation(participant string, operation types.Operation, transaction types.Transaction)
	GetParticipantTransactions(participant string) []types.Transaction
	GetParticipantOperations(participant string) map[int64]types.Operation
	PushStateChanges(stateChanges []types.StateChange)
	GetParticipants() set.Set[string]
	GetNumberOfTransactions() int
	GetAllTransactions() []types.Transaction
	GetAllStateChanges() []types.StateChange
	CalculateStateChangeOrder()
}

type TokenTransferProcessorInterface interface {
	ProcessTransaction(ctx context.Context, tx ingest.LedgerTransaction) ([]types.StateChange, error)
}

type ParticipantsProcessorInterface interface {
	GetTransactionParticipants(transaction ingest.LedgerTransaction) (set.Set[string], error)
	GetOperationsParticipants(transaction ingest.LedgerTransaction) (map[int64]processors.OperationParticipants, error)
}

type OperationProcessorInterface interface {
	ProcessOperation(ctx context.Context, opWrapper *operation_processor.TransactionOperationWrapper) ([]types.StateChange, error)
}

type Indexer struct {
	Buffer                  IndexerBufferInterface
	participantsProcessor   ParticipantsProcessorInterface
	tokenTransferProcessor  TokenTransferProcessorInterface
	effectsProcessor        OperationProcessorInterface
	contractDeployProcessor OperationProcessorInterface
	contractEventsProcessor OperationProcessorInterface
}

func NewIndexer(networkPassphrase string) *Indexer {
	return &Indexer{
		Buffer:                  NewIndexerBuffer(),
		participantsProcessor:   processors.NewParticipantsProcessor(networkPassphrase),
		tokenTransferProcessor:  processors.NewTokenTransferProcessor(networkPassphrase),
		effectsProcessor:        processors.NewEffectsProcessor(networkPassphrase),
		contractDeployProcessor: processors.NewContractDeployProcessor(networkPassphrase),
		contractEventsProcessor: processors.NewEventsProcessor(networkPassphrase),
	}
}

func (i *Indexer) ProcessTransaction(ctx context.Context, transaction ingest.LedgerTransaction) error {
	// 1. Index transaction txParticipants
	txParticipants, err := i.participantsProcessor.GetTransactionParticipants(transaction)
	if err != nil {
		return fmt.Errorf("getting transaction participants: %w", err)
	}

	dataTx, err := processors.ConvertTransaction(&transaction)
	if err != nil {
		return fmt.Errorf("creating data transaction: %w", err)
	}
	if txParticipants.Cardinality() != 0 {
		for participant := range txParticipants.Iter() {
			i.Buffer.PushParticipantTransaction(participant, *dataTx)
		}
	}

	// 2. Index tx.Operations() participants
	opsParticipants, err := i.participantsProcessor.GetOperationsParticipants(transaction)
	if err != nil {
		return fmt.Errorf("getting operations participants: %w", err)
	}
	var dataOp *types.Operation
	var effectsStateChanges, contractDeployStateChanges, contractEventsStateChanges []types.StateChange
	for opID, opParticipants := range opsParticipants {
		dataOp, err = processors.ConvertOperation(&transaction, &opParticipants.OpWrapper.Operation, opID)
		if err != nil {
			return fmt.Errorf("creating data operation: %w", err)
		}

		for participant := range opParticipants.Participants.Iter() {
			i.Buffer.PushParticipantOperation(participant, *dataOp, *dataTx)
		}

		// 2.1. Index effects state changes
		effectsStateChanges, err = i.effectsProcessor.ProcessOperation(ctx, opParticipants.OpWrapper)
		if err != nil {
			return fmt.Errorf("processing effects state changes: %w", err)
		}
		i.Buffer.PushStateChanges(effectsStateChanges)

		// 2.2. Index contract deploy state changes
		contractDeployStateChanges, err = i.contractDeployProcessor.ProcessOperation(ctx, opParticipants.OpWrapper)
		if err != nil && !errors.Is(err, processors.ErrInvalidOpType) {
			return fmt.Errorf("processing contract deploy state changes: %w", err)
		}
		i.Buffer.PushStateChanges(contractDeployStateChanges)

		// 2.3 Index contract events state changes
		contractEventsStateChanges, err = i.contractEventsProcessor.ProcessOperation(ctx, opParticipants.OpWrapper)
		if err != nil && !errors.Is(err, processors.ErrInvalidOpType) {
			return fmt.Errorf("processing contract events state changes: %w", err)
		}
		i.Buffer.PushStateChanges(contractEventsStateChanges)
	}

	// 3. Index token transfer state changes
	tokenTransferStateChanges, err := i.tokenTransferProcessor.ProcessTransaction(ctx, transaction)
	if err != nil {
		return fmt.Errorf("processing token transfer state changes: %w", err)
	}
	i.Buffer.PushStateChanges(tokenTransferStateChanges)

	// Generate IDs for state changes
	i.Buffer.CalculateStateChangeOrder()

	return nil
}
