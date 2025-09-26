package indexer

import (
	"context"
	"errors"
	"fmt"

	set "github.com/deckarep/golang-set/v2"
	"github.com/stellar/go/ingest"
	operation_processor "github.com/stellar/go/processors/operation"

	"github.com/stellar/wallet-backend/internal/indexer/processors"
	contract_processors "github.com/stellar/wallet-backend/internal/indexer/processors/contracts"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/store"
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
	Name() string
}

type Indexer struct {
	Buffer                 IndexerBufferInterface
	participantsProcessor  ParticipantsProcessorInterface
	tokenTransferProcessor TokenTransferProcessorInterface
	processors             []OperationProcessorInterface
	accountsStore          store.AccountsStore
}

func NewIndexer(networkPassphrase string, ledgerEntryProvider processors.LedgerEntryProvider, accountsStore store.AccountsStore) *Indexer {
	return &Indexer{
		Buffer:                 NewIndexerBuffer(),
		participantsProcessor:  processors.NewParticipantsProcessor(networkPassphrase),
		tokenTransferProcessor: processors.NewTokenTransferProcessor(networkPassphrase),
		processors: []OperationProcessorInterface{
			processors.NewEffectsProcessor(networkPassphrase, ledgerEntryProvider),
			processors.NewContractDeployProcessor(networkPassphrase),
			contract_processors.NewSACEventsProcessor(networkPassphrase),
		},
		accountsStore: accountsStore,
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
	for opID, opParticipants := range opsParticipants {
		dataOp, err = processors.ConvertOperation(&transaction, &opParticipants.OpWrapper.Operation, opID)
		if err != nil {
			return fmt.Errorf("creating data operation: %w", err)
		}

		for participant := range opParticipants.Participants.Iter() {
			i.Buffer.PushParticipantOperation(participant, *dataOp, *dataTx)
		}

		for _, processor := range i.processors {
			stateChanges, processorErr := processor.ProcessOperation(ctx, opParticipants.OpWrapper)
			if processorErr != nil && !errors.Is(processorErr, processors.ErrInvalidOpType) {
				return fmt.Errorf("processing %s state changes: %w", processor.Name(), processorErr)
			}
			i.Buffer.PushStateChanges(stateChanges)
		}
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
