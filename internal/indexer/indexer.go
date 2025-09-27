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
)

type IndexerBufferInterface interface {
	PushParticipantTransaction(participant string, transaction types.Transaction)
	PushParticipantOperation(participant string, operation types.Operation, transaction types.Transaction)
	GetParticipantTransactions(participant string) []types.Transaction
	GetParticipantOperations(participant string) map[int64]types.Operation
	PushStateChange(stateChange types.StateChange)
	GetParticipants() set.Set[string]
	GetNumberOfTransactions() int
	GetAllTransactions() []types.Transaction
	GetAllOperations() []types.Operation
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

type AccountModelInterface interface {
	BatchGetByIDs(ctx context.Context, accountIDs []string) ([]string, error)
}

type Indexer struct {
	Buffer                 IndexerBufferInterface
	participantsProcessor  ParticipantsProcessorInterface
	tokenTransferProcessor TokenTransferProcessorInterface
	processors             []OperationProcessorInterface
	accountModel           AccountModelInterface
}

func NewIndexer(networkPassphrase string, ledgerEntryProvider processors.LedgerEntryProvider, accountModel AccountModelInterface) *Indexer {
	return &Indexer{
		Buffer:                 NewIndexerBuffer(),
		participantsProcessor:  processors.NewParticipantsProcessor(networkPassphrase),
		tokenTransferProcessor: processors.NewTokenTransferProcessor(networkPassphrase),
		processors: []OperationProcessorInterface{
			processors.NewEffectsProcessor(networkPassphrase, ledgerEntryProvider),
			processors.NewContractDeployProcessor(networkPassphrase),
			contract_processors.NewSACEventsProcessor(networkPassphrase),
		},
		accountModel: accountModel,
	}
}

func (i *Indexer) ProcessTransaction(ctx context.Context, transaction ingest.LedgerTransaction) error {
	// Collect all participants from transaction, operations, and state changes
	allParticipants := set.NewSet[string]()

	// 1. Get transaction participants
	txParticipants, err := i.participantsProcessor.GetTransactionParticipants(transaction)
	if err != nil {
		return fmt.Errorf("getting transaction participants: %w", err)
	}
	allParticipants = allParticipants.Union(txParticipants)

	// 2. Get operations participants
	opsParticipants, err := i.participantsProcessor.GetOperationsParticipants(transaction)
	if err != nil {
		return fmt.Errorf("getting operations participants: %w", err)
	}
	for _, opParticipants := range opsParticipants {
		allParticipants = allParticipants.Union(opParticipants.Participants)
	}

	// 3. Process operations to get state changes and collect their participants
	stateChanges := []types.StateChange{}
	for _, opParticipants := range opsParticipants {
		for _, processor := range i.processors {
			processorStateChanges, processorErr := processor.ProcessOperation(ctx, opParticipants.OpWrapper)
			if processorErr != nil && !errors.Is(processorErr, processors.ErrInvalidOpType) {
				return fmt.Errorf("processing %s state changes: %w", processor.Name(), processorErr)
			}
			stateChanges = append(stateChanges, processorStateChanges...)
		}
	}

	// 4. Get token transfer state changes
	tokenTransferStateChanges, err := i.tokenTransferProcessor.ProcessTransaction(ctx, transaction)
	if err != nil {
		return fmt.Errorf("processing token transfer state changes: %w", err)
	}
	stateChanges = append(stateChanges, tokenTransferStateChanges...)

	// Add state change participants to the set
	for _, stateChange := range stateChanges {
		allParticipants.Add(stateChange.AccountID)
	}

	// Single batch lookup to check which participants exist in the database
	existingAccountsSlice, err := i.accountModel.BatchGetByIDs(ctx, allParticipants.ToSlice())
	if err != nil {
		return fmt.Errorf("batch checking participants: %w", err)
	}

	// Convert to map for fast lookups
	existingAccounts := make(map[string]bool)
	for _, account := range existingAccountsSlice {
		existingAccounts[account] = true
	}

	// Convert transaction data
	dataTx, err := processors.ConvertTransaction(&transaction)
	if err != nil {
		return fmt.Errorf("creating data transaction: %w", err)
	}

	// Process transaction participants
	if txParticipants.Cardinality() != 0 {
		for participant := range txParticipants.Iter() {
			if !existingAccounts[participant] {
				continue
			}
			i.Buffer.PushParticipantTransaction(participant, *dataTx)
		}
	}

	// Process operations participants
	var dataOp *types.Operation
	for opID, opParticipants := range opsParticipants {
		dataOp, err = processors.ConvertOperation(&transaction, &opParticipants.OpWrapper.Operation, opID)
		if err != nil {
			return fmt.Errorf("creating data operation: %w", err)
		}

		for participant := range opParticipants.Participants.Iter() {
			if !existingAccounts[participant] {
				continue
			}
			i.Buffer.PushParticipantOperation(participant, *dataOp, *dataTx)
		}
	}

	// Process state changes
	for _, stateChange := range stateChanges {
		if !existingAccounts[stateChange.AccountID] {
			continue
		}
		i.Buffer.PushStateChange(stateChange)
	}

	// Generate IDs for state changes
	i.Buffer.CalculateStateChangeOrder()

	return nil
}
