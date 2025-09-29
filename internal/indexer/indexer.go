package indexer

import (
	"context"
	"errors"
	"fmt"
	"sort"
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

// PrecomputedTransactionData holds all the data needed to process a transaction
// without re-computing participants, operations participants, and state changes.
type PrecomputedTransactionData struct {
	Transaction      ingest.LedgerTransaction
	TxParticipants   set.Set[string]
	OpsParticipants  map[int64]processors.OperationParticipants
	StateChanges     []types.StateChange
	AllParticipants  set.Set[string] // Union of all participants for this transaction
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

// ProcessPrecomputedTransaction processes a transaction using precomputed data without re-computing participants or state changes
func (i *Indexer) ProcessTransaction(ctx context.Context, precomputedData PrecomputedTransactionData, existingAccounts set.Set[string]) error {
	// Convert transaction data
	dataTx, err := processors.ConvertTransaction(&precomputedData.Transaction)
	if err != nil {
		return fmt.Errorf("creating data transaction: %w", err)
	}

	// Process transaction participants
	if precomputedData.TxParticipants.Cardinality() != 0 {
		for participant := range precomputedData.TxParticipants.Iter() {
			if !existingAccounts.Contains(participant) {
				continue
			}
			i.Buffer.PushParticipantTransaction(participant, *dataTx)
		}
	}

	// Process operations participants
	var dataOp *types.Operation
	for opID, opParticipants := range precomputedData.OpsParticipants {
		dataOp, err = processors.ConvertOperation(&precomputedData.Transaction, &opParticipants.OpWrapper.Operation, opID)
		if err != nil {
			return fmt.Errorf("creating data operation: %w", err)
		}

		for participant := range opParticipants.Participants.Iter() {
			if !existingAccounts.Contains(participant) {
				continue
			}
			i.Buffer.PushParticipantOperation(participant, *dataOp, *dataTx)
		}
	}

	// Sort state changes and set order for this transaction
	stateChanges := precomputedData.StateChanges
	sort.Slice(stateChanges, func(i, j int) bool {
		return stateChanges[i].SortKey < stateChanges[j].SortKey
	})

	perOpIdx := make(map[int64]int)
	for i := range stateChanges {
		sc := &stateChanges[i]

		// State changes are 1-indexed within an operation/transaction.
		if sc.OperationID != 0 {
			perOpIdx[sc.OperationID]++
			sc.StateChangeOrder = int64(perOpIdx[sc.OperationID])
		} else {
			sc.StateChangeOrder = 1
		}
	}

	// Process state changes
	for _, stateChange := range stateChanges {
		if !existingAccounts.Contains(stateChange.AccountID) {
			continue
		}
		i.Buffer.PushStateChange(stateChange)
	}

	return nil
}

// GetTransactionParticipants returns all participants for a transaction
func (i *Indexer) GetTransactionParticipants(transaction ingest.LedgerTransaction) (set.Set[string], error) {
	return i.participantsProcessor.GetTransactionParticipants(transaction)
}

// GetOperationsParticipants returns all participants for operations in a transaction
func (i *Indexer) GetOperationsParticipants(transaction ingest.LedgerTransaction) (map[int64]processors.OperationParticipants, error) {
	return i.participantsProcessor.GetOperationsParticipants(transaction)
}

// GetTransactionStateChanges gets state changes for a transaction without storing them in the buffer
func (i *Indexer) GetTransactionStateChanges(ctx context.Context, transaction ingest.LedgerTransaction, opsParticipants map[int64]processors.OperationParticipants) ([]types.StateChange, error) {
	// Process operations to get state changes
	stateChanges := []types.StateChange{}
	for _, opParticipants := range opsParticipants {
		for _, processor := range i.processors {
			processorStateChanges, processorErr := processor.ProcessOperation(ctx, opParticipants.OpWrapper)
			if processorErr != nil && !errors.Is(processorErr, processors.ErrInvalidOpType) {
				return nil, fmt.Errorf("processing %s state changes: %w", processor.Name(), processorErr)
			}
			stateChanges = append(stateChanges, processorStateChanges...)
		}
	}

	// Get token transfer state changes
	tokenTransferStateChanges, err := i.tokenTransferProcessor.ProcessTransaction(ctx, transaction)
	if err != nil {
		return nil, fmt.Errorf("processing token transfer state changes: %w", err)
	}
	stateChanges = append(stateChanges, tokenTransferStateChanges...)

	return stateChanges, nil
}

// GetAccountModel returns the account model for external access
func (i *Indexer) GetAccountModel() AccountModelInterface {
	return i.accountModel
}

// ProcessTransactionWithPrefetchedAccounts processes a transaction with pre-fetched existing accounts
func (i *Indexer) ProcessTransactionWithPrefetchedAccounts(ctx context.Context, transaction ingest.LedgerTransaction, existingAccounts set.Set[string]) error {
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

	// Convert transaction data
	dataTx, err := processors.ConvertTransaction(&transaction)
	if err != nil {
		return fmt.Errorf("creating data transaction: %w", err)
	}

	// Process transaction participants
	if txParticipants.Cardinality() != 0 {
		for participant := range txParticipants.Iter() {
			if !existingAccounts.Contains(participant) {
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
			if !existingAccounts.Contains(participant) {
				continue
			}
			i.Buffer.PushParticipantOperation(participant, *dataOp, *dataTx)
		}
	}

	sort.Slice(stateChanges, func(i, j int) bool {
		return stateChanges[i].SortKey < stateChanges[j].SortKey
	})

	perOpIdx := make(map[int64]int)
	for i := range stateChanges {
		sc := &stateChanges[i]

		// State changes are 1-indexed within an operation/transaction.
		if sc.OperationID != 0 {
			perOpIdx[sc.OperationID]++
			sc.StateChangeOrder = int64(perOpIdx[sc.OperationID])
		} else {
			sc.StateChangeOrder = 1
		}
	}

	// Process state changes
	for _, stateChange := range stateChanges {
		if !existingAccounts.Contains(stateChange.AccountID) {
			continue
		}
		i.Buffer.PushStateChange(stateChange)
	}

	return nil
}
