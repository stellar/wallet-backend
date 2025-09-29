package indexer

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/alitto/pond"
	set "github.com/deckarep/golang-set/v2"
	"github.com/stellar/go/ingest"
	operation_processor "github.com/stellar/go/processors/operation"
	"github.com/stellar/go/support/log"

	"github.com/stellar/wallet-backend/internal/indexer/processors"
	contract_processors "github.com/stellar/wallet-backend/internal/indexer/processors/contracts"
	"github.com/stellar/wallet-backend/internal/indexer/types"
)

const (
	numPoolWorkers = 8
)

type IndexerBufferInterface interface {
	PushParticipantTransaction(participant string, transaction types.Transaction)
	PushParticipantOperation(participant string, operation types.Operation, transaction types.Transaction)
	GetParticipantTransactions(participant string) []types.Transaction
	GetParticipantOperations(participant string) map[int64]types.Operation
	PushStateChange(transaction types.Transaction, operation types.Operation, stateChange types.StateChange)
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
	Transaction     ingest.LedgerTransaction
	TxParticipants  set.Set[string]
	OpsParticipants map[int64]processors.OperationParticipants
	StateChanges    []types.StateChange
	AllParticipants set.Set[string] // Union of all participants for this transaction
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

// CollectAllTransactionData collects all transaction data (participants, operations, state changes) from all transactions in a ledger in parallel
func (i *Indexer) CollectAllTransactionData(ctx context.Context, transactions []ingest.LedgerTransaction) ([]PrecomputedTransactionData, set.Set[string], error) {
	// Process transaction data collection in parallel
	dataPool := pond.New(numPoolWorkers, len(transactions), pond.Context(ctx))

	var precomputedData []PrecomputedTransactionData
	precomputedDataMu := sync.Mutex{}
	var errs []error
	errMu := sync.Mutex{}

	for _, tx := range transactions {
		dataPool.Submit(func() {
			txData := PrecomputedTransactionData{
				Transaction:     tx,
				AllParticipants: set.NewSet[string](),
			}

			// Get transaction participants
			txParticipants, err := i.participantsProcessor.GetTransactionParticipants(tx)
			if err != nil {
				errMu.Lock()
				errs = append(errs, fmt.Errorf("getting transaction participants: %w", err))
				errMu.Unlock()
				return
			}
			txData.TxParticipants = txParticipants
			txData.AllParticipants = txData.AllParticipants.Union(txParticipants)

			// Get operations participants
			opsParticipants, err := i.participantsProcessor.GetOperationsParticipants(tx)
			if err != nil {
				errMu.Lock()
				errs = append(errs, fmt.Errorf("getting operations participants: %w", err))
				errMu.Unlock()
				return
			}
			txData.OpsParticipants = opsParticipants
			for _, opParticipants := range opsParticipants {
				txData.AllParticipants = txData.AllParticipants.Union(opParticipants.Participants)
			}

			// Get state changes
			stateChanges, err := i.getTransactionStateChanges(ctx, tx, opsParticipants)
			if err != nil {
				errMu.Lock()
				errs = append(errs, fmt.Errorf("getting transaction state changes: %w", err))
				errMu.Unlock()
				return
			}
			txData.StateChanges = stateChanges
			for _, stateChange := range stateChanges {
				txData.AllParticipants.Add(stateChange.AccountID)
			}

			// Add to collection
			precomputedDataMu.Lock()
			precomputedData = append(precomputedData, txData)
			precomputedDataMu.Unlock()
		})
	}
	dataPool.StopAndWait()
	if len(errs) > 0 {
		return nil, nil, fmt.Errorf("collecting transaction data: %w", errors.Join(errs...))
	}

	// Merge all participant sets for the single DB call
	allParticipants := set.NewSet[string]()
	for _, txData := range precomputedData {
		allParticipants = allParticipants.Union(txData.AllParticipants)
	}

	return precomputedData, allParticipants, nil
}

// ProcessTransactions processes transactions, operations and state changes using precomputed data. It then inserts them into the indexer buffer.
func (i *Indexer) ProcessTransactions(ctx context.Context, precomputedData []PrecomputedTransactionData, existingAccounts set.Set[string]) error {
	// Process transactions in parallel using precomputed data
	pool := pond.New(numPoolWorkers, len(precomputedData), pond.Context(ctx))

	var errs []error
	errMu := sync.Mutex{}

	// Submit transaction processing tasks to the pool
	for _, txData := range precomputedData {
		pool.Submit(func() {
			if err := i.processPrecomputedTransaction(ctx, txData, existingAccounts); err != nil {
				errMu.Lock()
				defer errMu.Unlock()
				errs = append(errs, fmt.Errorf("processing precomputed transaction data at ledger=%d tx=%d: %w", txData.Transaction.Ledger.LedgerSequence(), txData.Transaction.Index, err))
			}
		})
	}

	pool.StopAndWait()
	if len(errs) > 0 {
		return fmt.Errorf("processing transactions: %w", errors.Join(errs...))
	}

	return nil
}

// processPrecomputedTransaction processes a transaction using precomputed data without re-computing participants or state changes
func (i *Indexer) processPrecomputedTransaction(ctx context.Context, precomputedData PrecomputedTransactionData, existingAccounts set.Set[string]) error {
	// Convert transaction data
	dataTx, err := processors.ConvertTransaction(&precomputedData.Transaction)
	if err != nil {
		return fmt.Errorf("creating data transaction: %w", err)
	}

	// Insert transaction participants
	if precomputedData.TxParticipants.Cardinality() != 0 {
		for participant := range precomputedData.TxParticipants.Iter() {
			if !existingAccounts.Contains(participant) {
				continue
			}
			i.Buffer.PushParticipantTransaction(participant, *dataTx)
		}
	}

	// Insert operations participants
	var dataOp *types.Operation
	operationsMap := make(map[int64]*types.Operation)
	for opID, opParticipants := range precomputedData.OpsParticipants {
		dataOp, err = processors.ConvertOperation(&precomputedData.Transaction, &opParticipants.OpWrapper.Operation, opID)
		if err != nil {
			return fmt.Errorf("creating data operation: %w", err)
		}
		operationsMap[opID] = dataOp
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

		// Get the correct operation for this state change
		var operation types.Operation
		if stateChange.OperationID != 0 {
			correctOp := operationsMap[stateChange.OperationID]
			if correctOp == nil {
				log.Ctx(ctx).Errorf("operation ID %d not found in operations map for state change %+v", stateChange.OperationID, fmt.Sprintf("%d-%d", stateChange.ToID, stateChange.StateChangeOrder))
				continue
			}
			operation = *correctOp
		}
		// For fee state changes (OperationID == 0), operation remains zero value
		i.Buffer.PushStateChange(*dataTx, operation, stateChange)
	}

	return nil
}

// getTransactionStateChanges processes operations of a transaction and calculates all state changes
func (i *Indexer) getTransactionStateChanges(ctx context.Context, transaction ingest.LedgerTransaction, opsParticipants map[int64]processors.OperationParticipants) ([]types.StateChange, error) {
	pool := pond.New(numPoolWorkers, len(opsParticipants), pond.Context(ctx))

	var errs []error
	errMu := sync.Mutex{}

	stateChanges := []types.StateChange{}
	stateChangesMu := sync.Mutex{}
	for _, opParticipants := range opsParticipants {
		pool.Submit(func() {
			for _, processor := range i.processors {
				processorStateChanges, processorErr := processor.ProcessOperation(ctx, opParticipants.OpWrapper)
				if processorErr != nil && !errors.Is(processorErr, processors.ErrInvalidOpType) {
					errMu.Lock()
					errs = append(errs, fmt.Errorf("processing %s state changes: %w", processor.Name(), processorErr))
					errMu.Unlock()
					return
				}
				stateChangesMu.Lock()
				stateChanges = append(stateChanges, processorStateChanges...)
				stateChangesMu.Unlock()
			}
		})
	}
	pool.StopAndWait()
	if len(errs) > 0 {
		return nil, fmt.Errorf("processing state changes: %w", errors.Join(errs...))
	}

	// Get token transfer state changes
	tokenTransferStateChanges, err := i.tokenTransferProcessor.ProcessTransaction(ctx, transaction)
	if err != nil {
		return nil, fmt.Errorf("processing token transfer state changes: %w", err)
	}
	stateChanges = append(stateChanges, tokenTransferStateChanges...)

	return stateChanges, nil
}
