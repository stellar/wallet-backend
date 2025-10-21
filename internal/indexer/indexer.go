package indexer

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/alitto/pond/v2"
	set "github.com/deckarep/golang-set/v2"
	"github.com/stellar/go/ingest"
	operation_processor "github.com/stellar/go/processors/operation"
	"github.com/stellar/go/support/log"

	"github.com/stellar/wallet-backend/internal/indexer/processors"
	contract_processors "github.com/stellar/wallet-backend/internal/indexer/processors/contracts"
	"github.com/stellar/wallet-backend/internal/indexer/types"
)

type IndexerBufferInterface interface {
	PushTransaction(participant string, transaction types.Transaction)
	PushOperation(participant string, operation types.Operation, transaction types.Transaction)
	PushStateChange(transaction types.Transaction, operation types.Operation, stateChange types.StateChange)
	GetTransactionsParticipants() map[string]set.Set[string]
	GetOperationsParticipants() map[int64]set.Set[string]
	GetNumberOfTransactions() int
	GetTransactions() []types.Transaction
	GetOperations() []types.Operation
	GetStateChanges() []types.StateChange
	GetTrustlineChanges() []types.TrustlineChange
	PushTrustlineChange(trustlineChange types.TrustlineChange)
	MergeBuffer(other IndexerBufferInterface)
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
	TrustlineChanges []types.TrustlineChange
	AllParticipants set.Set[string] // Union of all participants for this transaction
}

type Indexer struct {
	participantsProcessor  ParticipantsProcessorInterface
	tokenTransferProcessor TokenTransferProcessorInterface
	processors             []OperationProcessorInterface
	pool                   pond.Pool
}

func NewIndexer(networkPassphrase string, ledgerEntryProvider processors.LedgerEntryProvider, pool pond.Pool) *Indexer {
	return &Indexer{
		participantsProcessor:  processors.NewParticipantsProcessor(networkPassphrase),
		tokenTransferProcessor: processors.NewTokenTransferProcessor(networkPassphrase),
		processors: []OperationProcessorInterface{
			processors.NewEffectsProcessor(networkPassphrase, ledgerEntryProvider),
			processors.NewContractDeployProcessor(networkPassphrase),
			contract_processors.NewSACEventsProcessor(networkPassphrase),
		},
		pool: pool,
	}
}

// CollectAllTransactionData collects all transaction data (participants, operations, state changes) from all transactions in a ledger in parallel
func (i *Indexer) CollectAllTransactionData(ctx context.Context, transactions []ingest.LedgerTransaction) ([]PrecomputedTransactionData, set.Set[string], error) {
	// Process transaction data collection in parallel
	group := i.pool.NewGroupContext(ctx)

	precomputedData := make([]PrecomputedTransactionData, len(transactions))
	var errs []error
	errMu := sync.Mutex{}

	for idx, tx := range transactions {
		index := idx
		tx := tx
		group.Submit(func() {
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

			trustlineChanges := []types.TrustlineChange{}
			for _, stateChange := range stateChanges {
				switch stateChange.StateChangeCategory {
				case types.StateChangeCategoryTrustline:
					trustlineChange := types.TrustlineChange{
						AccountID: stateChange.AccountID,
						OperationID: stateChange.OperationID,
						Asset: stateChange.TrustlineAsset,
						LedgerNumber: tx.Ledger.LedgerSequence(),
					}
					switch *stateChange.StateChangeReason {
					case types.StateChangeReasonAdd:
						trustlineChange.Operation = types.TrustlineOpAdd
					case types.StateChangeReasonRemove:
						trustlineChange.Operation = types.TrustlineOpRemove
					case types.StateChangeReasonUpdate:
						continue
					}
					trustlineChanges = append(trustlineChanges, trustlineChange)
				default:
					continue
				}
			}
			txData.TrustlineChanges = trustlineChanges

			// Add to collection
			precomputedData[index] = txData
		})
	}
	if err := group.Wait(); err != nil {
		return nil, nil, fmt.Errorf("waiting for transaction data collection: %w", err)
	}
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
func (i *Indexer) ProcessTransactions(ctx context.Context, precomputedData []PrecomputedTransactionData, existingAccounts set.Set[string], ledgerBuffer IndexerBufferInterface) error {
	// Process transactions in parallel using precomputed data
	group := i.pool.NewGroupContext(ctx)
	var errs []error
	errMu := sync.Mutex{}
	txnBuffers := make([]*IndexerBuffer, len(precomputedData))
	for idx, txData := range precomputedData {
		index := idx
		txData := txData
		group.Submit(func() {
			buffer := NewIndexerBuffer()
			if err := i.processPrecomputedTransaction(ctx, txData, existingAccounts, buffer); err != nil {
				errMu.Lock()
				defer errMu.Unlock()
				errs = append(errs, fmt.Errorf("processing precomputed transaction data at ledger=%d tx=%d: %w", txData.Transaction.Ledger.LedgerSequence(), txData.Transaction.Index, err))
			}
			txnBuffers[index] = buffer
		})
	}
	if err := group.Wait(); err != nil {
		return fmt.Errorf("waiting for transaction processing: %w", err)
	}
	if len(errs) > 0 {
		return fmt.Errorf("processing transactions: %w", errors.Join(errs...))
	}
	for _, buffer := range txnBuffers {
		ledgerBuffer.MergeBuffer(buffer)
	}

	return nil
}

// processPrecomputedTransaction processes a transaction using precomputed data without re-computing participants or state changes
func (i *Indexer) processPrecomputedTransaction(ctx context.Context, precomputedData PrecomputedTransactionData, existingAccounts set.Set[string], buffer *IndexerBuffer) error {
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
			buffer.PushTransaction(participant, *dataTx)
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
			buffer.PushOperation(participant, *dataOp, *dataTx)
		}
	}

	// Insert trustline changes
	for _, trustlineChange := range precomputedData.TrustlineChanges {
		buffer.PushTrustlineChange(trustlineChange)
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
		buffer.PushStateChange(*dataTx, operation, stateChange)
	}

	return nil
}

// getTransactionStateChanges processes operations of a transaction and calculates all state changes
func (i *Indexer) getTransactionStateChanges(ctx context.Context, transaction ingest.LedgerTransaction, opsParticipants map[int64]processors.OperationParticipants) ([]types.StateChange, error) {
	stateChanges := []types.StateChange{}

	// Process operations sequentially since there are only 3 processors per operation
	// Creating a worker pool here adds unnecessary overhead
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
