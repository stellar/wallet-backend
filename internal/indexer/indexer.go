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

// isContractAddress determines if the given address is a contract address (C...) or account address (G...)
func isContractAddress(address string) bool {
	// Contract addresses start with 'C' and account addresses start with 'G'
	return len(address) > 0 && address[0] == 'C'
}

type IndexerBufferInterface interface {
	PushTransaction(participant string, transaction types.Transaction)
	PushOperation(participant string, operation types.Operation, transaction types.Transaction)
	PushStateChange(transaction types.Transaction, operation types.Operation, stateChange types.StateChange)
	GetTransactionsParticipants() map[string]set.Set[string]
	GetOperationsParticipants() map[int64]set.Set[string]
	GetNumberOfTransactions() int
	GetNumberOfOperations() int
	GetTransactions() []types.Transaction
	GetOperations() []types.Operation
	GetStateChanges() []types.StateChange
	GetTrustlineChanges() []types.TrustlineChange
	GetContractChanges() []types.ContractChange
	PushContractChange(contractChange types.ContractChange)
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

type Indexer struct {
	participantsProcessor  ParticipantsProcessorInterface
	tokenTransferProcessor TokenTransferProcessorInterface
	processors             []OperationProcessorInterface
	pool                   pond.Pool
	metricsService         processors.MetricsServiceInterface
	skipTxMeta             bool
}

func NewIndexer(networkPassphrase string, pool pond.Pool, metricsService processors.MetricsServiceInterface, skipTxMeta bool) *Indexer {
	return &Indexer{
		participantsProcessor:  processors.NewParticipantsProcessor(networkPassphrase),
		tokenTransferProcessor: processors.NewTokenTransferProcessor(networkPassphrase, metricsService),
		processors: []OperationProcessorInterface{
			processors.NewEffectsProcessor(networkPassphrase, metricsService),
			processors.NewContractDeployProcessor(networkPassphrase, metricsService),
			contract_processors.NewSACEventsProcessor(networkPassphrase, metricsService),
		},
		pool:           pool,
		metricsService: metricsService,
		skipTxMeta:     skipTxMeta,
	}
}

// ProcessLedgerTransactions processes all transactions in a ledger in parallel.
// It collects transaction data (participants, operations, state changes) and populates the buffer in a single pass.
// Returns the total participant count for metrics.
func (i *Indexer) ProcessLedgerTransactions(ctx context.Context, transactions []ingest.LedgerTransaction, ledgerBuffer IndexerBufferInterface) (int, error) {
	group := i.pool.NewGroupContext(ctx)

	txnBuffers := make([]*IndexerBuffer, len(transactions))
	participantCounts := make([]int, len(transactions))
	var errs []error
	errMu := sync.Mutex{}

	for idx, tx := range transactions {
		index := idx
		tx := tx
		group.Submit(func() {
			buffer := NewIndexerBuffer()
			count, err := i.processTransaction(ctx, tx, buffer)
			if err != nil {
				errMu.Lock()
				errs = append(errs, fmt.Errorf("processing transaction at ledger=%d tx=%d: %w", tx.Ledger.LedgerSequence(), tx.Index, err))
				errMu.Unlock()
				return
			}
			txnBuffers[index] = buffer
			participantCounts[index] = count
		})
	}

	if err := group.Wait(); err != nil {
		return 0, fmt.Errorf("waiting for transaction processing: %w", err)
	}
	if len(errs) > 0 {
		return 0, fmt.Errorf("processing transactions: %w", errors.Join(errs...))
	}

	// Merge buffers and count participants
	totalParticipants := 0
	for idx, buffer := range txnBuffers {
		ledgerBuffer.MergeBuffer(buffer)
		totalParticipants += participantCounts[idx]
	}

	return totalParticipants, nil
}

// processTransaction processes a single transaction - collects data and populates buffer.
// Returns participant count for metrics.
func (i *Indexer) processTransaction(ctx context.Context, tx ingest.LedgerTransaction, buffer *IndexerBuffer) (int, error) {
	// Get transaction participants
	txParticipants, err := i.participantsProcessor.GetTransactionParticipants(tx)
	if err != nil {
		return 0, fmt.Errorf("getting transaction participants: %w", err)
	}

	// Get operations participants
	opsParticipants, err := i.participantsProcessor.GetOperationsParticipants(tx)
	if err != nil {
		return 0, fmt.Errorf("getting operations participants: %w", err)
	}

	// Get state changes
	stateChanges, err := i.getTransactionStateChanges(ctx, tx, opsParticipants)
	if err != nil {
		return 0, fmt.Errorf("getting transaction state changes: %w", err)
	}

	// Convert transaction data
	dataTx, err := processors.ConvertTransaction(&tx, i.skipTxMeta)
	if err != nil {
		return 0, fmt.Errorf("creating data transaction: %w", err)
	}

	// Count all unique participants for metrics
	allParticipants := set.NewSet[string]()
	allParticipants = allParticipants.Union(txParticipants)
	for _, opParticipants := range opsParticipants {
		allParticipants = allParticipants.Union(opParticipants.Participants)
	}
	for _, stateChange := range stateChanges {
		allParticipants.Add(stateChange.AccountID)
	}

	// Insert transaction participants
	for participant := range txParticipants.Iter() {
		buffer.PushTransaction(participant, *dataTx)
	}

	// Insert operations participants
	operationsMap := make(map[int64]*types.Operation)
	for opID, opParticipants := range opsParticipants {
		dataOp, opErr := processors.ConvertOperation(&tx, &opParticipants.OpWrapper.Operation, opID)
		if opErr != nil {
			return 0, fmt.Errorf("creating data operation: %w", opErr)
		}
		operationsMap[opID] = dataOp
		for participant := range opParticipants.Participants.Iter() {
			buffer.PushOperation(participant, *dataOp, *dataTx)
		}
	}

	// Process state changes to extract trustline and contract changes
	for _, stateChange := range stateChanges {
		switch stateChange.StateChangeCategory {
		case types.StateChangeCategoryTrustline:
			trustlineChange := types.TrustlineChange{
				AccountID:    stateChange.AccountID,
				OperationID:  stateChange.OperationID,
				Asset:        stateChange.TrustlineAsset,
				LedgerNumber: tx.Ledger.LedgerSequence(),
			}
			//exhaustive:ignore
			switch *stateChange.StateChangeReason {
			case types.StateChangeReasonAdd:
				trustlineChange.Operation = types.TrustlineOpAdd
			case types.StateChangeReasonRemove:
				trustlineChange.Operation = types.TrustlineOpRemove
			case types.StateChangeReasonUpdate:
				continue
			}
			buffer.PushTrustlineChange(trustlineChange)
		case types.StateChangeCategoryBalance:
			// Only store contract changes when:
			// - Account is C-address, OR
			// - Account is G-address AND contract is NOT SAC or NATIVE (custom/SEP41 tokens): SAC token balances for G-addresses are stored in trustlines
			accountIsContract := isContractAddress(stateChange.AccountID)
			tokenIsSACOrNative := stateChange.ContractType == types.ContractTypeSAC || stateChange.ContractType == types.ContractTypeNative

			if accountIsContract || !tokenIsSACOrNative {
				contractChange := types.ContractChange{
					AccountID:    stateChange.AccountID,
					OperationID:  stateChange.OperationID,
					ContractID:   stateChange.TokenID.String,
					LedgerNumber: tx.Ledger.LedgerSequence(),
					ContractType: stateChange.ContractType,
				}
				buffer.PushContractChange(contractChange)
			}
		}
	}

	// Sort state changes and set order for this transaction
	sort.Slice(stateChanges, func(i, j int) bool {
		return stateChanges[i].SortKey < stateChanges[j].SortKey
	})

	perOpIdx := make(map[int64]int)
	for idx := range stateChanges {
		sc := &stateChanges[idx]

		// State changes are 1-indexed within an operation/transaction.
		if sc.OperationID != 0 {
			perOpIdx[sc.OperationID]++
			sc.StateChangeOrder = int64(perOpIdx[sc.OperationID])
		} else {
			sc.StateChangeOrder = 1
		}
	}

	// Insert state changes
	for _, stateChange := range stateChanges {
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

	return allParticipants.Cardinality(), nil
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
