package indexer

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/alitto/pond/v2"
	set "github.com/deckarep/golang-set/v2"
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/wallet-backend/internal/data"
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
	GetAllParticipants() []string
	GetNumberOfTransactions() int
	GetNumberOfOperations() int
	GetTransactions() []*types.Transaction
	GetOperations() []*types.Operation
	GetStateChanges() []types.StateChange
	GetTrustlineChanges() map[TrustlineChangeKey]types.TrustlineChange
	GetContractChanges() []types.ContractChange
	GetAccountChanges() map[string]types.AccountChange
	GetSACBalanceChanges() map[SACBalanceChangeKey]types.SACBalanceChange
	PushContractChange(contractChange types.ContractChange)
	PushTrustlineChange(trustlineChange types.TrustlineChange)
	PushAccountChange(accountChange types.AccountChange)
	PushSACBalanceChange(sacBalanceChange types.SACBalanceChange)
	PushSACContract(c *data.Contract)
	GetUniqueTrustlineAssets() []data.TrustlineAsset
	GetUniqueContractsByID() map[string]types.ContractType
	GetSACContracts() map[string]*data.Contract
	Merge(other IndexerBufferInterface)
	Clear()
}

type TokenTransferProcessorInterface interface {
	ProcessTransaction(ctx context.Context, tx ingest.LedgerTransaction) ([]types.StateChange, error)
}

type ParticipantsProcessorInterface interface {
	GetTransactionParticipants(transaction ingest.LedgerTransaction) (set.Set[string], error)
	GetOperationsParticipants(transaction ingest.LedgerTransaction) (map[int64]processors.OperationParticipants, error)
}

type OperationProcessorInterface interface {
	ProcessOperation(ctx context.Context, opWrapper *processors.TransactionOperationWrapper) ([]types.StateChange, error)
	Name() string
}

// LedgerChangeProcessor is a generic interface for processors that extract data from ledger changes.
type LedgerChangeProcessor[T any] interface {
	ProcessOperation(ctx context.Context, opWrapper *processors.TransactionOperationWrapper) ([]T, error)
	Name() string
}

type Indexer struct {
	participantsProcessor  ParticipantsProcessorInterface
	tokenTransferProcessor TokenTransferProcessorInterface
	trustlinesProcessor    LedgerChangeProcessor[types.TrustlineChange]
	accountsProcessor      LedgerChangeProcessor[types.AccountChange]
	sacBalancesProcessor   LedgerChangeProcessor[types.SACBalanceChange]
	processors             []OperationProcessorInterface
	pool                   pond.Pool
	metricsService         processors.MetricsServiceInterface
	skipTxMeta             bool
	skipTxEnvelope         bool
	networkPassphrase      string
}

func NewIndexer(networkPassphrase string, pool pond.Pool, metricsService processors.MetricsServiceInterface, skipTxMeta bool, skipTxEnvelope bool) *Indexer {
	return &Indexer{
		participantsProcessor:  processors.NewParticipantsProcessor(networkPassphrase),
		tokenTransferProcessor: processors.NewTokenTransferProcessor(networkPassphrase, metricsService),
		trustlinesProcessor:    processors.NewTrustlinesProcessor(networkPassphrase, metricsService),
		accountsProcessor:      processors.NewAccountsProcessor(networkPassphrase, metricsService),
		sacBalancesProcessor:   processors.NewSACBalancesProcessor(networkPassphrase, metricsService),
		processors: []OperationProcessorInterface{
			processors.NewEffectsProcessor(networkPassphrase, metricsService),
			processors.NewContractDeployProcessor(networkPassphrase, metricsService),
			contract_processors.NewSACEventsProcessor(networkPassphrase, metricsService),
		},
		pool:              pool,
		metricsService:    metricsService,
		skipTxMeta:        skipTxMeta,
		skipTxEnvelope:    skipTxEnvelope,
		networkPassphrase: networkPassphrase,
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
		ledgerBuffer.Merge(buffer)
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
	dataTx, err := processors.ConvertTransaction(&tx, i.skipTxMeta, i.skipTxEnvelope, i.networkPassphrase)
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

	// Process trustline, account, and SAC balance changes from ledger changes
	for _, opParticipants := range opsParticipants {
		trustlineChanges, tlErr := i.trustlinesProcessor.ProcessOperation(ctx, opParticipants.OpWrapper)
		if tlErr != nil {
			return 0, fmt.Errorf("processing trustline changes: %w", tlErr)
		}
		for _, tlChange := range trustlineChanges {
			buffer.PushTrustlineChange(tlChange)
		}

		accountChanges, accErr := i.accountsProcessor.ProcessOperation(ctx, opParticipants.OpWrapper)
		if accErr != nil {
			return 0, fmt.Errorf("processing account changes: %w", accErr)
		}
		for _, accChange := range accountChanges {
			buffer.PushAccountChange(accChange)
		}

		sacBalanceChanges, sacErr := i.sacBalancesProcessor.ProcessOperation(ctx, opParticipants.OpWrapper)
		if sacErr != nil {
			return 0, fmt.Errorf("processing SAC balance changes: %w", sacErr)
		}
		for _, sacChange := range sacBalanceChanges {
			buffer.PushSACBalanceChange(sacChange)
		}
	}

	// Process state changes to extract contract changes
	for _, stateChange := range stateChanges {
		//exhaustive:ignore
		switch stateChange.StateChangeCategory {
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
		// Skip empty state changes (no account to associate with)
		if stateChange.AccountID == "" {
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
