package indexer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/alitto/pond/v2"
	set "github.com/deckarep/golang-set/v2"
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer/processors"
	contract_processors "github.com/stellar/wallet-backend/internal/indexer/processors/contracts"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/utils"
)

type IndexerBufferInterface interface {
	BatchPushTransactionResult(result *TransactionResult)
	BatchPushChanges(trustlines []types.TrustlineChange, accounts []types.AccountChange, sacBalances []types.SACBalanceChange, sacContracts []*data.Contract)
	GetTransactionsParticipants() map[int64]types.ParticipantSet
	GetOperationsParticipants() map[int64]types.ParticipantSet
	GetNumberOfTransactions() int
	GetNumberOfOperations() int
	GetTransactions() []*types.Transaction
	GetOperations() []*types.Operation
	GetStateChanges() []types.StateChange
	GetTrustlineChanges() map[TrustlineChangeKey]types.TrustlineChange
	GetContractChanges() []types.ContractChange
	GetAccountChanges() map[string]types.AccountChange
	GetSACBalanceChanges() map[SACBalanceChangeKey]types.SACBalanceChange
	GetUniqueTrustlineAssets() []data.TrustlineAsset
	GetUniqueSEP41ContractTokensByID() map[string]types.ContractType
	GetSACContracts() map[string]*data.Contract
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
	sacInstancesProcessor  LedgerChangeProcessor[*data.Contract]
	processors             []OperationProcessorInterface
	pool                   pond.Pool
	ingestionMetrics       *metrics.IngestionMetrics
	networkPassphrase      string
}

func NewIndexer(networkPassphrase string, pool pond.Pool, ingestionMetrics *metrics.IngestionMetrics) *Indexer {
	return &Indexer{
		participantsProcessor:  processors.NewParticipantsProcessor(networkPassphrase),
		tokenTransferProcessor: processors.NewTokenTransferProcessor(networkPassphrase, ingestionMetrics),
		sacBalancesProcessor:   processors.NewSACBalancesProcessor(networkPassphrase, ingestionMetrics),
		sacInstancesProcessor:  processors.NewSACInstanceProcessor(networkPassphrase),
		accountsProcessor:      processors.NewAccountsProcessor(ingestionMetrics),
		trustlinesProcessor:    processors.NewTrustlinesProcessor(ingestionMetrics),
		processors: []OperationProcessorInterface{
			processors.NewEffectsProcessor(networkPassphrase, ingestionMetrics),
			processors.NewContractDeployProcessor(networkPassphrase, ingestionMetrics),
			contract_processors.NewSACEventsProcessor(networkPassphrase, ingestionMetrics),
		},
		pool:              pool,
		ingestionMetrics:  ingestionMetrics,
		networkPassphrase: networkPassphrase,
	}
}

// ProcessLedgerTransactions processes all transactions in a ledger in parallel.
// Each goroutine pushes directly into the shared ledger buffer (protected by mutex).
// The heavy XDR parsing work runs without the lock; only the brief map inserts lock.
// Returns the total participant count for metrics.
func (i *Indexer) ProcessLedgerTransactions(ctx context.Context, transactions []ingest.LedgerTransaction, ledgerBuffer IndexerBufferInterface) (int, error) {
	group := i.pool.NewGroupContext(ctx)

	participantCounts := make([]int, len(transactions))
	var errs []error
	errMu := sync.Mutex{}

	for idx, tx := range transactions {
		group.Submit(func() {
			count, err := i.processTransaction(ctx, tx, ledgerBuffer)
			if err != nil {
				errMu.Lock()
				errs = append(errs, fmt.Errorf("processing transaction at ledger=%d tx=%d: %w", tx.Ledger.LedgerSequence(), tx.Index, err))
				errMu.Unlock()
				return
			}
			participantCounts[idx] = count
		})
	}

	if err := group.Wait(); err != nil {
		return 0, fmt.Errorf("waiting for transaction processing: %w", err)
	}
	if len(errs) > 0 {
		return 0, fmt.Errorf("processing transactions: %w", errors.Join(errs...))
	}

	totalParticipants := 0
	for _, count := range participantCounts {
		totalParticipants += count
	}

	return totalParticipants, nil
}

// processTransaction processes a single transaction - collects data and populates buffer.
// All buffer writes are batched to minimize mutex contention during parallel processing.
// Returns participant count for metrics.
func (i *Indexer) processTransaction(ctx context.Context, tx ingest.LedgerTransaction, buffer IndexerBufferInterface) (int, error) {
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
	dataTx, err := processors.ConvertTransaction(&tx)
	if err != nil {
		return 0, fmt.Errorf("creating data transaction: %w", err)
	}

	// Get operation results for extracting result codes
	opResults, _ := tx.Result.OperationResults()

	// Build operations map and collect participants per operation
	operationsMap := make(map[int64]*types.Operation, len(opsParticipants))
	opParticipantMap := make(map[int64][]string, len(opsParticipants))
	for opID, opP := range opsParticipants {
		dataOp, opErr := processors.ConvertOperation(&tx, &opP.OpWrapper.Operation, opID, opP.OpWrapper.Index, opResults)
		if opErr != nil {
			return 0, fmt.Errorf("creating data operation: %w", opErr)
		}
		operationsMap[opID] = dataOp
		participants := make([]string, 0, opP.Participants.Cardinality())
		for p := range opP.Participants.Iter() {
			participants = append(participants, p)
		}
		opParticipantMap[opID] = participants
	}

	// Build contract changes from state changes
	var contractChanges []types.ContractChange
	for _, sc := range stateChanges {
		if sc.StateChangeCategory == types.StateChangeCategoryBalance && sc.ContractType == types.ContractTypeSEP41 {
			contractChanges = append(contractChanges, types.ContractChange{
				AccountID:    string(sc.AccountID),
				OperationID:  sc.OperationID,
				ContractID:   sc.TokenID.String(),
				LedgerNumber: tx.Ledger.LedgerSequence(),
				ContractType: sc.ContractType,
			})
		}
	}

	// Validate state change operation IDs (log warnings for mismatches)
	for _, sc := range stateChanges {
		if sc.AccountID == "" || sc.OperationID == 0 {
			continue
		}
		if operationsMap[sc.OperationID] == nil {
			log.Ctx(ctx).Errorf("operation ID %d not found in operations map for state change (to_id=%d, category=%s)", sc.OperationID, sc.ToID, sc.StateChangeCategory)
		}
	}

	// Collect tx participant strings
	txParticipantList := make([]string, 0, txParticipants.Cardinality())
	for p := range txParticipants.Iter() {
		txParticipantList = append(txParticipantList, p)
	}

	// Push all transaction data in a single lock acquisition
	buffer.BatchPushTransactionResult(&TransactionResult{
		Transaction:      dataTx,
		TxParticipants:   txParticipantList,
		Operations:       operationsMap,
		OpParticipants:   opParticipantMap,
		ContractChanges:  contractChanges,
		StateChanges:     stateChanges,
		StateChangeOpMap: operationsMap,
	})

	// Process trustline, account, and SAC balance changes from ledger changes.
	// Each operation's changes are pushed in a single lock acquisition.
	for _, opP := range opsParticipants {
		trustlineChanges, tlErr := i.trustlinesProcessor.ProcessOperation(ctx, opP.OpWrapper)
		if tlErr != nil {
			return 0, fmt.Errorf("processing trustline changes: %w", tlErr)
		}

		accountChanges, accErr := i.accountsProcessor.ProcessOperation(ctx, opP.OpWrapper)
		if accErr != nil {
			return 0, fmt.Errorf("processing account changes: %w", accErr)
		}

		sacBalanceChanges, sacErr := i.sacBalancesProcessor.ProcessOperation(ctx, opP.OpWrapper)
		if sacErr != nil {
			return 0, fmt.Errorf("processing SAC balance changes: %w", sacErr)
		}

		sacContracts, sacInstanceErr := i.sacInstancesProcessor.ProcessOperation(ctx, opP.OpWrapper)
		if sacInstanceErr != nil {
			return 0, fmt.Errorf("processing SAC instances: %w", sacInstanceErr)
		}

		buffer.BatchPushChanges(trustlineChanges, accountChanges, sacBalanceChanges, sacContracts)
	}

	// Count all unique participants for metrics
	allParticipants := make(map[string]struct{}, txParticipants.Cardinality())
	for _, p := range txParticipantList {
		allParticipants[p] = struct{}{}
	}
	for _, participants := range opParticipantMap {
		for _, p := range participants {
			allParticipants[p] = struct{}{}
		}
	}
	for _, sc := range stateChanges {
		allParticipants[string(sc.AccountID)] = struct{}{}
	}

	return len(allParticipants), nil
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

// GetLedgerTransactions extracts transactions from ledger close meta.
func GetLedgerTransactions(ctx context.Context, networkPassphrase string, ledgerMeta xdr.LedgerCloseMeta) ([]ingest.LedgerTransaction, error) {
	ledgerTxReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(networkPassphrase, ledgerMeta)
	if err != nil {
		return nil, fmt.Errorf("creating ledger transaction reader: %w", err)
	}
	defer utils.DeferredClose(ctx, ledgerTxReader, "closing ledger transaction reader")

	transactions := make([]ingest.LedgerTransaction, 0)
	for {
		tx, err := ledgerTxReader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("reading ledger: %w", err)
		}
		transactions = append(transactions, tx)
	}

	return transactions, nil
}

// ProcessLedgerTransactionsSequential processes all transactions in a ledger sequentially.
// Use this in backfill mode where inter-ledger parallelism (multiple process workers)
// already saturates CPU — adding intra-ledger pond pool fan-out causes contention.
func (i *Indexer) ProcessLedgerTransactionsSequential(ctx context.Context, transactions []ingest.LedgerTransaction, ledgerBuffer IndexerBufferInterface) (int, error) {
	participantCounts := make([]int, len(transactions))

	for idx, tx := range transactions {
		count, err := i.processTransaction(ctx, tx, ledgerBuffer)
		if err != nil {
			return 0, fmt.Errorf("processing transaction at ledger=%d tx=%d: %w", tx.Ledger.LedgerSequence(), tx.Index, err)
		}
		participantCounts[idx] = count
	}

	totalParticipants := 0
	for _, count := range participantCounts {
		totalParticipants += count
	}

	return totalParticipants, nil
}

// ProcessLedger extracts transactions from a ledger and indexes them.
// Returns the participant count for optional metrics recording.
func ProcessLedger(ctx context.Context, networkPassphrase string, ledgerMeta xdr.LedgerCloseMeta, ledgerIndexer *Indexer, buffer *IndexerBuffer) (int, error) {
	ledgerSeq := ledgerMeta.LedgerSequence()
	transactions, err := GetLedgerTransactions(ctx, networkPassphrase, ledgerMeta)
	if err != nil {
		return 0, fmt.Errorf("getting transactions for ledger %d: %w", ledgerSeq, err)
	}

	participantCount, err := ledgerIndexer.ProcessLedgerTransactions(ctx, transactions, buffer)
	if err != nil {
		return 0, fmt.Errorf("processing transactions for ledger %d: %w", ledgerSeq, err)
	}

	return participantCount, nil
}

// ProcessLedgerSequential extracts transactions and processes them sequentially.
// Used by the backfill pipeline where multiple process workers provide parallelism.
func ProcessLedgerSequential(ctx context.Context, networkPassphrase string, ledgerMeta xdr.LedgerCloseMeta, ledgerIndexer *Indexer, buffer *IndexerBuffer) (int, error) {
	ledgerSeq := ledgerMeta.LedgerSequence()
	transactions, err := GetLedgerTransactions(ctx, networkPassphrase, ledgerMeta)
	if err != nil {
		return 0, fmt.Errorf("getting transactions for ledger %d: %w", ledgerSeq, err)
	}

	participantCount, err := ledgerIndexer.ProcessLedgerTransactionsSequential(ctx, transactions, buffer)
	if err != nil {
		return 0, fmt.Errorf("processing transactions for ledger %d: %w", ledgerSeq, err)
	}

	return participantCount, nil
}
