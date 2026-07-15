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
	"github.com/stellar/go-stellar-sdk/strkey"
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
	PushTransaction(participant string, transaction types.Transaction)
	PushOperation(participant string, operation types.Operation, transaction types.Transaction)
	PushStateChange(transaction types.Transaction, operation types.Operation, stateChange types.StateChange)
	GetTransactionsParticipants() map[int64]set.Set[string]
	GetOperationsParticipants() map[int64]set.Set[string]
	GetNumberOfTransactions() int
	GetNumberOfOperations() int
	GetTransactions() []*types.Transaction
	GetOperations() []*types.Operation
	GetStateChanges() []types.StateChange
	GetTrustlineChanges() map[TrustlineChangeKey]types.TrustlineChange
	GetAccountChanges() map[string]types.AccountChange
	GetSACBalanceChanges() map[SACBalanceChangeKey]types.SACBalanceChange
	GetLiquidityPoolShareChanges() map[LiquidityPoolShareChangeKey]types.LiquidityPoolShareChange
	GetLiquidityPoolChanges() map[string]types.LiquidityPoolChange
	PushTrustlineChange(trustlineChange types.TrustlineChange)
	PushAccountChange(accountChange types.AccountChange)
	PushSACBalanceChange(sacBalanceChange types.SACBalanceChange)
	PushLiquidityPoolShareChange(change types.LiquidityPoolShareChange)
	PushLiquidityPoolChange(change types.LiquidityPoolChange)
	PushSACContract(c *data.Contract)
	PushProtocolWasm(wasm data.ProtocolWasms)
	PushProtocolWasmBytecode(wasmHash string, bytecode []byte)
	PushProtocolContracts(contract data.ProtocolContracts)
	PushContractEvents(key ContractEventKey, events []xdr.ContractEvent)
	GetUniqueTrustlineAssets() []data.TrustlineAsset
	GetSACContracts() map[string]*data.Contract
	GetProtocolWasms() map[string]data.ProtocolWasms
	// GetProtocolWasmBytecodes returns a shallow clone of the wasmHash → bytecode
	// map. The []byte values alias buffer-owned storage and MUST be treated as
	// read-only; bytecode is content-addressed and immutable.
	GetProtocolWasmBytecodes() map[string][]byte
	GetProtocolContracts() map[string]data.ProtocolContracts
	GetContractEvents() map[ContractEventKey][]xdr.ContractEvent
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

// AccountsProcessorInterface extends the per-operation contract with a
// transaction-level pass that folds fee-phase balance changes (fee debits and
// Soroban refunds) into native balances — moves that never appear in operation meta.
type AccountsProcessorInterface interface {
	ProcessOperation(ctx context.Context, opWrapper *processors.TransactionOperationWrapper) ([]types.AccountChange, error)
	ProcessTransactionFees(ctx context.Context, tx ingest.LedgerTransaction) ([]types.AccountChange, error)
	Name() string
}

type Indexer struct {
	participantsProcessor      ParticipantsProcessorInterface
	tokenTransferProcessor     TokenTransferProcessorInterface
	trustlinesProcessor        LedgerChangeProcessor[types.TrustlineChange]
	accountsProcessor          AccountsProcessorInterface
	sacBalancesProcessor       LedgerChangeProcessor[types.SACBalanceChange]
	lpSharesProcessor          LedgerChangeProcessor[types.LiquidityPoolShareChange]
	lpProcessor                LedgerChangeProcessor[types.LiquidityPoolChange]
	sacInstancesProcessor      LedgerChangeProcessor[*data.Contract]
	protocolWasmsProcessor     LedgerChangeProcessor[processors.ProtocolWasmObservation]
	protocolContractsProcessor LedgerChangeProcessor[data.ProtocolContracts]
	processors                 []OperationProcessorInterface
	pool                       pond.Pool
	ingestionMetrics           *metrics.IngestionMetrics
	networkPassphrase          string
}

// NewIndexer constructs an Indexer. The indexer captures raw WASM bytecode
// during ledger meta processing; classification is performed downstream
// (per-batch) by services.DispatchClassification — this keeps the indexer
// agnostic of any specific protocol or its validator shape.
func NewIndexer(networkPassphrase string, pool pond.Pool, ingestionMetrics *metrics.IngestionMetrics) *Indexer {
	return &Indexer{
		participantsProcessor:      processors.NewParticipantsProcessor(networkPassphrase),
		tokenTransferProcessor:     processors.NewTokenTransferProcessor(networkPassphrase, ingestionMetrics),
		sacBalancesProcessor:       processors.NewSACBalancesProcessor(networkPassphrase, ingestionMetrics),
		sacInstancesProcessor:      processors.NewSACInstanceProcessor(networkPassphrase),
		protocolWasmsProcessor:     processors.NewProtocolWasmProcessor(ingestionMetrics),
		protocolContractsProcessor: processors.NewProtocolContractsProcessor(ingestionMetrics),
		accountsProcessor:          processors.NewAccountsProcessor(ingestionMetrics),
		trustlinesProcessor:        processors.NewTrustlinesProcessor(ingestionMetrics),
		lpSharesProcessor:          processors.NewLiquidityPoolSharesProcessor(ingestionMetrics),
		lpProcessor:                processors.NewLiquidityPoolsProcessor(ingestionMetrics),
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
	dataTx, err := processors.ConvertTransaction(&tx)
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
		allParticipants.Add(string(stateChange.AccountID))
	}

	// Insert transaction participants
	for participant := range txParticipants.Iter() {
		buffer.PushTransaction(participant, *dataTx)
	}

	// Get operation results for extracting result codes
	opResults, _ := tx.Result.OperationResults()

	// Insert operations participants
	operationsMap := make(map[int64]*types.Operation)
	for opID, opParticipants := range opsParticipants {
		dataOp, opErr := processors.ConvertOperation(&tx, &opParticipants.OpWrapper.Operation, opID, opParticipants.OpWrapper.Index, opResults)
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

		lpShareChanges, lpShareErr := i.lpSharesProcessor.ProcessOperation(ctx, opParticipants.OpWrapper)
		if lpShareErr != nil {
			return 0, fmt.Errorf("processing liquidity pool share changes: %w", lpShareErr)
		}
		for _, lpShareChange := range lpShareChanges {
			buffer.PushLiquidityPoolShareChange(lpShareChange)
		}

		lpChanges, lpErr := i.lpProcessor.ProcessOperation(ctx, opParticipants.OpWrapper)
		if lpErr != nil {
			return 0, fmt.Errorf("processing liquidity pool changes: %w", lpErr)
		}
		for _, lpChange := range lpChanges {
			buffer.PushLiquidityPoolChange(lpChange)
		}

		sacContracts, sacInstanceErr := i.sacInstancesProcessor.ProcessOperation(ctx, opParticipants.OpWrapper)
		if sacInstanceErr != nil {
			return 0, fmt.Errorf("processing SAC instances: %w", sacInstanceErr)
		}
		for _, c := range sacContracts {
			buffer.PushSACContract(c)
		}

		protocolWasms, pwErr := i.protocolWasmsProcessor.ProcessOperation(ctx, opParticipants.OpWrapper)
		if pwErr != nil {
			return 0, fmt.Errorf("processing protocol wasms: %w", pwErr)
		}
		for _, wasm := range protocolWasms {
			buffer.PushProtocolWasm(wasm.Record)
			if len(wasm.Bytecode) > 0 {
				buffer.PushProtocolWasmBytecode(string(wasm.Record.WasmHash), wasm.Bytecode)
			}
		}

		protocolContracts, pcErr := i.protocolContractsProcessor.ProcessOperation(ctx, opParticipants.OpWrapper)
		if pcErr != nil {
			return 0, fmt.Errorf("processing protocol contracts: %w", pcErr)
		}
		for _, contract := range protocolContracts {
			buffer.PushProtocolContracts(contract)
		}
	}

	// Fold transaction fee-phase changes (fee debits + Soroban fee refunds) into
	// native balances. These are charged/refunded outside any operation's meta, so
	// the per-operation loop above never sees an account whose balance moves only in
	// those phases — e.g. a fee-bump fee source (issue #637).
	feeAccountChanges, feeErr := i.accountsProcessor.ProcessTransactionFees(ctx, tx)
	if feeErr != nil {
		return 0, fmt.Errorf("processing transaction fee account changes: %w", feeErr)
	}
	for _, accChange := range feeAccountChanges {
		buffer.PushAccountChange(accChange)
	}

	// Stash contract events into the buffer so protocol processors can consume
	// them without re-decoding LedgerCloseMeta. Only successful transactions are
	// indexed — protocol processors (e.g. SEP-41) only care about successful
	// invocations, and emitting events from failed txs would force every consumer
	// to re-filter. SACEventsProcessor still calls GetContractEventsForOperation
	// itself (see TODO at processors/contracts/sac.go); that consolidation is a
	// separate cleanup.
	if tx.Result.Successful() {
		for _, opParticipants := range opsParticipants {
			opWrapper := opParticipants.OpWrapper
			if opWrapper.Operation.Body.Type != xdr.OperationTypeInvokeHostFunction {
				continue
			}
			events, evErr := tx.GetContractEventsForOperation(opWrapper.Index)
			if evErr != nil {
				return 0, fmt.Errorf("extracting contract events for op %d: %w", opWrapper.Index, evErr)
			}
			if len(events) == 0 {
				continue
			}
			buffer.PushContractEvents(ContractEventKey{TxIdx: tx.Index, OpIdx: opWrapper.Index}, events)
		}
	}

	// Drop state changes with no account to associate with, then assign each
	// retained one a deterministic state_change_id: an ordinal numbered 1..N in
	// emission order within its (to_id, operation_id) group. Filtering first
	// keeps the ordinals gap-free for what's actually persisted.
	retainedStateChanges := stateChanges[:0]
	for _, stateChange := range stateChanges {
		if stateChange.AccountID == "" {
			continue
		}
		retainedStateChanges = append(retainedStateChanges, stateChange)
	}
	types.AssignStateChangeOrdinals(retainedStateChanges, types.StateChangeOrdinalBaseIndexer)

	// Insert state changes
	for _, stateChange := range retainedStateChanges {
		// Get the correct operation for this state change
		var operation types.Operation
		if stateChange.OperationID != 0 {
			correctOp := operationsMap[stateChange.OperationID]
			if correctOp == nil {
				log.Ctx(ctx).Errorf("operation ID %d not found in operations map for state change (to_id=%d, category=%s)", stateChange.OperationID, stateChange.ToID, stateChange.StateChangeCategory)
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

// ExtractContractEventsForLedger walks a ledger's transactions directly from the
// decoded LedgerCloseMeta and returns the (txIdx, opIdx) → []ContractEvent map
// that the full indexer pipeline would have pushed into the buffer. For each
// transaction index i it reads the result pair, filters operations by their
// result Tr type, and reads events from TxApplyProcessing(i) — without building
// a LedgerTransactionReader, which would re-hash every transaction envelope just
// to pair envelopes with metas we never read here. It is therefore a pure
// function of the decoded ledger and needs neither a context nor the network
// passphrase.
//
// The output is identical to the reader-based path; that equivalence is the
// merge gate (see extractContractEventsViaReader and
// TestExtractContractEventsForLedger_EquivalenceOnRealLedgers).
//
// Only events from successful transactions are returned, matching the live
// indexer's filter in processTransaction.
func ExtractContractEventsForLedger(ledgerMeta xdr.LedgerCloseMeta) (map[ContractEventKey][]xdr.ContractEvent, error) {
	out := make(map[ContractEventKey][]xdr.ContractEvent)
	for i := 0; i < ledgerMeta.CountTransactions(); i++ {
		result := ledgerMeta.TransactionResultPair(i).Result
		if !result.Successful() {
			continue
		}
		opResults, ok := result.OperationResults()
		if !ok {
			continue
		}
		meta := ledgerMeta.TxApplyProcessing(i)
		for opIdx, opr := range opResults {
			tr, trOK := opr.GetTr()
			if !trOK || tr.Type != xdr.OperationTypeInvokeHostFunction {
				continue
			}
			events, evErr := meta.GetContractEventsForOperation(uint32(opIdx))
			if evErr != nil {
				return nil, fmt.Errorf("extracting contract events for ledger %d tx %d op %d: %w",
					ledgerMeta.LedgerSequence(), i+1, opIdx, evErr)
			}
			if len(events) == 0 {
				continue
			}
			out[ContractEventKey{TxIdx: uint32(i + 1), OpIdx: uint32(opIdx)}] = events
		}
	}
	return out, nil
}

// ExtractContractDataChangesForLedger returns every ContractData ledger-entry
// change from a ledger's successful transactions, grouped by the owning
// contract's C-address strkey — but only after a cheap footprint gate: if no
// transaction's read-write footprint contains a ContractData key owned by a
// tracked contract, the ledger is skipped outright (empty map). Soroban
// guarantees writes ⊆ the declared read-write footprint (host storage is
// footprint-seeded and a write outside it traps; RestoreFootprint restores
// exactly the read-write keys; protocol-23 auto-restores are indices into it),
// so the gate can never skip a ledger that actually changes a tracked
// contract's entries.
//
// Both the gate and the extraction walk the already-decoded LedgerCloseMeta
// directly — GetChanges reads only the transaction meta, result, and ledger
// version, so no LedgerTransactionReader (which re-hashes every envelope just
// to pair envelopes with metas) is ever built. When the gate passes, the FULL
// ledger's ContractData changes are returned, not just the tracked subset:
// the map is shared across protocols and each processor filters by its own
// membership.
func ExtractContractDataChangesForLedger(ledgerMeta xdr.LedgerCloseMeta, trackedContracts map[xdr.ContractId]struct{}) (map[string][]ingest.Change, error) {
	if !ledgerTouchesTrackedContractData(ledgerMeta, trackedContracts) {
		return map[string][]ingest.Change{}, nil
	}

	ledgerSeq := ledgerMeta.LedgerSequence()
	out := make(map[string][]ingest.Change)
	for i := 0; i < ledgerMeta.CountTransactions(); i++ {
		resultPair := ledgerMeta.TransactionResultPair(i)
		if !resultPair.Result.Successful() {
			continue
		}
		// A minimal LedgerTransaction: GetChanges touches only these fields
		// (verified by the fixture equivalence test against the reader-based
		// reference) — the envelope is deliberately absent. Result pairs and
		// TxApplyProcessing share application order, the same index alignment
		// ExtractContractEventsForLedger relies on.
		tx := ingest.LedgerTransaction{
			Index:         uint32(i + 1),
			Result:        resultPair,
			UnsafeMeta:    ledgerMeta.TxApplyProcessing(i),
			LedgerVersion: ledgerMeta.ProtocolVersion(),
			Ledger:        ledgerMeta,
		}
		if err := collectContractDataChanges(&tx, ledgerSeq, out); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// ledgerTouchesTrackedContractData reports whether any transaction in the
// ledger declares a read-write footprint ContractData key owned by a tracked
// contract. It walks envelopes in whatever order the ledger stores them —
// fine for a ledger-level boolean, which is exactly why the gate is
// per-ledger rather than per-transaction: envelopes are in tx-set order while
// metas are in application order, and matching the two is the expensive
// pairing this function exists to avoid.
func ledgerTouchesTrackedContractData(ledgerMeta xdr.LedgerCloseMeta, trackedContracts map[xdr.ContractId]struct{}) bool {
	if len(trackedContracts) == 0 {
		return false
	}
	for _, env := range ledgerMeta.TransactionEnvelopes() {
		var ext xdr.TransactionExt
		switch env.Type {
		case xdr.EnvelopeTypeEnvelopeTypeTx:
			ext = env.V1.Tx.Ext
		case xdr.EnvelopeTypeEnvelopeTypeTxFeeBump:
			ext = env.FeeBump.Tx.InnerTx.V1.Tx.Ext
		default:
			// V0 envelopes predate Soroban: no footprint, no ContractData.
			continue
		}
		sorobanData, ok := ext.GetSorobanData()
		if !ok {
			continue
		}
		for _, key := range sorobanData.Resources.Footprint.ReadWrite {
			if key.Type != xdr.LedgerEntryTypeContractData {
				continue
			}
			contractID, ok := key.ContractData.Contract.GetContractId()
			if !ok {
				continue
			}
			if _, tracked := trackedContracts[contractID]; tracked {
				return true
			}
		}
	}
	return false
}

// ExtractContractDataChangesFromTransactions extracts the same
// contract-grouped ContractData changes as ExtractContractDataChangesForLedger
// over already-materialized transactions, ungated — live ingestion's main
// pipeline has the transactions in hand and processes one ledger per close,
// so a footprint gate buys nothing there. ledgerSeq is used only for error
// context.
func ExtractContractDataChangesFromTransactions(transactions []ingest.LedgerTransaction, ledgerSeq uint32) (map[string][]ingest.Change, error) {
	out := make(map[string][]ingest.Change)
	for i := range transactions {
		tx := transactions[i]
		if !tx.Result.Successful() {
			continue
		}
		if err := collectContractDataChanges(&tx, ledgerSeq, out); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// collectContractDataChanges appends tx's ContractData changes into out,
// grouped by the owning contract's C-address strkey.
//
// Within a contract, changes preserve transaction application order, so
// last-write-wins folding per entry key is deterministic. Ledger-level
// archival evictions are NOT surfaced (GetChanges only walks fee/tx/op meta);
// per-tx entry removals appear with Post == nil.
func collectContractDataChanges(tx *ingest.LedgerTransaction, ledgerSeq uint32, out map[string][]ingest.Change) error {
	changes, chErr := tx.GetChanges()
	if chErr != nil {
		return fmt.Errorf("getting changes for ledger %d tx %d: %w", ledgerSeq, tx.Index, chErr)
	}
	for _, change := range changes {
		if change.Type != xdr.LedgerEntryTypeContractData {
			continue
		}
		entry := change.Post
		if entry == nil {
			entry = change.Pre
		}
		if entry == nil {
			continue
		}
		contractData, ok := entry.Data.GetContractData()
		if !ok {
			continue
		}
		contractIDBytes, ok := contractData.Contract.GetContractId()
		if !ok {
			continue
		}
		addr, encErr := strkey.Encode(strkey.VersionByteContract, contractIDBytes[:])
		if encErr != nil {
			// Callers rely on this function returning every ContractData
			// change; silently dropping one would corrupt downstream state.
			return fmt.Errorf("encoding contract id for ledger %d tx %d: %w", ledgerSeq, tx.Index, encErr)
		}
		out[addr] = append(out[addr], change)
	}
	return nil
}

// ProcessLedger extracts transactions from a ledger and indexes them.
// Returns the participant count for optional metrics recording, plus the
// materialized transactions so callers with further per-transaction work
// (live ingestion's ContractData extraction) can reuse them instead of
// paying for a second LedgerTransactionReader build — its constructor
// re-hashes every transaction envelope.
func ProcessLedger(ctx context.Context, networkPassphrase string, ledgerMeta xdr.LedgerCloseMeta, ledgerIndexer *Indexer, buffer *IndexerBuffer) (int, []ingest.LedgerTransaction, error) {
	ledgerSeq := ledgerMeta.LedgerSequence()
	transactions, err := GetLedgerTransactions(ctx, networkPassphrase, ledgerMeta)
	if err != nil {
		return 0, nil, fmt.Errorf("getting transactions for ledger %d: %w", ledgerSeq, err)
	}

	participantCount, err := ledgerIndexer.ProcessLedgerTransactions(ctx, transactions, buffer)
	if err != nil {
		return 0, nil, fmt.Errorf("processing transactions for ledger %d: %w", ledgerSeq, err)
	}

	return participantCount, transactions, nil
}
