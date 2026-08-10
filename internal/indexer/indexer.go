package indexer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
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

// IndexerBufferInterface is the buffer seam the indexer writes through and the
// ingestion service's row inserts read through. Everything else the buffer
// exposes is reached on the concrete *IndexerBuffer.
type IndexerBufferInterface interface {
	// IngestTransactionResult folds a worker's per-transaction result into the buffer.
	IngestTransactionResult(result *TransactionResult)
	GetTransactions() []*types.Transaction
	GetTransactionsParticipants() map[int64]map[string]struct{}
	GetOperations() []*types.Operation
	GetOperationsParticipants() map[int64]map[string]struct{}
	GetStateChanges() []types.StateChange
}

type TokenTransferProcessorInterface interface {
	ProcessTransaction(ctx context.Context, tx ingest.LedgerTransaction) ([]types.StateChange, error)
}

type ParticipantsProcessorInterface interface {
	GetTransactionParticipants(transaction ingest.LedgerTransaction) (set.Set[string], error)
	GetOperationsParticipants(transaction ingest.LedgerTransaction) (map[int64]processors.OperationParticipants, error)
}

type OperationProcessorInterface interface {
	// ProcessOperation returns this operation's state changes. The slice must be
	// in canonical, reproducible order — derived from the transaction meta / XDR
	// walk order, never Go map iteration, goroutine completion, or channel
	// arrival order — because types.AssignStateChangeOrdinals freezes each
	// element's state_change_id from its position within the (to_id, operation_id)
	// group. Cross-operation ordering is free: ordinals are keyed per group, so
	// only order WITHIN one call's returned slice is load-bearing.
	ProcessOperation(ctx context.Context, opWrapper *processors.TransactionOperationWrapper) ([]types.StateChange, error)
	Name() string
	// StateChangeSubBase is this processor's slot in the indexer's state_change_id
	// sub-namespace registry (see types.StateChangeSubBase*). It is part of the
	// on-disk ID layout and must never change once rows exist with it.
	StateChangeSubBase() int64
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
}

// NewIndexer constructs an Indexer. The indexer captures raw WASM bytecode
// during ledger meta processing; classification is performed downstream
// (per-batch) by services.DispatchClassification — this keeps the indexer
// agnostic of any specific protocol or its validator shape.
//
// It validates the state_change_id sub-base registry of the built processor
// set (see validateStateChangeSubBases), so a stale or duplicated sub-base
// copied into a new processor fails fast at startup rather than surfacing as
// a state_changes primary-key violation on the first ledger where two
// processors emit for the same operation.
func NewIndexer(networkPassphrase string, pool pond.Pool, ingestionMetrics *metrics.IngestionMetrics) (*Indexer, error) {
	indexer := &Indexer{
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
		pool: pool,
	}
	if err := validateStateChangeSubBases(indexer.processors); err != nil {
		return nil, fmt.Errorf("validating state_change_id sub-bases: %w", err)
	}
	return indexer, nil
}

// validateStateChangeSubBases validates the state_change_id sub-base registry
// of the given processor set (see types.StateChangeSubBase*): every sub-base
// must be a non-negative multiple of types.StateChangeSubNamespaceWidth that
// fits inside the indexer's emitter namespace, and no two streams may share
// one. The token-transfer stream's slot is reserved up front since it is
// emitted outside the processors slice (see getTransactionStateChanges).
func validateStateChangeSubBases(procs []OperationProcessorInterface) error {
	streamBySubBase := map[int64]string{types.StateChangeSubBaseTokenTransfer: "token_transfer"}
	for _, p := range procs {
		subBase := p.StateChangeSubBase()
		if subBase <= 0 || subBase%types.StateChangeSubNamespaceWidth != 0 || subBase >= types.StateChangeOrdinalNamespaceWidth {
			return fmt.Errorf("processor %q has invalid state_change_id sub-base %d: "+
				"must be a positive multiple of %d below %d",
				p.Name(), subBase, types.StateChangeSubNamespaceWidth, types.StateChangeOrdinalNamespaceWidth)
		}
		if other, dup := streamBySubBase[subBase]; dup {
			return fmt.Errorf("indexer streams %q and %q share state_change_id sub-base %d",
				other, p.Name(), subBase)
		}
		streamBySubBase[subBase] = p.Name()
	}
	return nil
}

// ProcessLedgerTransactions processes all transactions in a ledger in parallel.
// Each worker builds an independent TransactionResult (no shared buffer, no locks); the results
// are then folded into the single ledger buffer serially. This avoids allocating a full
// IndexerBuffer per transaction and the subsequent buffer-to-buffer merge.
// Returns the total participant count for metrics.
func (i *Indexer) ProcessLedgerTransactions(ctx context.Context, transactions []ingest.LedgerTransaction, ledgerBuffer IndexerBufferInterface) (int, error) {
	group := i.pool.NewGroupContext(ctx)

	results := make([]*TransactionResult, len(transactions))
	var errs []error
	errMu := sync.Mutex{}

	for idx, tx := range transactions {
		index := idx
		tx := tx
		group.Submit(func() {
			result, err := i.processTransaction(ctx, tx)
			if err != nil {
				errMu.Lock()
				errs = append(errs, fmt.Errorf("processing transaction at ledger=%d tx=%d: %w", tx.Ledger.LedgerSequence(), tx.Index, err))
				errMu.Unlock()
				return
			}
			results[index] = result
		})
	}

	if err := group.Wait(); err != nil {
		return 0, fmt.Errorf("waiting for transaction processing: %w", err)
	}
	if len(errs) > 0 {
		return 0, fmt.Errorf("processing transactions: %w", errors.Join(errs...))
	}

	// Fold per-transaction results into the ledger buffer serially (single-owner, no locks).
	totalParticipants := 0
	for _, result := range results {
		if result == nil {
			continue
		}
		ledgerBuffer.IngestTransactionResult(result)
		totalParticipants += result.ParticipantCount
	}

	return totalParticipants, nil
}

// processTransaction processes a single transaction and returns its result bundle for the caller
// to fold into the ledger buffer. It performs no buffer writes itself, so workers can run fully in
// parallel with no shared state.
func (i *Indexer) processTransaction(ctx context.Context, tx ingest.LedgerTransaction) (*TransactionResult, error) {
	// Get transaction participants
	txParticipants, err := i.participantsProcessor.GetTransactionParticipants(tx)
	if err != nil {
		return nil, fmt.Errorf("getting transaction participants: %w", err)
	}

	// Get operations participants
	opsParticipants, err := i.participantsProcessor.GetOperationsParticipants(tx)
	if err != nil {
		return nil, fmt.Errorf("getting operations participants: %w", err)
	}

	// Get state changes
	stateChanges, err := i.getTransactionStateChanges(ctx, tx, opsParticipants)
	if err != nil {
		return nil, fmt.Errorf("getting transaction state changes: %w", err)
	}

	// Convert transaction data
	dataTx, err := processors.ConvertTransaction(&tx)
	if err != nil {
		return nil, fmt.Errorf("creating data transaction: %w", err)
	}

	// Counts unique participants across tx, ops, and state changes for metrics.
	txParticipantsSlice := txParticipants.ToSlice()
	allParticipants := make(map[string]struct{}, len(txParticipantsSlice))
	for _, participant := range txParticipantsSlice {
		allParticipants[participant] = struct{}{}
	}
	for _, opParticipants := range opsParticipants {
		opParticipants.Participants.Each(func(participant string) bool {
			allParticipants[participant] = struct{}{}
			return false
		})
	}
	for _, stateChange := range stateChanges {
		allParticipants[string(stateChange.AccountID)] = struct{}{}
	}

	result := &TransactionResult{
		Transaction:      dataTx,
		TxParticipants:   txParticipantsSlice,
		Operations:       make(map[int64]*types.Operation, len(opsParticipants)),
		OpParticipants:   make(map[int64][]string, len(opsParticipants)),
		ParticipantCount: len(allParticipants),
	}

	// Get operation results for extracting result codes
	opResults, _ := tx.Result.OperationResults()

	// Build operations and their participants
	for opID, opParticipants := range opsParticipants {
		dataOp, opErr := processors.ConvertOperation(&tx, &opParticipants.OpWrapper.Operation, opID, opParticipants.OpWrapper.Index, opResults)
		if opErr != nil {
			return nil, fmt.Errorf("creating data operation: %w", opErr)
		}
		result.Operations[opID] = dataOp
		result.OpParticipants[opID] = opParticipants.Participants.ToSlice()
	}

	// Process trustline, account, SAC balance, and liquidity-pool changes from ledger changes,
	// walking operations in ascending opID (chronological) order so each change family is emitted
	// deterministically in chronological order. The fold (pushHighestOrder) keeps the highest-order
	// change per key and no longer depends on arrival order for correctness, but ranging over the
	// opsParticipants map directly would still produce nondeterministic slice ordering (#653).
	sortedOpIDs := make([]int64, 0, len(opsParticipants))
	for opID := range opsParticipants {
		sortedOpIDs = append(sortedOpIDs, opID)
	}
	slices.Sort(sortedOpIDs)
	for _, opID := range sortedOpIDs {
		opParticipants := opsParticipants[opID]
		trustlineChanges, tlErr := i.trustlinesProcessor.ProcessOperation(ctx, opParticipants.OpWrapper)
		if tlErr != nil {
			return nil, fmt.Errorf("processing trustline changes: %w", tlErr)
		}
		result.TrustlineChanges = append(result.TrustlineChanges, trustlineChanges...)

		accountChanges, accErr := i.accountsProcessor.ProcessOperation(ctx, opParticipants.OpWrapper)
		if accErr != nil {
			return nil, fmt.Errorf("processing account changes: %w", accErr)
		}
		result.AccountChanges = append(result.AccountChanges, accountChanges...)

		sacBalanceChanges, sacErr := i.sacBalancesProcessor.ProcessOperation(ctx, opParticipants.OpWrapper)
		if sacErr != nil {
			return nil, fmt.Errorf("processing SAC balance changes: %w", sacErr)
		}
		result.SACBalanceChanges = append(result.SACBalanceChanges, sacBalanceChanges...)

		lpShareChanges, lpShareErr := i.lpSharesProcessor.ProcessOperation(ctx, opParticipants.OpWrapper)
		if lpShareErr != nil {
			return nil, fmt.Errorf("processing liquidity pool share changes: %w", lpShareErr)
		}
		result.LPShareChanges = append(result.LPShareChanges, lpShareChanges...)

		lpChanges, lpErr := i.lpProcessor.ProcessOperation(ctx, opParticipants.OpWrapper)
		if lpErr != nil {
			return nil, fmt.Errorf("processing liquidity pool changes: %w", lpErr)
		}
		result.LPChanges = append(result.LPChanges, lpChanges...)

		sacContracts, sacInstanceErr := i.sacInstancesProcessor.ProcessOperation(ctx, opParticipants.OpWrapper)
		if sacInstanceErr != nil {
			return nil, fmt.Errorf("processing SAC instances: %w", sacInstanceErr)
		}
		result.SACContracts = append(result.SACContracts, sacContracts...)

		protocolWasms, pwErr := i.protocolWasmsProcessor.ProcessOperation(ctx, opParticipants.OpWrapper)
		if pwErr != nil {
			return nil, fmt.Errorf("processing protocol wasms: %w", pwErr)
		}
		for _, wasm := range protocolWasms {
			result.ProtocolWasms = append(result.ProtocolWasms, wasm.Record)
			if len(wasm.Bytecode) > 0 {
				if result.ProtocolWasmBytecodes == nil {
					result.ProtocolWasmBytecodes = make(map[string][]byte)
				}
				result.ProtocolWasmBytecodes[string(wasm.Record.WasmHash)] = wasm.Bytecode
			}
		}

		protocolContracts, pcErr := i.protocolContractsProcessor.ProcessOperation(ctx, opParticipants.OpWrapper)
		if pcErr != nil {
			return nil, fmt.Errorf("processing protocol contracts: %w", pcErr)
		}
		result.ProtocolContracts = append(result.ProtocolContracts, protocolContracts...)
	}

	// Fold transaction fee-phase changes (fee debits + Soroban fee refunds) into
	// native balances. These are charged/refunded outside any operation's meta, so
	// the per-operation loop above never sees an account whose balance moves only in
	// those phases — e.g. a fee-bump fee source (issue #637).
	feeAccountChanges, feeErr := i.accountsProcessor.ProcessTransactionFees(ctx, tx)
	if feeErr != nil {
		return nil, fmt.Errorf("processing transaction fee account changes: %w", feeErr)
	}
	result.AccountChanges = append(result.AccountChanges, feeAccountChanges...)

	// Stash contract events so protocol processors can consume them without re-decoding
	// LedgerCloseMeta. Only successful transactions are indexed — protocol processors
	// (e.g. SEP-41) only care about successful invocations, and emitting events from failed
	// txs would force every consumer to re-filter. SACEventsProcessor still calls
	// GetContractEventsForOperation itself (see TODO at processors/contracts/sac.go); that
	// consolidation is a separate cleanup.
	if tx.Result.Successful() {
		for _, opParticipants := range opsParticipants {
			opWrapper := opParticipants.OpWrapper
			if opWrapper.Operation.Body.Type != xdr.OperationTypeInvokeHostFunction {
				continue
			}
			events, evErr := tx.GetContractEventsForOperation(opWrapper.Index)
			if evErr != nil {
				return nil, fmt.Errorf("extracting contract events for op %d: %w", opWrapper.Index, evErr)
			}
			if len(events) == 0 {
				continue
			}
			if result.ContractEvents == nil {
				result.ContractEvents = make(map[ContractEventKey][]xdr.ContractEvent)
			}
			result.ContractEvents[ContractEventKey{TxIdx: tx.Index, OpIdx: opWrapper.Index}] = events
		}
	}

	// Collect state changes, dropping those whose operation is missing. Empty-AccountID entries
	// are already filtered (and IDs assigned per processor stream) by getTransactionStateChanges.
	// The fold resolves each change's operation from result.Operations by OperationID.
	result.StateChanges = make([]types.StateChange, 0, len(stateChanges))
	for _, stateChange := range stateChanges {
		// Fee state changes (OperationID == 0) have no associated operation; the rest must
		// resolve to a known operation.
		if stateChange.OperationID != 0 && result.Operations[stateChange.OperationID] == nil {
			log.Ctx(ctx).Errorf("operation ID %d not found in operations map for state change (to_id=%d, category=%s)", stateChange.OperationID, stateChange.ToID, stateChange.StateChangeCategory)
			continue
		}
		result.StateChanges = append(result.StateChanges, stateChange)
	}

	return result, nil
}

// getTransactionStateChanges processes operations of a transaction and calculates all state
// changes. Each emitting processor's stream is filtered and given its deterministic
// state_change_ids independently, inside that processor's own sub-namespace (see the sub-base
// registry in types), so the returned changes are ready to persist and no processor's IDs
// depend on another processor's output or on registration order.
func (i *Indexer) getTransactionStateChanges(ctx context.Context, transaction ingest.LedgerTransaction, opsParticipants map[int64]processors.OperationParticipants) ([]types.StateChange, error) {
	streams := make([][]types.StateChange, len(i.processors))

	// Process operations sequentially since there are only 3 processors per operation
	// Creating a worker pool here adds unnecessary overhead
	for _, opParticipants := range opsParticipants {
		for procIdx, processor := range i.processors {
			processorStateChanges, processorErr := processor.ProcessOperation(ctx, opParticipants.OpWrapper)
			if processorErr != nil && !errors.Is(processorErr, processors.ErrInvalidOpType) {
				return nil, fmt.Errorf("processing %s state changes: %w", processor.Name(), processorErr)
			}
			streams[procIdx] = append(streams[procIdx], processorStateChanges...)
		}
	}

	// Get token transfer state changes
	tokenTransferStateChanges, err := i.tokenTransferProcessor.ProcessTransaction(ctx, transaction)
	if err != nil {
		return nil, fmt.Errorf("processing token transfer state changes: %w", err)
	}

	totalStateChanges := len(tokenTransferStateChanges)
	for _, stream := range streams {
		totalStateChanges += len(stream)
	}
	stateChanges := make([]types.StateChange, 0, totalStateChanges)
	for procIdx, processor := range i.processors {
		stateChanges = assignStateChangeStream(stateChanges, streams[procIdx], processor.StateChangeSubBase())
	}
	stateChanges = assignStateChangeStream(stateChanges, tokenTransferStateChanges, types.StateChangeSubBaseTokenTransfer)
	return stateChanges, nil
}

// assignStateChangeStream drops stream entries with no account to associate with, assigns
// the retained ones their deterministic state_change_ids at the emitting processor's slot in
// the indexer namespace, and appends them to dst. Filtering before assignment keeps ordinals
// gap-free (1..N per (to_id, operation_id) group) for what's actually persisted.
func assignStateChangeStream(dst, stream []types.StateChange, subBase int64) []types.StateChange {
	retained := stream[:0]
	for _, stateChange := range stream {
		if stateChange.AccountID == "" {
			continue
		}
		retained = append(retained, stateChange)
	}
	types.AssignStateChangeOrdinals(retained, types.StateChangeOrdinalBaseIndexer+subBase)
	return append(dst, retained...)
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
		// A minimal LedgerTransaction: GetChanges touches only these fields —
		// the envelope is deliberately absent, while Hash mirrors the reader's
		// value because GetChanges attaches this transaction to every Change
		// it returns. The fixture equivalence test compares the change fields
		// (type, reason, operation index, pre/post entries) against the
		// reader-based reference; the attached transaction is not compared.
		// Result pairs and TxApplyProcessing share application order, the same
		// index alignment ExtractContractEventsForLedger relies on.
		tx := ingest.LedgerTransaction{
			Index:         uint32(i + 1),
			Result:        resultPair,
			UnsafeMeta:    ledgerMeta.TxApplyProcessing(i),
			LedgerVersion: ledgerMeta.ProtocolVersion(),
			Ledger:        ledgerMeta,
			Hash:          resultPair.TransactionHash,
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
