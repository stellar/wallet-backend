package indexer

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"strings"
	"sync"

	"github.com/alitto/pond/v2"
	set "github.com/deckarep/golang-set/v2"
	"github.com/stellar/go-stellar-sdk/hash"
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer/processors"
	contract_processors "github.com/stellar/wallet-backend/internal/indexer/processors/contracts"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
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

	// Counts unique participants across tx, ops, and state changes for metrics. The tx-level
	// slice is snapshotted first, so folding the op and state-change accounts into the set
	// leaves the reported tx participants untouched.
	txParticipantsSlice := txParticipants.ToSlice()
	for _, opParticipants := range opsParticipants {
		opParticipants.Participants.Each(func(participant string) bool {
			txParticipants.Add(participant)
			return false
		})
	}
	for _, stateChange := range stateChanges {
		txParticipants.Add(string(stateChange.AccountID))
	}

	result := &TransactionResult{
		Transaction:      dataTx,
		TxParticipants:   txParticipantsSlice,
		Operations:       make(map[int64]*types.Operation, len(opsParticipants)),
		OpParticipants:   make(map[int64][]string, len(opsParticipants)),
		ParticipantCount: txParticipants.Cardinality(),
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
	// walking operations in ascending opID (chronological) order. pushWithTombstone's
	// create+remove netting at the fold requires each change family to arrive in ascending
	// order value per key — CREATE before REMOVE — and ranging over the opsParticipants map
	// would emit them in random order (#653).
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

	// Collect ContractData changes off the wrappers' memoized change slices, so
	// the persist stage reads them from the ledger buffer instead of rebuilding
	// (and re-sorting) every operation's changes via tx.GetChanges on the serial
	// persist goroutine.
	contractDataChanges, cdErr := transactionContractDataChanges(&tx, opsParticipants)
	if cdErr != nil {
		return nil, fmt.Errorf("collecting contract data changes: %w", cdErr)
	}
	result.ContractDataChanges = contractDataChanges

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

// errBadMetaVersion rejects a ledger whose metas predate TransactionMeta v2 on
// a protocol old enough that stellar-core wrote them ambiguously.
var errBadMetaVersion = errors.New("TransactionMeta.V=2 is required in protocol version older than version 10; " +
	"please process ledgers again using the latest stellar-core version")

// GetLedgerTransactions extracts transactions from ledger close meta, pairing
// each meta with the envelope that produced it.
//
// Pairing needs every envelope's hash. The transaction set carries envelopes in
// the order validators agreed on while the metas are sorted by hash, so the
// hash is the only thing that identifies which envelope belongs to which meta.
// Hashing is essentially this function's whole cost — it marshals every
// envelope's signature payload — and each envelope is independent of the rest,
// so it runs across pool's workers rather than on the caller's goroutine, where
// at loadtest ledger sizes it was the pipeline's largest serial stretch.
func GetLedgerTransactions(ctx context.Context, networkPassphrase string, ledgerMeta xdr.LedgerCloseMeta, pool pond.Pool) ([]ingest.LedgerTransaction, error) {
	envelopes := ledgerMeta.TransactionEnvelopes()

	// Protocol versions below 10 predate TransactionMeta v2, where a v0/v1 meta
	// carrying fee processing is ambiguous. The version is a property of the
	// ledger, so the guard is evaluated once here instead of per transaction.
	if ledgerMeta.ProtocolVersion() < 10 {
		for i := range envelopes {
			if ledgerMeta.TxApplyProcessing(i).V < 2 && len(ledgerMeta.FeeProcessing(i)) > 0 {
				return nil, errBadMetaVersion
			}
		}
	}

	hashes, err := hashEnvelopes(ctx, networkPassphrase, envelopes, pool)
	if err != nil {
		return nil, fmt.Errorf("hashing transaction set of ledger %d: %w", ledgerMeta.LedgerSequence(), err)
	}

	// Envelope order decides collisions and the last envelope wins: a
	// transaction hash covers the signature payload but not the signatures, so
	// one ledger can hold the same transaction body signed two different ways
	// as two distinct envelopes sharing a hash. Merged loadtest ledgers do
	// repeat transactions, so this is load-bearing rather than theoretical.
	envelopeByHash := make(map[xdr.Hash]int, len(envelopes))
	for i, hash := range hashes {
		envelopeByHash[hash] = i
	}

	// Hoisted out of the loop: both return their struct by value, so reading
	// them per transaction copied the whole ledger header and V2 body each time.
	ledgerVersion := uint32(ledgerMeta.LedgerHeaderHistoryEntry().Header.LedgerVersion)
	metaV2, hasMetaV2 := ledgerMeta.GetV2()

	transactions := make([]ingest.LedgerTransaction, ledgerMeta.CountTransactions())
	for i := range transactions {
		txHash := ledgerMeta.TransactionHash(i)
		envelopeIdx, ok := envelopeByHash[txHash]
		if !ok {
			return nil, fmt.Errorf("unknown tx hash in LedgerCloseMeta: %s", hex.EncodeToString(txHash[:]))
		}

		var postTxApplyFeeChanges xdr.LedgerEntryChanges
		if hasMetaV2 {
			postTxApplyFeeChanges = metaV2.TxProcessing[i].PostTxApplyFeeProcessing
		}

		transactions[i] = ingest.LedgerTransaction{
			Index:                 uint32(i + 1), // Transactions start at '1'.
			Envelope:              envelopes[envelopeIdx],
			Result:                ledgerMeta.TransactionResultPair(i),
			UnsafeMeta:            ledgerMeta.TxApplyProcessing(i),
			FeeChanges:            ledgerMeta.FeeProcessing(i),
			PostTxApplyFeeChanges: postTxApplyFeeChanges,
			LedgerVersion:         ledgerVersion,
			Ledger:                ledgerMeta,
			Hash:                  txHash,
		}
	}

	return transactions, nil
}

// hashEnvelopes returns the network-specific hash of every envelope, indexed as
// envelopes is. Work is split into one contiguous chunk per pool worker so each
// chunk can hold a single xdr.EncodingBuffer: the buffer reuses its scratch
// space across marshals, which is what keeps this off the allocator, and the
// SDK documents it as unsafe to share between goroutines.
func hashEnvelopes(ctx context.Context, networkPassphrase string, envelopes []xdr.TransactionEnvelope, pool pond.Pool) ([]xdr.Hash, error) {
	if strings.TrimSpace(networkPassphrase) == "" {
		return nil, errors.New("empty network passphrase")
	}
	hashes := make([]xdr.Hash, len(envelopes))
	if len(envelopes) == 0 {
		return hashes, nil
	}
	// The network id is a hash of the passphrase alone, so it is the same for
	// every transaction in the process.
	networkID := xdr.Hash(network.ID(networkPassphrase))

	// One chunk per worker, but never more than the machine can actually run at
	// once: an unbounded pool reports its concurrency as unlimited, which would
	// otherwise make a chunk — and a buffer — per transaction and defeat the
	// reuse this is built around.
	chunkCount := min(pool.MaxConcurrency(), runtime.GOMAXPROCS(0), len(envelopes))
	chunkSize := (len(envelopes) + chunkCount - 1) / chunkCount

	group := pool.NewGroupContext(ctx)
	var errs []error
	errMu := sync.Mutex{}

	for chunkStart := 0; chunkStart < len(envelopes); chunkStart += chunkSize {
		start, end := chunkStart, min(chunkStart+chunkSize, len(envelopes))
		group.Submit(func() {
			buf := xdr.NewEncodingBuffer()
			for i := start; i < end; i++ {
				envelopeHash, err := hashEnvelope(buf, networkID, &envelopes[i])
				if err != nil {
					errMu.Lock()
					errs = append(errs, fmt.Errorf("hashing transaction %d in tx set: %w", i, err))
					errMu.Unlock()
					return
				}
				hashes[i] = envelopeHash
			}
		})
	}

	if err := group.Wait(); err != nil {
		return nil, fmt.Errorf("waiting for envelope hashing: %w", err)
	}
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return hashes, nil
}

// hashEnvelope computes one transaction's network-specific hash. It mirrors
// network.HashTransactionInEnvelope, but marshals through the caller's buffer
// and a precomputed network id rather than allocating a fresh buffer and
// re-hashing the passphrase for every transaction, and it tags the envelope's
// transaction in place rather than by value.
func hashEnvelope(buf *xdr.EncodingBuffer, networkID xdr.Hash, envelope *xdr.TransactionEnvelope) (xdr.Hash, error) {
	var tagged xdr.TransactionSignaturePayloadTaggedTransaction
	//exhaustive:ignore
	switch envelope.Type {
	case xdr.EnvelopeTypeEnvelopeTypeTx:
		tagged = xdr.TransactionSignaturePayloadTaggedTransaction{
			Type: xdr.EnvelopeTypeEnvelopeTypeTx,
			Tx:   &envelope.V1.Tx,
		}
	case xdr.EnvelopeTypeEnvelopeTypeTxV0:
		// A v0 transaction is hashed as the v1 transaction it maps onto.
		v0 := envelope.V0.Tx
		sourceAccount, err := xdr.NewMuxedAccount(xdr.CryptoKeyTypeKeyTypeEd25519, v0.SourceAccountEd25519)
		if err != nil {
			return xdr.Hash{}, fmt.Errorf("converting v0 source account: %w", err)
		}
		tagged = xdr.TransactionSignaturePayloadTaggedTransaction{
			Type: xdr.EnvelopeTypeEnvelopeTypeTx,
			Tx: &xdr.Transaction{
				SourceAccount: sourceAccount,
				Fee:           v0.Fee,
				SeqNum:        v0.SeqNum,
				Cond:          xdr.NewPreconditionsWithTimeBounds(v0.TimeBounds),
				Memo:          v0.Memo,
				Operations:    v0.Operations,
			},
		}
	case xdr.EnvelopeTypeEnvelopeTypeTxFeeBump:
		tagged = xdr.TransactionSignaturePayloadTaggedTransaction{
			Type:    xdr.EnvelopeTypeEnvelopeTypeTxFeeBump,
			FeeBump: &envelope.FeeBump.Tx,
		}
	default:
		return xdr.Hash{}, fmt.Errorf("invalid transaction type %s", envelope.Type)
	}

	payload := xdr.TransactionSignaturePayload{NetworkId: networkID, TaggedTransaction: tagged}
	encoded, err := buf.UnsafeMarshalBinary(&payload)
	if err != nil {
		return xdr.Hash{}, fmt.Errorf("marshalling transaction signature payload: %w", err)
	}
	return xdr.Hash(hash.Hash(encoded)), nil
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
		if err := appendIfContractDataChange(out, change, ledgerSeq, tx.Index); err != nil {
			return err
		}
	}
	return nil
}

// appendIfContractDataChange appends change into out under its owning
// contract's C-address strkey when it is a ContractData change carrying an
// entry; every other change is ignored.
func appendIfContractDataChange(out map[string][]ingest.Change, change ingest.Change, ledgerSeq uint32, txIndex uint32) error {
	if change.Type != xdr.LedgerEntryTypeContractData {
		return nil
	}
	entry := change.Post
	if entry == nil {
		entry = change.Pre
	}
	if entry == nil {
		return nil
	}
	contractData, ok := entry.Data.GetContractData()
	if !ok {
		return nil
	}
	contractIDBytes, ok := contractData.Contract.GetContractId()
	if !ok {
		return nil
	}
	addr, encErr := strkey.Encode(strkey.VersionByteContract, contractIDBytes[:])
	if encErr != nil {
		// Callers rely on receiving every ContractData change; silently
		// dropping one would corrupt downstream state.
		return fmt.Errorf("encoding contract id for ledger %d tx %d: %w", ledgerSeq, txIndex, encErr)
	}
	out[addr] = append(out[addr], change)
	return nil
}

// ledgerEntryChangesContainContractData reports whether any element of the
// group references a ContractData entry or key. It lets the transaction-level
// segments of transactionContractDataChanges skip the Change build (and its
// per-change ledger-key sort) entirely — for almost every transaction those
// segments hold only fee-account entries.
func ledgerEntryChangesContainContractData(changes xdr.LedgerEntryChanges) bool {
	for _, c := range changes {
		switch c.Type {
		case xdr.LedgerEntryChangeTypeLedgerEntryCreated:
			if c.MustCreated().Data.Type == xdr.LedgerEntryTypeContractData {
				return true
			}
		case xdr.LedgerEntryChangeTypeLedgerEntryUpdated:
			if c.MustUpdated().Data.Type == xdr.LedgerEntryTypeContractData {
				return true
			}
		case xdr.LedgerEntryChangeTypeLedgerEntryRemoved:
			if c.MustRemoved().Type == xdr.LedgerEntryTypeContractData {
				return true
			}
		case xdr.LedgerEntryChangeTypeLedgerEntryState:
			if c.MustState().Data.Type == xdr.LedgerEntryTypeContractData {
				return true
			}
		case xdr.LedgerEntryChangeTypeLedgerEntryRestored:
			if c.MustRestored().Data.Type == xdr.LedgerEntryTypeContractData {
				return true
			}
		}
	}
	return false
}

// transactionContractDataChanges returns tx's ContractData changes grouped by
// the owning contract's C-address strkey — the same sequence
// collectContractDataChanges derives from tx.GetChanges() — while serving
// every operation segment from the wrappers' memoized Changes() slices, so
// each operation's meta is materialized and ledger-key-sorted once for the
// whole pipeline instead of a second time here. Only the small
// transaction-level segments are built in place, and only when their raw
// change groups mention a ContractData entry at all.
//
// Composition mirrors ingest.LedgerTransaction.GetChanges per meta version:
// transaction-level changes before, each operation's changes in operation
// order, then (V2+) transaction-level changes after. Returns nil when the
// transaction is unsuccessful or contributes no ContractData changes.
func transactionContractDataChanges(tx *ingest.LedgerTransaction, opsParticipants map[int64]processors.OperationParticipants) (map[string][]ingest.Change, error) {
	if !tx.Result.Successful() {
		return nil, nil
	}
	ledgerSeq := tx.Ledger.LedgerSequence()

	var before, after xdr.LedgerEntryChanges
	var opCount int
	switch tx.UnsafeMeta.V {
	case 1:
		meta := tx.UnsafeMeta.MustV1()
		before = meta.TxChanges
		opCount = len(meta.Operations)
	case 2:
		meta := tx.UnsafeMeta.MustV2()
		before, after = meta.TxChangesBefore, meta.TxChangesAfter
		opCount = len(meta.Operations)
	case 3:
		meta := tx.UnsafeMeta.MustV3()
		before, after = meta.TxChangesBefore, meta.TxChangesAfter
		opCount = len(meta.Operations)
	case 4:
		meta := tx.UnsafeMeta.MustV4()
		before, after = meta.TxChangesBefore, meta.TxChangesAfter
		opCount = len(meta.Operations)
	default:
		return nil, fmt.Errorf("unsupported TransactionMeta version %d in ledger %d tx %d", tx.UnsafeMeta.V, ledgerSeq, tx.Index)
	}

	out := map[string][]ingest.Change{}
	txLevel := func(ledgerEntryChanges xdr.LedgerEntryChanges) error {
		if !ledgerEntryChangesContainContractData(ledgerEntryChanges) {
			return nil
		}
		changes := ingest.GetChangesFromLedgerEntryChanges(ledgerEntryChanges)
		for i := range changes {
			changes[i].Reason = ingest.LedgerEntryChangeReasonTransaction
			changes[i].Transaction = tx
			changes[i].Ledger = &tx.Ledger
		}
		for _, change := range changes {
			if err := appendIfContractDataChange(out, change, ledgerSeq, tx.Index); err != nil {
				return err
			}
		}
		return nil
	}

	if err := txLevel(before); err != nil {
		return nil, err
	}

	wrappersByIndex := make(map[uint32]*processors.TransactionOperationWrapper, len(opsParticipants))
	for _, opParticipants := range opsParticipants {
		wrappersByIndex[opParticipants.OpWrapper.Index] = opParticipants.OpWrapper
	}
	for opIdx := 0; opIdx < opCount; opIdx++ {
		var changes []ingest.Change
		var chErr error
		if wrapper := wrappersByIndex[uint32(opIdx)]; wrapper != nil {
			changes, chErr = wrapper.Changes()
		} else {
			// Every operation normally has a wrapper — its source account is
			// always a participant — so this is a correctness backstop for an
			// operation filtered out of opsParticipants, not a hot path.
			changes, chErr = tx.GetOperationChanges(uint32(opIdx))
		}
		if chErr != nil {
			return nil, fmt.Errorf("getting operation %d changes for ledger %d tx %d: %w", opIdx, ledgerSeq, tx.Index, chErr)
		}
		for _, change := range changes {
			if err := appendIfContractDataChange(out, change, ledgerSeq, tx.Index); err != nil {
				return nil, err
			}
		}
	}

	if err := txLevel(after); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

// ProcessLedger extracts transactions from a ledger and indexes them.
// Returns the participant count for optional metrics recording. Everything a
// downstream stage needs — including the ledger's ContractData changes — is
// folded into the buffer.
func ProcessLedger(ctx context.Context, networkPassphrase string, ledgerMeta xdr.LedgerCloseMeta, ledgerIndexer *Indexer, buffer *IndexerBuffer) (int, error) {
	ledgerSeq := ledgerMeta.LedgerSequence()
	transactions, err := GetLedgerTransactions(ctx, networkPassphrase, ledgerMeta, ledgerIndexer.pool)
	if err != nil {
		return 0, fmt.Errorf("getting transactions for ledger %d: %w", ledgerSeq, err)
	}

	participantCount, err := ledgerIndexer.ProcessLedgerTransactions(ctx, transactions, buffer)
	if err != nil {
		return 0, fmt.Errorf("processing transactions for ledger %d: %w", ledgerSeq, err)
	}

	return participantCount, nil
}
