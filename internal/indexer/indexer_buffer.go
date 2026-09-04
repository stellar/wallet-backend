// Package indexer provides high-performance data buffering for Stellar blockchain ingestion.
// IndexerBuffer uses a canonical pointer architecture to minimize memory usage and eliminate
// duplicate checks during transaction/operation processing.
package indexer

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/txnbuild"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer/types"
)

// IndexerBuffer is a memory-efficient buffer for collecting blockchain data during ledger
// ingestion. It uses a two-level storage architecture:
//
// ARCHITECTURE:
// 1. Canonical Storage Layer:
//   - txByToID: Single pointer per unique transaction (keyed by ToID)
//   - opByID: Single pointer per unique operation (keyed by ID)
//   - This layer owns the actual data and ensures only ONE copy exists in memory
//
// 2. Transaction/Operation to Participants Mapping Layer:
//   - participantsByToID: Maps each transaction ToID to a set of participant IDs (a map used as a set)
//   - participantsByOpID: Maps each operation ID to a set of participant IDs (a map used as a set)
//   - Efficiently tracks which participants interacted with each tx/op
//
// Both layers of each pair share one key domain (ToID for transactions, ID for
// operations), so a participant entry can never exist without its canonical
// row. Keying transactions by hash instead would break that invariant on the
// streaming-loadtest backend, whose merged bootstrap ledgers can carry the
// same envelope at several tx-set positions — distinct ToIDs, one hash.
//
// MEMORY OPTIMIZATION:
// When multiple participants interact with the same transaction or operation, they all point
// to the SAME canonical pointer instead of storing duplicate copies.
//
// OWNERSHIP:
// Not safe for concurrent use. Each buffer instance is owned by a single goroutine:
// ProcessLedgerTransactions builds a per-transaction TransactionResult in parallel workers and
// folds each into one ledger buffer serially (IngestTransactionResult).

type TrustlineChangeKey struct {
	AccountID   string
	TrustlineID uuid.UUID
}

// SACBalanceChangeKey is a composite key for deduplicating SAC balance changes.
type SACBalanceChangeKey struct {
	AccountID  string
	ContractID string
}

// LiquidityPoolShareChangeKey is a composite key for deduplicating pool-share balance changes.
type LiquidityPoolShareChangeKey struct {
	AccountID string
	PoolID    string
}

// ContractEventKey identifies a contract-event group by transaction and
// operation index within a single ledger. The indexer extracts contract
// events once per InvokeHostFunction op (successful txs only) and stashes
// them under this key so downstream protocol processors can consume them
// without re-decoding LedgerCloseMeta.
type ContractEventKey struct {
	TxIdx uint32
	OpIdx uint32
}

type IndexerBuffer struct {
	txByToID                       map[int64]*types.Transaction
	participantsByToID             map[int64]map[string]struct{}
	opByID                         map[int64]*types.Operation
	participantsByOpID             map[int64]map[string]struct{}
	stateChanges                   []types.StateChange
	trustlineChangesByTrustlineKey map[TrustlineChangeKey]types.TrustlineChange
	accountChangesByAccountID      map[string]types.AccountChange
	sacBalanceChangesByKey         map[SACBalanceChangeKey]types.SACBalanceChange
	lpShareChangesByKey            map[LiquidityPoolShareChangeKey]types.LiquidityPoolShareChange
	lpChangesByPoolID              map[string]types.LiquidityPoolChange
	// Tombstones record the order value at which a create/add was cancelled by a same-ledger
	// remove. They keep the highest-order-wins invariant intact across the delete, so a later
	// lower-order change cannot resurrect a removed key. See pushWithTombstone.
	accountTombstones     map[string]int64
	trustlineTombstones   map[TrustlineChangeKey]int64
	sacTombstones         map[SACBalanceChangeKey]int64
	lpShareTombstones     map[LiquidityPoolShareChangeKey]int64
	lpTombstones          map[string]int64
	uniqueTrustlineAssets map[uuid.UUID]data.TrustlineAsset
	// parsedAssetsByString memoizes the parse + deterministic-ID derivation per unique asset
	// string (nil value = string is known-invalid). It is content-derived — the same string always
	// yields the same result — so it is never cleared in Clear(). Both ingestion paths reuse one
	// buffer across ledgers, so the memo warms for the life of the process and is bounded by the
	// number of unique asset strings that process has seen.
	parsedAssetsByString  map[string]*data.TrustlineAsset
	sacContractsByID      map[string]*data.Contract         // SAC contract metadata extracted from instance entries
	protocolWasmsByHash   map[string]data.ProtocolWasms     // wasmHash → ProtocolWasms (protocol_id stamped post-classification)
	wasmBytecodesByHash   map[string][]byte                 // wasmHash → raw bytecode (consumed by classification dispatch)
	protocolContractsByID map[string]data.ProtocolContracts // contractID → ProtocolContracts
	contractEventsByKey   map[ContractEventKey][]xdr.ContractEvent
	// contractDataChangesByContract groups the ledger's ContractData changes by the owning
	// contract's C-address strkey. Within a contract, changes arrive in transaction application
	// order (the fold is serial in transaction order), so last-write-wins folding per entry key
	// stays deterministic for the protocol processors that consume this at persist time.
	contractDataChangesByContract map[string][]ingest.Change

	// Prebuilt COPY rows for the five bulk tables, populated by BuildCopyRows on the
	// process stage so the persist stage streams them without building or encoding
	// anything per row. Read-only for consumers, valid until Clear(); the outer
	// backing arrays are reused across ledgers like the buffer's maps.
	transactionRows        [][]any
	transactionAccountRows [][]any
	operationRows          [][]any
	operationAccountRows   [][]any
	stateChangeRows        [][]any
	// addressByteaMemo caches strkey→BYTEA conversions for every builder that
	// touches addresses (state_changes and both account-link tables). Entries are
	// content-addressed like parsedAssetsByString, so the memo stays warm across
	// ledgers; unlike asset strings, unique addresses grow without bound over a
	// process lifetime, so Clear() drops it wholesale past a cap.
	addressByteaMemo types.AddressByteaMemo
}

// maxAddressByteaMemoEntries caps addressByteaMemo's growth (~40 bytes/entry:
// a 56-char strkey plus 33 decoded bytes). Past the cap, Clear() drops the
// whole memo and it rewarms from live traffic.
const maxAddressByteaMemoEntries = 200_000

// NewIndexerBuffer creates a new IndexerBuffer with initialized data structures.
// All maps are pre-allocated to avoid nil map access.
func NewIndexerBuffer() *IndexerBuffer {
	return &IndexerBuffer{
		txByToID:                       make(map[int64]*types.Transaction),
		participantsByToID:             make(map[int64]map[string]struct{}),
		opByID:                         make(map[int64]*types.Operation),
		participantsByOpID:             make(map[int64]map[string]struct{}),
		stateChanges:                   make([]types.StateChange, 0),
		trustlineChangesByTrustlineKey: make(map[TrustlineChangeKey]types.TrustlineChange),
		accountChangesByAccountID:      make(map[string]types.AccountChange),
		sacBalanceChangesByKey:         make(map[SACBalanceChangeKey]types.SACBalanceChange),
		lpShareChangesByKey:            make(map[LiquidityPoolShareChangeKey]types.LiquidityPoolShareChange),
		lpChangesByPoolID:              make(map[string]types.LiquidityPoolChange),
		accountTombstones:              make(map[string]int64),
		trustlineTombstones:            make(map[TrustlineChangeKey]int64),
		sacTombstones:                  make(map[SACBalanceChangeKey]int64),
		lpShareTombstones:              make(map[LiquidityPoolShareChangeKey]int64),
		lpTombstones:                   make(map[string]int64),
		uniqueTrustlineAssets:          make(map[uuid.UUID]data.TrustlineAsset),
		parsedAssetsByString:           make(map[string]*data.TrustlineAsset),
		sacContractsByID:               make(map[string]*data.Contract),
		protocolWasmsByHash:            make(map[string]data.ProtocolWasms),
		wasmBytecodesByHash:            make(map[string][]byte),
		protocolContractsByID:          make(map[string]data.ProtocolContracts),
		contractEventsByKey:            make(map[ContractEventKey][]xdr.ContractEvent),
		contractDataChangesByContract:  make(map[string][]ingest.Change),
		addressByteaMemo:               make(types.AddressByteaMemo),
	}
}

// BuildCopyRows materializes the five bulk tables' COPY rows from the buffer's
// current contents, replacing whatever rows a previous ledger left (their
// backing arrays are reused). It runs on the process stage so the persist
// stage's COPYs stream pre-encodable tuples instead of building rows on the
// connection's critical path. The transaction and operation rows come from the
// sorted getters, so every table's rows are already in primary-key order.
func (b *IndexerBuffer) BuildCopyRows() error {
	txs := b.GetTransactions()
	ops := b.GetOperations()

	var err error
	if b.transactionRows, err = data.BuildTransactionCopyRows(b.transactionRows[:0], txs); err != nil {
		return fmt.Errorf("building transactions COPY rows: %w", err)
	}
	if b.operationRows, err = data.BuildOperationCopyRows(b.operationRows[:0], ops); err != nil {
		return fmt.Errorf("building operations COPY rows: %w", err)
	}
	if b.stateChangeRows, err = data.BuildStateChangeCopyRows(b.stateChangeRows[:0], b.stateChanges, b.addressByteaMemo); err != nil {
		return fmt.Errorf("building state_changes COPY rows: %w", err)
	}
	if b.transactionAccountRows, err = data.BuildAccountLinkCopyRows(b.transactionAccountRows[:0], txs,
		func(tx *types.Transaction) (int64, time.Time) { return tx.ToID, tx.LedgerCreatedAt },
		b.participantsByToID, b.addressByteaMemo); err != nil {
		return fmt.Errorf("building transactions_accounts COPY rows: %w", err)
	}
	if b.operationAccountRows, err = data.BuildAccountLinkCopyRows(b.operationAccountRows[:0], ops,
		func(op *types.Operation) (int64, time.Time) { return op.ID, op.LedgerCreatedAt },
		b.participantsByOpID, b.addressByteaMemo); err != nil {
		return fmt.Errorf("building operations_accounts COPY rows: %w", err)
	}
	return nil
}

// GetTransactionCopyRows returns the buffer's prebuilt transactions COPY rows;
// callers must not modify them. Valid until Clear().
func (b *IndexerBuffer) GetTransactionCopyRows() [][]any { return b.transactionRows }

// GetTransactionAccountCopyRows returns the buffer's prebuilt
// transactions_accounts COPY rows; callers must not modify them. Valid until Clear().
func (b *IndexerBuffer) GetTransactionAccountCopyRows() [][]any { return b.transactionAccountRows }

// GetOperationCopyRows returns the buffer's prebuilt operations COPY rows;
// callers must not modify them. Valid until Clear().
func (b *IndexerBuffer) GetOperationCopyRows() [][]any { return b.operationRows }

// GetOperationAccountCopyRows returns the buffer's prebuilt operations_accounts
// COPY rows; callers must not modify them. Valid until Clear().
func (b *IndexerBuffer) GetOperationAccountCopyRows() [][]any { return b.operationAccountRows }

// GetStateChangeCopyRows returns the buffer's prebuilt state_changes COPY rows;
// callers must not modify them. Valid until Clear().
func (b *IndexerBuffer) GetStateChangeCopyRows() [][]any { return b.stateChangeRows }

// PushTransaction adds a transaction and associates it with a participant.
// Uses canonical pointer pattern: stores one copy of each transaction (by ToID) and tracks
// which participants interacted with it. Multiple participants can reference the same transaction.
func (b *IndexerBuffer) PushTransaction(participant string, transaction *types.Transaction) {
	b.recordTransaction(participant, transaction)
}

// recordTransaction is the shared internal helper that stores a transaction pointer and
// records the participant:
//
// 1. Check if transaction already exists in txByToID
// 2. If not, store the transaction pointer
// 3. Add participant to this transaction's participant set in participantsByToID
func (b *IndexerBuffer) recordTransaction(participant string, transaction *types.Transaction) {
	toID := transaction.ToID
	if _, exists := b.txByToID[toID]; !exists {
		b.txByToID[toID] = transaction
	}

	// Track this participant by ToID
	participants, exists := b.participantsByToID[toID]
	if !exists {
		participants = make(map[string]struct{})
		b.participantsByToID[toID] = participants
	}

	// Add participant - O(1) with automatic deduplication
	participants[participant] = struct{}{}
}

// GetNumberOfTransactions returns the count of unique transactions in the buffer.
func (b *IndexerBuffer) GetNumberOfTransactions() int {
	return len(b.txByToID)
}

// GetNumberOfOperations returns the count of unique operations in the buffer.
func (b *IndexerBuffer) GetNumberOfOperations() int {
	return len(b.opByID)
}

// GetTransactions returns all unique transactions in ascending ToID order. The transactions
// hypertable's primary key leads with to_id, so COPYing in this order walks the index left to right.
func (b *IndexerBuffer) GetTransactions() []*types.Transaction {
	txs := make([]*types.Transaction, 0, len(b.txByToID))
	for _, txPtr := range b.txByToID {
		txs = append(txs, txPtr)
	}
	slices.SortFunc(txs, func(a, b *types.Transaction) int { return cmp.Compare(a.ToID, b.ToID) })

	return txs
}

// GetTransactionsParticipants returns the buffer's live map of transaction ToIDs to their
// participants; callers must not modify it or the maps it holds.
func (b *IndexerBuffer) GetTransactionsParticipants() map[int64]map[string]struct{} {
	return b.participantsByToID
}

// pushWithTombstone deduplicates change into m, keeping the highest-ordered change per key.
//
// A create/add that is later removed within the same ledger nets to nothing: the key is deleted
// and a tombstone is recorded at the remove's order value. The tombstone drops any subsequent
// change whose order is <= it (a chronologically-earlier change can no longer resurrect the key),
// while a strictly-higher order — a genuine later re-create/re-add of the same key — lifts the
// tombstone and wins. This keeps the highest-order-wins invariant intact across the delete; a bare
// delete would break it, since the key would look absent and a lower-order change would re-insert a
// stale phantom.
func pushWithTombstone[K comparable, V any](
	m map[K]V,
	tombstones map[K]int64,
	key K,
	change V,
	order func(V) int64,
	isNoopRemove func(existing, incoming V) bool,
) {
	if tomb, ok := tombstones[key]; ok {
		if order(change) <= tomb {
			return
		}
		delete(tombstones, key)
	}

	existing, exists := m[key]
	if exists && order(existing) > order(change) {
		return
	}

	if exists && isNoopRemove(existing, change) {
		delete(m, key)
		tombstones[key] = order(change)
		return
	}

	m[key] = change
}

func accountOrder(c types.AccountChange) int64 { return c.SortKey }

func accountIsNoopRemove(existing, incoming types.AccountChange) bool {
	return existing.Operation == types.AccountOpCreate && incoming.Operation == types.AccountOpRemove
}

func trustlineOrder(c types.TrustlineChange) int64 { return c.OperationID }

func trustlineIsNoopRemove(existing, incoming types.TrustlineChange) bool {
	return existing.Operation == types.TrustlineOpAdd && incoming.Operation == types.TrustlineOpRemove
}

func sacBalanceOrder(c types.SACBalanceChange) int64 { return c.OperationID }

func sacBalanceIsNoopRemove(existing, incoming types.SACBalanceChange) bool {
	return existing.Operation == types.SACBalanceOpAdd && incoming.Operation == types.SACBalanceOpRemove
}

func lpShareOrder(c types.LiquidityPoolShareChange) int64 { return c.OperationID }

func lpShareIsNoopRemove(existing, incoming types.LiquidityPoolShareChange) bool {
	return existing.Operation == types.LiquidityPoolShareOpAdd && incoming.Operation == types.LiquidityPoolShareOpRemove
}

func lpOrder(c types.LiquidityPoolChange) int64 { return c.OperationID }

func lpIsNoopRemove(existing, incoming types.LiquidityPoolChange) bool {
	return existing.Operation == types.LiquidityPoolOpAdd && incoming.Operation == types.LiquidityPoolOpRemove
}

// PushTrustlineChange adds a trustline change to the buffer and tracks unique assets.
// The parse + deterministic-ID derivation is memoized per asset string (see parsedAssetsByString),
// so a repeated asset — valid or invalid — skips re-parsing and re-validation.
func (b *IndexerBuffer) PushTrustlineChange(trustlineChange types.TrustlineChange) {
	asset, cached := b.parsedAssetsByString[trustlineChange.Asset]
	if !cached {
		code, issuer, err := ParseAssetString(trustlineChange.Asset)
		if err == nil {
			trustlineID := data.DeterministicAssetID(code, issuer)
			asset = &data.TrustlineAsset{
				ID:     trustlineID,
				Code:   code,
				Issuer: issuer,
			}
		}
		// A nil asset records a known-invalid string so repeated invalid assets skip re-validation.
		b.parsedAssetsByString[trustlineChange.Asset] = asset
	}
	if asset == nil {
		return // Skip invalid assets
	}

	// Track unique asset with pre-computed deterministic ID
	if _, exists := b.uniqueTrustlineAssets[asset.ID]; !exists {
		b.uniqueTrustlineAssets[asset.ID] = *asset
	}

	changeKey := TrustlineChangeKey{
		AccountID:   trustlineChange.AccountID,
		TrustlineID: asset.ID,
	}
	pushWithTombstone(b.trustlineChangesByTrustlineKey, b.trustlineTombstones, changeKey, trustlineChange, trustlineOrder, trustlineIsNoopRemove)
}

// GetTrustlineChanges returns the buffer's internal map of trustline changes;
// callers must not modify it.
func (b *IndexerBuffer) GetTrustlineChanges() map[TrustlineChangeKey]types.TrustlineChange {
	return b.trustlineChangesByTrustlineKey
}

// PushAccountChange adds an account change to the buffer with deduplication.
// Keeps the change with highest SortKey per account. A CREATE→REMOVE within the same ledger nets
// to nothing and is tombstoned so a later lower-key change cannot resurrect it (see
// pushWithTombstone).
func (b *IndexerBuffer) PushAccountChange(accountChange types.AccountChange) {
	pushWithTombstone(b.accountChangesByAccountID, b.accountTombstones, accountChange.AccountID, accountChange, accountOrder, accountIsNoopRemove)
}

// GetAccountChanges returns the buffer's internal map of account changes;
// callers must not modify it.
func (b *IndexerBuffer) GetAccountChanges() map[string]types.AccountChange {
	return b.accountChangesByAccountID
}

// PushSACBalanceChange adds a SAC balance change to the buffer with deduplication.
// Keeps the change with highest OperationID per (AccountID, ContractID). An ADD→REMOVE within the
// same ledger nets to nothing and is tombstoned so a later lower-key change cannot resurrect it
// (see pushWithTombstone).
func (b *IndexerBuffer) PushSACBalanceChange(sacBalanceChange types.SACBalanceChange) {
	key := SACBalanceChangeKey{
		AccountID:  sacBalanceChange.AccountID,
		ContractID: sacBalanceChange.ContractID,
	}
	pushWithTombstone(b.sacBalanceChangesByKey, b.sacTombstones, key, sacBalanceChange, sacBalanceOrder, sacBalanceIsNoopRemove)
}

// GetSACBalanceChanges returns the buffer's internal map of SAC balance
// changes; callers must not modify it.
func (b *IndexerBuffer) GetSACBalanceChanges() map[SACBalanceChangeKey]types.SACBalanceChange {
	return b.sacBalanceChangesByKey
}

// PushLiquidityPoolShareChange adds a pool-share balance change to the buffer with deduplication.
// Keeps the change with highest OperationID per (AccountID, PoolID). An ADD→REMOVE within the same
// ledger nets to nothing and is tombstoned so a later lower-key change cannot resurrect it (see
// pushWithTombstone).
func (b *IndexerBuffer) PushLiquidityPoolShareChange(change types.LiquidityPoolShareChange) {
	key := LiquidityPoolShareChangeKey{
		AccountID: change.AccountID,
		PoolID:    change.PoolID,
	}
	pushWithTombstone(b.lpShareChangesByKey, b.lpShareTombstones, key, change, lpShareOrder, lpShareIsNoopRemove)
}

// GetLiquidityPoolShareChanges returns the buffer's internal map of
// pool-share balance changes; callers must not modify it.
func (b *IndexerBuffer) GetLiquidityPoolShareChanges() map[LiquidityPoolShareChangeKey]types.LiquidityPoolShareChange {
	return b.lpShareChangesByKey
}

// PushLiquidityPoolChange adds a pool reserve change to the buffer with deduplication.
// Keeps the change with highest OperationID per PoolID. An ADD→REMOVE within the same ledger nets
// to nothing and is tombstoned so a later lower-key change cannot resurrect it (see
// pushWithTombstone).
func (b *IndexerBuffer) PushLiquidityPoolChange(change types.LiquidityPoolChange) {
	pushWithTombstone(b.lpChangesByPoolID, b.lpTombstones, change.PoolID, change, lpOrder, lpIsNoopRemove)
}

// GetLiquidityPoolChanges returns the buffer's internal map of pool reserve
// changes; callers must not modify it.
func (b *IndexerBuffer) GetLiquidityPoolChanges() map[string]types.LiquidityPoolChange {
	return b.lpChangesByPoolID
}

// PushOperation adds an operation and its parent transaction, associating both with a participant.
// Uses canonical pointer pattern for both operations and transactions to avoid memory duplication.
func (b *IndexerBuffer) PushOperation(participant string, operation *types.Operation, transaction *types.Transaction) {
	b.recordOperation(participant, operation)
	b.recordTransaction(participant, transaction)
}

// GetOperations returns all unique operations from the canonical storage in ascending ID order.
// The operations primary key is its only index, so COPYing in this order walks it left to right.
func (b *IndexerBuffer) GetOperations() []*types.Operation {
	ops := make([]*types.Operation, 0, len(b.opByID))
	for _, opPtr := range b.opByID {
		ops = append(ops, opPtr)
	}
	slices.SortFunc(ops, func(a, b *types.Operation) int { return cmp.Compare(a.ID, b.ID) })
	return ops
}

// GetOperationsParticipants returns the buffer's live map of operation IDs to their
// participants; callers must not modify it or the maps it holds.
func (b *IndexerBuffer) GetOperationsParticipants() map[int64]map[string]struct{} {
	return b.participantsByOpID
}

// recordOperation is the shared internal helper that stores an operation pointer and records
// the participant. Stores one copy of each operation (by ID) and tracks which participants
// interacted with it.
func (b *IndexerBuffer) recordOperation(participant string, operation *types.Operation) {
	opID := operation.ID
	if _, exists := b.opByID[opID]; !exists {
		b.opByID[opID] = operation
	}

	// Track this participant globally
	participants, exists := b.participantsByOpID[opID]
	if !exists {
		participants = make(map[string]struct{})
		b.participantsByOpID[opID] = participants
	}
	participants[participant] = struct{}{}
}

// PushStateChange adds a state change along with its associated transaction and operation.
// operation may be nil for fee state changes, which have no associated operation.
func (b *IndexerBuffer) PushStateChange(transaction *types.Transaction, operation *types.Operation, stateChange types.StateChange) {
	b.stateChanges = append(b.stateChanges, stateChange)
	b.recordTransaction(string(stateChange.AccountID), transaction)
	// Fee changes dont have an operation ID associated with them
	if stateChange.OperationID != 0 && operation != nil {
		b.recordOperation(string(stateChange.AccountID), operation)
	}
}

// GetStateChanges returns the buffer's internal slice of state changes;
// callers must not modify it.
func (b *IndexerBuffer) GetStateChanges() []types.StateChange {
	return b.stateChanges
}

// TransactionResult is the per-transaction output produced by a parallel worker in
// ProcessLedgerTransactions. Workers build these independently (no shared buffer, no locks); the
// serial fold (IngestTransactionResult) then replays them into one ledger buffer. This avoids
// allocating a full IndexerBuffer per transaction and the subsequent buffer-to-buffer merge.
//
// Operations is keyed by operation ID and is shared by OpParticipants (participant tracking) and
// StateChanges (state-change → operation association). StateChanges is already filtered by the
// worker: entries with an empty AccountID or an OperationID with no matching operation are dropped.
//
// Netting at the fold (pushWithTombstone) requires only that a key's create/add precedes the remove
// that cancels it — not that a change-family slice is globally sorted by order value.
// processTransaction walks operations in ascending opID order, which gives that for every family
// (TrustlineChanges, AccountChanges, SACBalanceChanges, LPShareChanges, LPChanges). AccountChanges is
// the one slice that is not globally ascending: the fee-phase changes are appended after the operation
// walk even though phaseFee sorts below every operation (see processors.accountSortKey). That is
// harmless because a fee debit or Soroban refund always updates an account entry that already exists
// — it never creates or removes one — so those changes pair with nothing to net, and the
// highest-order-wins guard discards them whenever an operation already wrote a higher key.
type TransactionResult struct {
	Transaction           *types.Transaction
	TxParticipants        []string
	Operations            map[int64]*types.Operation
	OpParticipants        map[int64][]string
	StateChanges          []types.StateChange
	TrustlineChanges      []types.TrustlineChange
	AccountChanges        []types.AccountChange
	SACBalanceChanges     []types.SACBalanceChange
	LPShareChanges        []types.LiquidityPoolShareChange
	LPChanges             []types.LiquidityPoolChange
	SACContracts          []*data.Contract
	ProtocolWasms         []data.ProtocolWasms
	ProtocolWasmBytecodes map[string][]byte
	ProtocolContracts     []data.ProtocolContracts
	ContractEvents        map[ContractEventKey][]xdr.ContractEvent
	// ContractDataChanges groups the transaction's ContractData changes by owning contract
	// C-address, composed in GetChanges order (tx-level before, operations ascending, tx-level
	// after); nil when the transaction is unsuccessful or touches no ContractData.
	ContractDataChanges map[string][]ingest.Change
	ParticipantCount    int
}

// IngestTransactionResult folds a single transaction's result into the buffer, applying the same
// per-key deduplication as the individual Push* methods. It is called serially by
// ProcessLedgerTransactions after the parallel workers finish, so no locking is required.
func (b *IndexerBuffer) IngestTransactionResult(r *TransactionResult) {
	for _, participant := range r.TxParticipants {
		b.PushTransaction(participant, r.Transaction)
	}

	for opID, participants := range r.OpParticipants {
		// Invariant: every OpParticipants key must resolve to an operation in r.Operations.
		operation := r.Operations[opID]
		if operation == nil {
			log.Errorf("operation %d missing from TransactionResult.Operations (ledger %d, tx %s); dropping its participants", opID, r.Transaction.LedgerNumber, r.Transaction.Hash)
			continue
		}
		for _, participant := range participants {
			b.PushOperation(participant, operation, r.Transaction)
		}
	}

	for _, trustlineChange := range r.TrustlineChanges {
		b.PushTrustlineChange(trustlineChange)
	}
	for _, accountChange := range r.AccountChanges {
		b.PushAccountChange(accountChange)
	}
	for _, sacBalanceChange := range r.SACBalanceChanges {
		b.PushSACBalanceChange(sacBalanceChange)
	}
	for _, lpShareChange := range r.LPShareChanges {
		b.PushLiquidityPoolShareChange(lpShareChange)
	}
	for _, lpChange := range r.LPChanges {
		b.PushLiquidityPoolChange(lpChange)
	}
	for _, contract := range r.SACContracts {
		b.PushSACContract(contract)
	}
	for _, wasm := range r.ProtocolWasms {
		b.PushProtocolWasm(wasm)
	}
	for wasmHash, bytecode := range r.ProtocolWasmBytecodes {
		b.PushProtocolWasmBytecode(wasmHash, bytecode)
	}
	for _, contract := range r.ProtocolContracts {
		b.PushProtocolContracts(contract)
	}

	for _, stateChange := range r.StateChanges {
		var operation *types.Operation
		if stateChange.OperationID != 0 {
			operation = r.Operations[stateChange.OperationID]
		}
		b.PushStateChange(r.Transaction, operation, stateChange)
	}

	for key, events := range r.ContractEvents {
		b.PushContractEvents(key, events)
	}

	// Random map order over contract addresses is fine here: each per-address
	// slice is appended as one unit, and the fold itself runs in transaction
	// application order, which is the only ordering consumers rely on.
	for addr, changes := range r.ContractDataChanges {
		b.contractDataChangesByContract[addr] = append(b.contractDataChangesByContract[addr], changes...)
	}
}

// GetContractDataChanges returns the buffer's ContractData changes grouped by
// owning contract C-address, in transaction application order within each
// contract; callers must not modify it. The map is allocated at construction
// and never replaced, so a buffer from NewIndexerBuffer never returns nil here
// — the RequiresContractData processors range over the result unconditionally.
func (b *IndexerBuffer) GetContractDataChanges() map[string][]ingest.Change {
	return b.contractDataChangesByContract
}

// Clear resets the buffer to its initial empty state while preserving allocated capacity. Both
// ingestion paths reuse a single buffer and clear it around each unit of work: live before every
// ledger, backfill after every flushed batch.
//
// Clearing the balance-change maps and their tombstones is load-bearing for the live path, the only
// one that persists native balances: processors.accountSortKey deliberately omits the ledger from its
// key, so changes from two different ledgers must never coexist in those maps.
func (b *IndexerBuffer) Clear() {
	// Clear maps (keep allocated backing arrays)
	clear(b.txByToID)
	clear(b.participantsByToID)
	clear(b.opByID)
	clear(b.participantsByOpID)
	clear(b.uniqueTrustlineAssets)
	// parsedAssetsByString is intentionally NOT cleared: it is content-derived (same string always
	// yields the same parse result), so it stays valid across every ledger and flush.
	clear(b.trustlineChangesByTrustlineKey)
	clear(b.sacContractsByID)
	clear(b.protocolWasmsByHash)
	clear(b.wasmBytecodesByHash)
	clear(b.protocolContractsByID)
	clear(b.contractEventsByKey)
	clear(b.contractDataChangesByContract)

	// Reset slices (reuse underlying arrays by slicing to zero)
	b.stateChanges = b.stateChanges[:0]

	// Clear account, SAC, and liquidity-pool balance changes maps
	clear(b.accountChangesByAccountID)
	clear(b.sacBalanceChangesByKey)
	clear(b.lpShareChangesByKey)
	clear(b.lpChangesByPoolID)

	// Clear tombstones
	clear(b.accountTombstones)
	clear(b.trustlineTombstones)
	clear(b.sacTombstones)
	clear(b.lpShareTombstones)
	clear(b.lpTombstones)

	// Reset the prebuilt COPY rows (reuse outer backing arrays; elements past
	// the length pin the previous ledger's rows only until append overwrites
	// them, bounded by the high-water mark — the same trade stateChanges makes).
	b.transactionRows = b.transactionRows[:0]
	b.transactionAccountRows = b.transactionAccountRows[:0]
	b.operationRows = b.operationRows[:0]
	b.operationAccountRows = b.operationAccountRows[:0]
	b.stateChangeRows = b.stateChangeRows[:0]

	// addressByteaMemo is content-addressed, so it stays warm across ledgers
	// like parsedAssetsByString; it only drops — wholesale — once it outgrows
	// its cap, and rewarms from live traffic.
	if len(b.addressByteaMemo) > maxAddressByteaMemoEntries {
		clear(b.addressByteaMemo)
	}
}

// GetUniqueTrustlineAssets returns all unique trustline assets with pre-computed IDs.
func (b *IndexerBuffer) GetUniqueTrustlineAssets() []data.TrustlineAsset {
	assets := make([]data.TrustlineAsset, 0, len(b.uniqueTrustlineAssets))
	for _, asset := range b.uniqueTrustlineAssets {
		assets = append(assets, asset)
	}
	return assets
}

// PushSACContract adds a SAC contract with extracted metadata to the buffer.
func (b *IndexerBuffer) PushSACContract(c *data.Contract) {
	if _, exists := b.sacContractsByID[c.ContractID]; !exists {
		b.sacContractsByID[c.ContractID] = c
	}
}

// GetSACContracts returns the map of SAC contract IDs to their metadata.
func (b *IndexerBuffer) GetSACContracts() map[string]*data.Contract {
	return b.sacContractsByID
}

// PushProtocolWasm adds a protocol WASM record to the buffer (deduplicated by
// hash; first-write wins). The record's ProtocolID is left for the
// classification dispatcher to populate at persistence time.
func (b *IndexerBuffer) PushProtocolWasm(wasm data.ProtocolWasms) {
	key := string(wasm.WasmHash)
	if _, exists := b.protocolWasmsByHash[key]; !exists {
		b.protocolWasmsByHash[key] = wasm
	}
}

// PushProtocolWasmBytecode stores raw WASM bytecode keyed by hash. Used by the
// classification dispatcher in persistLedgerData to extract specs and run
// per-protocol validators. Bytecode is content-addressed by hash, so
// first-write wins is safe.
func (b *IndexerBuffer) PushProtocolWasmBytecode(wasmHash string, bytecode []byte) {
	if _, exists := b.wasmBytecodesByHash[wasmHash]; !exists {
		b.wasmBytecodesByHash[wasmHash] = bytecode
	}
}

// GetProtocolWasms returns the protocol WASMs map.
func (b *IndexerBuffer) GetProtocolWasms() map[string]data.ProtocolWasms {
	return b.protocolWasmsByHash
}

// GetProtocolWasmBytecodes returns the wasmHash → bytecode map. The []byte values
// alias the buffer's internal storage and MUST be treated as read-only by callers.
// Bytecode is content-addressed by wasmHash and immutable by construction; mutating
// a returned slice would corrupt the buffer's encapsulated state.
func (b *IndexerBuffer) GetProtocolWasmBytecodes() map[string][]byte {
	return b.wasmBytecodesByHash
}

// PushProtocolContracts adds a protocol contract to the buffer with deduplication (last-write-wins).
func (b *IndexerBuffer) PushProtocolContracts(contract data.ProtocolContracts) {
	b.protocolContractsByID[string(contract.ContractID)] = contract
}

// GetProtocolContracts returns the protocol contracts map.
func (b *IndexerBuffer) GetProtocolContracts() map[string]data.ProtocolContracts {
	return b.protocolContractsByID
}

// PushContractEvents stashes the contract events emitted by a single
// InvokeHostFunction operation. The caller is expected to extract events
// once per (txIdx, opIdx) on successful transactions only — protocol
// processors consume from this map instead of re-decoding LedgerCloseMeta.
// First-write wins on key collisions (which should not occur under the
// indexer's parallel-per-tx split).
func (b *IndexerBuffer) PushContractEvents(key ContractEventKey, events []xdr.ContractEvent) {
	if len(events) == 0 {
		return
	}
	if _, exists := b.contractEventsByKey[key]; !exists {
		b.contractEventsByKey[key] = events
	}
}

// GetContractEvents returns the contract-events map. Event slices alias
// buffer-owned storage and MUST be treated as read-only.
func (b *IndexerBuffer) GetContractEvents() map[ContractEventKey][]xdr.ContractEvent {
	return b.contractEventsByKey
}

// ParseAssetString parses a "CODE:ISSUER" formatted asset string into its components.
func ParseAssetString(asset string) (code, issuer string, err error) {
	parts := strings.SplitN(asset, ":", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid asset format: expected CODE:ISSUER, got %s", asset)
	}
	code, issuer = parts[0], parts[1]

	// Validate using txnbuild
	creditAsset := txnbuild.CreditAsset{Code: code, Issuer: issuer}
	if _, err := creditAsset.ToXDR(); err != nil {
		return "", "", fmt.Errorf("invalid asset %s: %w", asset, err)
	}
	return code, issuer, nil
}
