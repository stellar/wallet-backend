// Package indexer provides high-performance data buffering for Stellar blockchain ingestion.
// IndexerBuffer uses a canonical pointer + set-based architecture to minimize memory usage
// and eliminate duplicate checks during transaction/operation processing.
package indexer

import (
	"fmt"
	"maps"
	"strings"
	"sync"

	set "github.com/deckarep/golang-set/v2"
	"github.com/google/uuid"
	"github.com/stellar/go-stellar-sdk/txnbuild"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer/types"
)

// IndexerBuffer is a thread-safe, memory-efficient buffer for collecting blockchain data
// during ledger ingestion. It uses a two-level storage architecture:
//
// ARCHITECTURE:
// 1. Canonical Storage Layer:
//   - txByHash: Single pointer per unique transaction (keyed by hash)
//   - opByID: Single pointer per unique operation (keyed by ID)
//   - This layer owns the actual data and ensures only ONE copy exists in memory
//
// 2. Transaction/Operation to Participants Mapping Layer:
//   - participantsByTxHash: Maps each transaction hash to a SET of participant IDs
//   - participantsByOpID: Maps each operation ID to a SET of participant IDs
//   - Efficiently tracks which participants interacted with each tx/op
//
// MEMORY OPTIMIZATION:
// Transaction structs contain large XDR fields (10-50+ KB each). When multiple participants
// interact with the same transaction, they all point to the SAME canonical pointer instead
// of storing duplicate copies.
//
// PERFORMANCE:
// - Push operations: O(1) via set.Add() with automatic deduplication
// - No manual duplicate checking: Sets handle uniqueness automatically
// - MergeBuffer: O(n) with zero temporary map allocations
//
// THREAD SAFETY:
// All public methods use RWMutex for concurrent read/exclusive write access.
// Callers can safely use multiple buffers in parallel goroutines.

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
	mu                             sync.RWMutex
	txByHash                       map[string]*types.Transaction
	participantsByToID             map[int64]set.Set[string]
	opByID                         map[int64]*types.Operation
	participantsByOpID             map[int64]set.Set[string]
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
	sacContractsByID      map[string]*data.Contract         // SAC contract metadata extracted from instance entries
	protocolWasmsByHash   map[string]data.ProtocolWasms     // wasmHash → ProtocolWasms (protocol_id stamped post-classification)
	wasmBytecodesByHash   map[string][]byte                 // wasmHash → raw bytecode (consumed by classification dispatch)
	protocolContractsByID map[string]data.ProtocolContracts // contractID → ProtocolContracts
	contractEventsByKey   map[ContractEventKey][]xdr.ContractEvent
}

// NewIndexerBuffer creates a new IndexerBuffer with initialized data structures.
// All maps and sets are pre-allocated to avoid nil pointer issues during concurrent access.
func NewIndexerBuffer() *IndexerBuffer {
	return &IndexerBuffer{
		txByHash:                       make(map[string]*types.Transaction),
		participantsByToID:             make(map[int64]set.Set[string]),
		opByID:                         make(map[int64]*types.Operation),
		participantsByOpID:             make(map[int64]set.Set[string]),
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
		sacContractsByID:               make(map[string]*data.Contract),
		protocolWasmsByHash:            make(map[string]data.ProtocolWasms),
		wasmBytecodesByHash:            make(map[string][]byte),
		protocolContractsByID:          make(map[string]data.ProtocolContracts),
		contractEventsByKey:            make(map[ContractEventKey][]xdr.ContractEvent),
	}
}

// PushTransaction adds a transaction and associates it with a participant.
// Uses canonical pointer pattern: stores one copy of each transaction (by hash) and tracks
// which participants interacted with it. Multiple participants can reference the same transaction.
// Thread-safe: acquires write lock.
func (b *IndexerBuffer) PushTransaction(participant string, transaction types.Transaction) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.pushTransactionUnsafe(participant, &transaction)
}

// pushTransactionUnsafe is the internal implementation that assumes the caller
// already holds the write lock. This method implements the following pattern:
//
// 1. Check if transaction already exists in txByHash
// 2. If not, store the transaction pointer
// 3. Add participant to the global participants set
// 4. Add participant to this transaction's participant set in participantsByToID
//
// Caller must hold write lock.
func (b *IndexerBuffer) pushTransactionUnsafe(participant string, transaction *types.Transaction) {
	txHash := transaction.Hash.String()
	if _, exists := b.txByHash[txHash]; !exists {
		b.txByHash[txHash] = transaction
	}

	// Track this participant by ToID
	toID := transaction.ToID
	if _, exists := b.participantsByToID[toID]; !exists {
		b.participantsByToID[toID] = set.NewSet[string]()
	}

	// Add participant - O(1) with automatic deduplication
	b.participantsByToID[toID].Add(participant)
}

// GetNumberOfTransactions returns the count of unique transactions in the buffer.
// Thread-safe: uses read lock.
func (b *IndexerBuffer) GetNumberOfTransactions() int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return len(b.txByHash)
}

// GetNumberOfOperations returns the count of unique operations in the buffer.
// Thread-safe: uses read lock.
func (b *IndexerBuffer) GetNumberOfOperations() int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return len(b.opByID)
}

// GetTransactions returns all unique transactions.
// Thread-safe: uses read lock.
func (b *IndexerBuffer) GetTransactions() []*types.Transaction {
	b.mu.RLock()
	defer b.mu.RUnlock()

	txs := make([]*types.Transaction, 0, len(b.txByHash))
	for _, txPtr := range b.txByHash {
		txs = append(txs, txPtr)
	}

	return txs
}

// GetTransactionsParticipants returns a map of transaction ToIDs to its
// participants. The map itself is a shallow clone, but each set.Set[string]
// value is the buffer's live object; callers must not modify the sets.
func (b *IndexerBuffer) GetTransactionsParticipants() map[int64]set.Set[string] {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return maps.Clone(b.participantsByToID)
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

// mergeWithTombstone folds src into dst using the same tombstone-aware dedup as pushWithTombstone.
// It first merges src's tombstones into dst (keeping the higher order per key and evicting any dst
// entry the tombstone cancels), then replays src's surviving changes. Merging tombstones is
// required because a create->remove cancelled entirely within src leaves only a tombstone there,
// which must carry over so a lower-order change in dst cannot resurrect the key.
func mergeWithTombstone[K comparable, V any](
	dst, src map[K]V,
	dstTombstones, srcTombstones map[K]int64,
	order func(V) int64,
	isNoopRemove func(existing, incoming V) bool,
) {
	for key, tomb := range srcTombstones {
		if existing, ok := dstTombstones[key]; !ok || tomb > existing {
			dstTombstones[key] = tomb
		}
		if entry, ok := dst[key]; ok && order(entry) <= dstTombstones[key] {
			delete(dst, key)
		}
	}

	for key, change := range src {
		pushWithTombstone(dst, dstTombstones, key, change, order, isNoopRemove)
	}
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
// Thread-safe: acquires write lock.
func (b *IndexerBuffer) PushTrustlineChange(trustlineChange types.TrustlineChange) {
	b.mu.Lock()
	defer b.mu.Unlock()

	code, issuer, err := ParseAssetString(trustlineChange.Asset)
	if err != nil {
		return // Skip invalid assets
	}
	trustlineID := data.DeterministicAssetID(code, issuer)

	// Track unique asset with pre-computed deterministic ID
	if _, exists := b.uniqueTrustlineAssets[trustlineID]; !exists {
		b.uniqueTrustlineAssets[trustlineID] = data.TrustlineAsset{
			ID:     trustlineID,
			Code:   code,
			Issuer: issuer,
		}
	}

	changeKey := TrustlineChangeKey{
		AccountID:   trustlineChange.AccountID,
		TrustlineID: trustlineID,
	}
	pushWithTombstone(b.trustlineChangesByTrustlineKey, b.trustlineTombstones, changeKey, trustlineChange, trustlineOrder, trustlineIsNoopRemove)
}

// GetTrustlineChanges returns the buffer's internal map of trustline changes;
// callers must not modify it. Thread-safe: uses read lock.
func (b *IndexerBuffer) GetTrustlineChanges() map[TrustlineChangeKey]types.TrustlineChange {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.trustlineChangesByTrustlineKey
}

// PushAccountChange adds an account change to the buffer with deduplication.
// Keeps the change with highest SortKey per account. A CREATE→REMOVE within the same ledger nets
// to nothing and is tombstoned so a later lower-key change cannot resurrect it (see
// pushWithTombstone). Thread-safe: acquires write lock.
func (b *IndexerBuffer) PushAccountChange(accountChange types.AccountChange) {
	b.mu.Lock()
	defer b.mu.Unlock()

	pushWithTombstone(b.accountChangesByAccountID, b.accountTombstones, accountChange.AccountID, accountChange, accountOrder, accountIsNoopRemove)
}

// GetAccountChanges returns the buffer's internal map of account changes;
// callers must not modify it. Thread-safe: uses read lock.
func (b *IndexerBuffer) GetAccountChanges() map[string]types.AccountChange {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.accountChangesByAccountID
}

// PushSACBalanceChange adds a SAC balance change to the buffer with deduplication.
// Keeps the change with highest OperationID per (AccountID, ContractID). An ADD→REMOVE within the
// same ledger nets to nothing and is tombstoned so a later lower-key change cannot resurrect it
// (see pushWithTombstone). Thread-safe: acquires write lock.
func (b *IndexerBuffer) PushSACBalanceChange(sacBalanceChange types.SACBalanceChange) {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := SACBalanceChangeKey{
		AccountID:  sacBalanceChange.AccountID,
		ContractID: sacBalanceChange.ContractID,
	}
	pushWithTombstone(b.sacBalanceChangesByKey, b.sacTombstones, key, sacBalanceChange, sacBalanceOrder, sacBalanceIsNoopRemove)
}

// GetSACBalanceChanges returns the buffer's internal map of SAC balance
// changes; callers must not modify it. Thread-safe: uses read lock.
func (b *IndexerBuffer) GetSACBalanceChanges() map[SACBalanceChangeKey]types.SACBalanceChange {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.sacBalanceChangesByKey
}

// PushLiquidityPoolShareChange adds a pool-share balance change to the buffer with deduplication.
// Keeps the change with highest OperationID per (AccountID, PoolID). An ADD→REMOVE within the same
// ledger nets to nothing and is tombstoned so a later lower-key change cannot resurrect it (see
// pushWithTombstone). Thread-safe: acquires write lock.
func (b *IndexerBuffer) PushLiquidityPoolShareChange(change types.LiquidityPoolShareChange) {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := LiquidityPoolShareChangeKey{
		AccountID: change.AccountID,
		PoolID:    change.PoolID,
	}
	pushWithTombstone(b.lpShareChangesByKey, b.lpShareTombstones, key, change, lpShareOrder, lpShareIsNoopRemove)
}

// GetLiquidityPoolShareChanges returns the buffer's internal map of
// pool-share balance changes; callers must not modify it. Thread-safe: uses
// read lock.
func (b *IndexerBuffer) GetLiquidityPoolShareChanges() map[LiquidityPoolShareChangeKey]types.LiquidityPoolShareChange {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.lpShareChangesByKey
}

// PushLiquidityPoolChange adds a pool reserve change to the buffer with deduplication.
// Keeps the change with highest OperationID per PoolID. An ADD→REMOVE within the same ledger nets
// to nothing and is tombstoned so a later lower-key change cannot resurrect it (see
// pushWithTombstone). Thread-safe: acquires write lock.
func (b *IndexerBuffer) PushLiquidityPoolChange(change types.LiquidityPoolChange) {
	b.mu.Lock()
	defer b.mu.Unlock()

	pushWithTombstone(b.lpChangesByPoolID, b.lpTombstones, change.PoolID, change, lpOrder, lpIsNoopRemove)
}

// GetLiquidityPoolChanges returns the buffer's internal map of pool reserve
// changes; callers must not modify it. Thread-safe: uses read lock.
func (b *IndexerBuffer) GetLiquidityPoolChanges() map[string]types.LiquidityPoolChange {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.lpChangesByPoolID
}

// PushOperation adds an operation and its parent transaction, associating both with a participant.
// Uses canonical pointer pattern for both operations and transactions to avoid memory duplication.
// Thread-safe: acquires write lock.
func (b *IndexerBuffer) PushOperation(participant string, operation types.Operation, transaction types.Transaction) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.pushOperationUnsafe(participant, &operation)
	b.pushTransactionUnsafe(participant, &transaction)
}

// GetOperations returns all unique operations from the canonical storage.
// Thread-safe: uses read lock.
func (b *IndexerBuffer) GetOperations() []*types.Operation {
	b.mu.RLock()
	defer b.mu.RUnlock()

	ops := make([]*types.Operation, 0, len(b.opByID))
	for _, opPtr := range b.opByID {
		ops = append(ops, opPtr)
	}
	return ops
}

// GetOperationsParticipants returns a map of operation IDs to its
// participants. The map itself is a shallow clone, but each set.Set[string]
// value is the buffer's live object; callers must not modify the sets.
func (b *IndexerBuffer) GetOperationsParticipants() map[int64]set.Set[string] {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return maps.Clone(b.participantsByOpID)
}

// pushOperationUnsafe is the internal implementation for operation storage.
// Stores one copy of each operation (by ID) and tracks which participants interacted with it.
// Caller must hold write lock.
func (b *IndexerBuffer) pushOperationUnsafe(participant string, operation *types.Operation) {
	opID := operation.ID
	if _, exists := b.opByID[opID]; !exists {
		b.opByID[opID] = operation
	}

	// Track this participant globally
	if _, exists := b.participantsByOpID[opID]; !exists {
		b.participantsByOpID[opID] = set.NewSet[string]()
	}
	b.participantsByOpID[opID].Add(participant)
}

// PushStateChange adds a state change along with its associated transaction and operation.
// Thread-safe: acquires write lock.
func (b *IndexerBuffer) PushStateChange(transaction types.Transaction, operation types.Operation, stateChange types.StateChange) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.stateChanges = append(b.stateChanges, stateChange)
	b.pushTransactionUnsafe(string(stateChange.AccountID), &transaction)
	// Fee changes dont have an operation ID associated with them
	if stateChange.OperationID != 0 {
		b.pushOperationUnsafe(string(stateChange.AccountID), &operation)
	}
}

// GetStateChanges returns the buffer's internal slice of state changes;
// callers must not modify it. Thread-safe: uses read lock.
func (b *IndexerBuffer) GetStateChanges() []types.StateChange {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.stateChanges
}

// Merge merges another IndexerBuffer into this buffer. This is used to combine
// per-ledger or per-transaction buffers into a single buffer for batch DB insertion.
//
// MERGE STRATEGY:
// 1. Union global participant sets (O(m) set operation)
// 2. Copy storage maps (txByHash, opByID) using maps.Copy
// 3. For each transaction hash in other.participantsByTxHash:
//   - Merge other's participant set into our participant set for that tx hash
//   - Creates new set if tx doesn't exist in our mapping yet
//
// 4. For each operation ID in other.participantsByOpID:
//   - Merge other's participant set into our participant set for that op ID
//   - Creates new set if op doesn't exist in our mapping yet
//
// 5. Append other's state changes to ours
//
// MEMORY EFFICIENCY:
// Zero temporary allocations - uses direct map/set manipulation.
//
// Thread-safe: acquires write lock on this buffer, read lock on other buffer.
func (b *IndexerBuffer) Merge(other IndexerBufferInterface) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Type assert to get concrete buffer for efficient merging
	otherBuffer, ok := other.(*IndexerBuffer)
	if !ok {
		return
	}

	otherBuffer.mu.RLock()
	defer otherBuffer.mu.RUnlock()

	// Merge transactions (canonical storage) - this establishes our canonical pointers
	maps.Copy(b.txByHash, otherBuffer.txByHash)
	for toID, otherParticipants := range otherBuffer.participantsByToID {
		if existing, exists := b.participantsByToID[toID]; exists {
			// Merge into existing set - iterate and add (Union creates new set)
			for participant := range otherParticipants.Iter() {
				existing.Add(participant)
			}
		} else {
			// Clone the set instead of creating empty + iterating
			b.participantsByToID[toID] = otherParticipants.Clone()
		}
	}

	// Merge operations (canonical storage)
	maps.Copy(b.opByID, otherBuffer.opByID)
	for opID, otherParticipants := range otherBuffer.participantsByOpID {
		if existing, exists := b.participantsByOpID[opID]; exists {
			// Merge into existing set - iterate and add (Union creates new set)
			for participant := range otherParticipants.Iter() {
				existing.Add(participant)
			}
		} else {
			// Clone the set instead of creating empty + iterating
			b.participantsByOpID[opID] = otherParticipants.Clone()
		}
	}

	// Merge state changes
	b.stateChanges = append(b.stateChanges, otherBuffer.stateChanges...)

	// Merge trustline, account, and SAC balance changes with the same tombstone-aware dedup as
	// the Push* methods (highest order wins; a same-ledger create/add→remove nets to nothing and
	// tombstones the key so a lower-order change cannot resurrect it).
	mergeWithTombstone(b.trustlineChangesByTrustlineKey, otherBuffer.trustlineChangesByTrustlineKey, b.trustlineTombstones, otherBuffer.trustlineTombstones, trustlineOrder, trustlineIsNoopRemove)
	mergeWithTombstone(b.accountChangesByAccountID, otherBuffer.accountChangesByAccountID, b.accountTombstones, otherBuffer.accountTombstones, accountOrder, accountIsNoopRemove)
	mergeWithTombstone(b.sacBalanceChangesByKey, otherBuffer.sacBalanceChangesByKey, b.sacTombstones, otherBuffer.sacTombstones, sacBalanceOrder, sacBalanceIsNoopRemove)
	mergeWithTombstone(b.lpShareChangesByKey, otherBuffer.lpShareChangesByKey, b.lpShareTombstones, otherBuffer.lpShareTombstones, lpShareOrder, lpShareIsNoopRemove)
	mergeWithTombstone(b.lpChangesByPoolID, otherBuffer.lpChangesByPoolID, b.lpTombstones, otherBuffer.lpTombstones, lpOrder, lpIsNoopRemove)

	// Merge unique trustline assets
	maps.Copy(b.uniqueTrustlineAssets, otherBuffer.uniqueTrustlineAssets)

	// Merge SAC contracts (first-write wins for deduplication)
	for id, contract := range otherBuffer.sacContractsByID {
		if _, exists := b.sacContractsByID[id]; !exists {
			b.sacContractsByID[id] = contract
		}
	}

	// Merge protocol WASMs (first-write wins for deduplication)
	for hash, wasm := range otherBuffer.protocolWasmsByHash {
		if _, exists := b.protocolWasmsByHash[hash]; !exists {
			b.protocolWasmsByHash[hash] = wasm
		}
	}

	// Merge wasm bytecodes (first-write wins; bytecode for a given hash is content-addressed and immutable)
	for hash, code := range otherBuffer.wasmBytecodesByHash {
		if _, exists := b.wasmBytecodesByHash[hash]; !exists {
			b.wasmBytecodesByHash[hash] = code
		}
	}

	// Merge protocol contracts (last-write-wins: otherBuffer has later ledger data)
	maps.Copy(b.protocolContractsByID, otherBuffer.protocolContractsByID)

	// Merge contract events (first-write wins: each (txIdx, opIdx) key is
	// produced by exactly one goroutine in ProcessLedgerTransactions, so
	// collisions don't occur in normal operation. First-write keeps merges
	// idempotent if a caller ever merges twice.)
	for key, events := range otherBuffer.contractEventsByKey {
		if _, exists := b.contractEventsByKey[key]; !exists {
			b.contractEventsByKey[key] = events
		}
	}
}

// Clear resets the buffer to its initial empty state while preserving allocated capacity.
// Use this to reuse the buffer after flushing data to the database during backfill.
// Thread-safe: acquires write lock.
func (b *IndexerBuffer) Clear() {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Clear maps (keep allocated backing arrays)
	clear(b.txByHash)
	clear(b.participantsByToID)
	clear(b.opByID)
	clear(b.participantsByOpID)
	clear(b.uniqueTrustlineAssets)
	clear(b.trustlineChangesByTrustlineKey)
	clear(b.sacContractsByID)
	clear(b.protocolWasmsByHash)
	clear(b.wasmBytecodesByHash)
	clear(b.protocolContractsByID)
	clear(b.contractEventsByKey)

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
}

// GetUniqueTrustlineAssets returns all unique trustline assets with pre-computed IDs.
// Thread-safe: uses read lock.
func (b *IndexerBuffer) GetUniqueTrustlineAssets() []data.TrustlineAsset {
	b.mu.RLock()
	defer b.mu.RUnlock()

	assets := make([]data.TrustlineAsset, 0, len(b.uniqueTrustlineAssets))
	for _, asset := range b.uniqueTrustlineAssets {
		assets = append(assets, asset)
	}
	return assets
}

// PushSACContract adds a SAC contract with extracted metadata to the buffer.
// Thread-safe: acquires write lock.
func (b *IndexerBuffer) PushSACContract(c *data.Contract) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.sacContractsByID[c.ContractID]; !exists {
		b.sacContractsByID[c.ContractID] = c
	}
}

// GetSACContracts returns a map of SAC contract IDs to their metadata.
// Thread-safe: uses read lock.
func (b *IndexerBuffer) GetSACContracts() map[string]*data.Contract {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return maps.Clone(b.sacContractsByID)
}

// PushProtocolWasm adds a protocol WASM record to the buffer (deduplicated by
// hash; first-write wins). The record's ProtocolID is left for the
// classification dispatcher to populate at persistence time.
// Thread-safe: acquires write lock.
func (b *IndexerBuffer) PushProtocolWasm(wasm data.ProtocolWasms) {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := string(wasm.WasmHash)
	if _, exists := b.protocolWasmsByHash[key]; !exists {
		b.protocolWasmsByHash[key] = wasm
	}
}

// PushProtocolWasmBytecode stores raw WASM bytecode keyed by hash. Used by the
// classification dispatcher in persistLedgerData to extract specs and run
// per-protocol validators. Bytecode is content-addressed by hash, so
// first-write wins is safe.
// Thread-safe: acquires write lock.
func (b *IndexerBuffer) PushProtocolWasmBytecode(wasmHash string, bytecode []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.wasmBytecodesByHash[wasmHash]; !exists {
		b.wasmBytecodesByHash[wasmHash] = bytecode
	}
}

// GetProtocolWasms returns a clone of the protocol WASMs map.
// Thread-safe: uses read lock.
func (b *IndexerBuffer) GetProtocolWasms() map[string]data.ProtocolWasms {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return maps.Clone(b.protocolWasmsByHash)
}

// GetProtocolWasmBytecodes returns a clone of the wasmHash → bytecode map.
// The map is a shallow copy: the returned []byte values alias the buffer's
// internal storage and MUST be treated as read-only by callers. Bytecode is
// content-addressed by wasmHash and immutable by construction; mutating a
// returned slice would corrupt the buffer's encapsulated state.
// Thread-safe: uses read lock.
func (b *IndexerBuffer) GetProtocolWasmBytecodes() map[string][]byte {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return maps.Clone(b.wasmBytecodesByHash)
}

// PushProtocolContracts adds a protocol contract to the buffer with deduplication (last-write-wins).
// Thread-safe: acquires write lock.
func (b *IndexerBuffer) PushProtocolContracts(contract data.ProtocolContracts) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.protocolContractsByID[string(contract.ContractID)] = contract
}

// GetProtocolContracts returns a clone of the protocol contracts map.
// Thread-safe: uses read lock.
func (b *IndexerBuffer) GetProtocolContracts() map[string]data.ProtocolContracts {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return maps.Clone(b.protocolContractsByID)
}

// PushContractEvents stashes the contract events emitted by a single
// InvokeHostFunction operation. The caller is expected to extract events
// once per (txIdx, opIdx) on successful transactions only — protocol
// processors consume from this map instead of re-decoding LedgerCloseMeta.
// First-write wins on key collisions (which should not occur under the
// indexer's parallel-per-tx split).
// Thread-safe: acquires write lock.
func (b *IndexerBuffer) PushContractEvents(key ContractEventKey, events []xdr.ContractEvent) {
	if len(events) == 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.contractEventsByKey[key]; !exists {
		b.contractEventsByKey[key] = events
	}
}

// GetContractEvents returns a shallow clone of the contract-events map.
// Event slices alias buffer-owned storage and MUST be treated as read-only.
// Thread-safe: uses read lock.
func (b *IndexerBuffer) GetContractEvents() map[ContractEventKey][]xdr.ContractEvent {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return maps.Clone(b.contractEventsByKey)
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
