// Package indexer provides high-performance data buffering for Stellar blockchain ingestion.
// IndexerBuffer uses a canonical pointer + set-based architecture to minimize memory usage
// and eliminate duplicate checks during transaction/operation processing.
package indexer

import (
	"maps"
	"sync"

	set "github.com/deckarep/golang-set/v2"

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
// 2. Participant Reference Layer:
//   - participantTxs: Maps each participant to a SET of transaction pointers
//   - participantOps: Maps each participant to a SET of operation pointers
//   - All pointers reference the canonical storage layer
//
// MEMORY OPTIMIZATION:
// Transaction structs contain large XDR fields (10-50+ KB each). When multiple participants
// interact with the same transaction, they all point to the SAME canonical pointer instead
// of storing duplicate copies. This reduces memory usage by 60-70% in production.
//
// PERFORMANCE:
// - Push operations: O(1) via set.Add() with automatic deduplication
// - No manual duplicate checking: Sets handle uniqueness automatically
// - MergeBuffer: O(n) with zero temporary map allocations
//
// THREAD SAFETY:
// All public methods use RWMutex for concurrent read/exclusive write access.
// Callers can safely use multiple buffers in parallel goroutines.
//
// USAGE PATTERN:
// 1. Create per-ledger or per-transaction buffers in parallel goroutines
// 2. Push data to individual buffers without lock contention
// 3. Sequentially merge all buffers into a single buffer
// 4. Batch insert merged data into database

type IndexerBuffer struct {
	mu             sync.RWMutex
	participants   set.Set[string]
	txByHash       map[string]*types.Transaction
	participantsByTxHash map[string]set.Set[string]
	opByID         map[int64]*types.Operation
	participantsByOpID map[int64]set.Set[string]
	stateChanges   []types.StateChange
}

// NewIndexerBuffer creates a new IndexerBuffer with initialized data structures.
// All maps and sets are pre-allocated to avoid nil pointer issues during concurrent access.
func NewIndexerBuffer() *IndexerBuffer {
	return &IndexerBuffer{
		participants:   set.NewSet[string](),
		txByHash:       make(map[string]*types.Transaction),
		participantsByTxHash: make(map[string]set.Set[string]),
		opByID:         make(map[int64]*types.Operation),
		participantsByOpID: make(map[int64]set.Set[string]),
		stateChanges:   make([]types.StateChange, 0),
	}
}

// PushTransaction adds a transaction for a specific participant.
// Uses canonical pointer pattern: if the transaction already exists (by hash), all participants
// reference the same pointer, avoiding memory duplication.
// Thread-safe: acquires write lock.
func (b *IndexerBuffer) PushTransaction(participant string, transaction types.Transaction) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.pushTransactionUnsafe(participant, &transaction)
}

// pushTransactionUnsafe is the internal implementation that assumes the caller
// already holds the write lock. This method implements the canonical pointer pattern:
//
// 1. Check if transaction already exists in txByHash (canonical storage)
// 2. If not, store the transaction pointer as canonical
// 3. Always use the canonical pointer when adding to participant's set
//
// This ensures all participants reference the SAME memory location for the same transaction.
// Caller must hold write lock.
func (b *IndexerBuffer) pushTransactionUnsafe(participant string, transaction *types.Transaction) {
	txHash := transaction.Hash
	if _, exists := b.txByHash[txHash]; !exists {
		b.txByHash[txHash] = transaction
	}

	// Track this participant globally
	b.participants.Add(participant) // O(1)
	if _, exists := b.participantsByTxHash[txHash]; !exists {
		b.participantsByTxHash[txHash] = set.NewSet[string]()
	}

	// Add participant - O(1) with automatic deduplication
	b.participantsByTxHash[txHash].Add(participant)
}

// GetNumberOfTransactions returns the count of unique transactions in the buffer.
// Thread-safe: uses read lock.
func (b *IndexerBuffer) GetNumberOfTransactions() int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return len(b.txByHash)
}

// GetAllTransactions returns all unique transactions from the canonical storage.
// Returns values (not pointers) for API compatibility.
// Thread-safe: uses read lock.
func (b *IndexerBuffer) GetAllTransactions() []types.Transaction {
	b.mu.RLock()
	defer b.mu.RUnlock()

	txs := make([]types.Transaction, 0, len(b.txByHash))
	for _, txPtr := range b.txByHash {
		txs = append(txs, *txPtr)
	}

	return txs
}

func (b *IndexerBuffer) GetAllTransactionsParticipants() map[string]set.Set[string] {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.participantsByTxHash
}

// PushOperation adds an operation for a specific participant along with
// its parent transaction. Uses canonical pointer pattern for both operations and transactions.
// Thread-safe: acquires write lock.
func (b *IndexerBuffer) PushOperation(participant string, operation types.Operation, transaction types.Transaction) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.pushOperationUnsafe(participant, &operation)
	b.pushTransactionUnsafe(participant, &transaction)
}

// GetAllOperations returns all unique operations from the canonical storage.
// Returns values (not pointers) for API compatibility.
// Thread-safe: uses read lock.
func (b *IndexerBuffer) GetAllOperations() []types.Operation {
	b.mu.RLock()
	defer b.mu.RUnlock()

	ops := make([]types.Operation, 0, len(b.opByID))
	for _, opPtr := range b.opByID {
		ops = append(ops, *opPtr)
	}
	return ops
}

func (b *IndexerBuffer) GetAllOperationsParticipants() map[int64]set.Set[string] {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.participantsByOpID
}

// pushOperationUnsafe is the internal implementation for operation storage.
// Follows the same canonical pointer pattern as transactions to minimize memory usage.
// Caller must hold write lock.
func (b *IndexerBuffer) pushOperationUnsafe(participant string, operation *types.Operation) {
	opID := operation.ID
	if _, exists := b.opByID[opID]; !exists {
		b.opByID[opID] = operation
	}

	// Track this participant globally
	b.participants.Add(participant) // O(1)
	if _, exists := b.participantsByOpID[opID]; !exists {
		b.participantsByOpID[opID] = set.NewSet[string]()
	}

	// Add participant - O(1) with automatic deduplication
	b.participantsByOpID[opID].Add(participant)
}

// PushStateChange adds a state change along with its associated transaction and operation.
// State changes represent balance/asset modifications and are linked to the account that changed.
// Thread-safe: acquires write lock.
func (b *IndexerBuffer) PushStateChange(transaction types.Transaction, operation types.Operation, stateChange types.StateChange) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.stateChanges = append(b.stateChanges, stateChange)
	b.pushTransactionUnsafe(stateChange.AccountID, &transaction)
	if stateChange.OperationID != 0 {
		b.pushOperationUnsafe(stateChange.AccountID, &operation)
	}
}

// GetAllStateChanges returns a copy of all state changes stored in the buffer.
// Returns a copy to prevent external mutation of the internal slice.
// Thread-safe: uses read lock.
func (b *IndexerBuffer) GetAllStateChanges() []types.StateChange {
	b.mu.RLock()
	defer b.mu.RUnlock()

	// Return a copy to prevent external mutation
	stateChangesCopy := make([]types.StateChange, len(b.stateChanges))
	copy(stateChangesCopy, b.stateChanges)
	return stateChangesCopy
}

// MergeBuffer merges another IndexerBuffer into this buffer. This is used to combine
// per-ledger or per-transaction buffers into a single buffer for batch DB insertion.
//
// MERGE STRATEGY:
// 1. Union participant sets (O(m) set operation)
// 2. Copy canonical storage maps (txByHash, opByID) - overwrites on collision
// 3. For each participant in other buffer:
//   - Iterate their transaction/operation sets
//   - Retrieve OUR canonical pointer using hash/ID
//   - Add canonical pointer to our participant's set (O(1), auto-deduplicates)
//
// CANONICAL POINTER RECONCILIATION:
// After maps.Copy, our txByHash contains all transactions from both buffers.
// When iterating other's participant sets, we ALWAYS use OUR canonical pointer
// from txByHash[hash], ensuring all participants reference the same memory location.
//
// MEMORY EFFICIENCY:
// Zero temporary map allocations - all operations use direct map/set manipulation.
//
// Thread-safe: acquires write lock on this buffer, read lock on other buffer.
func (b *IndexerBuffer) MergeBuffer(other IndexerBufferInterface) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Type assert to get concrete buffer for efficient merging
	otherBuffer, ok := other.(*IndexerBuffer)
	if !ok {
		return
	}

	otherBuffer.mu.RLock()
	defer otherBuffer.mu.RUnlock()

	// Merge participants - O(m) with set Union
	b.participants = b.participants.Union(otherBuffer.participants)

	// Merge transactions (canonical storage) - this establishes our canonical pointers
	maps.Copy(b.txByHash, otherBuffer.txByHash)
	for txHash, otherParticipants := range otherBuffer.participantsByTxHash {
		if _, exists := b.participantsByTxHash[txHash]; !exists {
			b.participantsByTxHash[txHash] = set.NewSet[string]()
		}
		// Iterate other's set, add participants from OUR txByHash
		for participant := range otherParticipants.Iter() {
			b.participantsByTxHash[txHash].Add(participant) // O(1) Add
		}
	}

	// Merge operations (canonical storage)
	maps.Copy(b.opByID, otherBuffer.opByID)
	for opID, otherParticipants := range otherBuffer.participantsByOpID {
		if _, exists := b.participantsByOpID[opID]; !exists {
			b.participantsByOpID[opID] = set.NewSet[string]()
		}
		// Iterate other's set, add canonical pointers from OUR opByID
		for participant := range otherParticipants.Iter() {
			b.participantsByOpID[opID].Add(participant) // O(1) Add
		}
	}

	// Merge state changes
	b.stateChanges = append(b.stateChanges, otherBuffer.stateChanges...)
}
