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

type IndexerBuffer struct {
	mu                             sync.RWMutex
	txByHash                       map[string]*types.Transaction
	participantsByToID             map[int64]set.Set[string]
	opByID                         map[int64]*types.Operation
	participantsByOpID             map[int64]set.Set[string]
	stateChanges                   []types.StateChange
	trustlineChangesByTrustlineKey map[TrustlineChangeKey]types.TrustlineChange
	contractChanges                []types.ContractChange
	accountChangesByAccountID      map[string]types.AccountChange
	sacBalanceChangesByKey         map[SACBalanceChangeKey]types.SACBalanceChange
	allParticipants                set.Set[string]
	uniqueTrustlineAssets          map[uuid.UUID]data.TrustlineAsset
	uniqueSEP41ContractTokensByID  map[string]types.ContractType // contractID → type (SEP-41 only)
	sacContractsByID               map[string]*data.Contract     // SAC contract metadata extracted from instance entries
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
		contractChanges:                make([]types.ContractChange, 0),
		accountChangesByAccountID:      make(map[string]types.AccountChange),
		sacBalanceChangesByKey:         make(map[SACBalanceChangeKey]types.SACBalanceChange),
		allParticipants:                set.NewSet[string](),
		uniqueTrustlineAssets:          make(map[uuid.UUID]data.TrustlineAsset),
		uniqueSEP41ContractTokensByID:  make(map[string]types.ContractType),
		sacContractsByID:               make(map[string]*data.Contract),
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
	txHash := transaction.Hash
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

	// Track participant in global set for batch account insertion
	if participant != "" {
		b.allParticipants.Add(participant)
	}
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

// GetTransactionsParticipants returns a map of transaction ToIDs to its participants.
func (b *IndexerBuffer) GetTransactionsParticipants() map[int64]set.Set[string] {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.participantsByToID
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
	prevChange, exists := b.trustlineChangesByTrustlineKey[changeKey]
	if exists && prevChange.OperationID > trustlineChange.OperationID {
		return
	}

	// Handle ADD→REMOVE no-op case: if this is a remove operation and we have an add operation for the same trustline from previous operation,
	// it is a no-op for current ledger.
	if exists && trustlineChange.Operation == types.TrustlineOpRemove && prevChange.Operation == types.TrustlineOpAdd {
		delete(b.trustlineChangesByTrustlineKey, changeKey)
		return
	}

	b.trustlineChangesByTrustlineKey[changeKey] = trustlineChange
}

// GetTrustlineChanges returns all trustline changes stored in the buffer.
// Thread-safe: uses read lock.
func (b *IndexerBuffer) GetTrustlineChanges() map[TrustlineChangeKey]types.TrustlineChange {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.trustlineChangesByTrustlineKey
}

// PushContractChange adds a contract change to the buffer and tracks unique SEP-41 contracts.
// Thread-safe: acquires write lock.
func (b *IndexerBuffer) PushContractChange(contractChange types.ContractChange) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.contractChanges = append(b.contractChanges, contractChange)

	// Only track SEP-41 contracts for DB insertion
	if contractChange.ContractType != types.ContractTypeSEP41 {
		return
	}
	if contractChange.ContractID == "" {
		return
	}
	if _, exists := b.uniqueSEP41ContractTokensByID[contractChange.ContractID]; !exists {
		b.uniqueSEP41ContractTokensByID[contractChange.ContractID] = contractChange.ContractType
	}
}

// GetContractChanges returns all contract changes stored in the buffer.
// Thread-safe: uses read lock.
func (b *IndexerBuffer) GetContractChanges() []types.ContractChange {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.contractChanges
}

// PushAccountChange adds an account change to the buffer with deduplication.
// Keeps the change with highest OperationID per account. Handles CREATE→REMOVE no-op case.
// Thread-safe: acquires write lock.
func (b *IndexerBuffer) PushAccountChange(accountChange types.AccountChange) {
	b.mu.Lock()
	defer b.mu.Unlock()

	accountID := accountChange.AccountID
	existing, exists := b.accountChangesByAccountID[accountID]

	// Keep the change with highest OperationID
	if exists && existing.OperationID > accountChange.OperationID {
		return
	}

	// Handle CREATE→REMOVE no-op case: account created and removed in same batch
	// Note: UPDATE→REMOVE is NOT a no-op (account existed before, needs deletion)
	if exists && accountChange.Operation == types.AccountOpRemove && existing.Operation == types.AccountOpCreate {
		delete(b.accountChangesByAccountID, accountID)
		return
	}

	b.accountChangesByAccountID[accountID] = accountChange
}

// GetAccountChanges returns all account changes stored in the buffer.
// Thread-safe: uses read lock.
func (b *IndexerBuffer) GetAccountChanges() map[string]types.AccountChange {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.accountChangesByAccountID
}

// PushSACBalanceChange adds a SAC balance change to the buffer with deduplication.
// Keeps the change with highest OperationID per (AccountID, ContractID). Handles ADD→REMOVE no-op case.
// Thread-safe: acquires write lock.
func (b *IndexerBuffer) PushSACBalanceChange(sacBalanceChange types.SACBalanceChange) {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := SACBalanceChangeKey{
		AccountID:  sacBalanceChange.AccountID,
		ContractID: sacBalanceChange.ContractID,
	}
	existing, exists := b.sacBalanceChangesByKey[key]

	// Keep the change with highest OperationID
	if exists && existing.OperationID > sacBalanceChange.OperationID {
		return
	}

	// Handle ADD→REMOVE no-op case: balance created and removed in same batch
	if exists && sacBalanceChange.Operation == types.SACBalanceOpRemove && existing.Operation == types.SACBalanceOpAdd {
		delete(b.sacBalanceChangesByKey, key)
		return
	}

	b.sacBalanceChangesByKey[key] = sacBalanceChange
}

// GetSACBalanceChanges returns all SAC balance changes stored in the buffer.
// Thread-safe: uses read lock.
func (b *IndexerBuffer) GetSACBalanceChanges() map[SACBalanceChangeKey]types.SACBalanceChange {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.sacBalanceChangesByKey
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

// GetOperationsParticipants returns a map of operation IDs to its participants.
func (b *IndexerBuffer) GetOperationsParticipants() map[int64]set.Set[string] {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.participantsByOpID
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

	// Track participant in global set for batch account insertion
	if participant != "" {
		b.allParticipants.Add(participant)
	}
}

// PushStateChange adds a state change along with its associated transaction and operation.
// Thread-safe: acquires write lock.
func (b *IndexerBuffer) PushStateChange(transaction types.Transaction, operation types.Operation, stateChange types.StateChange) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.stateChanges = append(b.stateChanges, stateChange)
	b.pushTransactionUnsafe(stateChange.AccountID, &transaction)
	// Fee changes dont have an operation ID associated with them
	if stateChange.OperationID != 0 {
		b.pushOperationUnsafe(stateChange.AccountID, &operation)
	}
}

// GetStateChanges returns a copy of all state changes stored in the buffer.
// Thread-safe: uses read lock.
func (b *IndexerBuffer) GetStateChanges() []types.StateChange {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.stateChanges
}

// GetAllParticipants returns all unique participants (Stellar addresses) that have been
// recorded during transaction, operation, and state change processing.
// Thread-safe: uses read lock.
func (b *IndexerBuffer) GetAllParticipants() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.allParticipants.ToSlice()
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

	// Merge trustline changes
	for key, change := range otherBuffer.trustlineChangesByTrustlineKey {
		existing, exists := b.trustlineChangesByTrustlineKey[key]

		if exists && existing.OperationID > change.OperationID {
			continue
		}

		// Handle ADD→REMOVE no-op case
		if exists && change.Operation == types.TrustlineOpRemove && existing.Operation == types.TrustlineOpAdd {
			delete(b.trustlineChangesByTrustlineKey, key)
			continue
		}

		b.trustlineChangesByTrustlineKey[key] = change
	}

	// Merge contract changes
	b.contractChanges = append(b.contractChanges, otherBuffer.contractChanges...)

	// Merge account changes with deduplication (same logic as PushAccountChange)
	for accountID, change := range otherBuffer.accountChangesByAccountID {
		existing, exists := b.accountChangesByAccountID[accountID]

		if exists && existing.OperationID > change.OperationID {
			continue
		}

		// Handle CREATE→REMOVE no-op case
		if exists && change.Operation == types.AccountOpRemove && existing.Operation == types.AccountOpCreate {
			delete(b.accountChangesByAccountID, accountID)
			continue
		}

		b.accountChangesByAccountID[accountID] = change
	}

	// Merge SAC balance changes with deduplication (same logic as PushSACBalanceChange)
	for key, change := range otherBuffer.sacBalanceChangesByKey {
		existing, exists := b.sacBalanceChangesByKey[key]

		if exists && existing.OperationID > change.OperationID {
			continue
		}

		// Handle ADD→REMOVE no-op case
		if exists && change.Operation == types.SACBalanceOpRemove && existing.Operation == types.SACBalanceOpAdd {
			delete(b.sacBalanceChangesByKey, key)
			continue
		}

		b.sacBalanceChangesByKey[key] = change
	}

	// Merge all participants
	for participant := range otherBuffer.allParticipants.Iter() {
		b.allParticipants.Add(participant)
	}

	// Merge unique trustline assets
	maps.Copy(b.uniqueTrustlineAssets, otherBuffer.uniqueTrustlineAssets)

	// Merge unique contracts
	maps.Copy(b.uniqueSEP41ContractTokensByID, otherBuffer.uniqueSEP41ContractTokensByID)

	// Merge SAC contracts (first-write wins for deduplication)
	for id, contract := range otherBuffer.sacContractsByID {
		if _, exists := b.sacContractsByID[id]; !exists {
			b.sacContractsByID[id] = contract
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
	clear(b.uniqueSEP41ContractTokensByID)
	clear(b.trustlineChangesByTrustlineKey)
	clear(b.sacContractsByID)

	// Reset slices (reuse underlying arrays by slicing to zero)
	b.stateChanges = b.stateChanges[:0]
	b.contractChanges = b.contractChanges[:0]

	// Clear account and SAC balance changes maps
	clear(b.accountChangesByAccountID)
	clear(b.sacBalanceChangesByKey)

	// Clear all participants set
	b.allParticipants.Clear()
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

// GetUniqueSEP41ContractTokensByID returns a map of unique SEP-41 contract IDs to their types.
// Thread-safe: uses read lock.
func (b *IndexerBuffer) GetUniqueSEP41ContractTokensByID() map[string]types.ContractType {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return maps.Clone(b.uniqueSEP41ContractTokensByID)
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
