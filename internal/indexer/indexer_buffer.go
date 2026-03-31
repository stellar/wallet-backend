// Package indexer provides high-performance data buffering for Stellar blockchain ingestion.
// IndexerBuffer uses a canonical pointer + set-based architecture to minimize memory usage
// and eliminate duplicate checks during transaction/operation processing.
package indexer

import (
	"fmt"
	"maps"
	"strings"

	set "github.com/deckarep/golang-set/v2"
	"github.com/google/uuid"
	"github.com/stellar/go-stellar-sdk/txnbuild"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer/types"
)

// IndexerBuffer is a memory-efficient buffer for collecting blockchain data during ledger
// ingestion. It uses a two-level storage architecture:
//
// ARCHITECTURE:
// 1. Canonical Storage Layer:
//   - txByHash: Single pointer per unique transaction (keyed by hash)
//   - opByID: Single pointer per unique operation (keyed by ID)
//   - This layer owns the actual data and ensures only ONE copy exists in memory
//
// 2. Transaction/Operation to Participants Mapping Layer:
//   - participantsByToID: Maps each transaction ToID to a SET of participant IDs
//   - participantsByOpID: Maps each operation ID to a SET of participant IDs
//   - Efficiently tracks which participants interacted with each tx/op
//
// THREAD SAFETY:
// IndexerBuffer is NOT thread-safe. The architecture guarantees single-goroutine access:
//   - Per-transaction buffers (from sync.Pool): one goroutine fills, sequential merge after group.Wait()
//   - Ledger buffers (live/backfill): owned by a single goroutine throughout their lifecycle
//   - Merge phase: runs sequentially after parallel work completes

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
	txByHash                       map[string]*types.Transaction
	participantsByToID             map[int64]set.Set[string]
	opByID                         map[int64]*types.Operation
	participantsByOpID             map[int64]set.Set[string]
	stateChanges                   []types.StateChange
	trustlineChangesByTrustlineKey map[TrustlineChangeKey]types.TrustlineChange
	contractChanges                []types.ContractChange
	accountChangesByAccountID      map[string]types.AccountChange
	sacBalanceChangesByKey         map[SACBalanceChangeKey]types.SACBalanceChange
	uniqueTrustlineAssets          map[uuid.UUID]data.TrustlineAsset
	uniqueSEP41ContractTokensByID  map[string]types.ContractType // contractID → type (SEP-41 only)
	sacContractsByID               map[string]*data.Contract     // SAC contract metadata extracted from instance entries
}

// NewIndexerBuffer creates a new IndexerBuffer with initialized data structures.
// All maps and sets are pre-allocated to avoid nil pointer issues.
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
		uniqueTrustlineAssets:          make(map[uuid.UUID]data.TrustlineAsset),
		uniqueSEP41ContractTokensByID:  make(map[string]types.ContractType),
		sacContractsByID:               make(map[string]*data.Contract),
	}
}

// PushTransaction adds a transaction and associates it with a participant.
// Uses canonical pointer pattern: stores one copy of each transaction (by hash) and tracks
// which participants interacted with it.
func (b *IndexerBuffer) PushTransaction(participant string, transaction types.Transaction) {
	txHash := transaction.Hash.String()
	if _, exists := b.txByHash[txHash]; !exists {
		b.txByHash[txHash] = &transaction
	}

	toID := transaction.ToID
	if _, exists := b.participantsByToID[toID]; !exists {
		b.participantsByToID[toID] = set.NewSet[string]()
	}
	b.participantsByToID[toID].Add(participant)
}

// pushTransaction is the internal variant that accepts a pointer to avoid copying.
// Used by PushOperation and PushStateChange which already have a canonical pointer.
func (b *IndexerBuffer) pushTransaction(participant string, transaction *types.Transaction) {
	txHash := transaction.Hash.String()
	if _, exists := b.txByHash[txHash]; !exists {
		b.txByHash[txHash] = transaction
	}

	toID := transaction.ToID
	if _, exists := b.participantsByToID[toID]; !exists {
		b.participantsByToID[toID] = set.NewSet[string]()
	}
	b.participantsByToID[toID].Add(participant)
}

// GetNumberOfTransactions returns the count of unique transactions in the buffer.
func (b *IndexerBuffer) GetNumberOfTransactions() int {
	return len(b.txByHash)
}

// GetNumberOfOperations returns the count of unique operations in the buffer.
func (b *IndexerBuffer) GetNumberOfOperations() int {
	return len(b.opByID)
}

// GetTransactions returns all unique transactions.
func (b *IndexerBuffer) GetTransactions() []*types.Transaction {
	txs := make([]*types.Transaction, 0, len(b.txByHash))
	for _, txPtr := range b.txByHash {
		txs = append(txs, txPtr)
	}
	return txs
}

// GetTransactionsParticipants returns a map of transaction ToIDs to its participants.
// The returned map is a direct reference — callers must not mutate it.
func (b *IndexerBuffer) GetTransactionsParticipants() map[int64]set.Set[string] {
	return b.participantsByToID
}

// PushTrustlineChange adds a trustline change to the buffer and tracks unique assets.
func (b *IndexerBuffer) PushTrustlineChange(trustlineChange types.TrustlineChange) {
	b.pushTrustlineChange(trustlineChange)
}

func (b *IndexerBuffer) pushTrustlineChange(trustlineChange types.TrustlineChange) {
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

	// Handle ADD→REMOVE no-op case: if this is a remove operation and we have an add operation
	// for the same trustline from previous operation, it is a no-op for current ledger.
	if exists && trustlineChange.Operation == types.TrustlineOpRemove && prevChange.Operation == types.TrustlineOpAdd {
		delete(b.trustlineChangesByTrustlineKey, changeKey)
		return
	}

	b.trustlineChangesByTrustlineKey[changeKey] = trustlineChange
}

// GetTrustlineChanges returns all trustline changes stored in the buffer.
func (b *IndexerBuffer) GetTrustlineChanges() map[TrustlineChangeKey]types.TrustlineChange {
	return b.trustlineChangesByTrustlineKey
}

// PushContractChange adds a contract change to the buffer and tracks unique SEP-41 contracts.
func (b *IndexerBuffer) PushContractChange(contractChange types.ContractChange) {
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
func (b *IndexerBuffer) GetContractChanges() []types.ContractChange {
	return b.contractChanges
}

// pushAccountChange adds an account change with deduplication.
// Keeps the change with highest OperationID per account. Handles CREATE→REMOVE no-op case.
func (b *IndexerBuffer) pushAccountChange(accountChange types.AccountChange) {
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
func (b *IndexerBuffer) GetAccountChanges() map[string]types.AccountChange {
	return b.accountChangesByAccountID
}

// pushSACBalanceChange adds a SAC balance change with deduplication.
// Keeps the change with highest OperationID per (AccountID, ContractID). Handles ADD→REMOVE no-op case.
func (b *IndexerBuffer) pushSACBalanceChange(sacBalanceChange types.SACBalanceChange) {
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
func (b *IndexerBuffer) GetSACBalanceChanges() map[SACBalanceChangeKey]types.SACBalanceChange {
	return b.sacBalanceChangesByKey
}

// PushOperation adds an operation and its parent transaction, associating both with a participant.
// Uses canonical pointer pattern for both operations and transactions to avoid memory duplication.
func (b *IndexerBuffer) PushOperation(participant string, operation types.Operation, transaction types.Transaction) {
	b.pushOperation(participant, &operation)
	b.pushTransaction(participant, &transaction)
}

// GetOperations returns all unique operations from the canonical storage.
func (b *IndexerBuffer) GetOperations() []*types.Operation {
	ops := make([]*types.Operation, 0, len(b.opByID))
	for _, opPtr := range b.opByID {
		ops = append(ops, opPtr)
	}
	return ops
}

// GetOperationsParticipants returns a map of operation IDs to its participants.
// The returned map is a direct reference — callers must not mutate it.
func (b *IndexerBuffer) GetOperationsParticipants() map[int64]set.Set[string] {
	return b.participantsByOpID
}

// pushOperation stores one copy of each operation (by ID) and tracks participants.
func (b *IndexerBuffer) pushOperation(participant string, operation *types.Operation) {
	opID := operation.ID
	if _, exists := b.opByID[opID]; !exists {
		b.opByID[opID] = operation
	}

	if _, exists := b.participantsByOpID[opID]; !exists {
		b.participantsByOpID[opID] = set.NewSet[string]()
	}
	b.participantsByOpID[opID].Add(participant)
}

// PushStateChange adds a state change along with its associated transaction and operation.
func (b *IndexerBuffer) PushStateChange(transaction types.Transaction, operation types.Operation, stateChange types.StateChange) {
	b.stateChanges = append(b.stateChanges, stateChange)
	b.pushTransaction(string(stateChange.AccountID), &transaction)
	// Fee changes dont have an operation ID associated with them
	if stateChange.OperationID != 0 {
		b.pushOperation(string(stateChange.AccountID), &operation)
	}
}

// GetStateChanges returns all state changes stored in the buffer.
func (b *IndexerBuffer) GetStateChanges() []types.StateChange {
	return b.stateChanges
}

// Merge merges another IndexerBuffer into this buffer. This is used to combine
// per-ledger or per-transaction buffers into a single buffer for batch DB insertion.
//
// MERGE STRATEGY:
// 1. Copy storage maps (txByHash, opByID) using maps.Copy
// 2. For each transaction ToID in other.participantsByToID:
//   - Merge other's participant set into our participant set
//   - Creates new set if tx doesn't exist in our mapping yet
//
// 3. For each operation ID in other.participantsByOpID:
//   - Same merge strategy as transactions
//
// 4. Append slices (state changes, contract changes)
// 5. Deduplicate map-based changes (trustline, account, SAC balance)
func (b *IndexerBuffer) Merge(other IndexerBufferInterface) {
	otherBuffer, ok := other.(*IndexerBuffer)
	if !ok {
		return
	}

	// Merge transactions (canonical storage)
	maps.Copy(b.txByHash, otherBuffer.txByHash)
	for toID, otherParticipants := range otherBuffer.participantsByToID {
		if existing, exists := b.participantsByToID[toID]; exists {
			for participant := range otherParticipants.Iter() {
				existing.Add(participant)
			}
		} else {
			b.participantsByToID[toID] = otherParticipants.Clone()
		}
	}

	// Merge operations (canonical storage)
	maps.Copy(b.opByID, otherBuffer.opByID)
	for opID, otherParticipants := range otherBuffer.participantsByOpID {
		if existing, exists := b.participantsByOpID[opID]; exists {
			for participant := range otherParticipants.Iter() {
				existing.Add(participant)
			}
		} else {
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

	// Merge account changes with deduplication
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

	// Merge SAC balance changes with deduplication
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
func (b *IndexerBuffer) Clear() {
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
}

// GetUniqueTrustlineAssets returns all unique trustline assets with pre-computed IDs.
func (b *IndexerBuffer) GetUniqueTrustlineAssets() []data.TrustlineAsset {
	assets := make([]data.TrustlineAsset, 0, len(b.uniqueTrustlineAssets))
	for _, asset := range b.uniqueTrustlineAssets {
		assets = append(assets, asset)
	}
	return assets
}

// GetUniqueSEP41ContractTokensByID returns a map of unique SEP-41 contract IDs to their types.
// The returned map is a direct reference — callers must not mutate it.
func (b *IndexerBuffer) GetUniqueSEP41ContractTokensByID() map[string]types.ContractType {
	return b.uniqueSEP41ContractTokensByID
}

// pushSACContract adds a SAC contract with extracted metadata.
func (b *IndexerBuffer) pushSACContract(c *data.Contract) {
	if _, exists := b.sacContractsByID[c.ContractID]; !exists {
		b.sacContractsByID[c.ContractID] = c
	}
}

// BatchPushChanges pushes trustline, account, SAC balance, and SAC contract changes
// in a single call, reducing method call overhead.
func (b *IndexerBuffer) BatchPushChanges(
	trustlines []types.TrustlineChange,
	accounts []types.AccountChange,
	sacBalances []types.SACBalanceChange,
	sacContracts []*data.Contract,
) {
	for i := range trustlines {
		b.pushTrustlineChange(trustlines[i])
	}
	for i := range accounts {
		b.pushAccountChange(accounts[i])
	}
	for i := range sacBalances {
		b.pushSACBalanceChange(sacBalances[i])
	}
	for i := range sacContracts {
		b.pushSACContract(sacContracts[i])
	}
}

// GetSACContracts returns a map of SAC contract IDs to their metadata.
// The returned map is a direct reference — callers must not mutate it.
func (b *IndexerBuffer) GetSACContracts() map[string]*data.Contract {
	return b.sacContractsByID
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
