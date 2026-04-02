// Package indexer provides high-performance data buffering for Stellar blockchain ingestion.
// IndexerBuffer uses a canonical pointer + set-based architecture to minimize memory usage
// and eliminate duplicate checks during transaction/operation processing.
package indexer

import (
	"fmt"
	"strings"
	"sync"

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
// Push methods are protected by a sync.Mutex for concurrent writes during parallel
// transaction processing. Get methods are NOT locked — they are only called after
// all parallel processing completes (after group.Wait()).

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
	mu                             sync.Mutex
	txByHash                       map[string]*types.Transaction
	participantsByToID             map[int64]types.ParticipantSet
	opByID                         map[int64]*types.Operation
	participantsByOpID             map[int64]types.ParticipantSet
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
		participantsByToID:             make(map[int64]types.ParticipantSet),
		opByID:                         make(map[int64]*types.Operation),
		participantsByOpID:             make(map[int64]types.ParticipantSet),
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
// which participants interacted with it. Thread-safe.
func (b *IndexerBuffer) PushTransaction(participant string, transaction *types.Transaction) {
	b.mu.Lock()
	defer b.mu.Unlock()

	txHash := transaction.Hash.String()
	if _, exists := b.txByHash[txHash]; !exists {
		b.txByHash[txHash] = transaction
	}

	toID := transaction.ToID
	if _, exists := b.participantsByToID[toID]; !exists {
		b.participantsByToID[toID] = make(types.ParticipantSet)
	}
	b.participantsByToID[toID].Add(participant)
}

// PushOperation adds an operation and its parent transaction, associating both with a participant.
// Thread-safe.
func (b *IndexerBuffer) PushOperation(participant string, operation *types.Operation, transaction *types.Transaction) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.pushOperation(participant, operation)
	b.pushTransaction(participant, transaction)
}

// PushStateChange adds a state change along with its associated transaction and operation.
// Thread-safe.
func (b *IndexerBuffer) PushStateChange(transaction *types.Transaction, operation *types.Operation, stateChange types.StateChange) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.stateChanges = append(b.stateChanges, stateChange)
	b.pushTransaction(string(stateChange.AccountID), transaction)
	// Fee changes dont have an operation ID associated with them
	if stateChange.OperationID != 0 {
		b.pushOperation(string(stateChange.AccountID), operation)
	}
}

// PushTrustlineChange adds a trustline change to the buffer and tracks unique assets.
// Thread-safe.
func (b *IndexerBuffer) PushTrustlineChange(trustlineChange types.TrustlineChange) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.pushTrustlineChange(trustlineChange)
}

// PushContractChange adds a contract change to the buffer and tracks unique SEP-41 contracts.
// Thread-safe.
func (b *IndexerBuffer) PushContractChange(contractChange types.ContractChange) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.contractChanges = append(b.contractChanges, contractChange)

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

// BatchPushChanges pushes trustline, account, SAC balance, and SAC contract changes
// in a single lock acquisition. Thread-safe.
func (b *IndexerBuffer) BatchPushChanges(
	trustlines []types.TrustlineChange,
	accounts []types.AccountChange,
	sacBalances []types.SACBalanceChange,
	sacContracts []*data.Contract,
) {
	b.mu.Lock()
	defer b.mu.Unlock()

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

// TransactionResult holds all the data collected from processing a single transaction,
// ready to be pushed into the buffer in a single lock acquisition.
type TransactionResult struct {
	Transaction      *types.Transaction
	TxParticipants   []string
	Operations       map[int64]*types.Operation // opID → operation
	OpParticipants   map[int64][]string         // opID → participant list
	ContractChanges  []types.ContractChange
	StateChanges     []types.StateChange
	StateChangeOpMap map[int64]*types.Operation // opID → operation for state change association
}

// BatchPushTransactionResult pushes an entire transaction's worth of data into the buffer
// in a single lock acquisition. This reduces mutex contention from ~5-15 acquisitions
// per transaction down to 1. Thread-safe.
func (b *IndexerBuffer) BatchPushTransactionResult(result *TransactionResult) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Push transaction participants
	for _, participant := range result.TxParticipants {
		b.pushTransaction(participant, result.Transaction)
	}

	// Push operations and their participants
	for opID, participants := range result.OpParticipants {
		op := result.Operations[opID]
		for _, participant := range participants {
			b.pushOperation(participant, op)
			b.pushTransaction(participant, result.Transaction)
		}
	}

	// Push contract changes
	for _, cc := range result.ContractChanges {
		b.contractChanges = append(b.contractChanges, cc)
		if cc.ContractType == types.ContractTypeSEP41 && cc.ContractID != "" {
			if _, exists := b.uniqueSEP41ContractTokensByID[cc.ContractID]; !exists {
				b.uniqueSEP41ContractTokensByID[cc.ContractID] = cc.ContractType
			}
		}
	}

	// Push state changes
	for _, sc := range result.StateChanges {
		if sc.AccountID == "" {
			continue
		}
		b.stateChanges = append(b.stateChanges, sc)
		b.pushTransaction(string(sc.AccountID), result.Transaction)
		if sc.OperationID != 0 {
			if op := result.StateChangeOpMap[sc.OperationID]; op != nil {
				b.pushOperation(string(sc.AccountID), op)
			}
		}
	}
}

// Clear resets the buffer to its initial empty state while preserving allocated capacity.
// Used by backfill to reuse the buffer after flushing data to the database.
func (b *IndexerBuffer) Clear() {
	clear(b.txByHash)
	clear(b.participantsByToID)
	clear(b.opByID)
	clear(b.participantsByOpID)
	clear(b.uniqueTrustlineAssets)
	clear(b.uniqueSEP41ContractTokensByID)
	clear(b.trustlineChangesByTrustlineKey)
	clear(b.sacContractsByID)
	b.stateChanges = b.stateChanges[:0]
	b.contractChanges = b.contractChanges[:0]
	clear(b.accountChangesByAccountID)
	clear(b.sacBalanceChangesByKey)
}

// --- Unlocked getters (called only after parallel processing completes) ---

func (b *IndexerBuffer) GetNumberOfTransactions() int               { return len(b.txByHash) }
func (b *IndexerBuffer) GetNumberOfOperations() int                 { return len(b.opByID) }
func (b *IndexerBuffer) GetStateChanges() []types.StateChange       { return b.stateChanges }
func (b *IndexerBuffer) GetContractChanges() []types.ContractChange { return b.contractChanges }
func (b *IndexerBuffer) GetAccountChanges() map[string]types.AccountChange {
	return b.accountChangesByAccountID
}

func (b *IndexerBuffer) GetSACBalanceChanges() map[SACBalanceChangeKey]types.SACBalanceChange {
	return b.sacBalanceChangesByKey
}

func (b *IndexerBuffer) GetTransactionsParticipants() map[int64]types.ParticipantSet {
	return b.participantsByToID
}

func (b *IndexerBuffer) GetOperationsParticipants() map[int64]types.ParticipantSet {
	return b.participantsByOpID
}

func (b *IndexerBuffer) GetTrustlineChanges() map[TrustlineChangeKey]types.TrustlineChange {
	return b.trustlineChangesByTrustlineKey
}

func (b *IndexerBuffer) GetUniqueSEP41ContractTokensByID() map[string]types.ContractType {
	return b.uniqueSEP41ContractTokensByID
}
func (b *IndexerBuffer) GetSACContracts() map[string]*data.Contract { return b.sacContractsByID }

func (b *IndexerBuffer) GetTransactions() []*types.Transaction {
	txs := make([]*types.Transaction, 0, len(b.txByHash))
	for _, txPtr := range b.txByHash {
		txs = append(txs, txPtr)
	}
	return txs
}

func (b *IndexerBuffer) GetOperations() []*types.Operation {
	ops := make([]*types.Operation, 0, len(b.opByID))
	for _, opPtr := range b.opByID {
		ops = append(ops, opPtr)
	}
	return ops
}

func (b *IndexerBuffer) GetUniqueTrustlineAssets() []data.TrustlineAsset {
	assets := make([]data.TrustlineAsset, 0, len(b.uniqueTrustlineAssets))
	for _, asset := range b.uniqueTrustlineAssets {
		assets = append(assets, asset)
	}
	return assets
}

// --- Internal helpers (caller must hold the lock) ---

func (b *IndexerBuffer) pushTransaction(participant string, transaction *types.Transaction) {
	txHash := transaction.Hash.String()
	if _, exists := b.txByHash[txHash]; !exists {
		b.txByHash[txHash] = transaction
	}
	toID := transaction.ToID
	if _, exists := b.participantsByToID[toID]; !exists {
		b.participantsByToID[toID] = make(types.ParticipantSet)
	}
	b.participantsByToID[toID].Add(participant)
}

func (b *IndexerBuffer) pushOperation(participant string, operation *types.Operation) {
	opID := operation.ID
	if _, exists := b.opByID[opID]; !exists {
		b.opByID[opID] = operation
	}
	if _, exists := b.participantsByOpID[opID]; !exists {
		b.participantsByOpID[opID] = make(types.ParticipantSet)
	}
	b.participantsByOpID[opID].Add(participant)
}

func (b *IndexerBuffer) pushTrustlineChange(trustlineChange types.TrustlineChange) {
	code, issuer, err := ParseAssetString(trustlineChange.Asset)
	if err != nil {
		return
	}
	trustlineID := data.DeterministicAssetID(code, issuer)

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

	if exists && trustlineChange.Operation == types.TrustlineOpRemove && prevChange.Operation == types.TrustlineOpAdd {
		delete(b.trustlineChangesByTrustlineKey, changeKey)
		return
	}

	b.trustlineChangesByTrustlineKey[changeKey] = trustlineChange
}

func (b *IndexerBuffer) pushAccountChange(accountChange types.AccountChange) {
	accountID := accountChange.AccountID
	existing, exists := b.accountChangesByAccountID[accountID]

	if exists && existing.OperationID > accountChange.OperationID {
		return
	}

	if exists && accountChange.Operation == types.AccountOpRemove && existing.Operation == types.AccountOpCreate {
		delete(b.accountChangesByAccountID, accountID)
		return
	}

	b.accountChangesByAccountID[accountID] = accountChange
}

func (b *IndexerBuffer) pushSACBalanceChange(sacBalanceChange types.SACBalanceChange) {
	key := SACBalanceChangeKey{
		AccountID:  sacBalanceChange.AccountID,
		ContractID: sacBalanceChange.ContractID,
	}
	existing, exists := b.sacBalanceChangesByKey[key]

	if exists && existing.OperationID > sacBalanceChange.OperationID {
		return
	}

	if exists && sacBalanceChange.Operation == types.SACBalanceOpRemove && existing.Operation == types.SACBalanceOpAdd {
		delete(b.sacBalanceChangesByKey, key)
		return
	}

	b.sacBalanceChangesByKey[key] = sacBalanceChange
}

func (b *IndexerBuffer) pushSACContract(c *data.Contract) {
	if _, exists := b.sacContractsByID[c.ContractID]; !exists {
		b.sacContractsByID[c.ContractID] = c
	}
}

// ParseAssetString parses a "CODE:ISSUER" formatted asset string into its components.
func ParseAssetString(asset string) (code, issuer string, err error) {
	parts := strings.SplitN(asset, ":", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid asset format: expected CODE:ISSUER, got %s", asset)
	}
	code, issuer = parts[0], parts[1]

	creditAsset := txnbuild.CreditAsset{Code: code, Issuer: issuer}
	if _, err := creditAsset.ToXDR(); err != nil {
		return "", "", fmt.Errorf("invalid asset %s: %w", asset, err)
	}
	return code, issuer, nil
}
