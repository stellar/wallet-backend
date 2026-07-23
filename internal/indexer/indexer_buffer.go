// Package indexer provides high-performance data buffering for Stellar blockchain ingestion.
// IndexerBuffer uses a canonical pointer + set-based architecture to minimize memory usage
// and eliminate duplicate checks during transaction/operation processing.
package indexer

import (
	"fmt"
	"strings"

	set "github.com/deckarep/golang-set/v2"
	"github.com/google/uuid"
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
//   - txByHash: Single pointer per unique transaction (keyed by hash)
//   - opByID: Single pointer per unique operation (keyed by ID)
//   - This layer owns the actual data and ensures only ONE copy exists in memory
//
// 2. Transaction/Operation to Participants Mapping Layer:
//   - participantsByToID: Maps each transaction ToID to a SET of participant IDs
//   - participantsByOpID: Maps each operation ID to a SET of participant IDs
//   - Efficiently tracks which participants interacted with each tx/op
//
// MEMORY OPTIMIZATION:
// When multiple participants interact with the same transaction or operation, they all point
// to the SAME canonical pointer instead of storing duplicate copies.
//
// OWNERSHIP:
// Not safe for concurrent use. Each buffer instance is owned by a single goroutine:
// ProcessLedgerTransactions builds a per-transaction TransactionResult in parallel workers and
// folds each into one ledger buffer serially (IngestTransactionResult). The participant sets are
// thread-unsafe for the same reason.

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
// All maps are pre-allocated to avoid nil map access.
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
func (b *IndexerBuffer) PushTransaction(participant string, transaction *types.Transaction) {
	b.recordTransaction(participant, transaction)
}

// recordTransaction is the shared internal helper that stores a transaction pointer and
// records the participant:
//
// 1. Check if transaction already exists in txByHash
// 2. If not, store the transaction pointer
// 3. Add participant to this transaction's participant set in participantsByToID
func (b *IndexerBuffer) recordTransaction(participant string, transaction *types.Transaction) {
	txHash := transaction.Hash.String()
	if _, exists := b.txByHash[txHash]; !exists {
		b.txByHash[txHash] = transaction
	}

	// Track this participant by ToID
	toID := transaction.ToID
	if _, exists := b.participantsByToID[toID]; !exists {
		b.participantsByToID[toID] = set.NewThreadUnsafeSet[string]()
	}

	// Add participant - O(1) with automatic deduplication
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

// GetTransactionsParticipants returns the buffer's live map of transaction ToIDs to their
// participants; callers must not modify it or the sets it holds.
func (b *IndexerBuffer) GetTransactionsParticipants() map[int64]set.Set[string] {
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
func (b *IndexerBuffer) PushTrustlineChange(trustlineChange types.TrustlineChange) {
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

// GetOperations returns all unique operations from the canonical storage.
func (b *IndexerBuffer) GetOperations() []*types.Operation {
	ops := make([]*types.Operation, 0, len(b.opByID))
	for _, opPtr := range b.opByID {
		ops = append(ops, opPtr)
	}
	return ops
}

// GetOperationsParticipants returns the buffer's live map of operation IDs to their
// participants; callers must not modify it or the sets it holds.
func (b *IndexerBuffer) GetOperationsParticipants() map[int64]set.Set[string] {
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
	if _, exists := b.participantsByOpID[opID]; !exists {
		b.participantsByOpID[opID] = set.NewThreadUnsafeSet[string]()
	}
	b.participantsByOpID[opID].Add(participant)
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
	ParticipantCount      int
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
			log.Errorf("operation %d missing from TransactionResult.Operations (tx %s); dropping its participants", opID, r.Transaction.Hash)
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
