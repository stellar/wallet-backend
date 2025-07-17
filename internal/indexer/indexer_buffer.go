package indexer

import (
	"sync"

	set "github.com/deckarep/golang-set/v2"

	"github.com/stellar/wallet-backend/internal/indexer/types"
)

type IndexerBuffer struct {
	mu                    sync.RWMutex
	Participants          set.Set[string]
	txByHash              map[string]types.Transaction
	txHashesByParticipant map[string]set.Set[string]
	opByID                map[int64]types.Operation
	opIDsByParticipant    map[string]set.Set[int64]
	stateChanges          []types.StateChange
}

func NewIndexerBuffer() *IndexerBuffer {
	return &IndexerBuffer{
		Participants:          set.NewSet[string](),
		txByHash:              make(map[string]types.Transaction),
		txHashesByParticipant: make(map[string]set.Set[string]),
		opByID:                make(map[int64]types.Operation),
		opIDsByParticipant:    make(map[string]set.Set[int64]),
		stateChanges:          make([]types.StateChange, 0),
	}
}

func (b *IndexerBuffer) GetParticipants() set.Set[string] {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.Participants
}

func (b *IndexerBuffer) PushParticipantTransaction(participant string, transaction types.Transaction) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.pushParticipantTransactionUnsafe(participant, transaction)
}

func (b *IndexerBuffer) pushParticipantTransactionUnsafe(participant string, transaction types.Transaction) {
	b.txByHash[transaction.Hash] = transaction
	b.Participants.Add(participant)

	if _, ok := b.txHashesByParticipant[participant]; !ok {
		b.txHashesByParticipant[participant] = set.NewSet[string]()
	}
	b.txHashesByParticipant[participant].Add(transaction.Hash)
}

func (b *IndexerBuffer) GetNumberOfTransactions() int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return len(b.txByHash)
}

func (b *IndexerBuffer) GetParticipantTransactions(participant string) []types.Transaction {
	b.mu.RLock()
	defer b.mu.RUnlock()

	txHashes, ok := b.txHashesByParticipant[participant]
	if !ok {
		return nil
	}

	txs := make([]types.Transaction, 0, txHashes.Cardinality())
	for txHash := range txHashes.Iter() {
		txs = append(txs, b.txByHash[txHash])
	}

	return txs
}

func (b *IndexerBuffer) GetAllTransactions() []types.Transaction {
	b.mu.RLock()
	defer b.mu.RUnlock()

	txs := make([]types.Transaction, 0, len(b.txByHash))
	for _, tx := range b.txByHash {
		txs = append(txs, tx)
	}

	return txs
}

func (b *IndexerBuffer) PushParticipantOperation(participant string, operation types.Operation, transaction types.Transaction) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.pushParticipantOperationUnsafe(participant, operation)

	b.pushParticipantTransactionUnsafe(participant, transaction)
}

func (b *IndexerBuffer) pushParticipantOperationUnsafe(participant string, operation types.Operation) {
	b.opByID[operation.ID] = operation
	b.Participants.Add(participant)

	if _, ok := b.opIDsByParticipant[participant]; !ok {
		b.opIDsByParticipant[participant] = set.NewSet[int64]()
	}
	b.opIDsByParticipant[participant].Add(operation.ID)
}

func (b *IndexerBuffer) GetParticipantOperations(participant string) map[int64]types.Operation {
	b.mu.RLock()
	defer b.mu.RUnlock()

	opIDs, ok := b.opIDsByParticipant[participant]
	if !ok {
		return nil
	}

	ops := make(map[int64]types.Operation, opIDs.Cardinality())
	for opID := range opIDs.Iter() {
		ops[opID] = b.opByID[opID]
	}

	return ops
}

func (b *IndexerBuffer) PushStateChanges(stateChanges []types.StateChange) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.stateChanges = append(b.stateChanges, stateChanges...)

	for _, stateChange := range stateChanges {
		if stateChange.OperationID != 0 {
			if op, ok := b.opByID[stateChange.OperationID]; ok {
				b.pushParticipantOperationUnsafe(stateChange.AccountID, op)
			}
		}

		if stateChange.TxHash != "" {
			if tx, ok := b.txByHash[stateChange.TxHash]; ok {
				b.pushParticipantTransactionUnsafe(stateChange.AccountID, tx)
			}
		}
	}
}

func (b *IndexerBuffer) GetAllStateChanges() []types.StateChange {
	b.mu.RLock()
	defer b.mu.RUnlock()

	// Return a copy to prevent race conditions on the slice.
	stateChangesCopy := make([]types.StateChange, len(b.stateChanges))
	copy(stateChangesCopy, b.stateChanges)
	return stateChangesCopy
}
