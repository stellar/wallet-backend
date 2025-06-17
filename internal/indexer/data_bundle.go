package indexer

import (
	"sync"

	"github.com/stellar/wallet-backend/internal/indexer/types"
)

// Set is a set of strings that enforces uniqueness.
type Set[T comparable] map[T]struct{}

// Add adds a value to the set
func (s Set[T]) Add(value T) {
	if s == nil {
		s = make(Set[T])
	}
	s[value] = struct{}{}
}

// Append adds all values from the input set to the current set.
func (s Set[T]) Append(inSet Set[T]) {
	for k := range inSet {
		s.Add(k)
	}
}

// Contains checks if a value exists in the set.
func (s Set[T]) Contains(value T) bool {
	_, exists := s[value]
	return exists
}

// ToSlice converts the set to a slice.
func (s Set[T]) ToSlice() []T {
	result := make([]T, 0, len(s))
	for k := range s {
		result = append(result, k)
	}
	return result
}

type DataBundle struct {
	mu                    sync.RWMutex
	Network               string
	Participants          Set[string]
	OpByID                map[string]types.Operation
	TxByHash              map[string]types.Transaction
	TxHashesByParticipant map[string]Set[string]
	ParticipantsByTxHash  map[string]Set[string]
	OpIDsByParticipant    map[string]Set[string]
	ParticipantsByOpID    map[string]Set[string]
}

func NewDataBundle(network string) DataBundle {
	return DataBundle{
		Network:               network,
		Participants:          Set[string]{},
		OpByID:                map[string]types.Operation{},
		TxByHash:              map[string]types.Transaction{},
		TxHashesByParticipant: map[string]Set[string]{},
		OpIDsByParticipant:    map[string]Set[string]{},
	}
}

func (b *DataBundle) PushTransactionWithParticipant(participant string, transaction types.Transaction) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.TxByHash[transaction.Hash] = transaction
	b.Participants.Add(participant)

	b.TxHashesByParticipant[participant].Add(transaction.Hash)
	b.ParticipantsByTxHash[transaction.Hash].Add(participant)
}

func (b *DataBundle) PushOperationWithParticipant(participant string, operation types.Operation) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.OpByID[operation.ID] = operation
	b.Participants.Add(participant)

	b.OpIDsByParticipant[participant].Add(operation.ID)
	b.ParticipantsByOpID[operation.ID].Add(participant)

	b.TxHashesByParticipant[participant].Add(operation.TxHash)
	b.ParticipantsByTxHash[operation.TxHash].Add(participant)
}

func (b *DataBundle) GetParticipantOperationIDs(participant string) Set[string] {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.OpIDsByParticipant[participant]
}

func (b *DataBundle) GetParticipantOperations(participant string) []types.Operation {
	b.mu.RLock()
	defer b.mu.RUnlock()

	ops := []types.Operation{}
	for _, opID := range b.OpIDsByParticipant[participant].ToSlice() {
		ops = append(ops, b.OpByID[opID])
	}
	return ops
}

func (b *DataBundle) GetOperationParticipants(operationID string) Set[string] {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.ParticipantsByOpID[operationID]
}

func (b *DataBundle) GetParticipantTransactionHashes(participant string) Set[string] {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.TxHashesByParticipant[participant]
}

func (b *DataBundle) GetParticipantTransactions(participant string) []types.Transaction {
	b.mu.RLock()
	defer b.mu.RUnlock()

	txs := []types.Transaction{}
	for _, txHash := range b.TxHashesByParticipant[participant].ToSlice() {
		txs = append(txs, b.TxByHash[txHash])
	}
	return txs
}

func (b *DataBundle) GetTransactionParticipants(transactionHash string) Set[string] {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.ParticipantsByTxHash[transactionHash]
}
