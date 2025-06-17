package indexer

import "github.com/stellar/wallet-backend/internal/indexer/types"

type Participant string

// Set is a set of strings that enforces uniqueness
type Set[T comparable] map[T]struct{}

// Add adds a value to the set
func (s Set[T]) Add(value T) {
	if s == nil {
		s = make(Set[T])
	}
	s[value] = struct{}{}
}

// Contains checks if a value exists in the set
func (s Set[T]) Contains(value T) bool {
	_, exists := s[value]
	return exists
}

// ToSlice converts the set to a slice
func (s Set[T]) ToSlice() []T {
	result := make([]T, 0, len(s))
	for k := range s {
		result = append(result, k)
	}
	return result
}

type DataBundle struct {
	Network               string
	Participants          Set[Participant]
	OpByID                map[string]types.Operation
	TxByHash              map[string]types.Transaction
	TxHashesByParticipant map[Participant]Set[string]
	OpIDsByParticipant    map[Participant]Set[string]
}

func NewDataBundle(network string) DataBundle {
	return DataBundle{
		Network:               network,
		Participants:          Set[Participant]{},
		OpByID:                map[string]types.Operation{},
		TxByHash:              map[string]types.Transaction{},
		TxHashesByParticipant: map[Participant]Set[string]{},
		OpIDsByParticipant:    map[Participant]Set[string]{},
	}
}

func (b *DataBundle) PushTransactionWithParticipant(participant Participant, transaction types.Transaction) {
	b.TxByHash[transaction.Hash] = transaction
	b.TxHashesByParticipant[participant].Add(transaction.Hash)
	b.Participants.Add(participant)
}

func (b *DataBundle) PushOperationWithParticipant(participant Participant, operation types.Operation) {
	b.OpByID[operation.ID] = operation
	b.OpIDsByParticipant[participant].Add(operation.ID)
	b.TxHashesByParticipant[participant].Add(operation.TxHash)
	b.Participants.Add(participant)
}

func (b *DataBundle) GetParticipantOperationIDs(participant Participant) Set[string] {
	return b.OpIDsByParticipant[participant]
}

func (b *DataBundle) GetParticipantOperations(participant Participant) []types.Operation {
	ops := []types.Operation{}
	for _, opID := range b.OpIDsByParticipant[participant].ToSlice() {
		ops = append(ops, b.OpByID[opID])
	}
	return ops
}

func (b *DataBundle) GetParticipantTransactionHashes(participant Participant) Set[string] {
	return b.TxHashesByParticipant[participant]
}

func (b *DataBundle) GetParticipantTransactions(participant Participant) []types.Transaction {
	txs := []types.Transaction{}
	for _, txHash := range b.TxHashesByParticipant[participant].ToSlice() {
		txs = append(txs, b.TxByHash[txHash])
	}
	return txs
}
