package indexer

import (
	"sync"

	set "github.com/deckarep/golang-set/v2"

	"github.com/stellar/wallet-backend/internal/indexer/types"
)

func NewIngestionBuffer() *IngestionBuffer {
	return &IngestionBuffer{
		Participants:           set.NewSet[string](),
		txByHash:               make(map[string]types.Transaction),
		participantTxHashBimap: NewBiMap[string, string](),
	}
}

type IngestionBuffer struct {
	mu                     sync.RWMutex
	Participants           set.Set[string]
	txByHash               map[string]types.Transaction
	participantTxHashBimap *BiMap[string, string]
}

func (b *IngestionBuffer) PushParticipantTransaction(participant string, transaction types.Transaction) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.txByHash[transaction.Hash] = transaction
	b.Participants.Add(participant)

	b.participantTxHashBimap.Add(participant, transaction.Hash)
}

func (b *IngestionBuffer) GetNumberOfTransactions() int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return len(b.txByHash)
}

func (b *IngestionBuffer) GetParticipantTransactionHashes(participant string) set.Set[string] {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.participantTxHashBimap.GetForward(participant)
}

func (b *IngestionBuffer) GetParticipantTransactions(participant string) []types.Transaction {
	b.mu.RLock()
	defer b.mu.RUnlock()

	txs := []types.Transaction{}
	it := b.GetParticipantTransactionHashes(participant).Iterator()
	for txHash := range it.C {
		txs = append(txs, b.txByHash[txHash])
	}

	return txs
}

func (b *IngestionBuffer) GetTransactionParticipants(transactionHash string) set.Set[string] {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.participantTxHashBimap.GetBackward(transactionHash)
}

func (b *IngestionBuffer) GetTransaction(transactionHash string) types.Transaction {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.txByHash[transactionHash]
}
