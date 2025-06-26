package indexer

import (
	"sync"

	set "github.com/deckarep/golang-set/v2"

	"github.com/stellar/wallet-backend/internal/indexer/types"
)

func NewIndexerBuffer() IndexerBuffer {
	return IndexerBuffer{
		Participants:           set.NewSet[string](),
		txByHash:               make(map[string]types.Transaction),
		participantTxHashBimap: NewBiMap[string, string](),
	}
}

type IndexerBuffer struct {
	mu                     sync.RWMutex
	Participants           set.Set[string]
	txByHash               map[string]types.Transaction
	participantTxHashBimap *BiMap[string, string]
}

func (b *IndexerBuffer) PushParticipantTransaction(participant string, transaction types.Transaction) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.txByHash[transaction.Hash] = transaction
	b.Participants.Add(participant)

	b.participantTxHashBimap.Add(participant, transaction.Hash)
}

func (b *IndexerBuffer) GetNumberOfTransactions() int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return len(b.txByHash)
}

func (b *IndexerBuffer) GetParticipantTransactionHashes(participant string) set.Set[string] {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.participantTxHashBimap.GetForward(participant).Clone()
}

func (b *IndexerBuffer) GetParticipantTransactions(participant string) []types.Transaction {
	b.mu.RLock()
	defer b.mu.RUnlock()

	txs := []types.Transaction{}
	it := b.participantTxHashBimap.GetForward(participant).Iterator()
	for txHash := range it.C {
		txs = append(txs, b.txByHash[txHash])
	}

	return txs
}

func (b *IndexerBuffer) GetTransactionParticipants(transactionHash string) set.Set[string] {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.participantTxHashBimap.GetBackward(transactionHash).Clone()
}

func (b *IndexerBuffer) GetTransaction(transactionHash string) types.Transaction {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.txByHash[transactionHash]
}
