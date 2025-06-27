package indexer

import (
	"sync"

	set "github.com/deckarep/golang-set/v2"

	"github.com/stellar/wallet-backend/internal/indexer/types"
)

func NewIndexerBuffer() IndexerBuffer {
	return IndexerBuffer{
		Participants:          set.NewSet[string](),
		txByHash:              make(map[string]types.Transaction),
		txHashesByParticipant: make(map[string]set.Set[string]),
	}
}

type IndexerBuffer struct {
	mu                    sync.RWMutex
	Participants          set.Set[string]
	txByHash              map[string]types.Transaction
	txHashesByParticipant map[string]set.Set[string]
}

func (b *IndexerBuffer) PushParticipantTransaction(participant string, transaction types.Transaction) {
	b.mu.Lock()
	defer b.mu.Unlock()

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
	for txHash := range txHashes.Iterator().C {
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
