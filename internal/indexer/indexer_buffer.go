package indexer

import (
	"sync"

	set "github.com/deckarep/golang-set/v2"

	"github.com/stellar/go/ingest"
	"github.com/stellar/wallet-backend/internal/indexer/types"
)

func NewIndexerBuffer() IndexerBuffer {
	return IndexerBuffer{
		Participants:          set.NewSet[string](),
		txByHash:              make(map[string]ingest.LedgerTransaction),
		txHashesByParticipant: make(map[string]set.Set[string]),
		opByID:                make(map[int64]types.Operation),
		opIDsByParticipant:    make(map[string]set.Set[int64]),
	}
}

type IndexerBuffer struct {
	mu                    sync.RWMutex
	Participants          set.Set[string]
	txByHash              map[string]ingest.LedgerTransaction
	txHashesByParticipant map[string]set.Set[string]
	opByID                map[int64]types.Operation
	opIDsByParticipant    map[string]set.Set[int64]
}

func (b *IndexerBuffer) PushParticipantTransaction(participant string, transaction ingest.LedgerTransaction) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.pushParticipantTransactionUnsafe(participant, transaction)
}

func (b *IndexerBuffer) pushParticipantTransactionUnsafe(participant string, transaction ingest.LedgerTransaction) {
	txHash := transaction.Hash.HexString()
	b.txByHash[txHash] = transaction
	b.Participants.Add(participant)

	if _, ok := b.txHashesByParticipant[participant]; !ok {
		b.txHashesByParticipant[participant] = set.NewSet[string]()
	}
	b.txHashesByParticipant[participant].Add(txHash)
}

func (b *IndexerBuffer) GetNumberOfTransactions() int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return len(b.txByHash)
}

func (b *IndexerBuffer) GetParticipantTransactions(participant string) []ingest.LedgerTransaction {
	b.mu.RLock()
	defer b.mu.RUnlock()

	txHashes, ok := b.txHashesByParticipant[participant]
	if !ok {
		return nil
	}

	txs := make([]ingest.LedgerTransaction, 0, txHashes.Cardinality())
	for txHash := range txHashes.Iter() {
		txs = append(txs, b.txByHash[txHash])
	}

	return txs
}

func (b *IndexerBuffer) GetAllTransactions() []ingest.LedgerTransaction {
	b.mu.RLock()
	defer b.mu.RUnlock()

	txs := make([]ingest.LedgerTransaction, 0, len(b.txByHash))
	for _, tx := range b.txByHash {
		txs = append(txs, tx)
	}

	return txs
}

func (b *IndexerBuffer) PushParticipantOperation(participant string, operation types.Operation, transaction ingest.LedgerTransaction) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.opByID[operation.ID] = operation
	b.Participants.Add(participant)

	if _, ok := b.opIDsByParticipant[participant]; !ok {
		b.opIDsByParticipant[participant] = set.NewSet[int64]()
	}
	b.opIDsByParticipant[participant].Add(operation.ID)

	b.pushParticipantTransactionUnsafe(participant, transaction)
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
