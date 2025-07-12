package indexer

import (
	"sync"

	set "github.com/deckarep/golang-set/v2"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
)

func NewIndexerBuffer() IndexerBuffer {
	return IndexerBuffer{
		Participants:          set.NewSet[string](),
		txByHash:              make(map[string]ingest.LedgerTransaction),
		txHashesByParticipant: make(map[string]set.Set[string]),
		opByID:                make(map[int64]xdr.Operation),
		opIDsByParticipant:    make(map[string]set.Set[int64]),
		opIdxByOpID:           make(map[int64]uint32),
		txHashByOpID:          make(map[int64]string),
	}
}

type IndexerBuffer struct {
	mu                    sync.RWMutex
	Participants          set.Set[string]
	txByHash              map[string]ingest.LedgerTransaction
	txHashesByParticipant map[string]set.Set[string]
	opByID                map[int64]xdr.Operation
	opIDsByParticipant    map[string]set.Set[int64]
	opIdxByOpID           map[int64]uint32
	txHashByOpID          map[int64]string
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

func (b *IndexerBuffer) GetOperationIndex(opID int64) uint32 {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.opIdxByOpID[opID]
}

func (b *IndexerBuffer) GetOperationTransaction(opID int64) ingest.LedgerTransaction {
	b.mu.RLock()
	defer b.mu.RUnlock()

	txHash, ok := b.txHashByOpID[opID]
	if !ok {
		return ingest.LedgerTransaction{}
	}

	return b.txByHash[txHash]
}

func (b *IndexerBuffer) PushParticipantOperation(participant string, opID int64, operation xdr.Operation, transaction ingest.LedgerTransaction, operationIdx uint32) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.opByID[opID] = operation
	b.Participants.Add(participant)

	if _, ok := b.opIDsByParticipant[participant]; !ok {
		b.opIDsByParticipant[participant] = set.NewSet[int64]()
	}
	b.opIDsByParticipant[participant].Add(opID)

	if _, ok := b.opIdxByOpID[opID]; !ok {
		b.opIdxByOpID[opID] = operationIdx
	}

	if _, ok := b.txHashByOpID[opID]; !ok {
		b.txHashByOpID[opID] = transaction.Hash.HexString()
	}

	b.pushParticipantTransactionUnsafe(participant, transaction)
}

func (b *IndexerBuffer) GetParticipantOperations(participant string) map[int64]xdr.Operation {
	b.mu.RLock()
	defer b.mu.RUnlock()

	opIDs, ok := b.opIDsByParticipant[participant]
	if !ok {
		return nil
	}

	ops := make(map[int64]xdr.Operation, opIDs.Cardinality())
	for opID := range opIDs.Iter() {
		ops[opID] = b.opByID[opID]
	}

	return ops
}
