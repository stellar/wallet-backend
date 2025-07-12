package indexer

import (
	"fmt"

	"github.com/stellar/go/ingest"

	"github.com/stellar/wallet-backend/internal/processors"
)

type Indexer struct {
	IndexerBuffer
	participantsProcessor processors.ParticipantsProcessor
}

func NewIndexer(networkPassphrase string) *Indexer {
	return &Indexer{
		IndexerBuffer:         NewIndexerBuffer(),
		participantsProcessor: processors.NewParticipantsProcessor(networkPassphrase),
	}
}

func (i *Indexer) ProcessTransaction(transaction ingest.LedgerTransaction) error {
	// 1. Index transaction txParticipants
	txParticipants, err := i.participantsProcessor.GetTransactionParticipants(transaction)
	if err != nil {
		return fmt.Errorf("getting transaction participants: %w", err)
	}

	// dataTx, err := processors.ConvertTransaction(&transaction)
	// if err != nil {
	// 	return fmt.Errorf("creating data transaction: %w", err)
	// }
	if txParticipants.Cardinality() != 0 {
		for participant := range txParticipants.Iter() {
			i.IndexerBuffer.PushParticipantTransaction(participant, transaction)
		}
	}

	// 2. Index tx.Operations() participants
	opsParticipants, err := i.participantsProcessor.GetOperationsParticipants(transaction)
	if err != nil {
		return fmt.Errorf("getting operations participants: %w", err)
	}
	for opID, opParticipants := range opsParticipants {
		// dataOp, err := processors.ConvertOperation(&transaction, &opParticipants.Operation, opID)
		// if err != nil {
		// 	return fmt.Errorf("creating data operation: %w", err)
		// }

		for participant := range opParticipants.Participants.Iter() {
			i.IndexerBuffer.PushParticipantOperation(participant, opID, opParticipants.Operation, transaction, opParticipants.OperationIdx)

			// Even though the participant is not part of a transaction, we still want to index the transaction since the participant
			// is part of one of its operations.
			i.IndexerBuffer.PushParticipantTransaction(participant, transaction)
		}
	}

	return nil
}
