package indexer

import (
	"fmt"

	"github.com/stellar/go/ingest"

	"github.com/stellar/wallet-backend/internal/indexer/types"
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
	participants, err := i.participantsProcessor.GetTransactionParticipants(transaction)
	if err != nil {
		return fmt.Errorf("getting transaction participants: %w", err)
	}

	var dataTx *types.Transaction
	if participants.Cardinality() != 0 {
		dataTx, err = processors.ConvertTransaction(&transaction)
		if err != nil {
			return fmt.Errorf("creating data transaction: %w", err)
		}

		for participant := range participants.Iterator().C {
			i.IndexerBuffer.PushParticipantTransaction(participant, *dataTx)
		}
	}

	return nil
}
