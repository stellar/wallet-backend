package indexer

import (
	"context"
	"fmt"

	"github.com/stellar/go/ingest"

	"github.com/stellar/wallet-backend/internal/processors"
	statechangeprocessors "github.com/stellar/wallet-backend/internal/indexer/processors"
)

type Indexer struct {
	IndexerBuffer
	participantsProcessor processors.ParticipantsProcessor
	tokenTransferProcessor *statechangeprocessors.TokenTransferProcessor
	effectsProcessor *statechangeprocessors.EffectsProcessor
}

func NewIndexer(networkPassphrase string) *Indexer {
	return &Indexer{
		IndexerBuffer:         NewIndexerBuffer(),
		participantsProcessor: processors.NewParticipantsProcessor(networkPassphrase),
		tokenTransferProcessor: statechangeprocessors.NewTokenTransferProcessor(networkPassphrase),
		effectsProcessor: statechangeprocessors.NewEffectsProcessor(networkPassphrase),
	}
}

func (i *Indexer) ProcessTransaction(ctx context.Context, transaction ingest.LedgerTransaction) error {
	// 1. Index transaction txParticipants
	txParticipants, err := i.participantsProcessor.GetTransactionParticipants(transaction)
	if err != nil {
		return fmt.Errorf("getting transaction participants: %w", err)
	}

	dataTx, err := processors.ConvertTransaction(&transaction)
	if err != nil {
		return fmt.Errorf("creating data transaction: %w", err)
	}
	if txParticipants.Cardinality() != 0 {
		for participant := range txParticipants.Iter() {
			i.IndexerBuffer.PushParticipantTransaction(participant, *dataTx)
		}
	}

	// 2. Index tx.Operations() participants
	opsParticipants, err := i.participantsProcessor.GetOperationsParticipants(transaction)
	if err != nil {
		return fmt.Errorf("getting operations participants: %w", err)
	}
	for opID, opParticipants := range opsParticipants {
		dataOp, err := processors.ConvertOperation(&transaction, &opParticipants.Operation, opID)
		if err != nil {
			return fmt.Errorf("creating data operation: %w", err)
		}

		for participant := range opParticipants.Participants.Iter() {
			i.IndexerBuffer.PushParticipantOperation(participant, *dataOp, *dataTx)
		}

		// 2.1. Index effects state changes
		effectsStateChanges, err := i.effectsProcessor.ProcessOperation(ctx, transaction, opParticipants.Operation, opParticipants.OperationIdx)
		if err != nil {
			return fmt.Errorf("processing effects state changes: %w", err)
		}
		i.IndexerBuffer.PushStateChanges(effectsStateChanges)
	}

	// 3. Index token transfer state changes
	tokenTransferStateChanges, err := i.tokenTransferProcessor.ProcessTransaction(ctx, transaction)
	if err != nil {
		return fmt.Errorf("processing token transfer state changes: %w", err)
	}
	i.IndexerBuffer.PushStateChanges(tokenTransferStateChanges)

	return nil
}
