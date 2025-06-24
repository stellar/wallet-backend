package indexer

import (
	"fmt"
	"time"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/toid"
	"github.com/stellar/go/xdr"

	"github.com/stellar/wallet-backend/internal/indexer/types"
)

// ParticipantsProcessor is a processor which ingests various participants
// from different sources (transactions, operations, etc).
type ParticipantsProcessor struct {
	IngestionBuffer IngestionBuffer
}

func NewParticipantsProcessor() *ParticipantsProcessor {
	return &ParticipantsProcessor{
		IngestionBuffer: *NewIngestionBuffer(),
	}
}

func participantsForChanges(changes xdr.LedgerEntryChanges) ([]xdr.AccountId, error) {
	var participants []xdr.AccountId

	for _, c := range changes {
		var participant *xdr.AccountId

		switch c.Type {
		case xdr.LedgerEntryChangeTypeLedgerEntryCreated:
			participant = participantsForLedgerEntry(c.MustCreated())
		case xdr.LedgerEntryChangeTypeLedgerEntryRemoved:
			participant = participantsForLedgerKey(c.MustRemoved())
		case xdr.LedgerEntryChangeTypeLedgerEntryUpdated:
			participant = participantsForLedgerEntry(c.MustUpdated())
		case xdr.LedgerEntryChangeTypeLedgerEntryState:
			participant = participantsForLedgerEntry(c.MustState())
		default:
			return nil, fmt.Errorf("unknown ledger entrychange type %d", c.Type)
		}

		if participant != nil {
			participants = append(participants, *participant)
		}
	}

	return participants, nil
}

func participantsForLedgerEntry(le xdr.LedgerEntry) *xdr.AccountId {
	if le.Data.Type != xdr.LedgerEntryTypeAccount {
		return nil
	}
	aid := le.Data.MustAccount().AccountId
	return &aid
}

func participantsForLedgerKey(lk xdr.LedgerKey) *xdr.AccountId {
	if lk.Type != xdr.LedgerEntryTypeAccount {
		return nil
	}
	aid := lk.MustAccount().AccountId
	return &aid
}

func participantsForMeta(meta xdr.TransactionMeta) ([]xdr.AccountId, error) {
	var participants []xdr.AccountId
	if meta.Operations == nil {
		return participants, nil
	}

	for _, op := range *meta.Operations {
		var accounts []xdr.AccountId
		accounts, err := participantsForChanges(op.Changes)
		if err != nil {
			return nil, fmt.Errorf("identifying participants for changes: %w", err)
		}

		participants = append(participants, accounts...)
	}

	return participants, nil
}

func (p *ParticipantsProcessor) addTransactionParticipants(transaction ingest.LedgerTransaction) error {
	// 1. Get direct participants involved in the transaction
	participants := []xdr.AccountId{
		transaction.Envelope.SourceAccount().ToAccountId(),
	}
	if transaction.Envelope.IsFeeBump() {
		participants = append(participants, transaction.Envelope.FeeBumpAccount().ToAccountId())
	}

	feeParticipants, err := participantsForChanges(transaction.FeeChanges)
	if err != nil {
		return fmt.Errorf("identifying participants for changes: %w", err)
	}
	participants = append(participants, feeParticipants...)

	// 1.1. Get participants involved in the transaction meta (if successful)
	if transaction.Result.Successful() {
		metaParticipants, metaErr := participantsForMeta(transaction.UnsafeMeta)
		if metaErr != nil {
			return fmt.Errorf("identifying participants for meta: %w", metaErr)
		}
		participants = append(participants, metaParticipants...)
	}

	// 2. Build database transaction object
	envelopeXDR, err := xdr.MarshalBase64(transaction.Envelope)
	if err != nil {
		return fmt.Errorf("marshalling transaction envelope: %w", err)
	}

	resultXDR, err := xdr.MarshalBase64(transaction.Result)
	if err != nil {
		return fmt.Errorf("marshalling transaction result: %w", err)
	}

	metaXDR, err := xdr.MarshalBase64(transaction.UnsafeMeta)
	if err != nil {
		return fmt.Errorf("marshalling transaction meta: %w", err)
	}

	ledgerSequence := transaction.Ledger.LedgerSequence()
	transactionID := toid.New(int32(ledgerSequence), int32(transaction.Index), 0).ToInt64()

	dbTx := types.Transaction{
		ToID:            transactionID,
		Hash:            transaction.Hash.HexString(),
		LedgerCreatedAt: transaction.Ledger.ClosedAt(),
		IngestedAt:      time.Now(),
		EnvelopeXDR:     envelopeXDR,
		ResultXDR:       resultXDR,
		MetaXDR:         metaXDR,
		LedgerNumber:    ledgerSequence,
	}

	// 3. Push transaction and participants to data bundle
	for _, xdrParticipant := range participants {
		p.IngestionBuffer.PushParticipantTransaction(xdrParticipant.Address(), dbTx)
	}

	// TODO: verify if we should inject multiple transactions in case of a fee bump transaction.
	// I believe not because it'd be covered when we support operations ingestion.

	return nil
}

func (p *ParticipantsProcessor) ProcessTransactionData(transaction ingest.LedgerTransaction) error {
	if err := p.addTransactionParticipants(transaction); err != nil {
		return fmt.Errorf("adding transaction participants: %w", err)
	}

	return nil
}
