package indexer

import (
	"fmt"
	"time"

	"github.com/stellar/go/ingest"
	operation_processor "github.com/stellar/go/processors/operation"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/xdr"

	"github.com/stellar/wallet-backend/internal/indexer/types"
)

// ParticipantsProcessor is a processor which ingests various participants
// from different sources (transactions, operations, etc)
type ParticipantsProcessor struct {
	network    string
	DataBundle DataBundle
}

func NewParticipantsProcessor(network string) *ParticipantsProcessor {
	return &ParticipantsProcessor{
		network:    network,
		DataBundle: NewDataBundle(network),
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
			return nil, errors.Errorf("Unknown change type: %s", c.Type)
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
			return nil, err
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

	metaParticipants, err := participantsForMeta(transaction.UnsafeMeta)
	if err != nil {
		return fmt.Errorf("identifying participants for meta: %w", err)
	}
	participants = append(participants, metaParticipants...)

	feeParticipants, err := participantsForChanges(transaction.FeeChanges)
	if err != nil {
		return fmt.Errorf("identifying participants for changes: %w", err)
	}
	participants = append(participants, feeParticipants...)

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

	dbTx := types.Transaction{
		Hash:            transaction.Hash.HexString(),
		LedgerCreatedAt: transaction.Ledger.ClosedAt(),
		IngestedAt:      time.Now(),
		EnvelopeXDR:     envelopeXDR,
		ResultXDR:       resultXDR,
		MetaXDR:         metaXDR,
		LedgerNumber:    transaction.LedgerVersion,
	}

	// 3. Push transaction and participants to data bundle
	for _, xdrParticipant := range participants {
		p.DataBundle.PushTransactionWithParticipant(xdrParticipant.Address(), dbTx)
	}

	return nil
}

func (p *ParticipantsProcessor) addOperationsParticipants(sequence uint32, transaction ingest.LedgerTransaction) error {
	if !transaction.Successful() {
		return nil
	}

	now := time.Now()
	ledgerCreatedAt := transaction.Ledger.ClosedAt()
	txHash := transaction.Hash.HexString()

	for opi, xdrOp := range transaction.Envelope.Operations() {
		// 1. Build op wrapper, so we can use its methods
		op := operation_processor.TransactionOperationWrapper{
			Index:          uint32(opi),
			Transaction:    transaction,
			Operation:      xdrOp,
			LedgerSequence: sequence,
			Network:        p.network,
		}
		opID := fmt.Sprintf("%d", op.ID())

		// 2. Get participants for the operation
		participants, err := op.Participants()
		if err != nil {
			return fmt.Errorf("reading operation %s participants: %w", opID, err)
		}

		// 3. Build database operation object
		xdrOpStr, err := xdr.MarshalBase64(xdrOp)
		if err != nil {
			return fmt.Errorf("marshalling operation %s: %w", opID, err)
		}
		dbOp := types.Operation{
			ID:              opID,
			OperationType:   types.OperationTypeFromXDR(op.OperationType()),
			OperationXDR:    xdrOpStr,
			LedgerCreatedAt: ledgerCreatedAt,
			IngestedAt:      now,
			TxHash:          txHash,
		}

		// 4. Push operation and participants to data bundle
		for _, xdrParticipant := range participants {
			p.DataBundle.PushOperationWithParticipant(xdrParticipant.Address(), dbOp)
		}
	}

	return nil
}

func (p *ParticipantsProcessor) ProcessTransactionData(lcm xdr.LedgerCloseMeta, transaction ingest.LedgerTransaction) error {
	if err := p.addOperationsParticipants(lcm.LedgerSequence(), transaction); err != nil {
		return err
	}

	if err := p.addTransactionParticipants(transaction); err != nil {
		return err
	}

	return nil
}
