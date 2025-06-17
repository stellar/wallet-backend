package ingest

import (
	"fmt"
	"sort"
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
	TxsByParticipant map[Participant][]types.Transaction
	OpsByParticipant map[Participant][]types.Operation
	network          string
}

func NewParticipantsProcessor(network string) *ParticipantsProcessor {
	return &ParticipantsProcessor{
		network:          network,
		TxsByParticipant: make(map[Participant][]types.Transaction),
		OpsByParticipant: make(map[Participant][]types.Operation),
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

func (p *ParticipantsProcessor) addTransactionParticipants(
	sequence uint32,
	transaction ingest.LedgerTransaction,
) error {
	if len(p.OpsByParticipant) == 0 {
		err := p.addOperationsParticipants(sequence, transaction)
		if err != nil {
			return fmt.Errorf("could not determine operation participants: %w", err)
		}
	}

	txsByParticipant, err := participantsForTransaction(sequence, transaction, p.OpsByParticipant)
	if err != nil {
		return fmt.Errorf("could not determine participants for transaction: %w", err)
	}
	p.TxsByParticipant = txsByParticipant

	return nil
}

func (p *ParticipantsProcessor) addOperationsParticipants(
	sequence uint32,
	transaction ingest.LedgerTransaction,
) error {
	participants, err := operationsParticipants(transaction, sequence, p.network)
	if err != nil {
		return errors.Wrap(err, "could not determine operation participants")
	}

	p.OpsByParticipant = participants

	return nil
}

type Participant string

// operationsParticipants returns a map with all participants per operation.
func operationsParticipants(transaction ingest.LedgerTransaction, sequence uint32, network string) (allOpsParticipants map[Participant][]types.Operation, err error) {
	now := time.Now()
	ledgerCreatedAt := transaction.Ledger.ClosedAt()
	txHash := transaction.Hash.HexString()

	for opi, xdrOp := range transaction.Envelope.Operations() {
		op := operation_processor.TransactionOperationWrapper{
			Index:          uint32(opi),
			Transaction:    transaction,
			Operation:      xdrOp,
			LedgerSequence: sequence,
			Network:        network,
		}
		opID := op.ID()

		p, err := op.Participants()
		if err != nil {
			return allOpsParticipants, errors.Wrapf(err, "reading operation %v participants", opID)
		}

		xdrOpStr, err := xdr.MarshalBase64(xdrOp)
		if err != nil {
			return allOpsParticipants, fmt.Errorf("marshalling operation %v: %w", opID, err)
		}
		dbOp := types.Operation{
			ID:              string(opID),
			OperationType:   types.OperationTypeFromXDR(op.OperationType()),
			OperationXDR:    xdrOpStr,
			LedgerCreatedAt: ledgerCreatedAt,
			IngestedAt:      now,
			TxHash:          txHash,
		}

		for _, xdrParticipant := range dedupeParticipants(p) {
			participant := Participant(xdrParticipant.Address())
			allOpsParticipants[participant] = append(allOpsParticipants[participant], dbOp)
		}
	}

	return allOpsParticipants, nil
}

func (p *ParticipantsProcessor) ProcessTransaction(lcm xdr.LedgerCloseMeta, transaction ingest.LedgerTransaction) error {
	if err := p.addOperationsParticipants(lcm.LedgerSequence(), transaction); err != nil {
		return err
	}

	if err := p.addTransactionParticipants(lcm.LedgerSequence(), transaction); err != nil {
		return err
	}

	return nil
}

func directParticipantsForTransaction(transaction ingest.LedgerTransaction) (participantStrs []Participant, err error) {
	participants := []xdr.AccountId{
		transaction.Envelope.SourceAccount().ToAccountId(),
	}
	if transaction.Envelope.IsFeeBump() {
		participants = append(participants, transaction.Envelope.FeeBumpAccount().ToAccountId())
	}

	p, err := participantsForMeta(transaction.UnsafeMeta)
	if err != nil {
		return nil, fmt.Errorf("identifying participants for meta: %w", err)
	}
	participants = append(participants, p...)

	p, err = participantsForChanges(transaction.FeeChanges)
	if err != nil {
		return nil, fmt.Errorf("identifying participants for changes: %w", err)
	}
	participants = append(participants, p...)
	participants = dedupeParticipants(participants)

	participantStrs = make([]Participant, len(participants))
	for _, p := range participants {
		participantStrs = append(participantStrs, Participant(p.Address()))
	}

	return participantStrs, nil
}

func participantsForTransaction(
	sequence uint32,
	transaction ingest.LedgerTransaction,
	opsByParticipant map[Participant][]types.Operation,
) (txsByParticipant map[Participant][]types.Transaction, err error) {
	txDirectParticipants, err := directParticipantsForTransaction(transaction)
	if err != nil {
		return nil, fmt.Errorf("could not determine participants for transaction: %w", err)
	}

	opsParticipants := make([]Participant, 0, len(opsByParticipant))
	for k := range opsByParticipant {
		opsParticipants = append(opsParticipants, k)
	}

	participants := make([]Participant, 0, len(txDirectParticipants)+len(opsParticipants))
	participants = append(participants, txDirectParticipants...)
	participants = append(participants, opsParticipants...)
	participants = DedupeComparable(participants)

	envelopeXDR, err := xdr.MarshalBase64(transaction.Envelope)
	if err != nil {
		return nil, errors.Wrapf(err, "marshalling transaction envelope")
	}

	resultXDR, err := xdr.MarshalBase64(transaction.Result)
	if err != nil {
		return nil, errors.Wrapf(err, "marshalling transaction result")
	}

	metaXDR, err := xdr.MarshalBase64(transaction.UnsafeMeta)
	if err != nil {
		return nil, errors.Wrapf(err, "marshalling transaction meta")
	}

	now := time.Now()
	txDB := types.Transaction{
		Hash:            transaction.Hash.HexString(),
		LedgerCreatedAt: transaction.Ledger.ClosedAt(),
		IngestedAt:      now,
		EnvelopeXDR:     envelopeXDR,
		ResultXDR:       resultXDR,
		MetaXDR:         metaXDR,
		LedgerNumber:    transaction.LedgerVersion,
	}
	for _, participant := range participants {
		txsByParticipant[participant] = append(txsByParticipant[participant], txDB)
	}

	return txsByParticipant, nil
}

// dedupeParticipants remove any duplicate ids from `in`
func dedupeParticipants(in []xdr.AccountId) []xdr.AccountId {
	if len(in) <= 1 {
		return in
	}
	sort.Slice(in, func(i, j int) bool {
		return in[i].Address() < in[j].Address()
	})
	insert := 1
	for cur := 1; cur < len(in); cur++ {
		if in[cur].Equals(in[cur-1]) {
			continue
		}
		if insert != cur {
			in[insert] = in[cur]
		}
		insert++
	}
	return in[:insert]
}

// Dedupe removes duplicate elements from a slice while preserving order.
// It works with any comparable type.
func DedupeComparable[T comparable](slice []T) []T {
	if len(slice) == 0 {
		return slice
	}

	// Create a map to track seen elements
	seen := make(map[T]struct{}, len(slice))
	result := make([]T, 0, len(slice))

	// Iterate through the slice and only add elements we haven't seen before
	for _, item := range slice {
		if _, exists := seen[item]; !exists {
			seen[item] = struct{}{}
			result = append(result, item)
		}
	}

	return result
}
