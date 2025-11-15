package processors

import (
	"fmt"

	set "github.com/deckarep/golang-set/v2"
	"github.com/stellar/go/ingest"

	"github.com/stellar/go/xdr"
)

type ParticipantsProcessor struct {
	networkPassphrase string
}

func NewParticipantsProcessor(networkPassphrase string) *ParticipantsProcessor {
	return &ParticipantsProcessor{
		networkPassphrase: networkPassphrase,
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

// participantsForMeta identifies the participants involved in a transaction meta by looping through the operations' changes.
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

func (p *ParticipantsProcessor) GetTransactionParticipants(transaction ingest.LedgerTransaction) (set.Set[string], error) {
	// 1. Get direct participants involved in the transaction
	participants := []xdr.AccountId{
		transaction.Envelope.SourceAccount().ToAccountId(), // in case of a fee bump, this is the innerTx source account
	}
	if transaction.Envelope.IsFeeBump() {
		participants = append(participants, transaction.Envelope.FeeBumpAccount().ToAccountId())
	}

	feeParticipants, err := participantsForChanges(transaction.FeeChanges)
	if err != nil {
		return nil, fmt.Errorf("identifying participants for changes: %w", err)
	}
	participants = append(participants, feeParticipants...)

	// 1.1. Get participants involved in the transaction meta (if successful)
	if transaction.Result.Successful() {
		metaParticipants, metaErr := participantsForMeta(transaction.UnsafeMeta)
		if metaErr != nil {
			return nil, fmt.Errorf("identifying participants for meta: %w", metaErr)
		}
		participants = append(participants, metaParticipants...)
	}

	// 2. Push transaction and participants to data bundle
	participantsSet := set.NewSet[string]()
	for _, xdrParticipant := range participants {
		participantsSet.Add(xdrParticipant.Address())
	}

	return participantsSet, nil
}

type OperationParticipants struct {
	OpWrapper    *TransactionOperationWrapper
	Participants set.Set[string]
}

// GetOperationsParticipants returns a map of operation ID to its participants.
func (p *ParticipantsProcessor) GetOperationsParticipants(transaction ingest.LedgerTransaction) (map[int64]OperationParticipants, error) {
	if !transaction.Successful() {
		return nil, nil
	}

	ledgerSequence := transaction.Ledger.LedgerSequence()
	operationsParticipants := map[int64]OperationParticipants{}

	for opi, xdrOp := range transaction.Envelope.Operations() {
		// 1. Build op wrapper, so we can use its methods
		op := &TransactionOperationWrapper{
			Index:          uint32(opi),
			Transaction:    transaction,
			Operation:      xdrOp,
			LedgerSequence: ledgerSequence,
			Network:        p.networkPassphrase,
		}
		opID := op.ID()

		// 2. Get participants for the operation
		participants, err := p.GetOperationParticipants(op)
		if err != nil {
			return nil, fmt.Errorf("getting operation %d participants: %w", opID, err)
		} else if participants.IsEmpty() {
			continue
		}

		// 3. Add participants to the map
		if _, ok := operationsParticipants[opID]; !ok {
			operationsParticipants[opID] = OperationParticipants{
				OpWrapper:    op,
				Participants: participants,
			}
		} else {
			operationsParticipants[opID].Participants.Append(participants.ToSlice()...)
		}
	}

	return operationsParticipants, nil
}

// GetOperationParticipants returns the participants for a transaction operation.
// In case of a Soroban operation, it calls the participantsForSorobanOp function to get the Soroban participants.
func (p *ParticipantsProcessor) GetOperationParticipants(op *TransactionOperationWrapper) (set.Set[string], error) {
	// 1. Calculate participants using the default stellar/go methods that only look for G-accounts
	participantsAccountIDs, err := op.Participants()
	if err != nil {
		return nil, fmt.Errorf("reading operation %d participants: %w", op.ID(), err)
	}
	participants := set.NewSet[string]()
	for _, accountID := range participantsAccountIDs {
		participants.Add(accountID.Address())
	}

	// 1.1. Return early if the operation is not a Soroban operation
	if !op.Transaction.IsSorobanTx() {
		return participants, nil
	}

	// 2. Get Soroban participants
	sorobanParticipants, err := participantsForSorobanOp(op)
	if err != nil {
		return nil, fmt.Errorf("getting soroban participants: %w", err)
	}

	return participants.Union(sorobanParticipants), nil
}
