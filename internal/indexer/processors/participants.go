package processors

import (
	"fmt"

	set "github.com/deckarep/golang-set/v2"
	"github.com/stellar/go-stellar-sdk/ingest"

	"github.com/stellar/go-stellar-sdk/xdr"
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

// addChangedAccounts adds to participants every account whose entry was created,
// removed, or updated with different data. stellar-core also emits an entry it
// only loaded for write (every Soroban read-write footprint key, for example) as
// an update with identical data, and a submitter chooses the footprint, so an
// update counts only when the entry's data differs. LastModifiedLedgerSeq is
// outside the compared data because it changes on every write.
func addChangedAccounts(changes []ingest.Change, participants set.Set[string]) error {
	for _, c := range changes {
		if c.Type != xdr.LedgerEntryTypeAccount {
			continue
		}
		switch {
		case c.Pre == nil && c.Post != nil:
			participants.Add(c.Post.Data.MustAccount().AccountId.Address())
		case c.Pre != nil && c.Post == nil:
			participants.Add(c.Pre.Data.MustAccount().AccountId.Address())
		case c.Pre != nil && c.Post != nil:
			same, err := xdr.Equals(c.Pre.Data, c.Post.Data)
			if err != nil {
				return fmt.Errorf("comparing account entry data: %w", err)
			}
			if !same {
				participants.Add(c.Post.Data.MustAccount().AccountId.Address())
			}
		}
	}
	return nil
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

	// Accounts whose entry an operation changed are operation participants
	// (GetOperationParticipants); the indexer buffer records every operation
	// participant on the transaction as well.

	// 2. Push transaction and participants to data bundle
	participantsSet := set.NewThreadUnsafeSet[string]()
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
// This function processes ALL operations regardless of transaction success.
func (p *ParticipantsProcessor) GetOperationsParticipants(transaction ingest.LedgerTransaction) (map[int64]OperationParticipants, error) {
	ledgerSequence := transaction.Ledger.LedgerSequence()
	ledgerClosed := transaction.Ledger.ClosedAt()
	ops := transaction.Envelope.Operations()
	operationsParticipants := make(map[int64]OperationParticipants, len(ops))

	// Every operation's wrapper shares one transaction.
	tx := &transaction

	for opi, xdrOp := range ops {
		// 1. Build op wrapper, so we can use its methods
		op := &TransactionOperationWrapper{
			Index:          uint32(opi),
			Transaction:    tx,
			Operation:      xdrOp,
			LedgerSequence: ledgerSequence,
			LedgerClosed:   ledgerClosed,
		}
		opID := op.ID()

		// 2. Get participants for the operation
		participants, err := p.GetOperationParticipants(op)
		if err != nil {
			return nil, fmt.Errorf("getting operation %d participants: %w", opID, err)
		} else if participants.IsEmpty() {
			continue
		}

		// 3. Add participants to the map. opID embeds the operation index, so every iteration
		// writes a distinct key and no merge with an existing entry is possible.
		operationsParticipants[opID] = OperationParticipants{
			OpWrapper:    op,
			Participants: participants,
		}
	}

	return operationsParticipants, nil
}

// GetOperationParticipants returns the participants for a transaction operation.
// In case of a Soroban operation, it calls the participantsForSorobanOp function to get the Soroban participants.
func (p *ParticipantsProcessor) GetOperationParticipants(op *TransactionOperationWrapper) (set.Set[string], error) {
	// A failed transaction changes no state beyond its fee. Payment destinations,
	// signer keys and auth entries are unverified submitter input, so only the accounts
	// whose state changed are attributed. The set is never empty, so the operation row
	// still exists for the submitter.
	if !op.Transaction.Successful() {
		participants := set.NewThreadUnsafeSet(op.Transaction.Envelope.SourceAccount().ToAccountId().Address())
		if op.Transaction.Envelope.IsFeeBump() {
			participants.Add(op.Transaction.Envelope.FeeBumpAccount().ToAccountId().Address())
		}
		return participants, nil
	}

	// 1. Participants from the operation body (G-accounts named by classic operations)
	participantsAccountIDs, err := op.Participants()
	if err != nil {
		return nil, fmt.Errorf("reading operation %d participants: %w", op.ID(), err)
	}
	participants := set.NewThreadUnsafeSetWithSize[string](len(participantsAccountIDs))
	for _, accountID := range participantsAccountIDs {
		participants.Add(accountID.Address())
	}

	// 2. Accounts whose entry the operation changed, read from the executed meta.
	changes, err := op.Changes()
	if err != nil {
		return nil, fmt.Errorf("reading operation %d changes: %w", op.ID(), err)
	}
	if err := addChangedAccounts(changes, participants); err != nil {
		return nil, fmt.Errorf("getting operation %d changed accounts: %w", op.ID(), err)
	}

	if !op.Transaction.IsSorobanTx() {
		return participants, nil
	}

	// 3. Soroban participants, from the operation body and the executed meta.
	if err := participantsForSorobanOp(op, participants); err != nil {
		return nil, fmt.Errorf("getting soroban participants: %w", err)
	}
	return participants, nil
}
