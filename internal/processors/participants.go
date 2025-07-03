package processors

import (
	"fmt"

	set "github.com/deckarep/golang-set/v2"
	"github.com/stellar/go/ingest"
	operation_processor "github.com/stellar/go/processors/operation"
	"github.com/stellar/go/strkey"

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
	Operation    xdr.Operation
	Participants set.Set[string]
	OperationIdx uint32
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
		op := operation_processor.TransactionOperationWrapper{
			Index:          uint32(opi),
			Transaction:    transaction,
			Operation:      xdrOp,
			LedgerSequence: ledgerSequence,
			Network:        p.networkPassphrase,
		}
		opID := op.ID()

		// 2. Get participants for the operation
		participants, err := op.Participants()
		if err != nil {
			return nil, fmt.Errorf("reading operation %d participants: %w", opID, err)
		}
		if len(participants) == 0 {
			continue
		}

		// 3. Add participants to the map
		if _, ok := operationsParticipants[opID]; !ok {
			operationsParticipants[opID] = OperationParticipants{
				Operation:    xdrOp,
				Participants: set.NewSet[string](),
				OperationIdx: uint32(opi),
			}
		}
		for _, participant := range participants {
			operationsParticipants[opID].Participants.Add(participant.Address())
		}
	}

	return operationsParticipants, nil
}

// getScValScvAddresses returns all ScAddresses that can be found in a ScVal. If the ScVal is a ScvVec or ScvMap,
// it recursively iterates over its inner values to find all existing ScAddresses.
func getScValScvAddresses(scVal xdr.ScVal) set.Set[xdr.ScAddress] {
	scAddresses := set.NewSet[xdr.ScAddress]()
	switch scVal.Type {
	case xdr.ScValTypeScvAddress:
		scAddresses.Add(scVal.MustAddress())

	case xdr.ScValTypeScvVec:
		for _, innerVal := range *scVal.MustVec() {
			scAddresses = scAddresses.Union(getScValScvAddresses(innerVal))
		}

	case xdr.ScValTypeScvMap:
		for _, mapEntry := range *scVal.MustMap() {
			scAddresses = scAddresses.Union(getScValScvAddresses(mapEntry.Key))
			scAddresses = scAddresses.Union(getScValScvAddresses(mapEntry.Val))
		}

	case xdr.ScValTypeScvBytes:
		// xdr.ScValTypeScvBytes is sometimes used to store either a contractID or a public key:
		b := scVal.MustBytes()
		if len(b) != 32 {
			break
		}

		if address, err := strkey.Encode(strkey.VersionByteAccountID, b); err == nil {
			accountID := xdr.MustAddress(address)
			scAddresses.Add(xdr.ScAddress{
				Type:      xdr.ScAddressTypeScAddressTypeAccount,
				AccountId: &accountID,
			})
		}
		if _, err := strkey.Encode(strkey.VersionByteContract, b); err == nil {
			contractID := xdr.Hash(b)
			scAddresses.Add(xdr.ScAddress{
				Type:       xdr.ScAddressTypeScAddressTypeContract,
				ContractId: &contractID,
			})
		}

	default:
		break
	}

	return scAddresses
}

// getScValParticipants extracts all participant addresses from an ScVal.
func getScValParticipants(scVal xdr.ScVal) (set.Set[string], error) {
	scvAddresses := getScValScvAddresses(scVal)
	participants := set.NewSet[string]()

	for scvAddress := range scvAddresses.Iterator().C {
		scAddressStr, err := scvAddress.String()
		if err != nil {
			return nil, fmt.Errorf("converting ScAddress to string: %w", err)
		}
		participants.Add(scAddressStr)
	}

	return participants, nil
}

// getAuthEntryParticipants extracts all participant addresses from a SorobanAuthorizationEntry slice.
func getAuthEntryParticipants(authEntries []xdr.SorobanAuthorizationEntry) (set.Set[string], error) {
	participants := set.NewSet[string]()
	for _, authEntry := range authEntries {
		switch authEntry.Credentials.Type {
		case xdr.SorobanCredentialsTypeSorobanCredentialsAddress:
			participant, err := authEntry.Credentials.MustAddress().Address.String()
			if err != nil {
				return nil, fmt.Errorf("converting ScAddress to string: %w", err)
			}
			participants.Add(participant)
		default:
			continue
		}
	}

	return participants, nil
}

func GetContractOpParticipants(op xdr.Operation, tx ingest.LedgerTransaction) ([]string, error) {
	// 1. Source Account
	participants := set.NewSet[string]()
	if op.SourceAccount != nil {
		participants.Add(op.SourceAccount.Address())
	} else {
		participants.Add(tx.Envelope.SourceAccount().ToAccountId().Address())
	}

	invokeHostFunctionOp := op.Body.MustInvokeHostFunctionOp()

	// 2. ContractID
	contractID := invokeHostFunctionOp.HostFunction.InvokeContract.ContractAddress.ContractId
	contractIDStr := strkey.MustEncode(strkey.VersionByteContract, contractID[:])
	participants.Add(contractIDStr)

	// 3. Args
	var argsScVec xdr.ScVec = invokeHostFunctionOp.HostFunction.InvokeContract.Args
	argsScVal, err := xdr.NewScVal(xdr.ScValTypeScvVec, &argsScVec)
	if err != nil {
		return nil, fmt.Errorf("creating NewScVal for the args vector: %w", err)
	}
	argParticipants, err := getScValParticipants(argsScVal)
	if err != nil {
		return nil, fmt.Errorf("getting scVal participants: %w", err)
	}
	participants = participants.Union(argParticipants)

	// 4. AuthEntries
	authEntriesParticipants, err := getAuthEntryParticipants(invokeHostFunctionOp.Auth)
	if err != nil {
		return nil, fmt.Errorf("getting authEntry participants: %w", err)
	}
	participants = participants.Union(authEntriesParticipants)

	return participants.ToSlice(), nil
}
