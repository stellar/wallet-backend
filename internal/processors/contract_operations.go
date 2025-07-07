package processors

import (
	"crypto/sha256"
	"fmt"

	set "github.com/deckarep/golang-set/v2"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/xdr"
)

// scAddressesForScVal returns all ScAddresses that can be found in a ScVal. If the ScVal is a ScvVec or ScvMap,
// it recursively iterates over its inner values to find all existing ScAddresses.
func scAddressesForScVal(scVal xdr.ScVal) set.Set[xdr.ScAddress] {
	scAddresses := set.NewSet[xdr.ScAddress]()
	switch scVal.Type {
	case xdr.ScValTypeScvAddress:
		scAddresses.Add(scVal.MustAddress())

	case xdr.ScValTypeScvVec:
		for _, innerVal := range *scVal.MustVec() {
			scAddresses = scAddresses.Union(scAddressesForScVal(innerVal))
		}

	case xdr.ScValTypeScvMap:
		for _, mapEntry := range *scVal.MustMap() {
			scAddresses = scAddresses.Union(scAddressesForScVal(mapEntry.Key))
			scAddresses = scAddresses.Union(scAddressesForScVal(mapEntry.Val))
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

// participantsForScVal extracts all participant addresses from an ScVal.
func participantsForScVal(scVal xdr.ScVal) (set.Set[string], error) {
	scvAddresses := scAddressesForScVal(scVal)
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

// participantsForAuthEntry extracts all participant addresses from a SorobanAuthorizationEntry slice.
func participantsForAuthEntry(authEntries []xdr.SorobanAuthorizationEntry) (set.Set[string], error) {
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
	argParticipants, err := participantsForScVal(argsScVal)
	if err != nil {
		return nil, fmt.Errorf("getting scVal participants: %w", err)
	}
	participants = participants.Union(argParticipants)

	// 4. AuthEntries
	authEntriesParticipants, err := participantsForAuthEntry(invokeHostFunctionOp.Auth)
	if err != nil {
		return nil, fmt.Errorf("getting authEntry participants: %w", err)
	}
	participants = participants.Union(authEntriesParticipants)

	return participants.ToSlice(), nil
}

// func GetOperationParticipants(op operation_processor.TransactionOperationWrapper) (set.Set[string], error) {
// 	accountIDParticipants, err := op.Participants()
// 	if err != nil {
// 		return nil, fmt.Errorf("reading operation %d participants: %w", op.ID(), err)
// 	}
// 	participants := set.NewSet[string]()
// 	for _, accountID := range accountIDParticipants {
// 		participants.Add(accountID.Address())
// 	}

// 	sorobanOperationTypes := []xdr.OperationType{
// 		xdr.OperationTypeInvokeHostFunction,
// 		xdr.OperationTypeExtendFootprintTtl,
// 		xdr.OperationTypeRestoreFootprint,
// 	}
// 	if slices.Contains(sorobanOperationTypes, op.OperationType()) {
// 		contractOpParticipants, err := GetContractOpParticipants(op.Operation, op.Transaction)
// 		if err != nil {
// 			return nil, fmt.Errorf("getting participants for soroban operation %s: %w", op.OperationType(), err)
// 		}
// 		participants = participants.Union(contractOpParticipants)
// 	}

// 	return participants, nil
// }

// func GetContractOpParticipants(op xdr.Operation, tx ingest.LedgerTransaction) (set.Set[string], error) {
// 	// 1. Source Account
// 	participants := set.NewSet[string]()
// 	if op.SourceAccount != nil {
// 		participants.Add(op.SourceAccount.Address())
// 	} else {
// 		participants.Add(tx.Envelope.SourceAccount().ToAccountId().Address())
// 	}

// 	h := tx.Hash.HexString()
// 	fmt.Println("tx hash", h)

// 	invokeHostFunctionOp := op.Body.MustInvokeHostFunctionOp()

// 	// 2. ContractID
// 	contractID := invokeHostFunctionOp.HostFunction.InvokeContract.ContractAddress.ContractId
// 	contractIDStr := strkey.MustEncode(strkey.VersionByteContract, contractID[:])
// 	participants.Add(contractIDStr)

// 	// 3. Args
// 	if op.Body.Type == xdr.OperationTypeInvokeHostFunction {
// 		var argsScVec xdr.ScVec = invokeHostFunctionOp.HostFunction.InvokeContract.Args
// 		argsScVal, err := xdr.NewScVal(xdr.ScValTypeScvVec, &argsScVec)
// 		if err != nil {
// 			return nil, fmt.Errorf("creating NewScVal for the args vector: %w", err)
// 		}
// 		argParticipants, err := participantsForScVal(argsScVal)
// 		if err != nil {
// 			return nil, fmt.Errorf("getting scVal participants: %w", err)
// 		}
// 		participants = participants.Union(argParticipants)
// 	}

// 	// 4. AuthEntries
// 	if op.Body.Type == xdr.OperationTypeInvokeHostFunction {
// 		authEntriesParticipants, err := participantsForAuthEntry(invokeHostFunctionOp.Auth)
// 		if err != nil {
// 			return nil, fmt.Errorf("getting authEntry participants: %w", err)
// 		}
// 		participants = participants.Union(authEntriesParticipants)
// 	}

// 	return participants, nil
// }

func contractIDForOperation(networkPassphrase string, op xdr.Operation) (string, error) {
	if op.Body.Type != xdr.OperationTypeInvokeHostFunction {
		return "", nil
	}
	invokeHostFunctionOp := op.Body.MustInvokeHostFunctionOp()

	switch invokeHostFunctionOp.HostFunction.Type {
	case xdr.HostFunctionTypeHostFunctionTypeInvokeContract: // InvokeHostFunction->InvokeContract
		contractIDHash := invokeHostFunctionOp.HostFunction.MustInvokeContract().ContractAddress.ContractId
		return strkey.MustEncode(strkey.VersionByteContract, contractIDHash[:]), nil

	case xdr.HostFunctionTypeHostFunctionTypeCreateContract, xdr.HostFunctionTypeHostFunctionTypeCreateContractV2:
		var preimage xdr.ContractIdPreimage
		switch invokeHostFunctionOp.HostFunction.Type {
		case xdr.HostFunctionTypeHostFunctionTypeCreateContract:
			preimage = invokeHostFunctionOp.HostFunction.MustCreateContract().ContractIdPreimage
		case xdr.HostFunctionTypeHostFunctionTypeCreateContractV2:
			preimage = invokeHostFunctionOp.HostFunction.MustCreateContractV2().ContractIdPreimage
		default:
			return "", nil
		}

		if preimage.Type == xdr.ContractIdPreimageTypeContractIdPreimageFromAddress {
			return calculateContractID(networkPassphrase, preimage.MustFromAddress())
		}

	default:
		break
	}

	return "", nil
}

// calculateContractID calculates the contract ID for a wallet creation transaction based on the network passphrase, deployer account and salt.
//
// More info: https://developers.stellar.org/docs/build/smart-contracts/example-contracts/deployer#how-it-works
func calculateContractID(
	networkPassphrase string,
	fromAddress xdr.ContractIdPreimageFromAddress,
) (string, error) {
	networkHash := xdr.Hash(sha256.Sum256([]byte(networkPassphrase)))

	hashIDPreimage := xdr.HashIdPreimage{
		Type: xdr.EnvelopeTypeEnvelopeTypeContractId,
		ContractId: &xdr.HashIdPreimageContractId{
			NetworkId: networkHash,
			ContractIdPreimage: xdr.ContractIdPreimage{
				Type:        xdr.ContractIdPreimageTypeContractIdPreimageFromAddress,
				FromAddress: &fromAddress,
			},
		},
	}

	preimageXDR, err := hashIDPreimage.MarshalBinary()
	if err != nil {
		return "", fmt.Errorf("marshaling preimage: %w", err)
	}

	contractIDHash := sha256.Sum256(preimageXDR)
	contractID, err := strkey.Encode(strkey.VersionByteContract, contractIDHash[:])
	if err != nil {
		return "", fmt.Errorf("encoding contract ID: %w", err)
	}

	return contractID, nil
}
