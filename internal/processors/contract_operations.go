package processors

import (
	"errors"
	"fmt"

	set "github.com/deckarep/golang-set/v2"
	operation_processor "github.com/stellar/go/processors/operation"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/xdr"

	"github.com/stellar/wallet-backend/internal/utils"
)

var ErrNotSorobanOperation = errors.New("not a soroban operation")

func contractIDForSorobanOperation(op operation_processor.TransactionOperationWrapper) (string, error) {
	if !op.Transaction.IsSorobanTx() {
		return "", ErrNotSorobanOperation
	}

	// 👋 This shouldn't be needed, but it's a workaround for a potential bug where `ContractIdFromTxEnvelope` returns
	// an empty string for when there are a mix of Soroban and non-Soroban ledger entries. More info in the internal
	// discussion: https://stellarfoundation.slack.com/archives/C02B04RMK/p1751935606699499.
	if op.Operation.Body.Type == xdr.OperationTypeInvokeHostFunction {
		invokeHostOp := op.Operation.Body.MustInvokeHostFunctionOp()
		if invokeHostOp.HostFunction.Type == xdr.HostFunctionTypeHostFunctionTypeInvokeContract {
			invokeContract := invokeHostOp.HostFunction.MustInvokeContract()
			contractIDHash := invokeContract.ContractAddress.ContractId
			if contractIDHash != nil {
				return strkey.MustEncode(strkey.VersionByteContract, contractIDHash[:]), nil
			}
		}
	}

	contractID, ok := op.Transaction.ContractIdFromTxEnvelope()
	if ok && contractID != "" {
		return contractID, nil
	}

	return "", nil
}

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

// participantsForAuthEntries extracts all participant addresses from a SorobanAuthorizationEntry slice.
func participantsForAuthEntries(authEntries []xdr.SorobanAuthorizationEntry) (set.Set[string], error) {
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

// participantsForSorobanOp returns the participants for a Soroban contract operation.
// It returns an error ErrNotSorobanOperation if the operation is not a Soroban operation.
func participantsForSorobanOp(op operation_processor.TransactionOperationWrapper) (set.Set[string], error) {
	if !op.Transaction.IsSorobanTx() {
		return nil, ErrNotSorobanOperation
	}

	// 1. Source Account
	participants := set.NewSet[string]()
	participants.Add(op.SourceAccount().Address())

	// 2. ContractID
	contractID, err := contractIDForSorobanOperation(op)
	if err != nil {
		return nil, fmt.Errorf("getting contract ID for soroban operation: %w", err)
	}
	participants.Add(contractID)

	// 3. Return early if the operation is not an InvokeHostFunction operation
	if op.Operation.Body.Type != xdr.OperationTypeInvokeHostFunction {
		return participants, nil
	}
	invokeHostFunctionOp := op.Operation.Body.MustInvokeHostFunctionOp()

	// 4. InvokeHostFunction.Args
	if invokeHostFunctionOp.HostFunction.Type == xdr.HostFunctionTypeHostFunctionTypeInvokeContract {
		var argsScVec xdr.ScVec = invokeHostFunctionOp.HostFunction.MustInvokeContract().Args
		var argParticipants set.Set[string]
		argParticipants, err = participantsForScVal(xdr.ScVal{Type: xdr.ScValTypeScvVec, Vec: utils.PointOf(&argsScVec)})
		if err != nil {
			return nil, fmt.Errorf("getting scVal participants: %w", err)
		}
		participants = participants.Union(argParticipants)
	}

	// 5. InvokeHostFunction.Auth
	authEntriesParticipants, err := participantsForAuthEntries(invokeHostFunctionOp.Auth)
	if err != nil {
		return nil, fmt.Errorf("getting authEntry participants: %w", err)
	}
	participants = participants.Union(authEntriesParticipants)

	return participants, nil
}
