package processors

import (
	"crypto/sha256"
	"errors"
	"fmt"

	set "github.com/deckarep/golang-set/v2"
	"github.com/stellar/go/ingest"
	operation_processor "github.com/stellar/go/processors/operation"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/xdr"

	"github.com/stellar/wallet-backend/internal/utils"
)

var ErrNotSorobanOperation = errors.New("not a soroban operation")

// calculateContractID calculates the contract ID for a wallet creation transaction based on the network passphrase, deployer account and salt.
//
// More info: https://developers.stellar.org/docs/build/smart-contracts/example-contracts/deployer#how-it-works
func calculateContractID(networkPassphrase string, fromAddress xdr.ContractIdPreimageFromAddress) (string, error) {
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

// contractIDForInvokeHostFunctionOp returns the contract ID for an InvokeHostFunction operation:
// - It returns an error ErrNotSorobanOperation if the operation is not a Soroban operation.
// - It returns the host's contract ID for InvokeContract operations.
// - It calculates the contractID based on the network, salt and deployer, for CreateContract (V1 & V2) operations.
func contractIDForInvokeHostFunctionOp(networkPassphrase string, invokeHostFunctionOp xdr.InvokeHostFunctionOp) (string, error) {
	switch invokeHostFunctionOp.HostFunction.Type {
	case xdr.HostFunctionTypeHostFunctionTypeInvokeContract: // InvokeHostFunction->InvokeContract
		contractIDHash := invokeHostFunctionOp.HostFunction.MustInvokeContract().ContractAddress.ContractId
		return strkey.MustEncode(strkey.VersionByteContract, contractIDHash[:]), nil

	case xdr.HostFunctionTypeHostFunctionTypeCreateContract, xdr.HostFunctionTypeHostFunctionTypeCreateContractV2:
		var preimage xdr.ContractIdPreimage
		if invokeHostFunctionOp.HostFunction.Type == xdr.HostFunctionTypeHostFunctionTypeCreateContract {
			preimage = invokeHostFunctionOp.HostFunction.MustCreateContract().ContractIdPreimage
		} else {
			preimage = invokeHostFunctionOp.HostFunction.MustCreateContractV2().ContractIdPreimage
		}

		switch preimage.Type {
		case xdr.ContractIdPreimageTypeContractIdPreimageFromAddress:
			return calculateContractID(networkPassphrase, preimage.MustFromAddress())

		case xdr.ContractIdPreimageTypeContractIdPreimageFromAsset:
			fromAsset := preimage.MustFromAsset()
			assetContractID, err := fromAsset.ContractID(networkPassphrase)
			if err != nil {
				return "", fmt.Errorf("getting asset contract ID: %w", err)
			}
			return strkey.MustEncode(strkey.VersionByteContract, assetContractID[:]), nil
		}

	default:
		break
	}

	return "", nil
}

// contractIDFromContractDataOrAccount returns either the contract ID or the account ID for a contract data or account ledger key.
// It returns an empty string and false if the ledger key is not a contract data or account ledger key.
func contractIDFromContractDataOrAccount(ledgerKey xdr.LedgerKey) (string, bool) {
	switch ledgerKey.Type {
	case xdr.LedgerEntryTypeContractData:
		contractData := ledgerKey.MustContractData()
		if contractIDHash, ok := contractData.Contract.GetContractId(); ok {
			contractIDByte, err := contractIDHash.MarshalBinary()
			if err != nil {
				panic(err)
			}
			return strkey.MustEncode(strkey.VersionByteContract, contractIDByte), true
		}

	case xdr.LedgerEntryTypeAccount:
		account := ledgerKey.MustAccount()
		return account.AccountId.Address(), true

	default:
		break
	}

	return "", false
}

// contractIDsFromTxEnvelope returns the contract IDs for a transaction envelope.
// It returns an empty string if the transaction envelope is not a Soroban transaction envelope.
func contractIDsFromTxEnvelope(t *ingest.LedgerTransaction) set.Set[string] {
	v1Envelope, ok := t.GetTransactionV1Envelope()
	if !ok {
		return nil
	}

	contractIDs := set.NewSet[string]()
	readWrite := v1Envelope.Tx.Ext.SorobanData.Resources.Footprint.ReadWrite
	readOnly := v1Envelope.Tx.Ext.SorobanData.Resources.Footprint.ReadOnly
	for _, ledgerKey := range append(readWrite, readOnly...) {
		contractID, ok := contractIDFromContractDataOrAccount(ledgerKey)
		if ok && contractID != "" {
			contractIDs.Add(contractID)
		}
	}

	return contractIDs
}

func contractIDsForSorobanOperation(op operation_processor.TransactionOperationWrapper) (set.Set[string], error) {
	if !op.Transaction.IsSorobanTx() {
		return nil, ErrNotSorobanOperation
	}

	switch op.Operation.Body.Type {
	case xdr.OperationTypeInvokeHostFunction:
		invokeHostOp := op.Operation.Body.MustInvokeHostFunctionOp()
		contractID, err := contractIDForInvokeHostFunctionOp(op.Network, invokeHostOp)
		if err != nil {
			return nil, fmt.Errorf("getting contract ID for invoke host function operation: %w", err)
		} else if contractID == "" {
			return nil, nil
		}
		return set.NewSet(contractID), nil

	case xdr.OperationTypeExtendFootprintTtl, xdr.OperationTypeRestoreFootprint:
		return contractIDsFromTxEnvelope(&op.Transaction), nil

	default:
		break
	}

	return nil, nil
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

	for scvAddress := range scvAddresses.Iter() {
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

	// 2. ContractIDs
	contractIDs, err := contractIDsForSorobanOperation(op)
	if err != nil {
		return nil, fmt.Errorf("getting contract ID for soroban operation: %w", err)
	} else if contractIDs != nil && contractIDs.Cardinality() > 0 {
		participants = participants.Union(contractIDs)
	}

	// 3. Return early if the operation is not an InvokeHostFunction operation
	if op.Operation.Body.Type != xdr.OperationTypeInvokeHostFunction {
		return participants, nil
	}
	invokeHostFunctionOp := op.Operation.Body.MustInvokeHostFunctionOp()

	// 4. Invocation Args
	args := argsForInvokeHostFunctionOp(invokeHostFunctionOp)
	if len(args) > 0 {
		var argParticipants set.Set[string]
		argParticipants, err = participantsForScVal(scVecToScVal(args))
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

func argsForInvokeHostFunctionOp(invokeHostFunctionOp xdr.InvokeHostFunctionOp) []xdr.ScVal {
	switch invokeHostFunctionOp.HostFunction.Type {
	case xdr.HostFunctionTypeHostFunctionTypeInvokeContract:
		invokeContractOp := invokeHostFunctionOp.HostFunction.MustInvokeContract()
		args := invokeContractOp.Args

		return args

	case xdr.HostFunctionTypeHostFunctionTypeCreateContractV2:
		createContractV2Op := invokeHostFunctionOp.HostFunction.MustCreateContractV2()
		return createContractV2Op.ConstructorArgs

	default:
		return nil
	}
}

func scVecToScVal(scVec xdr.ScVec) xdr.ScVal {
	return xdr.ScVal{Type: xdr.ScValTypeScvVec, Vec: utils.PointOf(&scVec)}
}
