package processors

import (
	"crypto/sha256"
	"errors"
	"fmt"

	set "github.com/deckarep/golang-set/v2"
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

func scVecToScVal(scVec xdr.ScVec) xdr.ScVal {
	return xdr.ScVal{Type: xdr.ScValTypeScvVec, Vec: utils.PointOf(&scVec)}
}

// participantsForSorobanOp returns the participants for a Soroban contract operation.
// It returns an error ErrNotSorobanOperation if the operation is not a Soroban operation.
func participantsForSorobanOp(op operation_processor.TransactionOperationWrapper) (set.Set[string], error) {
	if !op.Transaction.IsSorobanTx() {
		return nil, ErrNotSorobanOperation
	}

	participants := set.NewSet(op.SourceAccount().Address())

	switch op.Operation.Body.Type {
	case xdr.OperationTypeExtendFootprintTtl, xdr.OperationTypeRestoreFootprint:
		footprintOpProcessor := FootprintOpProcessor{op: &op}
		footprintOpParticipants, err := footprintOpProcessor.Participants()
		if err != nil {
			return nil, fmt.Errorf("getting footprint participants: %w", err)
		}
		participants = participants.Union(footprintOpParticipants)

	case xdr.OperationTypeInvokeHostFunction:
		invokeHostOp := op.Operation.Body.MustInvokeHostFunctionOp()

		switch invokeHostOp.HostFunction.Type {
		case xdr.HostFunctionTypeHostFunctionTypeCreateContract:
			createContractOpProcessor := CreateContractV1OpProcessor{op: &op}
			createContractOpParticipants, err := createContractOpProcessor.Participants()
			if err != nil {
				return nil, fmt.Errorf("getting create contract participants: %w", err)
			}
			participants = participants.Union(createContractOpParticipants)

		case xdr.HostFunctionTypeHostFunctionTypeCreateContractV2:
			createContractV2OpProcessor := CreateContractV2OpProcessor{op: &op}
			createContractV2OpParticipants, err := createContractV2OpProcessor.Participants()
			if err != nil {
				return nil, fmt.Errorf("getting create contract participants: %w", err)
			}
			participants = participants.Union(createContractV2OpParticipants)

		case xdr.HostFunctionTypeHostFunctionTypeInvokeContract:
			invokeContractOpProcessor := InvokeContractOpProcessor{op: &op}
			invokeContractOpParticipants, err := invokeContractOpProcessor.Participants()
			if err != nil {
				return nil, fmt.Errorf("getting invoke contract participants: %w", err)
			}
			participants = participants.Union(invokeContractOpParticipants)

		case xdr.HostFunctionTypeHostFunctionTypeUploadContractWasm:
			break
		}

	default:
		break
	}

	return participants, nil
}

func contractIDForPreimage(preimage xdr.ContractIdPreimage, networkPassphrase string) (string, error) {
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

	default:
		return "", fmt.Errorf("invalid contract id preimage type")
	}
}

type CreateContractV1OpProcessor struct {
	op *operation_processor.TransactionOperationWrapper
}

func (p *CreateContractV1OpProcessor) GetCreateContract() (xdr.CreateContractArgs, bool) {
	if p.op.OperationType() != xdr.OperationTypeInvokeHostFunction {
		return xdr.CreateContractArgs{}, false
	}

	invokeHostFunctionOp := p.op.Operation.Body.MustInvokeHostFunctionOp()
	if invokeHostFunctionOp.HostFunction.Type != xdr.HostFunctionTypeHostFunctionTypeCreateContract {
		return xdr.CreateContractArgs{}, false
	}

	return invokeHostFunctionOp.HostFunction.MustCreateContract(), true
}

var ErrInvalidOpType = errors.New("invalid operation type")

func (p *CreateContractV1OpProcessor) ContractID() (string, error) {
	createContractOp, ok := p.GetCreateContract()
	if !ok {
		return "", fmt.Errorf("not a create contract operation: %w", ErrInvalidOpType)
	}

	return contractIDForPreimage(createContractOp.ContractIdPreimage, p.op.Network)
}

func (p *CreateContractV1OpProcessor) Participants() (set.Set[string], error) {
	participants := set.NewSet(p.op.SourceAccount().Address())

	if contractID, err := p.ContractID(); err != nil {
		return nil, fmt.Errorf("getting contract ID: %w", err)
	} else if contractID != "" {
		participants.Add(contractID)
	}

	return participants, nil
}

type CreateContractV2OpProcessor struct {
	op *operation_processor.TransactionOperationWrapper
}

func (p *CreateContractV2OpProcessor) GetCreateContract() (xdr.CreateContractArgsV2, bool) {
	if p.op.OperationType() != xdr.OperationTypeInvokeHostFunction {
		return xdr.CreateContractArgsV2{}, false
	}

	invokeHostFunctionOp := p.op.Operation.Body.MustInvokeHostFunctionOp()
	if invokeHostFunctionOp.HostFunction.Type != xdr.HostFunctionTypeHostFunctionTypeCreateContractV2 {
		return xdr.CreateContractArgsV2{}, false
	}

	return invokeHostFunctionOp.HostFunction.MustCreateContractV2(), true
}

func (p *CreateContractV2OpProcessor) ContractID() (string, error) {
	createContractOp, ok := p.GetCreateContract()
	if !ok {
		return "", fmt.Errorf("not a create contract operation: %w", ErrInvalidOpType)
	}

	return contractIDForPreimage(createContractOp.ContractIdPreimage, p.op.Network)
}

func (p *CreateContractV2OpProcessor) ParticipantsFromArgs() (set.Set[string], error) {
	createContractOp, ok := p.GetCreateContract()
	if !ok {
		return nil, fmt.Errorf("not a create contract operation: %w", ErrInvalidOpType)
	}

	argsParticipants, err := participantsForScVal(scVecToScVal(createContractOp.ConstructorArgs))
	if err != nil {
		return nil, fmt.Errorf("getting constructor args participants: %w", err)
	}

	return argsParticipants, nil
}

func (p *CreateContractV2OpProcessor) Participants() (set.Set[string], error) {
	participants := set.NewSet(p.op.SourceAccount().Address())

	if contractID, err := p.ContractID(); err != nil {
		return nil, fmt.Errorf("getting contract ID: %w", err)
	} else if contractID != "" {
		participants.Add(contractID)
	}

	if argsParticipants, err := p.ParticipantsFromArgs(); err != nil {
		return nil, fmt.Errorf("getting constructor args participants: %w", err)
	} else {
		participants = participants.Union(argsParticipants)
	}

	return participants, nil
}

type FootprintOpProcessor struct {
	op *operation_processor.TransactionOperationWrapper
}

func (p *FootprintOpProcessor) ValidateType() bool {
	if p.op.OperationType() != xdr.OperationTypeExtendFootprintTtl && p.op.OperationType() != xdr.OperationTypeRestoreFootprint {
		return false
	}

	return true
}

// contractParticipantsFromLedgerKey returns either the contract ID or the account ID for a contract data or account ledger key.
// It returns an empty string and false if the ledger key is not a contract data or account ledger key.
func (p *FootprintOpProcessor) contractParticipantsFromLedgerKey(ledgerKey xdr.LedgerKey) (string, bool) {
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

// contractParticipantsFromTxSorobanData returns the contract IDs for a transaction envelope.
// It returns an empty string if the transaction envelope is not a Soroban transaction envelope.
func (p *FootprintOpProcessor) contractParticipantsFromTxSorobanData() set.Set[string] {
	v1Envelope, ok := p.op.Transaction.GetTransactionV1Envelope()
	if !ok {
		return nil
	}

	contractIDs := set.NewSet[string]()
	readWrite := v1Envelope.Tx.Ext.SorobanData.Resources.Footprint.ReadWrite
	readOnly := v1Envelope.Tx.Ext.SorobanData.Resources.Footprint.ReadOnly
	for _, ledgerKey := range append(readWrite, readOnly...) {
		contractID, ok := p.contractParticipantsFromLedgerKey(ledgerKey)
		if ok && contractID != "" {
			contractIDs.Add(contractID)
		}
	}

	return contractIDs
}

func (p *FootprintOpProcessor) Participants() (set.Set[string], error) {
	if !p.ValidateType() {
		return nil, fmt.Errorf("invalid operation type: %w", ErrInvalidOpType)
	}

	participants := set.NewSet(p.op.SourceAccount().Address())

	participants = participants.Union(p.contractParticipantsFromTxSorobanData())

	return participants, nil
}

type InvokeContractOpProcessor struct {
	op *operation_processor.TransactionOperationWrapper
}

func (p *InvokeContractOpProcessor) GetInvokeContract() (xdr.InvokeHostFunctionOp, bool) {
	if p.op.OperationType() != xdr.OperationTypeInvokeHostFunction {
		return xdr.InvokeHostFunctionOp{}, false
	}

	invokeHostFunctionOp := p.op.Operation.Body.MustInvokeHostFunctionOp()
	if invokeHostFunctionOp.HostFunction.Type != xdr.HostFunctionTypeHostFunctionTypeInvokeContract {
		return xdr.InvokeHostFunctionOp{}, false
	}

	return invokeHostFunctionOp, true
}

func (p *InvokeContractOpProcessor) ContractID() (string, error) {
	invokeContractOp, ok := p.GetInvokeContract()
	if !ok {
		return "", fmt.Errorf("not a invoke contract operation: %w", ErrInvalidOpType)
	}

	contractID, err := invokeContractOp.HostFunction.MustInvokeContract().ContractAddress.String()
	if err != nil {
		return "", fmt.Errorf("getting contract address' string representation: %w", err)
	}

	return contractID, nil
}

func (p *InvokeContractOpProcessor) ParticipantsFromArgs() (set.Set[string], error) {
	invokeContractOp, ok := p.GetInvokeContract()
	if !ok {
		return nil, fmt.Errorf("not a invoke contract operation: %w", ErrInvalidOpType)
	}

	return participantsForScVal(scVecToScVal(invokeContractOp.HostFunction.MustInvokeContract().Args))
}

func (p *InvokeContractOpProcessor) ParticipantsFromAuth() (set.Set[string], error) {
	invokeContractOp, ok := p.GetInvokeContract()
	if !ok {
		return nil, fmt.Errorf("not a invoke contract operation: %w", ErrInvalidOpType)
	}

	return participantsForAuthEntries(invokeContractOp.Auth)
}

func (p *InvokeContractOpProcessor) Participants() (set.Set[string], error) {
	participants := set.NewSet(p.op.SourceAccount().Address())

	if contractID, err := p.ContractID(); err != nil {
		return nil, fmt.Errorf("getting contract ID: %w", err)
	} else if contractID != "" {
		participants.Add(contractID)
	}

	if argsParticipants, err := p.ParticipantsFromArgs(); err != nil {
		return nil, fmt.Errorf("getting args participants: %w", err)
	} else {
		participants = participants.Union(argsParticipants)
	}

	if authParticipants, err := p.ParticipantsFromAuth(); err != nil {
		return nil, fmt.Errorf("getting auth participants: %w", err)
	} else {
		participants = participants.Union(authParticipants)
	}

	return participants, nil
}
