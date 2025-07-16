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

var (
	ErrNotSorobanOperation = errors.New("not a soroban operation")
	ErrInvalidOpType       = errors.New("invalid operation type")
)

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

// participantsFromInvocationAndSubInvocations recursively collects all ScAddresses from a SorobanAuthorizedInvocation
// and its subinvocations.
func participantsFromInvocationAndSubInvocations(networkPassphrase string, invocation xdr.SorobanAuthorizedInvocation) (set.Set[string], error) {
	participants := set.NewSet[string]()
	if utils.IsEmpty(invocation) {
		return participants, nil
	}

	switch invocation.Function.Type {
	case xdr.SorobanAuthorizedFunctionTypeSorobanAuthorizedFunctionTypeContractFn:
		contractFn, ok := invocation.Function.GetContractFn()
		if !ok {
			break
		}

		contractID, err := contractFn.ContractAddress.String()
		if err != nil {
			return nil, fmt.Errorf("converting contract address to string: %w", err)
		}
		participants.Add(contractID)

	case xdr.SorobanAuthorizedFunctionTypeSorobanAuthorizedFunctionTypeCreateContractHostFn:
		createContractHostFn, ok := invocation.Function.GetCreateContractHostFn()
		if !ok {
			break
		}

		contractIDs, err := contractIDsForPreimage(networkPassphrase, createContractHostFn.ContractIdPreimage)
		if err != nil {
			return nil, fmt.Errorf("getting contract ID: %w", err)
		}
		participants = participants.Union(contractIDs)

	case xdr.SorobanAuthorizedFunctionTypeSorobanAuthorizedFunctionTypeCreateContractV2HostFn:
		createContractV2HostFn, ok := invocation.Function.GetCreateContractV2HostFn()
		if !ok {
			break
		}

		contractIDs, err := contractIDsForPreimage(networkPassphrase, createContractV2HostFn.ContractIdPreimage)
		if err != nil {
			return nil, fmt.Errorf("getting contract ID: %w", err)
		}
		participants = participants.Union(contractIDs)
	}

	for _, sub := range invocation.SubInvocations {
		subParticipants, err := participantsFromInvocationAndSubInvocations(networkPassphrase, sub)
		if err != nil {
			return nil, fmt.Errorf("collecting participants from subinvocation: %w", err)
		}
		participants = participants.Union(subParticipants)
	}

	return participants, nil
}

// participantsForAuthEntries extracts all participant addresses from a []SorobanAuthorizationEntry.
func participantsForAuthEntries(networkPassphrase string, authEntries []xdr.SorobanAuthorizationEntry) (set.Set[string], error) {
	participants := set.NewSet[string]()
	for _, authEntry := range authEntries {
		if authEntry.Credentials.Type == xdr.SorobanCredentialsTypeSorobanCredentialsAddress {
			participant, err := authEntry.Credentials.MustAddress().Address.String()
			if err != nil {
				return nil, fmt.Errorf("converting ScAddress to string: %w", err)
			}
			participants.Add(participant)
		}

		invocationParticipants, err := participantsFromInvocationAndSubInvocations(networkPassphrase, authEntry.RootInvocation)
		if err != nil {
			return nil, fmt.Errorf("getting invocation participants: %w", err)
		}
		participants = participants.Union(invocationParticipants)
	}

	return participants, nil
}

// participantsForSorobanOp identifies participants (AddressId or ContractId) from Soroban operations.
// The source account is always included. Additional participants are gathered based on the operation type:
//
// - For `ExtendFootprintTtl` and `RestoreFootprint` operations: only the source account is included.
// - For `InvokeHostFunction.UploadWasm` operations: only the source account is included.
// - For `InvokeHostFunction.InvokeContract`: includes the ContractId being invoked.
// - For `InvokeHostFunction.CreateContract(V1/V2)`, it includes the fromAddress, and if the subtype is:
//   - `FromAsset`: includes the SAC ID derived from the classic asset being deployed
//   - `FromAccount`: includes the fromAccount address and the calculated contract ID (from preimage)
//
// For CreateContract (V1/V2) and InvokeContract operations, we also:
//   - Include all AccountId and ContractId addresses found in AuthEntries
//   - Recursively include any nested InvokeContract or CreateContract (V1/V2) calls found in subinvocations,
//     applying the same extraction logic as above
//
// It can return `ErrNotSorobanOperation` if the operation is not a Soroban operation.
func participantsForSorobanOp(op operation_processor.TransactionOperationWrapper) (set.Set[string], error) {
	if !op.Transaction.IsSorobanTx() {
		return nil, ErrNotSorobanOperation
	}

	participants := set.NewSet(op.SourceAccount().Address())

	switch op.Operation.Body.Type {
	case xdr.OperationTypeExtendFootprintTtl, xdr.OperationTypeRestoreFootprint:
		break

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

// contractIDsForPreimage returns the contract IDs for a ContractIdPreimage.
// if the preimage is FromAsset, it returns the SAC contract ID.
// if the preimage is FromAddress, it returns the contract ID calculated from the deployer address, salt and the network passphrase.
// It also returns the deployer account ID.
func contractIDsForPreimage(networkPassphrase string, preimage xdr.ContractIdPreimage) (set.Set[string], error) {
	switch preimage.Type {
	case xdr.ContractIdPreimageTypeContractIdPreimageFromAddress:
		contractID, err := calculateContractID(networkPassphrase, preimage.MustFromAddress())
		if err != nil {
			return nil, fmt.Errorf("calculating contract ID: %w", err)
		}

		fromAccountID, err := preimage.MustFromAddress().Address.String()
		if err != nil {
			return nil, fmt.Errorf("getting from address' string representation: %w", err)
		}
		return set.NewSet(contractID, fromAccountID), nil

	case xdr.ContractIdPreimageTypeContractIdPreimageFromAsset:
		fromAsset := preimage.MustFromAsset()
		assetContractID, err := fromAsset.ContractID(networkPassphrase)
		if err != nil {
			return nil, fmt.Errorf("getting asset contract ID: %w", err)
		}
		return set.NewSet(strkey.MustEncode(strkey.VersionByteContract, assetContractID[:])), nil

	default:
		return nil, fmt.Errorf("invalid contract id preimage type")
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

func (p *CreateContractV1OpProcessor) Participants() (set.Set[string], error) {
	createContractOp, ok := p.GetCreateContract()
	if !ok {
		return nil, fmt.Errorf("not a create contract operation: %w", ErrInvalidOpType)
	}

	// Source account
	participants := set.NewSet(p.op.SourceAccount().Address())

	// Contract IDs
	contractIDs, err := contractIDsForPreimage(p.op.Network, createContractOp.ContractIdPreimage)
	if err != nil {
		return nil, fmt.Errorf("getting contract ID: %w", err)
	}
	participants = participants.Union(contractIDs)

	// Auth participants
	authEntries := p.op.Operation.Body.MustInvokeHostFunctionOp().Auth
	authParticipants, err := participantsForAuthEntries(p.op.Network, authEntries)
	if err != nil {
		return nil, fmt.Errorf("getting auth participants: %w", err)
	}
	participants = participants.Union(authParticipants)

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

func (p *CreateContractV2OpProcessor) Participants() (set.Set[string], error) {
	createContractOp, ok := p.GetCreateContract()
	if !ok {
		return nil, fmt.Errorf("not a create contract v2 operation: %w", ErrInvalidOpType)
	}

	// Source account
	participants := set.NewSet(p.op.SourceAccount().Address())

	// Contract IDs
	contractIDs, err := contractIDsForPreimage(p.op.Network, createContractOp.ContractIdPreimage)
	if err != nil {
		return nil, fmt.Errorf("getting contract ID: %w", err)
	}
	participants = participants.Union(contractIDs)

	// Auth participants
	authEntries := p.op.Operation.Body.MustInvokeHostFunctionOp().Auth
	authParticipants, err := participantsForAuthEntries(p.op.Network, authEntries)
	if err != nil {
		return nil, fmt.Errorf("getting auth participants: %w", err)
	}
	participants = participants.Union(authParticipants)

	return participants, nil
}

type InvokeContractOpProcessor struct {
	op *operation_processor.TransactionOperationWrapper
}

func (p *InvokeContractOpProcessor) GetInvokeContract() (xdr.InvokeContractArgs, bool) {
	if p.op.OperationType() != xdr.OperationTypeInvokeHostFunction {
		return xdr.InvokeContractArgs{}, false
	}

	invokeHostFunctionOp := p.op.Operation.Body.MustInvokeHostFunctionOp()
	if invokeHostFunctionOp.HostFunction.Type != xdr.HostFunctionTypeHostFunctionTypeInvokeContract {
		return xdr.InvokeContractArgs{}, false
	}

	return invokeHostFunctionOp.HostFunction.MustInvokeContract(), true
}

func (p *InvokeContractOpProcessor) Participants() (set.Set[string], error) {
	invokeContractOp, ok := p.GetInvokeContract()
	if !ok {
		return nil, fmt.Errorf("not a invoke contract operation: %w", ErrInvalidOpType)
	}

	// Source account
	participants := set.NewSet(p.op.SourceAccount().Address())

	// Contract ID
	contractID, err := invokeContractOp.ContractAddress.String()
	if err != nil {
		return nil, fmt.Errorf("converting contract address to string: %w", err)
	}
	if contractID != "" {
		participants.Add(contractID)
	}

	// Auth participants
	authEntries := p.op.Operation.Body.MustInvokeHostFunctionOp().Auth
	authParticipants, err := participantsForAuthEntries(p.op.Network, authEntries)
	if err != nil {
		return nil, fmt.Errorf("getting auth participants: %w", err)
	}
	participants = participants.Union(authParticipants)

	return participants, nil
}
