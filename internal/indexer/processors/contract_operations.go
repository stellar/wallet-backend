package processors

import (
	"crypto/sha256"
	"errors"
	"fmt"

	set "github.com/deckarep/golang-set/v2"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"
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

// participantsFromInvocationAndSubInvocations recursively adds all ScAddresses from a
// SorobanAuthorizedInvocation and its subinvocations to the participants accumulator.
func participantsFromInvocationAndSubInvocations(networkPassphrase string, invocation *xdr.SorobanAuthorizedInvocation, participants set.Set[string]) error {
	// A zero-value invocation reports the ContractFn arm (type 0) with a nil pointer,
	// which GetContractFn would dereference. Skip such nodes entirely.
	if invocation.Function.Type == xdr.SorobanAuthorizedFunctionTypeSorobanAuthorizedFunctionTypeContractFn && invocation.Function.ContractFn == nil {
		return nil
	}

	switch invocation.Function.Type {
	case xdr.SorobanAuthorizedFunctionTypeSorobanAuthorizedFunctionTypeContractFn:
		contractFn, ok := invocation.Function.GetContractFn()
		if !ok {
			break
		}

		contractID, err := contractFn.ContractAddress.String()
		if err != nil {
			return fmt.Errorf("converting contract address to string: %w", err)
		}
		participants.Add(contractID)

	case xdr.SorobanAuthorizedFunctionTypeSorobanAuthorizedFunctionTypeCreateContractHostFn:
		createContractHostFn, ok := invocation.Function.GetCreateContractHostFn()
		if !ok {
			break
		}

		if err := addContractIDsForPreimage(networkPassphrase, createContractHostFn.ContractIdPreimage, participants); err != nil {
			return fmt.Errorf("getting contract ID: %w", err)
		}

	case xdr.SorobanAuthorizedFunctionTypeSorobanAuthorizedFunctionTypeCreateContractV2HostFn:
		createContractV2HostFn, ok := invocation.Function.GetCreateContractV2HostFn()
		if !ok {
			break
		}

		if err := addContractIDsForPreimage(networkPassphrase, createContractV2HostFn.ContractIdPreimage, participants); err != nil {
			return fmt.Errorf("getting contract ID: %w", err)
		}
	}

	for i := range invocation.SubInvocations {
		if err := participantsFromInvocationAndSubInvocations(networkPassphrase, &invocation.SubInvocations[i], participants); err != nil {
			return fmt.Errorf("collecting participants from subinvocation: %w", err)
		}
	}

	return nil
}

// participantsForAuthEntries adds all participant addresses from a
// []SorobanAuthorizationEntry to the participants accumulator.
func participantsForAuthEntries(networkPassphrase string, authEntries []xdr.SorobanAuthorizationEntry, participants set.Set[string]) error {
	for i := range authEntries {
		authEntry := &authEntries[i]
		if authEntry.Credentials.Type == xdr.SorobanCredentialsTypeSorobanCredentialsAddress {
			participant, err := authEntry.Credentials.MustAddress().Address.String()
			if err != nil {
				return fmt.Errorf("converting ScAddress to string: %w", err)
			}
			participants.Add(participant)
		}

		if err := participantsFromInvocationAndSubInvocations(networkPassphrase, &authEntry.RootInvocation, participants); err != nil {
			return fmt.Errorf("getting invocation participants: %w", err)
		}
	}

	return nil
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
//
// The whole path folds into the single participants accumulator the caller passes in:
// one set per operation instead of one per processor, auth entry, and invocation-tree
// node, with no Union merges. The accumulator must be thread-unsafe — it is built and
// consumed within a single indexer worker goroutine (see Indexer.ProcessLedgerTransactions),
// so a thread-safe set's mutex would be pure overhead on the hot path.
func participantsForSorobanOp(op *TransactionOperationWrapper, participants set.Set[string]) error {
	if !op.Transaction.IsSorobanTx() {
		return ErrNotSorobanOperation
	}

	// The op source is the one participant every Soroban op shape shares; adding it
	// here keeps the sub-processors source-free and encodes the address once.
	participants.Add(op.SourceAccount().Address())

	switch op.Operation.Body.Type {
	case xdr.OperationTypeExtendFootprintTtl, xdr.OperationTypeRestoreFootprint:
		break

	case xdr.OperationTypeInvokeHostFunction:
		invokeHostOp := op.Operation.Body.MustInvokeHostFunctionOp()

		switch invokeHostOp.HostFunction.Type {
		case xdr.HostFunctionTypeHostFunctionTypeCreateContract:
			createContractOpProcessor := CreateContractV1OpProcessor{op: op}
			if err := createContractOpProcessor.AddParticipants(participants); err != nil {
				return fmt.Errorf("getting create contract participants: %w", err)
			}

		case xdr.HostFunctionTypeHostFunctionTypeCreateContractV2:
			createContractV2OpProcessor := CreateContractV2OpProcessor{op: op}
			if err := createContractV2OpProcessor.AddParticipants(participants); err != nil {
				return fmt.Errorf("getting create contract participants: %w", err)
			}

		case xdr.HostFunctionTypeHostFunctionTypeInvokeContract:
			invokeContractOpProcessor := InvokeContractOpProcessor{op: op}
			if err := invokeContractOpProcessor.AddParticipants(participants); err != nil {
				return fmt.Errorf("getting invoke contract participants: %w", err)
			}

		case xdr.HostFunctionTypeHostFunctionTypeUploadContractWasm:
			break
		}

	default:
		break
	}

	return nil
}

// addContractIDsForPreimage adds the contract IDs for a ContractIdPreimage to the
// participants accumulator.
// If the preimage is FromAsset, it adds the SAC contract ID.
// If the preimage is FromAddress, it adds the contract ID calculated from the deployer
// address, salt and the network passphrase, plus the deployer account ID.
func addContractIDsForPreimage(networkPassphrase string, preimage xdr.ContractIdPreimage, participants set.Set[string]) error {
	switch preimage.Type {
	case xdr.ContractIdPreimageTypeContractIdPreimageFromAddress:
		contractID, err := calculateContractID(networkPassphrase, preimage.MustFromAddress())
		if err != nil {
			return fmt.Errorf("calculating contract ID: %w", err)
		}

		fromAccountID, err := preimage.MustFromAddress().Address.String()
		if err != nil {
			return fmt.Errorf("getting from address' string representation: %w", err)
		}
		participants.Add(contractID)
		participants.Add(fromAccountID)
		return nil

	case xdr.ContractIdPreimageTypeContractIdPreimageFromAsset:
		fromAsset := preimage.MustFromAsset()
		assetContractID, err := fromAsset.ContractID(networkPassphrase)
		if err != nil {
			return fmt.Errorf("getting asset contract ID: %w", err)
		}
		participants.Add(strkey.MustEncode(strkey.VersionByteContract, assetContractID[:]))
		return nil

	default:
		return fmt.Errorf("invalid contract id preimage type")
	}
}

type CreateContractV1OpProcessor struct {
	op *TransactionOperationWrapper
}

// AddParticipants adds the operation's contract IDs and auth-entry participants to
// the accumulator. The op source account is added by participantsForSorobanOp.
func (p *CreateContractV1OpProcessor) AddParticipants(participants set.Set[string]) error {
	if p.op.OperationType() != xdr.OperationTypeInvokeHostFunction {
		return fmt.Errorf("not a create contract operation: %w", ErrInvalidOpType)
	}
	invokeHostFunctionOp := p.op.Operation.Body.MustInvokeHostFunctionOp()
	if invokeHostFunctionOp.HostFunction.Type != xdr.HostFunctionTypeHostFunctionTypeCreateContract {
		return fmt.Errorf("not a create contract operation: %w", ErrInvalidOpType)
	}
	createContractOp := invokeHostFunctionOp.HostFunction.MustCreateContract()

	// Contract IDs
	if err := addContractIDsForPreimage(p.op.Network, createContractOp.ContractIdPreimage, participants); err != nil {
		return fmt.Errorf("getting contract ID: %w", err)
	}

	// Auth participants
	if err := participantsForAuthEntries(p.op.Network, invokeHostFunctionOp.Auth, participants); err != nil {
		return fmt.Errorf("getting auth participants: %w", err)
	}

	return nil
}

type CreateContractV2OpProcessor struct {
	op *TransactionOperationWrapper
}

// AddParticipants adds the operation's contract IDs and auth-entry participants to
// the accumulator. The op source account is added by participantsForSorobanOp.
func (p *CreateContractV2OpProcessor) AddParticipants(participants set.Set[string]) error {
	if p.op.OperationType() != xdr.OperationTypeInvokeHostFunction {
		return fmt.Errorf("not a create contract v2 operation: %w", ErrInvalidOpType)
	}
	invokeHostFunctionOp := p.op.Operation.Body.MustInvokeHostFunctionOp()
	if invokeHostFunctionOp.HostFunction.Type != xdr.HostFunctionTypeHostFunctionTypeCreateContractV2 {
		return fmt.Errorf("not a create contract v2 operation: %w", ErrInvalidOpType)
	}
	createContractOp := invokeHostFunctionOp.HostFunction.MustCreateContractV2()

	// Contract IDs
	if err := addContractIDsForPreimage(p.op.Network, createContractOp.ContractIdPreimage, participants); err != nil {
		return fmt.Errorf("getting contract ID: %w", err)
	}

	// Auth participants
	if err := participantsForAuthEntries(p.op.Network, invokeHostFunctionOp.Auth, participants); err != nil {
		return fmt.Errorf("getting auth participants: %w", err)
	}

	return nil
}

type InvokeContractOpProcessor struct {
	op *TransactionOperationWrapper
}

// AddParticipants adds the invoked contract ID and auth-entry participants to the
// accumulator. The op source account is added by participantsForSorobanOp.
func (p *InvokeContractOpProcessor) AddParticipants(participants set.Set[string]) error {
	if p.op.OperationType() != xdr.OperationTypeInvokeHostFunction {
		return fmt.Errorf("not a invoke contract operation: %w", ErrInvalidOpType)
	}
	invokeHostFunctionOp := p.op.Operation.Body.MustInvokeHostFunctionOp()
	if invokeHostFunctionOp.HostFunction.Type != xdr.HostFunctionTypeHostFunctionTypeInvokeContract {
		return fmt.Errorf("not a invoke contract operation: %w", ErrInvalidOpType)
	}
	invokeContractOp := invokeHostFunctionOp.HostFunction.MustInvokeContract()

	// Contract ID
	contractID, err := invokeContractOp.ContractAddress.String()
	if err != nil {
		return fmt.Errorf("converting contract address to string: %w", err)
	}
	if contractID != "" {
		participants.Add(contractID)
	}

	// Auth participants
	if err := participantsForAuthEntries(p.op.Network, invokeHostFunctionOp.Auth, participants); err != nil {
		return fmt.Errorf("getting auth participants: %w", err)
	}

	return nil
}
