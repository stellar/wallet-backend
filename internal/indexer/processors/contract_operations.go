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

// participantsForSorobanOp identifies participants (account or contract addresses) of a
// Soroban operation. Every address comes from the operation body or from what the host
// recorded in the transaction meta. Nothing is read from the declared authorization tree:
// the submitter controls it and the host never checks entries that no require_auth call
// matched, so any address in it is forgeable.
//
//   - The operation source account is always included.
//   - InvokeContract: the invoked contract.
//   - CreateContract(V1/V2): the deployer address and the derived contract ID (FromAddress),
//     or the SAC ID derived from the asset (FromAsset). The host calls require_auth on the
//     FromAddress, so a successful top-level deploy is authenticated.
//   - Every address that authorised the invocation. After a signature verifies, the host
//     writes a temporary ContractData entry with key type SCV_LEDGER_KEY_NONCE under the
//     authorising address; only the host can write such a key. Source-account credentials
//     consume no nonce, and that account is already included.
//   - Every contract that emitted an event while executing.
//
// ExtendFootprintTtl, RestoreFootprint and UploadContractWasm contribute only the source.
// Returns ErrNotSorobanOperation for non-Soroban operations.
//
// Every set here is thread-unsafe: they are built and consumed within a single indexer
// worker goroutine (see Indexer.ProcessLedgerTransactions).
func participantsForSorobanOp(op *TransactionOperationWrapper) (set.Set[string], error) {
	if !op.Transaction.IsSorobanTx() {
		return nil, ErrNotSorobanOperation
	}

	participants := set.NewThreadUnsafeSet(op.SourceAccount().Address())
	if op.Operation.Body.Type != xdr.OperationTypeInvokeHostFunction {
		return participants, nil
	}

	hostFn := op.Operation.Body.MustInvokeHostFunctionOp().HostFunction
	switch hostFn.Type {
	case xdr.HostFunctionTypeHostFunctionTypeInvokeContract:
		contractID, err := hostFn.MustInvokeContract().ContractAddress.String()
		if err != nil {
			return nil, fmt.Errorf("converting contract address to string: %w", err)
		}
		participants.Add(contractID)
	case xdr.HostFunctionTypeHostFunctionTypeCreateContract:
		if err := addContractIDsForPreimage(participants, op.Network, hostFn.MustCreateContract().ContractIdPreimage); err != nil {
			return nil, err
		}
	case xdr.HostFunctionTypeHostFunctionTypeCreateContractV2:
		if err := addContractIDsForPreimage(participants, op.Network, hostFn.MustCreateContractV2().ContractIdPreimage); err != nil {
			return nil, err
		}
	case xdr.HostFunctionTypeHostFunctionTypeUploadContractWasm:
		// only the source account
	}

	if err := addExecutedParticipants(participants, op); err != nil {
		return nil, err
	}
	return participants, nil
}

// addContractIDsForPreimage adds the contract ID a ContractIdPreimage resolves to. For a
// FromAddress preimage it also adds the deployer address.
func addContractIDsForPreimage(participants set.Set[string], networkPassphrase string, preimage xdr.ContractIdPreimage) error {
	switch preimage.Type {
	case xdr.ContractIdPreimageTypeContractIdPreimageFromAddress:
		fromAddress := preimage.MustFromAddress()
		contractID, err := calculateContractID(networkPassphrase, fromAddress)
		if err != nil {
			return fmt.Errorf("calculating contract ID: %w", err)
		}
		deployer, err := fromAddress.Address.String()
		if err != nil {
			return fmt.Errorf("getting from address' string representation: %w", err)
		}
		participants.Add(contractID)
		participants.Add(deployer)
		return nil

	case xdr.ContractIdPreimageTypeContractIdPreimageFromAsset:
		assetContractID, err := preimage.MustFromAsset().ContractID(networkPassphrase)
		if err != nil {
			return fmt.Errorf("getting asset contract ID: %w", err)
		}
		participants.Add(strkey.MustEncode(strkey.VersionByteContract, assetContractID[:]))
		return nil

	default:
		return fmt.Errorf("invalid contract id preimage type %d", preimage.Type)
	}
}

// addExecutedParticipants adds the addresses the host recorded in the operation meta:
// authorising addresses via their created nonce entries, and event-emitting contracts.
func addExecutedParticipants(participants set.Set[string], op *TransactionOperationWrapper) error {
	changes, err := op.Transaction.GetOperationChanges(op.Index)
	if err != nil {
		return fmt.Errorf("getting operation changes: %w", err)
	}
	for _, change := range changes {
		// The host only ever creates nonce entries (consume_nonce errors if the key exists).
		if change.Type != xdr.LedgerEntryTypeContractData || change.ChangeType != xdr.LedgerEntryChangeTypeLedgerEntryCreated || change.Post == nil {
			continue
		}
		contractData := change.Post.Data.MustContractData()
		if contractData.Key.Type != xdr.ScValTypeScvLedgerKeyNonce {
			continue
		}
		authorizer, err := contractData.Contract.String()
		if err != nil {
			return fmt.Errorf("converting nonce entry address to string: %w", err)
		}
		participants.Add(authorizer)
	}

	events, err := op.Transaction.GetContractEventsForOperation(op.Index)
	if err != nil {
		return fmt.Errorf("getting contract events: %w", err)
	}
	for _, event := range events {
		if event.ContractId == nil {
			continue
		}
		participants.Add(strkey.MustEncode(strkey.VersionByteContract, event.ContractId[:]))
	}
	return nil
}
