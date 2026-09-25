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

// participantsForSorobanOp returns the participants of a Soroban operation: the addresses
// that authorised it. There are two inputs.
//
//   - The operation source account, always. Source-account credentials consume no nonce,
//     so the meta holds no record of that authorisation.
//   - The owner of every nonce entry the host created for the operation: a temporary
//     ContractData entry with key type SCV_LEDGER_KEY_NONCE. The host writes it only after
//     a signature or a custom account's __check_auth verifies, and only the host can write
//     such a key. The owner is a G account or a C custom account.
//
// Nothing is read from the declared authorization tree: the submitter controls it and the
// host never checks entries that no require_auth call matched, so any address in it is
// forgeable. The invoked contract, deployed contracts, deployers and event emitters are
// not participants. A contract appears only as an authorising custom account.
// Returns ErrNotSorobanOperation for non-Soroban operations.
//
// Every set here is thread-unsafe: they are built and consumed within a single indexer
// worker goroutine (see Indexer.ProcessLedgerTransactions).
func participantsForSorobanOp(op *TransactionOperationWrapper) (set.Set[string], error) {
	if !op.Transaction.IsSorobanTx() {
		return nil, ErrNotSorobanOperation
	}

	participants := set.NewThreadUnsafeSet(op.SourceAccount().ToAccountId().Address())
	if op.Operation.Body.Type != xdr.OperationTypeInvokeHostFunction {
		return participants, nil
	}

	changes, err := op.Transaction.GetOperationChanges(op.Index)
	if err != nil {
		return nil, fmt.Errorf("getting operation changes: %w", err)
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
			return nil, fmt.Errorf("converting nonce entry address to string: %w", err)
		}
		participants.Add(authorizer)
	}
	return participants, nil
}
