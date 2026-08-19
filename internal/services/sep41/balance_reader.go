// Package sep41 — balance_reader.go reads a holder's authoritative balance by
// simulating the contract's balance(id) view function. The repair path writes
// this value over event-derived rows.
//
// The value is the raw i128 as a decimal string, no decimals scaling — the
// same form the processor persists. Scaling here would make the two
// incomparable.
package sep41

import (
	"context"
	"fmt"

	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/wallet-backend/internal/services"
)

// BalanceReader reads on-chain SEP-41 balances via RPC simulation.
type BalanceReader struct {
	rpc services.ContractMetadataService
}

// NewBalanceReader returns a reader backed by the supplied
// ContractMetadataService, which provides the generic simulation primitive.
func NewBalanceReader(rpc services.ContractMetadataService) *BalanceReader {
	if rpc == nil {
		return nil
	}
	return &BalanceReader{rpc: rpc}
}

// ReadBalance simulates balance(holder) and returns the raw i128 decimal
// string plus the ledger it is true at. tokenContractAddress is a C-address;
// holderAddress is a G- or C-address. Any failure (RPC error, revert, non-i128
// result) is returned; callers treat it as skip-and-report.
func (r *BalanceReader) ReadBalance(ctx context.Context, tokenContractAddress, holderAddress string) (string, uint32, error) {
	if r == nil {
		return "", 0, fmt.Errorf("sep41: nil BalanceReader")
	}

	holderArg, err := holderAddressScVal(holderAddress)
	if err != nil {
		return "", 0, fmt.Errorf("sep41: reading balance of %s on %s: %w", holderAddress, tokenContractAddress, err)
	}

	val, ledger, err := r.rpc.FetchSingleFieldWithLedger(ctx, tokenContractAddress, "balance", holderArg)
	if err != nil {
		return "", 0, fmt.Errorf("sep41: simulating balance(%s) on %s: %w", holderAddress, tokenContractAddress, err)
	}

	balance, err := extractI128(val)
	if err != nil {
		return "", 0, fmt.Errorf("sep41: balance(%s) on %s returned an unexpected result: %w", holderAddress, tokenContractAddress, err)
	}

	return balance.String(), ledger, nil
}

// holderAddressScVal encodes a token holder — an account G-address or a
// contract C-address — as the Soroban `Address` value that a
// `balance(id: Address)` parameter expects.
func holderAddressScVal(holderAddress string) (xdr.ScVal, error) {
	address := xdr.ScAddress{}
	switch {
	case strkey.IsValidEd25519PublicKey(holderAddress):
		accountID, err := xdr.AddressToAccountId(holderAddress)
		if err != nil {
			return xdr.ScVal{}, fmt.Errorf("decoding account address %q: %w", holderAddress, err)
		}
		address.Type = xdr.ScAddressTypeScAddressTypeAccount
		address.AccountId = &accountID
	case strkey.IsValidContractAddress(holderAddress):
		raw, err := strkey.Decode(strkey.VersionByteContract, holderAddress)
		if err != nil {
			return xdr.ScVal{}, fmt.Errorf("decoding contract address %q: %w", holderAddress, err)
		}
		contractID := xdr.ContractId{}
		copy(contractID[:], raw)
		address.Type = xdr.ScAddressTypeScAddressTypeContract
		address.ContractId = &contractID
	default:
		return xdr.ScVal{}, fmt.Errorf("holder address %q is neither an account nor a contract address", holderAddress)
	}

	return xdr.ScVal{Type: xdr.ScValTypeScvAddress, Address: &address}, nil
}
