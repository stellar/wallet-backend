package utils

import (
	"fmt"

	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// contractIDLen is the raw byte length of a strkey-decoded contract address
// (a 32-byte sha256 hash), matching xdr.ContractId's array size.
const contractIDLen = 32

// ContractAddressScVal encodes a contract C-address as the Soroban `Address`
// value a contract-only parameter expects, e.g. a `token: Address` argument.
// strkey.Decode validates the version byte and checksum but not the payload
// length, so the length is checked here: a short payload would otherwise be
// zero-padded into a different, valid-looking contract ID.
func ContractAddressScVal(contractAddress string) (xdr.ScVal, error) {
	raw, err := strkey.Decode(strkey.VersionByteContract, contractAddress)
	if err != nil {
		return xdr.ScVal{}, fmt.Errorf("decoding contract address %q: %w", contractAddress, err)
	}
	if len(raw) != contractIDLen {
		return xdr.ScVal{}, fmt.Errorf("contract address %q decoded to %d bytes, want %d", contractAddress, len(raw), contractIDLen)
	}
	var cid xdr.ContractId
	copy(cid[:], raw)
	return xdr.ScVal{
		Type: xdr.ScValTypeScvAddress,
		Address: &xdr.ScAddress{
			Type:       xdr.ScAddressTypeScAddressTypeContract,
			ContractId: &cid,
		},
	}, nil
}

// AddressScVal encodes an account G-address or a contract C-address as the
// Soroban `Address` value a parameter accepting either expects, e.g. the id in
// `balance(id: Address)`.
func AddressScVal(address string) (xdr.ScVal, error) {
	switch {
	case strkey.IsValidEd25519PublicKey(address):
		accountID, err := xdr.AddressToAccountId(address)
		if err != nil {
			return xdr.ScVal{}, fmt.Errorf("decoding account address %q: %w", address, err)
		}
		return xdr.ScVal{
			Type: xdr.ScValTypeScvAddress,
			Address: &xdr.ScAddress{
				Type:      xdr.ScAddressTypeScAddressTypeAccount,
				AccountId: &accountID,
			},
		}, nil
	case strkey.IsValidContractAddress(address):
		return ContractAddressScVal(address)
	default:
		return xdr.ScVal{}, fmt.Errorf("address %q is neither an account nor a contract address", address)
	}
}
