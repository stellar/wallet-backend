package utils

import (
	"testing"

	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testAccountAddress  = "GCYNTH5HDQRNIQ3BSSYPWFO5AHH5ERVZ32C37QRXT6TXK3OJFFOIVXDE"
	testContractAddress = "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"
)

// shortContractAddress is a well-formed C-strkey — correct version byte, valid
// checksum, so strkey.Decode and IsValidContractAddress both accept it — whose
// payload is half the length of a contract ID.
func shortContractAddress(t *testing.T) string {
	t.Helper()
	addr, err := strkey.Encode(strkey.VersionByteContract, make([]byte, contractIDLen/2))
	require.NoError(t, err)
	require.True(t, strkey.IsValidContractAddress(addr))
	return addr
}

func TestContractAddressScVal(t *testing.T) {
	t.Run("encodes a contract address", func(t *testing.T) {
		got, err := ContractAddressScVal(testContractAddress)
		require.NoError(t, err)

		require.Equal(t, xdr.ScValTypeScvAddress, got.Type)
		require.Equal(t, xdr.ScAddressTypeScAddressTypeContract, got.Address.Type)
		raw, err := strkey.Decode(strkey.VersionByteContract, testContractAddress)
		require.NoError(t, err)
		assert.Equal(t, raw, got.Address.ContractId[:])
	})

	t.Run("rejects a malformed address", func(t *testing.T) {
		_, err := ContractAddressScVal("not-a-strkey-address")
		assert.Error(t, err)
	})

	t.Run("rejects a well-formed account address", func(t *testing.T) {
		// A valid G... strkey decodes cleanly but fails the
		// VersionByteContract check inside strkey.Decode.
		_, err := ContractAddressScVal(testAccountAddress)
		assert.Error(t, err)
	})

	t.Run("rejects a contract address with a short payload", func(t *testing.T) {
		_, err := ContractAddressScVal(shortContractAddress(t))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "want 32")
	})
}

func TestAddressScVal(t *testing.T) {
	t.Run("encodes an account address", func(t *testing.T) {
		got, err := AddressScVal(testAccountAddress)
		require.NoError(t, err)

		require.Equal(t, xdr.ScValTypeScvAddress, got.Type)
		require.Equal(t, xdr.ScAddressTypeScAddressTypeAccount, got.Address.Type)
		assert.Equal(t, testAccountAddress, got.Address.AccountId.Address())
	})

	t.Run("encodes a contract address the same way ContractAddressScVal does", func(t *testing.T) {
		want, err := ContractAddressScVal(testContractAddress)
		require.NoError(t, err)

		got, err := AddressScVal(testContractAddress)
		require.NoError(t, err)
		assert.Equal(t, want, got)
	})

	t.Run("rejects an address that is neither an account nor a contract", func(t *testing.T) {
		_, err := AddressScVal("not-an-address")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "neither an account nor a contract address")
	})

	t.Run("rejects a contract address with a short payload", func(t *testing.T) {
		_, err := AddressScVal(shortContractAddress(t))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "want 32")
	})
}
