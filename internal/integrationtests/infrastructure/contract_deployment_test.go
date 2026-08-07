package infrastructure

import (
	"testing"

	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stretchr/testify/require"
)

func TestPrecomputeContractAddress(t *testing.T) {
	deployer := keypair.Root(networkPassphrase).Address()
	var salt [32]byte
	salt[0] = 1

	// Golden value pinning the create-contract-v2 address preimage derivation
	// (deployer account + salt + network ID) for the standalone network.
	const expected = "CBXMDM5V2LLKT4IFSMNIW6PI3MK2IMLWUIK27GMX3HIX523X7BRBUQJC"

	address, err := PrecomputeContractAddress(deployer, salt)
	require.NoError(t, err)
	require.Equal(t, expected, address)

	// The derivation is deterministic: same inputs yield the same address.
	again, err := PrecomputeContractAddress(deployer, salt)
	require.NoError(t, err)
	require.Equal(t, address, again)

	// A different salt yields a different address.
	var otherSalt [32]byte
	otherSalt[0] = 2
	otherAddress, err := PrecomputeContractAddress(deployer, otherSalt)
	require.NoError(t, err)
	require.NotEqual(t, address, otherAddress)
}
