package infrastructure

import (
	"context"
	"testing"

	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/txnbuild"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/require"
)

// mintSEP41Tokens invokes the mint function on a SEP-41 token contract to create new tokens.
// This is typically called by the contract admin to issue tokens to an address.
//
// The mint function signature: mint(to: Address, amount: i128)
//
// Note: The soroban-examples token contract requires the transaction to be signed by the
// admin account that was set during initialization. Only the admin can mint new tokens.
//
// For G-addresses (accounts), this creates a contract data entry with key [Balance, G-address]
// that represents the account's balance in the contract's token. This is different from classic
// trustlines - it's pure contract state storage.
//
// Parameters:
//   - ctx: Context for logging
//   - t: Testing instance for assertions
//   - tokenContractAddress: The contract address of the SEP-41 token (C...)
//   - toAddress: Recipient address (can be G... for accounts or C... for contracts)
//   - amount: Amount to mint in stroops/base units (e.g., 5000000000 = 500 tokens with 7 decimals)
func (s *SharedContainers) mintSEP41Tokens(ctx context.Context, t *testing.T, tokenContractAddress, toAddress string, amount int64) {
	// Decode token contract address to get contract ID
	tokenContractID, err := strkey.Decode(strkey.VersionByteContract, tokenContractAddress)
	require.NoError(t, err, "failed to decode token contract address")
	var tokenID xdr.ContractId
	copy(tokenID[:], tokenContractID)

	// Build recipient ScAddress using helper
	toSCAddress, err := parseAddressToScAddress(toAddress)
	require.NoError(t, err, "failed to parse recipient address")

	// Build the mint invocation
	// Function signature: mint(to: Address, amount: i128)
	mintSym := xdr.ScSymbol("mint")
	invokeOp := &txnbuild.InvokeHostFunction{
		HostFunction: xdr.HostFunction{
			Type: xdr.HostFunctionTypeHostFunctionTypeInvokeContract,
			InvokeContract: &xdr.InvokeContractArgs{
				ContractAddress: xdr.ScAddress{
					Type:       xdr.ScAddressTypeScAddressTypeContract,
					ContractId: &tokenID,
				},
				FunctionName: mintSym,
				Args: xdr.ScVec{
					// Arg 1: recipient address (to: Address)
					{Type: xdr.ScValTypeScvAddress, Address: &toSCAddress},
					// Arg 2: amount to mint (amount: i128)
					// i128 is a 128-bit signed integer used for token amounts.
					{
						Type: xdr.ScValTypeScvI128,
						I128: &xdr.Int128Parts{
							Hi: 0,                  // High 64 bits (for very large amounts)
							Lo: xdr.Uint64(amount), // Low 64 bits (sufficient for test amounts)
						},
					},
				},
			},
		},
		SourceAccount: s.masterKeyPair.Address(), // Must be admin for mint to succeed
	}

	// Get RPC URL
	rpcURL, err := s.RPCContainer.GetConnectionString(ctx)
	require.NoError(t, err, "failed to get RPC URL")

	// Execute using helper with extended retries for mint operations
	_, err = ExecuteSorobanOperation(s.httpClient, rpcURL, s.masterAccount, s.masterKeyPair, invokeOp, true, ExtendedConfirmationRetries)
	require.NoError(t, err, "failed to mint SEP-41 tokens")
}

// invokeContractTransfer invokes the transfer function on a SEP-41 token contract.
// This transfers tokens from one address to another. Both sender and recipient can be
// either account addresses (G...) or contract addresses (C...).
//
// When transferring to a contract address (C...), this creates a contract data entry
// in the ledger with key [Balance, C-address] that stores the contract's token balance.
//
// Parameters:
//   - ctx: Context for the operation
//   - t: Testing instance for assertions
//   - tokenContractAddress: The contract address of the token (SEP-41/SAC)
//   - fromAddress: Sender address (G... or C...)
//   - toAddress: Recipient address (G... or C...)
//   - amount: Amount to transfer (in stroops/atomic units)
func (s *SharedContainers) invokeContractTransfer(ctx context.Context, t *testing.T, tokenContractAddress, fromAddress, toAddress string, amount int64) {
	// Decode token contract address to get contract ID
	tokenContractID, err := strkey.Decode(strkey.VersionByteContract, tokenContractAddress)
	require.NoError(t, err, "failed to decode token contract address")
	var tokenID xdr.ContractId
	copy(tokenID[:], tokenContractID)

	// Build addresses using helper
	fromSCAddress, err := parseAddressToScAddress(fromAddress)
	require.NoError(t, err, "failed to parse sender address")

	toSCAddress, err := parseAddressToScAddress(toAddress)
	require.NoError(t, err, "failed to parse recipient address")

	// Build transfer invocation
	transferSym := xdr.ScSymbol("transfer")
	invokeOp := &txnbuild.InvokeHostFunction{
		HostFunction: xdr.HostFunction{
			Type: xdr.HostFunctionTypeHostFunctionTypeInvokeContract,
			InvokeContract: &xdr.InvokeContractArgs{
				ContractAddress: xdr.ScAddress{
					Type:       xdr.ScAddressTypeScAddressTypeContract,
					ContractId: &tokenID,
				},
				FunctionName: transferSym,
				Args: xdr.ScVec{
					{Type: xdr.ScValTypeScvAddress, Address: &fromSCAddress},
					{Type: xdr.ScValTypeScvAddress, Address: &toSCAddress},
					{
						Type: xdr.ScValTypeScvI128,
						I128: &xdr.Int128Parts{
							Hi: 0,
							Lo: xdr.Uint64(amount),
						},
					},
				},
			},
		},
		SourceAccount: s.masterKeyPair.Address(),
	}

	// Get RPC URL
	rpcURL, err := s.RPCContainer.GetConnectionString(ctx)
	require.NoError(t, err, "failed to get RPC URL")

	// Execute using helper
	_, err = ExecuteSorobanOperation(s.httpClient, rpcURL, s.masterAccount, s.masterKeyPair, invokeOp, true, DefaultConfirmationRetries)
	require.NoError(t, err, "failed to transfer tokens")
}
