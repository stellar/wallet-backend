package infrastructure

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"testing"

	"github.com/stellar/go/strkey"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/require"
)

// deployNativeAssetSAC deploys the Stellar Asset Contract for the native asset (XLM)
func (s *SharedContainers) deployNativeAssetSAC(ctx context.Context, t *testing.T) {
	// Create the InvokeHostFunction operation to deploy the native asset contract
	nativeAsset := xdr.Asset{Type: xdr.AssetTypeAssetTypeNative}
	deployOp := &txnbuild.InvokeHostFunction{
		HostFunction: xdr.HostFunction{
			Type: xdr.HostFunctionTypeHostFunctionTypeCreateContract,
			CreateContract: &xdr.CreateContractArgs{
				ContractIdPreimage: xdr.ContractIdPreimage{
					Type:      xdr.ContractIdPreimageTypeContractIdPreimageFromAsset,
					FromAsset: &nativeAsset,
				},
				Executable: xdr.ContractExecutable{
					Type: xdr.ContractExecutableTypeContractExecutableStellarAsset,
				},
			},
		},
		SourceAccount: s.masterKeyPair.Address(),
	}

	// Execute the deployment operation
	_, err := executeSorobanOperation(ctx, t, s, deployOp, false, DefaultConfirmationRetries)
	require.NoError(t, err, "failed to deploy native asset SAC")
}

// deployCreditAssetSAC deploys the Stellar Asset Contract for a credit asset (non-native)
func (s *SharedContainers) deployCreditAssetSAC(ctx context.Context, t *testing.T, assetCode, issuer string) {
	// Create the credit asset XDR based on asset code length
	var creditAsset xdr.Asset

	if len(assetCode) <= 4 {
		// AlphaNum4 for asset codes 1-4 characters (e.g., "USDC")
		var assetCode4 xdr.AssetCode4
		copy(assetCode4[:], assetCode)
		creditAsset = xdr.Asset{
			Type: xdr.AssetTypeAssetTypeCreditAlphanum4,
			AlphaNum4: &xdr.AlphaNum4{
				AssetCode: assetCode4,
				Issuer:    xdr.MustAddress(issuer),
			},
		}
	} else {
		// AlphaNum12 for asset codes 5-12 characters
		var assetCode12 xdr.AssetCode12
		copy(assetCode12[:], assetCode)
		creditAsset = xdr.Asset{
			Type: xdr.AssetTypeAssetTypeCreditAlphanum12,
			AlphaNum12: &xdr.AlphaNum12{
				AssetCode: assetCode12,
				Issuer:    xdr.MustAddress(issuer),
			},
		}
	}

	// Create the InvokeHostFunction operation to deploy the credit asset contract
	deployOp := &txnbuild.InvokeHostFunction{
		HostFunction: xdr.HostFunction{
			Type: xdr.HostFunctionTypeHostFunctionTypeCreateContract,
			CreateContract: &xdr.CreateContractArgs{
				ContractIdPreimage: xdr.ContractIdPreimage{
					Type:      xdr.ContractIdPreimageTypeContractIdPreimageFromAsset,
					FromAsset: &creditAsset,
				},
				Executable: xdr.ContractExecutable{
					Type: xdr.ContractExecutableTypeContractExecutableStellarAsset,
				},
			},
		},
		SourceAccount: s.masterKeyPair.Address(),
	}

	// Execute the deployment operation
	_, err := executeSorobanOperation(ctx, t, s, deployOp, false, DefaultConfirmationRetries)
	require.NoError(t, err, "failed to deploy credit asset SAC for %s", assetCode)
}

// uploadContractWasm uploads a Soroban contract WASM bytecode to the ledger.
// This is the first step in deploying a Soroban smart contract. The WASM code
// is stored in the ledger and can be referenced by its hash for deployment.
//
// Parameters:
//   - ctx: Context for the operation
//   - t: Testing instance for assertions
//   - wasmBytes: The compiled WASM bytecode to upload
//
// Returns:
//   - xdr.Hash: The hash of the uploaded WASM code, used for deployment
//
// The function:
//  1. Builds an InvokeHostFunction operation with HostFunctionTypeUploadContractWasm
//  2. Simulates the transaction to get resource footprint and fees
//  3. Signs the transaction with the master key
//  4. Submits to RPC and waits for confirmation
func (s *SharedContainers) uploadContractWasm(ctx context.Context, t *testing.T, wasmBytes []byte) xdr.Hash {
	// Create the InvokeHostFunction operation to upload WASM
	uploadOp := &txnbuild.InvokeHostFunction{
		HostFunction: xdr.HostFunction{
			Type: xdr.HostFunctionTypeHostFunctionTypeUploadContractWasm,
			Wasm: &wasmBytes,
		},
		SourceAccount: s.masterKeyPair.Address(),
	}

	// Execute the WASM upload operation
	_, err := executeSorobanOperation(ctx, t, s, uploadOp, false, DefaultConfirmationRetries)
	require.NoError(t, err, "failed to upload WASM")

	// Compute and return WASM hash from the uploaded bytecode
	wasmHash := xdr.Hash(sha256.Sum256(wasmBytes))
	return wasmHash
}

// deployContractWithConstructor deploys a Soroban contract with optional constructor arguments.
// This function uses CreateContractArgsV2 which supports passing arguments to the contract's
// __constructor function during deployment (if the contract has one).
//
// For contracts WITHOUT a constructor:
//   - Pass an empty slice: []xdr.ScVal{}
//   - The contract will deploy normally without any initialization
//
// For contracts WITH a constructor (like soroban-examples token):
//   - Pass constructor arguments as []xdr.ScVal
//   - The constructor runs atomically during deployment
//   - Example: token contract needs [admin, decimal, name, symbol]
//
// Parameters:
//   - ctx: Context for logging
//   - t: Testing context for assertions
//   - wasmHash: Hash of the uploaded WASM bytecode
//   - constructorArgs: Constructor arguments (empty slice if no constructor)
//
// Returns:
//   - Contract address (C...) of the deployed contract
//
// Process:
//  1. Generate random salt for unique contract address
//  2. Build CreateContractArgsV2 with constructor arguments
//  3. Simulate to get resource footprint
//  4. Sign and submit deployment transaction
//  5. Wait for confirmation
//  6. Calculate and return contract address
func (s *SharedContainers) deployContractWithConstructor(ctx context.Context, t *testing.T, wasmHash xdr.Hash, constructorArgs []xdr.ScVal) string {
	// Step 1: Generate random salt for unique contract address
	// The salt ensures each deployment creates a different contract address
	salt := make([]byte, 32)
	_, err := rand.Read(salt)
	require.NoError(t, err, "failed to generate salt")
	var saltHash xdr.Uint256
	copy(saltHash[:], salt)

	// Step 2: Create deployer address from master account
	deployerAccountID := xdr.MustAddress(s.masterKeyPair.Address())

	// Step 3: Create ContractIdPreimage for the deployment
	// This will be used both for the deployment operation and to compute the contract address
	preimage := xdr.ContractIdPreimage{
		Type: xdr.ContractIdPreimageTypeContractIdPreimageFromAddress,
		FromAddress: &xdr.ContractIdPreimageFromAddress{
			Address: xdr.ScAddress{
				Type:      xdr.ScAddressTypeScAddressTypeAccount,
				AccountId: &deployerAccountID,
			},
			Salt: saltHash,
		},
	}

	// Step 4: Create the InvokeHostFunction operation to deploy the contract
	// We use CreateContractV2 which supports constructor arguments
	deployOp := &txnbuild.InvokeHostFunction{
		HostFunction: xdr.HostFunction{
			Type: xdr.HostFunctionTypeHostFunctionTypeCreateContractV2, // V2 for constructor support
			CreateContractV2: &xdr.CreateContractArgsV2{
				ContractIdPreimage: preimage,
				Executable: xdr.ContractExecutable{
					Type:     xdr.ContractExecutableTypeContractExecutableWasm,
					WasmHash: &wasmHash,
				},
				ConstructorArgs: constructorArgs, // Pass constructor args (empty slice if none)
			},
		},
		SourceAccount: s.masterKeyPair.Address(),
	}

	// Step 5: Execute the contract deployment operation
	// requireAuth=true because CreateContractV2 with constructor requires deployer authorization
	_, err = executeSorobanOperation(ctx, t, s, deployOp, true, DefaultConfirmationRetries)
	require.NoError(t, err, "failed to deploy contract with constructor")

	// Step 6: Calculate and return the contract address from the preimage
	// The contract address is deterministically computed from the deployer address and salt
	contractAddress, err := s.calculateContractID(networkPassphrase, preimage.MustFromAddress())
	require.NoError(t, err, "failed to calculate contract ID")
	return contractAddress
}

// calculateContractID calculates the contract ID for a wallet creation transaction based on the network passphrase, deployer account and salt.
//
// More info: https://developers.stellar.org/docs/build/smart-contracts/example-contracts/deployer#how-it-works
func (s *SharedContainers) calculateContractID(networkPassphrase string, deployerAddress xdr.ContractIdPreimageFromAddress) (string, error) {
	networkHash := xdr.Hash(sha256.Sum256([]byte(networkPassphrase)))

	hashIDPreimage := xdr.HashIdPreimage{
		Type: xdr.EnvelopeTypeEnvelopeTypeContractId,
		ContractId: &xdr.HashIdPreimageContractId{
			NetworkId: networkHash,
			ContractIdPreimage: xdr.ContractIdPreimage{
				Type:        xdr.ContractIdPreimageTypeContractIdPreimageFromAddress,
				FromAddress: &deployerAddress,
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
