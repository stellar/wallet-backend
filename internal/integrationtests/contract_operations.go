package integrationtests

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"math"
	"math/big"

	"github.com/stellar/go/amount"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"

	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/pkg/sorobanauth"
	"github.com/stellar/wallet-backend/pkg/utils"
)

// prepareInvokeContractOp prepares an invoke contract operation and simulates it to get the auth entries, sign them,
// and retrieve the final simulation result.
func (it *IntegrationTests) prepareInvokeContractOp() (opXDR, simResultXDR string, err error) {
	invokeXLMTransferSAC, err := it.createInvokeContractOp()
	if err != nil {
		return "", "", fmt.Errorf("creating invoke contract operation: %w", err)
	}

	return it.prepareSimulateAndSignTransaction(invokeXLMTransferSAC)
}

// createInvokeContractOp creates an invoke contract operation for the native asset contract.
func (it *IntegrationTests) createInvokeContractOp() (txnbuild.InvokeHostFunction, error) {
	var nativeAssetContractID xdr.Hash
	var err error
	nativeAssetContractID, err = xdr.Asset{Type: xdr.AssetTypeAssetTypeNative}.ContractID(it.NetworkPassphrase)
	if err != nil {
		return txnbuild.InvokeHostFunction{}, fmt.Errorf("getting native asset contract ID: %w", err)
	}

	fromSCAddress, err := SCAccountID(it.SourceAccountKP.Address())
	if err != nil {
		return txnbuild.InvokeHostFunction{}, fmt.Errorf("marshalling from address: %w", err)
	}
	toSCAddress := fromSCAddress

	invokeXLMTransferSAC := txnbuild.InvokeHostFunction{
		HostFunction: xdr.HostFunction{
			Type: xdr.HostFunctionTypeHostFunctionTypeInvokeContract,
			InvokeContract: &xdr.InvokeContractArgs{
				ContractAddress: xdr.ScAddress{
					Type:       xdr.ScAddressTypeScAddressTypeContract,
					ContractId: &nativeAssetContractID,
				},
				FunctionName: "transfer",
				Args: xdr.ScVec{
					{
						Type:    xdr.ScValTypeScvAddress,
						Address: &fromSCAddress,
					},
					{
						Type:    xdr.ScValTypeScvAddress,
						Address: &toSCAddress,
					},
					{
						Type: xdr.ScValTypeScvI128,
						I128: &xdr.Int128Parts{
							Hi: xdr.Int64(0),
							Lo: xdr.Uint64(uint64(amount.MustParse("10"))),
						},
					},
				},
			},
		},
	}

	return invokeXLMTransferSAC, nil
}

// prepareSimulateAndSignTransaction simulates a transaction with a disposable source account, to get the auth entries
// and simulation results.
func (it *IntegrationTests) prepareSimulateAndSignTransaction(op txnbuild.InvokeHostFunction) (opXDR, simResultXDR string, err error) {
	// Step 1: Get health to get the latest ledger
	healthResult, err := it.RPCService.GetHealth()
	if err != nil {
		return "", "", fmt.Errorf("getting health: %w", err)
	}
	latestLedger := healthResult.LatestLedger

	// Step 2: Simulation a transaction with a disposable txSourceAccount, to get the auth entries and simulation results.
	simulationSourceAccKP := keypair.MustRandom()
	simulationSourceAcc := txnbuild.SimpleAccount{AccountID: simulationSourceAccKP.Address(), Sequence: 0}
	tx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount: &simulationSourceAcc,
		Operations:    []txnbuild.Operation{&op},
		BaseFee:       txnbuild.MinBaseFee,
		Preconditions: txnbuild.Preconditions{
			TimeBounds: txnbuild.NewTimeout(300),
		},
		IncrementSequenceNum: true,
	})
	if err != nil {
		return "", "", fmt.Errorf("building transaction (1): %w", err)
	}
	txXDR, err := tx.Base64()
	if err != nil {
		return "", "", fmt.Errorf("encoding transaction to base64 (1): %w", err)
	}

	simulationResult, err := it.RPCService.SimulateTransaction(txXDR, entities.RPCResourceConfig{})
	if err != nil {
		return "", "", fmt.Errorf("simulating transaction (1): %w", err)
	}
	if simulationResult.Error != "" {
		return "", "", fmt.Errorf("transaction simulation (1) failed with error=%s", simulationResult.Error)
	}

	// TODO: remove these logs
	log.Warnf("🧪 transactionData: %+v", simulationResult.TransactionData)
	log.Warnf("🧪 auth: %+v", simulationResult.Results[0].Auth)
	log.Warnf("🧪 simulateResult(1): %+v", simulationResult)
	fmt.Println("")

	if len(simulationResult.Results) > 0 {
		authSigner := sorobanauth.AuthSigner{NetworkPassphrase: it.NetworkPassphrase}
		// 3.1 If there are auth entries, sign them
		simulateResults := make([]entities.RPCSimulateHostFunctionResult, len(simulationResult.Results))
		for i, result := range simulationResult.Results {
			updatedResult := result
			for j, auth := range result.Auth {
				var nonce *big.Int
				nonce, err = rand.Int(rand.Reader, big.NewInt(math.MaxInt64))
				if err != nil {
					return "", "", fmt.Errorf("generating random nonce: %w", err)
				}
				updatedResult.Auth[j], err = authSigner.AuthorizeEntry(auth, nonce.Int64(), latestLedger+100, it.SourceAccountKP)
				if err != nil {
					return "", "", fmt.Errorf("signing auth at [i=%d,j=%d]: %w", i, j, err)
				}
			}

			simulateResults[i] = updatedResult
		}

		op.Auth = simulateResults[0].Auth

		// 3.2 If there are auth entries, simulate the transaction again
		tx, err = txnbuild.NewTransaction(txnbuild.TransactionParams{
			SourceAccount: &simulationSourceAcc,
			Operations:    []txnbuild.Operation{&op},
			BaseFee:       txnbuild.MinBaseFee,
			Preconditions: txnbuild.Preconditions{
				TimeBounds: txnbuild.NewTimeout(300),
			},
			IncrementSequenceNum: true,
		})
		if err != nil {
			return "", "", fmt.Errorf("building transaction (2): %w", err)
		}
		txXDR, err = tx.Base64()
		if err != nil {
			return "", "", fmt.Errorf("encoding transaction to base64 (2): %w", err)
		}
		simulationResult, err = it.RPCService.SimulateTransaction(txXDR, entities.RPCResourceConfig{})
		if err != nil {
			return "", "", fmt.Errorf("simulating transaction (2): %w", err)
		}
		if simulationResult.Error != "" {
			return "", "", fmt.Errorf("transaction simulation (2) failed with error=%s", simulationResult.Error)
		}
	}

	opXDRObj, err := op.BuildXDR()
	if err != nil {
		return "", "", fmt.Errorf("building operation XDR: %w", err)
	}
	opXDR, err = utils.OperationXDRToBase64(opXDRObj)
	if err != nil {
		return "", "", fmt.Errorf("encoding operation XDR to base64: %w", err)
	}

	// TODO: fix this by creating a custom JSON marshaller for the simulation result.
	simResBytes, err := json.Marshal(simulationResult)
	if err != nil {
		return "", "", fmt.Errorf("encoding simulation result to JSON: %w", err)
	}

	return opXDR, string(simResBytes), nil
}
