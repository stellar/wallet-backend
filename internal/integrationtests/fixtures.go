package integrationtests

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"

	"github.com/stellar/go/amount"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"

	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/services"
	"github.com/stellar/wallet-backend/pkg/sorobanauth"
	"github.com/stellar/wallet-backend/pkg/utils"
)

type Fixtures struct {
	NetworkPassphrase string
	SourceAccountKP   *keypair.Full
	RPCService        services.RPCService
}

// prepareClassicOps prepares a slice of strings, each representing a payment operation XDR. Currently only returns one operation (payment).
func (f *Fixtures) prepareClassicOps() ([]string, error) {
	paymentOp := &txnbuild.Payment{
		SourceAccount: f.SourceAccountKP.Address(),
		Destination:   f.SourceAccountKP.Address(),
		Amount:        "10",
		Asset:         txnbuild.NativeAsset{},
	}

	paymentOpXDR, err := paymentOp.BuildXDR()
	if err != nil {
		return nil, fmt.Errorf("building payment operation XDR: %w", err)
	}
	b64OpXDR, err := utils.OperationXDRToBase64(paymentOpXDR)
	if err != nil {
		return nil, fmt.Errorf("encoding payment operation XDR to base64: %w", err)
	}

	return []string{b64OpXDR}, nil
}

// prepareInvokeContractOp prepares an invokeContractOp, signs its auth entries, and returns the operation XDR and simulation result JSON.
func (f *Fixtures) prepareInvokeContractOp(ctx context.Context) (opXDR, simResultJSON string, err error) {
	invokeXLMTransferSAC, err := f.createInvokeContractOp()
	if err != nil {
		return "", "", fmt.Errorf("creating invoke contract operation: %w", err)
	}

	return f.prepareSimulateAndSignTransaction(ctx, invokeXLMTransferSAC)
}

// createInvokeContractOp creates an invokeContractOp.
func (f *Fixtures) createInvokeContractOp() (txnbuild.InvokeHostFunction, error) {
	var nativeAssetContractID xdr.Hash
	var err error
	nativeAssetContractID, err = xdr.Asset{Type: xdr.AssetTypeAssetTypeNative}.ContractID(f.NetworkPassphrase)
	if err != nil {
		return txnbuild.InvokeHostFunction{}, fmt.Errorf("getting native asset contract ID: %w", err)
	}

	fromSCAddress, err := SCAccountID(f.SourceAccountKP.Address())
	if err != nil {
		return txnbuild.InvokeHostFunction{}, fmt.Errorf("marshalling from address: %w", err)
	}
	toSCAddress := fromSCAddress

	invokeXLMTransferSAC := txnbuild.InvokeHostFunction{
		SourceAccount: f.SourceAccountKP.Address(),
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

// prepareSimulateAndSignTransaction simulates a transaction containing a contractInvokeOp, signs its auth entries,
// and returns the operation XDR and simulation result JSON.
func (f *Fixtures) prepareSimulateAndSignTransaction(ctx context.Context, op txnbuild.InvokeHostFunction) (opXDR, simResultJSON string, err error) {
	// Step 1: Get health to get the latest ledger
	healthResult, err := f.RPCService.GetHealth()
	if err != nil {
		return "", "", fmt.Errorf("getting health: %w", err)
	}
	latestLedger := healthResult.LatestLedger

	// Step 2: Simulate a transaction with a disposable txSourceAccount, to get the auth entries and simulation results.
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

	simulationResult, err := f.RPCService.SimulateTransaction(txXDR, entities.RPCResourceConfig{})
	if err != nil {
		return "", "", fmt.Errorf("simulating transaction (1): %w", err)
	}
	if simulationResult.Error != "" {
		return "", "", fmt.Errorf("transaction simulation (1) failed with error=%s", simulationResult.Error)
	}

	// 3. If there are auth entries, sign them
	if len(simulationResult.Results) > 0 {
		op, err = f.signInvokeContractOp(ctx, op, latestLedger+100, simulationResult.Results)
		if err != nil {
			return "", "", fmt.Errorf("signing auth entries: %w", err)
		}

		// 4. Simulate the transaction again to get the final simulation result with the signed auth entries.
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
		if txXDR, err = tx.Base64(); err != nil {
			return "", "", fmt.Errorf("encoding transaction to base64 (2): %w", err)
		}
		if simulationResult, err = f.RPCService.SimulateTransaction(txXDR, entities.RPCResourceConfig{}); err != nil {
			return "", "", fmt.Errorf("simulating transaction (2): %w", err)
		}
		if simulationResult.Error != "" {
			return "", "", fmt.Errorf("transaction simulation (2) failed with error=%s", simulationResult.Error)
		}
	}

	// 5. Build the operation XDR and encode it to base64.
	opXDRObj, err := op.BuildXDR()
	if err != nil {
		return "", "", fmt.Errorf("building operation XDR: %w", err)
	}
	opXDR, err = utils.OperationXDRToBase64(opXDRObj)
	if err != nil {
		return "", "", fmt.Errorf("encoding operation XDR to base64: %w", err)
	}

	// 6. Encode the simulation result to JSON.
	simResBytes, err := json.Marshal(simulationResult)
	if err != nil {
		return "", "", fmt.Errorf("encoding simulation result to JSON: %w", err)
	}

	return opXDR, string(simResBytes), nil
}

// signInvokeContractOp signs the auth entries of an invokeContractOp.
func (f *Fixtures) signInvokeContractOp(ctx context.Context, op txnbuild.InvokeHostFunction, validUntilLedgerSeq uint32, simulationResponseResults []entities.RPCSimulateHostFunctionResult) (txnbuild.InvokeHostFunction, error) {
	authSigner := sorobanauth.AuthSigner{NetworkPassphrase: f.NetworkPassphrase}

	simulateResults := make([]entities.RPCSimulateHostFunctionResult, len(simulationResponseResults))
	for i, result := range simulationResponseResults {
		updatedResult := result
		for j, auth := range result.Auth {
			nonce, err := rand.Int(rand.Reader, big.NewInt(math.MaxInt64))
			if err != nil {
				return txnbuild.InvokeHostFunction{}, fmt.Errorf("generating random nonce: %w", err)
			}
			if updatedResult.Auth[j], err = authSigner.AuthorizeEntry(auth, nonce.Int64(), validUntilLedgerSeq, f.SourceAccountKP); err != nil {
				var unsupportedCredentialsTypeError *sorobanauth.UnsupportedCredentialsTypeError
				if errors.As(err, &unsupportedCredentialsTypeError) {
					log.Ctx(ctx).Warnf("Skipping auth entry signature at [i=%d,j=%d]: %v", i, j, err)
					updatedResult.Auth[j] = auth
					continue
				}
				return txnbuild.InvokeHostFunction{}, fmt.Errorf("signing auth at [i=%d,j=%d]: %w", i, j, err)
			}
		}

		simulateResults[i] = updatedResult
	}

	op.Auth = simulateResults[0].Auth
	return op, nil
}
