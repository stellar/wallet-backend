package integrationtests

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strings"
	"time"

	"github.com/stellar/go/amount"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"

	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/services"
	"github.com/stellar/wallet-backend/pkg/sorobanauth"
	"github.com/stellar/wallet-backend/pkg/utils"
	"github.com/stellar/wallet-backend/pkg/wbclient/types"
)

type Fixtures struct {
	NetworkPassphrase  string
	PrimaryAccountKP   *keypair.Full
	SecondaryAccountKP *keypair.Full
	RPCService         services.RPCService
}

// preparePaymentOp prepares a payment operation XDR and encodes it to base64.
func (f *Fixtures) preparePaymentOp() (string, *Set[*keypair.Full], error) {
	paymentOp := &txnbuild.Payment{
		SourceAccount: f.PrimaryAccountKP.Address(),
		Destination:   f.PrimaryAccountKP.Address(),
		Amount:        "10",
		Asset:         txnbuild.NativeAsset{},
	}

	paymentOpXDR, err := paymentOp.BuildXDR()
	if err != nil {
		return "", nil, fmt.Errorf("building payment operation XDR: %w", err)
	}
	b64OpXDR, err := utils.OperationXDRToBase64(paymentOpXDR)
	if err != nil {
		return "", nil, fmt.Errorf("encoding payment operation XDR to base64: %w", err)
	}

	return b64OpXDR, NewSet(f.PrimaryAccountKP), nil
}

// prepareSponsoredAccountCreationOps creates an account using sponsored reserves and adds a (bonus) manageData
// operation. The operations used here are:
// - BeginSponsoringFutureReserves
// - CreateAccount
// - ManageData
// - EndSponsoringFutureReserves
func (f *Fixtures) prepareSponsoredAccountCreationOps(newAccountKP *keypair.Full) ([]string, *Set[*keypair.Full], error) {
	operations := []txnbuild.Operation{
		&txnbuild.BeginSponsoringFutureReserves{
			SponsoredID:   newAccountKP.Address(),
			SourceAccount: f.PrimaryAccountKP.Address(),
		},
		&txnbuild.CreateAccount{
			Amount:        "0",
			Destination:   newAccountKP.Address(),
			SourceAccount: f.PrimaryAccountKP.Address(),
		},
		&txnbuild.ManageData{
			Name:          "foo",
			Value:         []byte("bar"),
			SourceAccount: f.PrimaryAccountKP.Address(),
		},
		&txnbuild.EndSponsoringFutureReserves{
			SourceAccount: newAccountKP.Address(),
		},
	}

	b64OpsXDRs := make([]string, len(operations))
	for i, op := range operations {
		opXDR, err := op.BuildXDR()
		if err != nil {
			return nil, nil, fmt.Errorf("building operation XDR: %w", err)
		}
		b64OpsXDRs[i], err = utils.OperationXDRToBase64(opXDR)
		if err != nil {
			return nil, nil, fmt.Errorf("encoding operation XDR to base64: %w", err)
		}
	}

	return b64OpsXDRs, NewSet(f.PrimaryAccountKP, newAccountKP), nil
}

// prepareAccountMergeOp prepares an account merge operation XDR and encodes it to base64.
func (f *Fixtures) prepareAccountMergeOp(newAccountKP *keypair.Full) (string, *Set[*keypair.Full], error) {
	op := &txnbuild.AccountMerge{
		SourceAccount: newAccountKP.Address(),
		Destination:   f.PrimaryAccountKP.Address(),
	}

	opXDR, err := op.BuildXDR()
	if err != nil {
		return "", nil, fmt.Errorf("building account merge operation XDR: %w", err)
	}
	b64OpXDR, err := utils.OperationXDRToBase64(opXDR)
	if err != nil {
		return "", nil, fmt.Errorf("encoding account merge operation XDR to base64: %w", err)
	}

	return b64OpXDR, NewSet(newAccountKP), nil
}

// prepareInvokeContractOp prepares an invokeContractOp, wither with a signed auth entry or not.
func (f *Fixtures) prepareInvokeContractOp(ctx context.Context, sourceAccountKP *keypair.Full) (opXDR string, txSigners *Set[*keypair.Full], simulationResponse entities.RPCSimulateTransactionResult, err error) {
	invokeXLMTransferSAC, err := f.createInvokeContractOp(sourceAccountKP)
	if err != nil {
		return "", nil, entities.RPCSimulateTransactionResult{}, fmt.Errorf("creating invoke contract operation: %w", err)
	}

	opXDR, simulationResponse, err = f.prepareSimulateAndSignContractOp(ctx, invokeXLMTransferSAC)
	if err != nil {
		return "", nil, entities.RPCSimulateTransactionResult{}, fmt.Errorf("preparing simulate and sign contract operation: %w", err)
	}

	return opXDR, NewSet(sourceAccountKP), simulationResponse, nil
}

// createInvokeContractOp creates an invokeContractOp.
func (f *Fixtures) createInvokeContractOp(sourceAccountKP *keypair.Full) (txnbuild.InvokeHostFunction, error) {
	opSourceAccount := ""
	if sourceAccountKP != nil {
		opSourceAccount = sourceAccountKP.Address()
	}

	var nativeAssetContractID xdr.Hash
	var err error
	nativeAssetContractID, err = xdr.Asset{Type: xdr.AssetTypeAssetTypeNative}.ContractID(f.NetworkPassphrase)
	if err != nil {
		return txnbuild.InvokeHostFunction{}, fmt.Errorf("getting native asset contract ID: %w", err)
	}

	fromSCAddress, err := SCAccountID(f.PrimaryAccountKP.Address())
	if err != nil {
		return txnbuild.InvokeHostFunction{}, fmt.Errorf("marshalling from address: %w", err)
	}
	toSCAddress := fromSCAddress

	invokeXLMTransferSAC := txnbuild.InvokeHostFunction{
		SourceAccount: opSourceAccount,
		// The HostFunction must be constructed using `xdr` objects, unlike other operations that utilize `txnbuild` objects or native Go types.
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

// prepareSimulateAndSignContractOp processes a raw contractInvokeOp and returns a signed version along with its simulation result.
// The function performs two simulations:
// 1. The first simulation retrieves the authorization entries and the initial simulation result.
// 2. The second simulation verifies that the authorization entries are correctly signed and obtains the updated simulation result with the signed entries.
func (f *Fixtures) prepareSimulateAndSignContractOp(ctx context.Context, op txnbuild.InvokeHostFunction) (opXDR string, simulationResponse entities.RPCSimulateTransactionResult, err error) {
	// Step 1: Get health to get the latest ledger
	healthResult, err := f.RPCService.GetHealth()
	if err != nil {
		return "", entities.RPCSimulateTransactionResult{}, fmt.Errorf("getting health: %w", err)
	}
	latestLedger := healthResult.LatestLedger

	// Step 2: Simulate a transaction with a disposable txSourceAccount, to get the auth entries and simulation results.
	simulationSourceAccKP := keypair.MustRandom() // NOTE: for simulation, the transaction source account doesn't need to be an existing account.
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
		return "", entities.RPCSimulateTransactionResult{}, fmt.Errorf("building transaction (1): %w", err)
	}
	txXDR, err := tx.Base64()
	if err != nil {
		return "", entities.RPCSimulateTransactionResult{}, fmt.Errorf("encoding transaction to base64 (1): %w", err)
	}

	simulationResponse, err = f.RPCService.SimulateTransaction(txXDR, entities.RPCResourceConfig{})
	if err != nil {
		return "", entities.RPCSimulateTransactionResult{}, fmt.Errorf("simulating transaction (1): %w", err)
	}
	if simulationResponse.Error != "" {
		return "", entities.RPCSimulateTransactionResult{}, fmt.Errorf("transaction simulation (1) failed with error=%s", simulationResponse.Error)
	}

	// 3. If there are auth entries, sign them
	if len(simulationResponse.Results) > 0 {
		op, err = f.signInvokeContractOp(ctx, op, latestLedger+100, simulationResponse.Results)
		if err != nil {
			return "", entities.RPCSimulateTransactionResult{}, fmt.Errorf("signing auth entries: %w", err)
		}

		// 4. Re-simulate the transaction to confirm the auth entries are correctly signed and obtain the final simulation result with these signed entries.
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
			return "", entities.RPCSimulateTransactionResult{}, fmt.Errorf("building transaction (2): %w", err)
		}
		if txXDR, err = tx.Base64(); err != nil {
			return "", entities.RPCSimulateTransactionResult{}, fmt.Errorf("encoding transaction to base64 (2): %w", err)
		}
		if simulationResponse, err = f.RPCService.SimulateTransaction(txXDR, entities.RPCResourceConfig{}); err != nil {
			return "", entities.RPCSimulateTransactionResult{}, fmt.Errorf("simulating transaction (2): %w", err)
		}
		if simulationResponse.Error != "" {
			return "", entities.RPCSimulateTransactionResult{}, fmt.Errorf("transaction simulation (2) failed with error=%s", simulationResponse.Error)
		}
	}

	// 5. Build the operation XDR and encode it to base64.
	opXDRObj, err := op.BuildXDR()
	if err != nil {
		return "", entities.RPCSimulateTransactionResult{}, fmt.Errorf("building operation XDR: %w", err)
	}
	opXDR, err = utils.OperationXDRToBase64(opXDRObj)
	if err != nil {
		return "", entities.RPCSimulateTransactionResult{}, fmt.Errorf("encoding operation XDR to base64: %w", err)
	}

	return opXDR, simulationResponse, nil
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
			if updatedResult.Auth[j], err = authSigner.AuthorizeEntry(auth, nonce.Int64(), validUntilLedgerSeq, f.PrimaryAccountKP); err != nil {
				var unsupportedCredentialsTypeError *sorobanauth.UnsupportedCredentialsTypeError
				if errors.As(err, &unsupportedCredentialsTypeError) {
					log.Ctx(ctx).Warnf("Skipping auth entry signature at [i=%d,j=%d]", i, j)
					updatedResult.Auth[j] = auth
					continue
				}
				return txnbuild.InvokeHostFunction{}, fmt.Errorf("signing auth at [i=%d,j=%d]: %w", i, j, err)
			}
		}

		simulateResults[i] = updatedResult
	}

	// SimulateResults is a slice because the original design aimed to support multiple contract invocations within a
	// single transaction. That plan was later dropped, and now only one contract invocation is allowed per transaction.
	// The slice structure is just a leftover from that earlier design that never fully landed.
	op.Auth = simulateResults[0].Auth
	return op, nil
}

type category string

const (
	categoryStellarClassic category = "STELLAR_CLASSIC"
	categorySoroban        category = "SOROBAN"
)

type UseCase struct {
	name                     string
	category                 category
	delayTime                time.Duration
	txSigners                *Set[*keypair.Full]
	requestedTransaction     types.Transaction
	builtTransactionXDR      string
	signedTransactionXDR     string
	feeBumpedTransactionXDR  string
	feeBumpedTransactionHash string
}

func (u *UseCase) Name() string {
	category := strings.ReplaceAll(string(u.category), "_", "")
	category = cases.Title(language.English).String(category)
	return fmt.Sprintf("%s/%s", category, u.name)
}

func (f *Fixtures) PrepareUseCases(ctx context.Context) ([]UseCase, error) {
	useCases := []UseCase{}
	timeoutSeconds := int64(txTimeout.Seconds())

	// PaymentOp
	if paymentOpXDR, txSigners, err := f.preparePaymentOp(); err != nil {
		return nil, fmt.Errorf("preparing payment operation: %w", err)
	} else {
		useCases = append(useCases, UseCase{
			name:                 "paymentOp",
			category:             categoryStellarClassic,
			txSigners:            txSigners,
			requestedTransaction: types.Transaction{Operations: []string{paymentOpXDR}, Timeout: timeoutSeconds},
		})
	}

	// InvokeContractOp w/ SorobanAuth
	invokeContractOp, txSigners, simulationResponse, err := f.prepareInvokeContractOp(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("preparing invoke contract operation: %w", err)
	} else {
		useCases = append(useCases, UseCase{
			name:                 "invokeContractOp/SorobanAuth",
			category:             categorySoroban,
			txSigners:            txSigners,
			requestedTransaction: types.Transaction{Operations: []string{invokeContractOp}, Timeout: timeoutSeconds, SimulationResult: simulationResponse},
		})
	}

	// InvokeContractOp w/ SourceAccountAuth
	invokeContractOp, txSigners, simulationResponse, err = f.prepareInvokeContractOp(ctx, f.SecondaryAccountKP)
	if err != nil {
		return nil, fmt.Errorf("preparing invoke contract operation: %w", err)
	} else {
		useCases = append(useCases, UseCase{
			name:                 "invokeContractOp/SourceAccountAuth",
			category:             categoryStellarClassic,
			txSigners:            txSigners,
			requestedTransaction: types.Transaction{Operations: []string{invokeContractOp}, Timeout: timeoutSeconds, SimulationResult: simulationResponse},
		})
	}

	newAccountKP := keypair.MustRandom()
	sponsoredAccountCreationOps, txSigners, err := f.prepareSponsoredAccountCreationOps(newAccountKP)
	if err != nil {
		return nil, fmt.Errorf("preparing sponsored account creation operations: %w", err)
	} else {
		useCases = append(useCases, UseCase{
			name:                 "sponsoredAccountCreationOps",
			category:             categoryStellarClassic,
			txSigners:            txSigners,
			requestedTransaction: types.Transaction{Operations: sponsoredAccountCreationOps, Timeout: timeoutSeconds},
		})
	}

	accountMergeOp, txSigners, err := f.prepareAccountMergeOp(newAccountKP)
	if err != nil {
		return nil, fmt.Errorf("preparing account merge operation: %w", err)
	} else {
		useCases = append(useCases, UseCase{
			name:                 "accountMergeOp",
			category:             categoryStellarClassic,
			txSigners:            txSigners,
			delayTime:            10 * time.Second,
			requestedTransaction: types.Transaction{Operations: []string{accountMergeOp}, Timeout: timeoutSeconds},
		})
	}

	return useCases, nil
}
