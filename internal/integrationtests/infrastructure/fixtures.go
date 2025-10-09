package infrastructure

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

// Tested
// --- Classic:
// &txnbuild.Payment{},
// &txnbuild.CreateAccount{},
// &txnbuild.AccountMerge{},
// &txnbuild.BeginSponsoringFutureReserves{},
// &txnbuild.EndSponsoringFutureReserves{},
// &txnbuild.ManageData{},
// &txnbuild.ChangeTrust{},
// &txnbuild.CreatePassiveSellOffer{},
// &txnbuild.ManageSellOffer{},
// &txnbuild.PathPaymentStrictReceive{},
// &txnbuild.PathPaymentStrictSend{},
// &txnbuild.ManageBuyOffer{},
// &txnbuild.SetOptions{},
// &txnbuild.Clawback{},
// &txnbuild.SetTrustLineFlags{},
// --- Soroban:
// &txnbuild.InvokeHostFunction{},

// Missing
// --- Classic:
// &txnbuild.ClawbackClaimableBalance{},
// &txnbuild.LiquidityPoolDeposit{},
// &txnbuild.LiquidityPoolWithdraw{},
// &txnbuild.CreateClaimableBalance{},
// &txnbuild.ClaimClaimableBalance{},
// &txnbuild.RevokeSponsorship{},
// --- Soroban:
// &txnbuild.ExtendFootprintTtl{},
// &txnbuild.RestoreFootprint{},

type Fixtures struct {
	NetworkPassphrase     string
	PrimaryAccountKP      *keypair.Full
	SecondaryAccountKP    *keypair.Full
	SponsoredNewAccountKP *keypair.Full
	RPCService            services.RPCService
}

// preparePaymentOp creates a payment operation.
func (f *Fixtures) preparePaymentOp() (string, *Set[*keypair.Full], error) {
	/*
		Should generate 3 state changes:
		- 1 BALANCE/DEBIT change for wallet-backend's distribution account for the fee of the transaction
		- 1 BALANCE/CREDIT change for secondary account for the amount of the payment
		- 1 BALANCE/DEBIT change for primary account for the amount of the payment
	*/
	paymentOp := &txnbuild.Payment{
		SourceAccount: f.PrimaryAccountKP.Address(),
		Destination:   f.SecondaryAccountKP.Address(),
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

// prepareSponsoredAccountCreationOps creates an account using sponsored reserves.
// NOTE: the account created here is meant to be deleted at a later time through an account merge operation.
// NOTE 2: one manageData operation is included here as a bonus.
func (f *Fixtures) prepareSponsoredAccountCreationOps() ([]string, *Set[*keypair.Full], error) {
	/*
		Should generate 8 state changes:
		- 1 ACCOUNT/CREATE change for the new account
		- 1 BALANCE/CREDIT change for the new account (amount is 5)
		- 1 BALANCE/DEBIT change from primary account (amount is 5)
		- 1 METADATA/DATA_ENTRY creation change for primary account with keyvalue "foo"="bar"
		- 1 RESERVES/SPONSOR change for primary account with sponsored account = new account
		- 1 RESERVES/SPONSOR change for new account with sponsor = primary account
		- 1 RESERVES/UNSPONSOR change for primary account with sponsored account = new account
		- 1 RESERVES/UNSPONSOR change for new account with sponsor = primary account
	*/
	operations := []txnbuild.Operation{
		&txnbuild.BeginSponsoringFutureReserves{
			SponsoredID:   f.SponsoredNewAccountKP.Address(),
			SourceAccount: f.PrimaryAccountKP.Address(),
		},
		&txnbuild.CreateAccount{
			Amount:        "5",
			Destination:   f.SponsoredNewAccountKP.Address(),
			SourceAccount: f.PrimaryAccountKP.Address(),
		},
		&txnbuild.ManageData{
			Name:          "foo",
			Value:         []byte("bar"),
			SourceAccount: f.PrimaryAccountKP.Address(),
		},
		&txnbuild.EndSponsoringFutureReserves{
			SourceAccount: f.SponsoredNewAccountKP.Address(),
		},
	}

	b64OpsXDRs, err := ConvertOperationsToBase64XDR(operations)
	if err != nil {
		return nil, nil, fmt.Errorf("encoding operations to base64 XDR: %w", err)
	}

	return b64OpsXDRs, NewSet(f.PrimaryAccountKP, f.SponsoredNewAccountKP), nil
}

// prepareCustomAssetsOps creates a customAsset, creates liquidity for it through a passive sell offer, and then
// consumes that liquidity through path payments and manage offers.
func (f *Fixtures) prepareCustomAssetsOps() ([]string, *Set[*keypair.Full], error) {
	/*
		Should generate ~15+ state changes (variable based on trade execution):

		Guaranteed state changes (minimum 7):
		- 2 changes for creating trustline (1 TRUSTLINE/ADD + 1 BALANCE_AUTHORIZATION based on issuer flags)
		- 3 changes for TEST2 payment (1 BALANCE/MINT for Primary as issuer, 1 BALANCE/CREDIT for Secondary, 1 BALANCE/DEBIT from Primary)
		- 1 TRUSTLINE/REMOVE change for removing trustline
		- 1 BALANCE/DEBIT for Secondary when all remaining TEST2 is sent back

		Variable trade-related changes (7+ additional):
		- CreatePassiveSellOffer: May not generate state changes if not immediately matched
		- PathPaymentStrictSend: Generates BALANCE/DEBIT for sender and BALANCE/CREDIT for receiver per trade
		- ManageSellOffer: Generates trade state changes when matched
		- ManageBuyOffer: Generates trade state changes when matched
		- PathPaymentStrictReceive: Generates BALANCE/DEBIT for sender and BALANCE/CREDIT for receiver per trade
		- Each trade execution creates additional debit/credit pairs based on liquidity consumed
	*/
	xlmAsset := txnbuild.NativeAsset{}
	customAsset := txnbuild.CreditAsset{
		Issuer: f.PrimaryAccountKP.Address(),
		Code:   "TEST2",
	}

	operations := []txnbuild.Operation{
		// The Secondary account creates a trustline and gets customAsset minted by the Primary account.
		&txnbuild.ChangeTrust{
			Line:          txnbuild.ChangeTrustAssetWrapper{Asset: customAsset},
			SourceAccount: f.SecondaryAccountKP.Address(),
		},
		&txnbuild.Payment{
			Destination:   f.SecondaryAccountKP.Address(),
			Amount:        "3000",
			Asset:         customAsset,
			SourceAccount: f.PrimaryAccountKP.Address(),
		},

		// The Primary account creates a passive sell offer to create customAsset liquidity.
		&txnbuild.CreatePassiveSellOffer{
			Selling: xlmAsset,
			Buying:  customAsset,
			Amount:  "3",
			Price: xdr.Price{
				N: xdr.Int32(1000),
				D: xdr.Int32(1),
			},
			SourceAccount: f.PrimaryAccountKP.Address(),
		},

		// The secondary account uses the customAsset liquidity through offers and path payments.
		&txnbuild.PathPaymentStrictSend{
			SendAsset:     customAsset,
			SendAmount:    "1000",
			DestAsset:     xlmAsset,
			DestMin:       "1",
			Destination:   f.PrimaryAccountKP.Address(),
			SourceAccount: f.SecondaryAccountKP.Address(),
		},
		&txnbuild.ManageSellOffer{
			Selling: customAsset,
			Buying:  xlmAsset,
			Amount:  "500",
			Price: xdr.Price{
				N: xdr.Int32(1),
				D: xdr.Int32(1000),
			},
			SourceAccount: f.SecondaryAccountKP.Address(),
		},
		&txnbuild.ManageBuyOffer{
			Selling: customAsset,
			Buying:  xlmAsset,
			Amount:  "0.5",
			Price: xdr.Price{
				N: xdr.Int32(1000),
				D: xdr.Int32(1),
			},
			SourceAccount: f.SecondaryAccountKP.Address(),
		},
		&txnbuild.PathPaymentStrictReceive{
			SendAsset:     customAsset,
			SendMax:       "1000",
			DestAsset:     xlmAsset,
			DestAmount:    "1",
			Destination:   f.PrimaryAccountKP.Address(),
			SourceAccount: f.SecondaryAccountKP.Address(),
		},

		// With the liquidity worn out, and all the customAsset being burned back to the Primary account,
		// the Secondary account removes the trustline.
		&txnbuild.ChangeTrust{
			Line:          txnbuild.ChangeTrustAssetWrapper{Asset: customAsset},
			Limit:         "0",
			SourceAccount: f.SecondaryAccountKP.Address(),
		},
	}

	b64OpsXDRs, err := ConvertOperationsToBase64XDR(operations)
	if err != nil {
		return nil, nil, fmt.Errorf("encoding operations to base64 XDR: %w", err)
	}

	return b64OpsXDRs, NewSet(f.PrimaryAccountKP, f.SecondaryAccountKP), nil
}

// prepareAuthRequiredOps creates a flow to mint and then clawback SEP-8 auth required customAsset funds.
func (f *Fixtures) preparedAuthRequiredOps() ([]string, *Set[*keypair.Full], error) {
	/*
		Should generate ~18 state changes:
		- 3 FLAGS/SET changes for setting auth flags (auth_required, auth_revocable, auth_clawback_enabled)
		- 2 changes for creating trustline (1 TRUSTLINE/ADD + 1 BALANCE_AUTHORIZATION based on issuer flags)
		- 1 BALANCE_AUTHORIZATION/SET change for setting AUTHORIZED flag on trustline
		- 3 changes for TEST1 payment (1 BALANCE/MINT for Primary as issuer, 1 BALANCE/CREDIT for Secondary, 1 BALANCE/DEBIT from Primary)
		- 1 BALANCE_AUTHORIZATION/CLEAR change for clearing AUTHORIZED flag on trustline
		- 2 changes for clawback (1 BALANCE/BURN for Primary as issuer, 1 BALANCE/DEBIT from Secondary)
		- 1 TRUSTLINE/REMOVE change for removing trustline
		- 3 FLAGS/CLEAR changes for clearing auth flags (auth_required, auth_revocable, auth_clawback_enabled)
	*/
	customAsset := txnbuild.CreditAsset{
		Issuer: f.PrimaryAccountKP.Address(),
		Code:   "TEST1",
	}

	operations := []txnbuild.Operation{
		// Prepare Primary account to be a SEP-8 auth required issuer
		&txnbuild.SetOptions{
			SetFlags: []txnbuild.AccountFlag{
				txnbuild.AuthRequired,
				txnbuild.AuthRevocable,
				txnbuild.AuthClawbackEnabled,
			},
			SourceAccount: f.PrimaryAccountKP.Address(),
		},

		// The Secondary account creates a trustline for customAsset.
		&txnbuild.ChangeTrust{
			Line:          txnbuild.ChangeTrustAssetWrapper{Asset: customAsset},
			SourceAccount: f.SecondaryAccountKP.Address(),
		},

		// Sandwitch authorization to mint customAsset funds from the Primary account to the Secondary account.
		&txnbuild.SetTrustLineFlags{
			Trustor: f.SecondaryAccountKP.Address(),
			Asset:   customAsset,
			SetFlags: []txnbuild.TrustLineFlag{
				txnbuild.TrustLineAuthorized,
			},
			SourceAccount: f.PrimaryAccountKP.Address(),
		},
		&txnbuild.Payment{
			Destination:   f.SecondaryAccountKP.Address(),
			Amount:        "1000",
			Asset:         customAsset,
			SourceAccount: f.PrimaryAccountKP.Address(),
		},
		&txnbuild.SetTrustLineFlags{
			Trustor: f.SecondaryAccountKP.Address(),
			Asset:   customAsset,
			ClearFlags: []txnbuild.TrustLineFlag{
				txnbuild.TrustLineAuthorized,
			},
			SourceAccount: f.PrimaryAccountKP.Address(),
		},

		// Clawback the funds from the Secondary account back to the Primary account.
		&txnbuild.Clawback{
			From:          f.SecondaryAccountKP.Address(),
			Amount:        "1000",
			Asset:         customAsset,
			SourceAccount: f.PrimaryAccountKP.Address(),
		},

		// Remove the trustline from the Secondary account.
		&txnbuild.ChangeTrust{
			Line:          txnbuild.ChangeTrustAssetWrapper{Asset: customAsset},
			SourceAccount: f.SecondaryAccountKP.Address(),
			Limit:         "0",
		},

		// The Primary account stops being a SEP-8 auth required issuer.
		&txnbuild.SetOptions{
			ClearFlags: []txnbuild.AccountFlag{
				txnbuild.AuthRequired,
				txnbuild.AuthRevocable,
				txnbuild.AuthClawbackEnabled,
			},
			SourceAccount: f.PrimaryAccountKP.Address(),
		},
	}

	b64OpsXDRs, err := ConvertOperationsToBase64XDR(operations)
	if err != nil {
		return nil, nil, fmt.Errorf("encoding operations to base64 XDR: %w", err)
	}

	return b64OpsXDRs, NewSet(f.PrimaryAccountKP, f.SecondaryAccountKP), nil
}

// prepareAccountMergeOp creates an account merge operation.
func (f *Fixtures) prepareAccountMergeOp() (string, *Set[*keypair.Full], error) {
	/*
		Should generate 3 state changes:
		- 1 BALANCE/DEBIT change for transaction fee
		- 1 ACCOUNT/MERGE change for the merged account (includes implicit debit of all balance)
		- 1 BALANCE/CREDIT change for the destination account receiving the merged balance
	*/
	op := &txnbuild.AccountMerge{
		SourceAccount: f.SponsoredNewAccountKP.Address(),
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

	return b64OpXDR, NewSet(f.SponsoredNewAccountKP), nil
}

// prepareInvokeContractOp creates an invokeContractOp. The signature type will be one of the following:
// - If sourceAccountKP == nil: SorobanAuthEntry will be used, and the transaction won't require another signature later.
// - If sourceAccountKP != nil: this contract invocation will rely on the sourceAccount being a transaction signer at a later point.
func (f *Fixtures) prepareInvokeContractOp(ctx context.Context, sourceAccountKP *keypair.Full) (opXDR string, txSigners *Set[*keypair.Full], simulationResponse entities.RPCSimulateTransactionResult, err error) {
	/*
		Should generate 3 state changes:
		- 1 BALANCE/DEBIT change for transaction fee
		- 2 BALANCE changes for the XLM transfer (1 BALANCE/DEBIT from source, 1 BALANCE/CREDIT to destination)
		Note: Even self-transfers (Primary to Primary) generate both debit and credit state changes
	*/
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

// createInvokeContractOp creates an invokeContractOp, optionally setting a source account.
func (f *Fixtures) createInvokeContractOp(sourceAccountKP *keypair.Full) (txnbuild.InvokeHostFunction, error) {
	opSourceAccount := ""
	if sourceAccountKP != nil {
		opSourceAccount = sourceAccountKP.Address()
	}

	var nativeAssetContractID xdr.ContractId
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
	categoryStellarClassic category      = "STELLAR_CLASSIC"
	categorySoroban        category      = "SOROBAN"
	timeout                time.Duration = 45 * time.Second
)

type UseCase struct {
	name                    string
	category                category
	DelayTime               time.Duration
	TxSigners               *Set[*keypair.Full]
	RequestedTransaction    types.Transaction
	BuiltTransactionXDR     string
	SignedTransactionXDR    string
	FeeBumpedTransactionXDR string
	SendTransactionResult   entities.RPCSendTransactionResult
	GetTransactionResult    entities.RPCGetTransactionResult
}

func (u *UseCase) Name() string {
	category := strings.ReplaceAll(string(u.category), "_", "")
	category = cases.Title(language.English).String(category)
	return fmt.Sprintf("%s/%s", category, u.name)
}

func (f *Fixtures) PrepareUseCases(ctx context.Context) ([]*UseCase, error) {
	useCases := []*UseCase{}
	timeoutSeconds := int64(timeout.Seconds())

	// PaymentOp
	if paymentOpXDR, txSigners, err := f.preparePaymentOp(); err != nil {
		return nil, fmt.Errorf("preparing payment operation: %w", err)
	} else {
		txXDR, err := f.buildTransactionXDR([]string{paymentOpXDR}, timeoutSeconds)
		if err != nil {
			return nil, fmt.Errorf("building transaction XDR for paymentOp: %w", err)
		}
		useCases = append(useCases, &UseCase{
			name:                 "paymentOp",
			category:             categoryStellarClassic,
			TxSigners:            txSigners,
			RequestedTransaction: types.Transaction{TransactionXdr: txXDR},
		})
	}

	// SponsoredAccountCreationOps
	sponsoredAccountCreationOps, txSigners, err := f.prepareSponsoredAccountCreationOps()
	if err != nil {
		return nil, fmt.Errorf("preparing sponsored account creation operations: %w", err)
	} else {
		txXDR, txErr := f.buildTransactionXDR(sponsoredAccountCreationOps, timeoutSeconds)
		if txErr != nil {
			return nil, fmt.Errorf("building transaction XDR for sponsoredAccountCreationOps: %w", txErr)
		}
		useCases = append(useCases, &UseCase{
			name:                 "sponsoredAccountCreationOps",
			category:             categoryStellarClassic,
			TxSigners:            txSigners,
			RequestedTransaction: types.Transaction{TransactionXdr: txXDR},
		})
	}

	// CustomAssetsOps
	customAssetsOps, txSigners, err := f.prepareCustomAssetsOps()
	if err != nil {
		return nil, fmt.Errorf("preparing custom assets operations: %w", err)
	} else {
		txXDR, txErr := f.buildTransactionXDR(customAssetsOps, timeoutSeconds)
		if txErr != nil {
			return nil, fmt.Errorf("building transaction XDR for customAssetsOps: %w", txErr)
		}
		useCases = append(useCases, &UseCase{
			name:                 "customAssetsOps",
			category:             categoryStellarClassic,
			TxSigners:            txSigners,
			RequestedTransaction: types.Transaction{TransactionXdr: txXDR},
		})
	}

	// AuthRequiredOps
	authRequiredOps, txSigners, err := f.preparedAuthRequiredOps()
	if err != nil {
		return nil, fmt.Errorf("preparing auth required operations: %w", err)
	} else {
		txXDR, txErr := f.buildTransactionXDR(authRequiredOps, timeoutSeconds)
		if txErr != nil {
			return nil, fmt.Errorf("building transaction XDR for authRequiredOps: %w", txErr)
		}
		useCases = append(useCases, &UseCase{
			name:                 "authRequiredOps",
			category:             categoryStellarClassic,
			TxSigners:            txSigners,
			RequestedTransaction: types.Transaction{TransactionXdr: txXDR},
		})
	}

	// AccountMergeOp
	accountMergeOp, txSigners, err := f.prepareAccountMergeOp()
	if err != nil {
		return nil, fmt.Errorf("preparing account merge operation: %w", err)
	} else {
		txXDR, txErr := f.buildTransactionXDR([]string{accountMergeOp}, timeoutSeconds)
		if txErr != nil {
			return nil, fmt.Errorf("building transaction XDR for accountMergeOp: %w", txErr)
		}
		useCases = append(useCases, &UseCase{
			name:                 "accountMergeOp",
			category:             categoryStellarClassic,
			TxSigners:            txSigners,
			DelayTime:            6 * time.Second,
			RequestedTransaction: types.Transaction{TransactionXdr: txXDR},
		})
	}

	// InvokeContractOp w/ SorobanAuth
	invokeContractOp, txSigners, simulationResponse, err := f.prepareInvokeContractOp(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("preparing invoke contract operation: %w", err)
	} else {
		txXDR, txErr := f.buildTransactionXDR([]string{invokeContractOp}, timeoutSeconds)
		if txErr != nil {
			return nil, fmt.Errorf("building transaction XDR for invokeContractOp/SorobanAuth: %w", txErr)
		}
		useCases = append(useCases, &UseCase{
			name:                 "invokeContractOp/SorobanAuth",
			category:             categorySoroban,
			TxSigners:            txSigners,
			RequestedTransaction: types.Transaction{TransactionXdr: txXDR, SimulationResult: simulationResponse},
		})
	}

	// InvokeContractOp w/ SourceAccountAuth
	invokeContractOp, txSigners, simulationResponse, err = f.prepareInvokeContractOp(ctx, f.SecondaryAccountKP)
	if err != nil {
		return nil, fmt.Errorf("preparing invoke contract operation: %w", err)
	} else {
		txXDR, txErr := f.buildTransactionXDR([]string{invokeContractOp}, timeoutSeconds)
		if txErr != nil {
			return nil, fmt.Errorf("building transaction XDR for invokeContractOp/SourceAccountAuth: %w", txErr)
		}
		useCases = append(useCases, &UseCase{
			name:                 "invokeContractOp/SourceAccountAuth",
			category:             categorySoroban,
			TxSigners:            txSigners,
			RequestedTransaction: types.Transaction{TransactionXdr: txXDR, SimulationResult: simulationResponse},
		})
	}

	return useCases, nil
}

// buildTransactionXDR builds a complete transaction XDR from operation XDR strings
func (f *Fixtures) buildTransactionXDR(operationXDRs []string, timeoutSeconds int64) (string, error) {
	// Convert operation XDR strings to txnbuild operations
	operations := make([]txnbuild.Operation, len(operationXDRs))
	for i, opXDRStr := range operationXDRs {
		opXDR, err := utils.OperationXDRFromBase64(opXDRStr)
		if err != nil {
			return "", fmt.Errorf("converting operation XDR from base64: %w", err)
		}
		op, err := utils.OperationXDRToTxnBuildOp(opXDR)
		if err != nil {
			return "", fmt.Errorf("converting operation XDR to txnbuild operation: %w", err)
		}
		operations[i] = op
	}

	// Create a disposable source account for the transaction
	sourceAccKP := keypair.MustRandom()
	sourceAcc := txnbuild.SimpleAccount{AccountID: sourceAccKP.Address(), Sequence: 0}

	// Build the transaction
	tx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount: &sourceAcc,
		Operations:    operations,
		BaseFee:       txnbuild.MinBaseFee,
		Preconditions: txnbuild.Preconditions{
			TimeBounds: txnbuild.NewTimeout(timeoutSeconds),
		},
		IncrementSequenceNum: true,
	})
	if err != nil {
		return "", fmt.Errorf("building transaction: %w", err)
	}

	// Convert to XDR string
	txXDR, err := tx.Base64()
	if err != nil {
		return "", fmt.Errorf("encoding transaction to base64: %w", err)
	}

	return txXDR, nil
}
