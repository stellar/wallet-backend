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
// &txnbuild.CreateClaimableBalance{},
// &txnbuild.LiquidityPoolDeposit{},
// &txnbuild.LiquidityPoolWithdraw{},
// &txnbuild.RevokeSponsorship{},
// &txnbuild.ClaimClaimableBalance{},
// &txnbuild.ClawbackClaimableBalance{},
// --- Soroban:
// &txnbuild.InvokeHostFunction{},

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
		- 1 SIGNER/ADD change for the sponsored account with signer address = sponsored account, weight = 1
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

// prepareAuthRequiredIssuerSetupOps sets up the Primary account as a SEP-8 auth required issuer.
// This must be executed in a separate transaction before prepareAuthRequiredAssetOps to ensure
// the issuer's flags are persisted and queryable when the trustline is created.
func (f *Fixtures) prepareAuthRequiredIssuerSetupOps() ([]string, *Set[*keypair.Full], error) {
	/*
		Should generate 1 state change:
		- 1 FLAGS/SET change for setting auth flags (auth_required_flag, auth_revocable_flag, auth_clawback_enabled_flag)
	*/
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
	}

	b64OpsXDRs, err := ConvertOperationsToBase64XDR(operations)
	if err != nil {
		return nil, nil, fmt.Errorf("encoding operations to base64 XDR: %w", err)
	}

	return b64OpsXDRs, NewSet(f.PrimaryAccountKP), nil
}

// prepareAuthRequiredAssetOps creates a flow to mint and then clawback SEP-8 auth required customAsset funds.
// This should be executed after prepareAuthRequiredIssuerSetupOps to ensure the issuer's auth flags are
// already set and queryable.
func (f *Fixtures) prepareAuthRequiredAssetOps() ([]string, *Set[*keypair.Full], error) {
	/*
		Should generate 9 state changes:
		- 1 TRUSTLINE/ADD change for the trustline for TEST1 for the secondary account
		- 1 BALANCE_AUTHORIZATION/SET change with clawback_enabled flag (from trustline creation inheriting issuer's clawback flag)
		- 1 BALANCE_AUTHORIZATION/SET change with authorized flag (from SetTrustLineFlags authorizing the trustline)
		- 2 changes for TEST1 payment (1 BALANCE/MINT for Primary as issuer, 1 BALANCE/CREDIT for Secondary)
		- 1 BALANCE_AUTHORIZATION/CLEAR change for clearing AUTHORIZED flag on trustline
		- 2 changes for clawback (1 BALANCE/BURN for Primary as issuer, 1 BALANCE/DEBIT from Secondary)
		- 1 TRUSTLINE/REMOVE change for removing trustline
	*/
	customAsset := txnbuild.CreditAsset{
		Issuer: f.PrimaryAccountKP.Address(),
		Code:   "TEST1",
	}

	operations := []txnbuild.Operation{
		// The Secondary account creates a trustline for customAsset.
		&txnbuild.ChangeTrust{
			Line:          txnbuild.ChangeTrustAssetWrapper{Asset: customAsset},
			SourceAccount: f.SecondaryAccountKP.Address(),
		},

		// Sandwich authorization to mint customAsset funds from the Primary account to the Secondary account.
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
	}

	b64OpsXDRs, err := ConvertOperationsToBase64XDR(operations)
	if err != nil {
		return nil, nil, fmt.Errorf("encoding operations to base64 XDR: %w", err)
	}

	return b64OpsXDRs, NewSet(f.PrimaryAccountKP, f.SecondaryAccountKP), nil
}

// prepareClaimableBalanceOps creates claimable balance operations.
func (f *Fixtures) prepareCreateClaimableBalanceOps() ([]string, *Set[*keypair.Full], error) {
	/*
		Should generate 7 state changes:
		- 1 BALANCE_AUTHORIZATION/SET (clawback_enabled)                                                                                                     │ │
		- 1 TRUSTLINE/ADD                                                                                                                                    │ │
		- 1 BALANCE_AUTHORIZATION/SET (authorized)                                                                                                           │ │
		- 2 BALANCE/MINT (one per CB)                                                                                                                        │ │
		- 2 RESERVES/SPONSOR for primary account (one per CB, sponsor's view only)
	*/
	customAsset := txnbuild.CreditAsset{
		Issuer: f.PrimaryAccountKP.Address(),
		Code:   "TEST3",
	}

	operations := []txnbuild.Operation{
		// Secondary account creates trustline for TEST3
		&txnbuild.ChangeTrust{
			Line:          txnbuild.ChangeTrustAssetWrapper{Asset: customAsset},
			SourceAccount: f.SecondaryAccountKP.Address(),
		},

		// Primary authorizes the trustline
		&txnbuild.SetTrustLineFlags{
			Trustor: f.SecondaryAccountKP.Address(),
			Asset:   customAsset,
			SetFlags: []txnbuild.TrustLineFlag{
				txnbuild.TrustLineAuthorized,
			},
			SourceAccount: f.PrimaryAccountKP.Address(),
		},

		// Primary creates a claimable balance for Secondary with unconditional predicate
		&txnbuild.CreateClaimableBalance{
			Amount: "1",
			Asset:  customAsset,
			Destinations: []txnbuild.Claimant{
				txnbuild.NewClaimant(f.SecondaryAccountKP.Address(), nil),
			},
			SourceAccount: f.PrimaryAccountKP.Address(),
		},

		// Primary creates another claimable balance for Secondary with unconditional predicate
		&txnbuild.CreateClaimableBalance{
			Amount: "1",
			Asset:  customAsset,
			Destinations: []txnbuild.Claimant{
				txnbuild.NewClaimant(f.SecondaryAccountKP.Address(), nil),
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

// prepareClaimClaimableBalanceOp creates a claim claimable balance operation.
func (f *Fixtures) prepareClaimClaimableBalanceOp(balanceID string) (string, *Set[*keypair.Full], error) {
	/*
		Should generate state changes for claiming a claimable balance:
		- BALANCE/CREDIT change for the claiming account receiving the balance
		- Claimable balance entry removal
	*/
	op := &txnbuild.ClaimClaimableBalance{
		BalanceID:     balanceID,
		SourceAccount: f.SecondaryAccountKP.Address(),
	}

	opXDR, err := op.BuildXDR()
	if err != nil {
		return "", nil, fmt.Errorf("building claim claimable balance operation XDR: %w", err)
	}
	b64OpXDR, err := utils.OperationXDRToBase64(opXDR)
	if err != nil {
		return "", nil, fmt.Errorf("encoding claim claimable balance operation XDR to base64: %w", err)
	}

	return b64OpXDR, NewSet(f.SecondaryAccountKP), nil
}

// prepareClawbackClaimableBalanceOp creates a clawback claimable balance operation.
func (f *Fixtures) prepareClawbackClaimableBalanceOp(balanceID string) (string, *Set[*keypair.Full], error) {
	/*
		Should generate state changes for clawing back a claimable balance:
		- BALANCE/DEBIT or BURN for the claimable balance being clawed back
		- Claimable balance entry removal
	*/
	op := &txnbuild.ClawbackClaimableBalance{
		BalanceID:     balanceID,
		SourceAccount: f.PrimaryAccountKP.Address(),
	}

	opXDR, err := op.BuildXDR()
	if err != nil {
		return "", nil, fmt.Errorf("building clawback claimable balance operation XDR: %w", err)
	}
	b64OpXDR, err := utils.OperationXDRToBase64(opXDR)
	if err != nil {
		return "", nil, fmt.Errorf("encoding clawback claimable balance operation XDR to base64: %w", err)
	}

	return b64OpXDR, NewSet(f.PrimaryAccountKP), nil
}

// prepareClearAuthFlagsOps creates operations to clear all auth flags from the Primary account.
// This should be executed after all operations that require auth flags are completed.
func (f *Fixtures) prepareClearAuthFlagsOps() ([]string, *Set[*keypair.Full], error) {
	/*
	Should generate 1 state change:
		- 1 FLAGS/CLEAR change for clearing auth flags (auth_required, auth_revocable, auth_clawback_enabled)
	*/
	operations := []txnbuild.Operation{
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

	return b64OpsXDRs, NewSet(f.PrimaryAccountKP), nil
}

// PrepareClaimAndClawbackUseCases creates use cases for claiming and clawing back claimable balances
// using the actual balance IDs from the confirmed createClaimableBalance transaction.
func (f *Fixtures) PrepareClaimAndClawbackUseCases(balanceIDToBeClaimed, balanceIDToBeClawbacked string) ([]*UseCase, error) {
	useCases := []*UseCase{}
	timeoutSeconds := int64(timeout.Seconds())

	// ClaimClaimableBalanceOp
	claimOpXDR, claimSigners, err := f.prepareClaimClaimableBalanceOp(balanceIDToBeClaimed)
	if err != nil {
		return nil, fmt.Errorf("preparing claim claimable balance operation: %w", err)
	}
	txXDR, err := f.buildTransactionXDR([]string{claimOpXDR}, timeoutSeconds)
	if err != nil {
		return nil, fmt.Errorf("building transaction XDR for claimClaimableBalanceOp: %w", err)
	}
	useCases = append(useCases, &UseCase{
		name:                 "claimClaimableBalanceOp",
		category:             categoryStellarClassic,
		TxSigners:            claimSigners,
		DelayTime:            2 * time.Second,
		RequestedTransaction: types.Transaction{TransactionXdr: txXDR},
	})

	// ClawbackClaimableBalanceOp
	clawbackOpXDR, clawbackSigners, err := f.prepareClawbackClaimableBalanceOp(balanceIDToBeClawbacked)
	if err != nil {
		return nil, fmt.Errorf("preparing clawback claimable balance operation: %w", err)
	}
	txXDR, err = f.buildTransactionXDR([]string{clawbackOpXDR}, timeoutSeconds)
	if err != nil {
		return nil, fmt.Errorf("building transaction XDR for clawbackClaimableBalanceOp: %w", err)
	}
	useCases = append(useCases, &UseCase{
		name:                 "clawbackClaimableBalanceOp",
		category:             categoryStellarClassic,
		TxSigners:            clawbackSigners,
		DelayTime:            2 * time.Second,
		RequestedTransaction: types.Transaction{TransactionXdr: txXDR},
	})

	// ClearAuthFlagsOps
	clearAuthFlagsOps, clearAuthFlagsSigners, err := f.prepareClearAuthFlagsOps()
	if err != nil {
		return nil, fmt.Errorf("preparing clear auth flags operations: %w", err)
	}
	txXDR, err = f.buildTransactionXDR(clearAuthFlagsOps, timeoutSeconds)
	if err != nil {
		return nil, fmt.Errorf("building transaction XDR for clearAuthFlagsOps: %w", err)
	}
	useCases = append(useCases, &UseCase{
		name:                 "clearAuthFlagsOps",
		category:             categoryStellarClassic,
		TxSigners:            clearAuthFlagsSigners,
		DelayTime:            2 * time.Second,
		RequestedTransaction: types.Transaction{TransactionXdr: txXDR},
	})

	return useCases, nil
}

// prepareLiquidityPoolOps creates liquidity pool operations.
// Currently commented out pending further testing.
func (f *Fixtures) prepareLiquidityPoolOps() ([]string, *Set[*keypair.Full], error) {
	/*
	Should generate 7 state changes:
		1. BALANCE_AUTHORIZATION/SET - LP trustline authorization (empty flags, no tokenId, has liquidity_pool_id in keyValue)
		Note: LPs don't support authorization semantics, so flags array is empty
		2. TRUSTLINE/ADD - Create LP shares trustline (no tokenId, has liquidity_pool_id in keyValue, has limit)
		3. BALANCE/DEBIT - XLM deposited into pool (has tokenId = XLM contract address, amount = 1000000000)
		4. BALANCE/MINT - TEST2 minted to LP (Primary is issuer, has tokenId = TEST2 contract address, amount = 1000000000)
		5. BALANCE/BURN - TEST2 burned from LP back to issuer (has tokenId = TEST2 contract address, amount = 1000000000)
		6. BALANCE/CREDIT - XLM withdrawn from pool (has tokenId = XLM contract address, amount = 1000000000)
		7. TRUSTLINE/REMOVE - Remove LP shares trustline (no tokenId, has liquidity_pool_id in keyValue, no limit)
	*/
	xlmAsset := txnbuild.NativeAsset{}
	customAsset := txnbuild.CreditAsset{
		Issuer: f.PrimaryAccountKP.Address(),
		Code:   "TEST2",
	}

	// Create liquidity pool ID for XLM:TEST2 pair
	poolID, err := txnbuild.NewLiquidityPoolId(xlmAsset, customAsset)
	if err != nil {
		return nil, nil, fmt.Errorf("creating liquidity pool ID: %w", err)
	}

	// Create ChangeTrustAsset for the liquidity pool
	poolAsset := txnbuild.LiquidityPoolShareChangeTrustAsset{
		LiquidityPoolParameters: txnbuild.LiquidityPoolParameters{
			AssetA: xlmAsset,
			AssetB: customAsset,
			Fee:    txnbuild.LiquidityPoolFeeV18,
		},
	}

	operations := []txnbuild.Operation{
		// Primary account establishes trustline to the liquidity pool
		&txnbuild.ChangeTrust{
			Line:          poolAsset,
			SourceAccount: f.PrimaryAccountKP.Address(),
		},

		// Primary deposits into the liquidity pool
		&txnbuild.LiquidityPoolDeposit{
			LiquidityPoolID: poolID,
			MaxAmountA:      "100", // 100 XLM
			MaxAmountB:      "100", // 100 TEST2
			MinPrice: xdr.Price{
				N: xdr.Int32(1),
				D: xdr.Int32(1),
			},
			MaxPrice: xdr.Price{
				N: xdr.Int32(1),
				D: xdr.Int32(1),
			},
			SourceAccount: f.PrimaryAccountKP.Address(),
		},

		// Primary withdraws from the liquidity pool
		&txnbuild.LiquidityPoolWithdraw{
			LiquidityPoolID: poolID,
			Amount:          "100", // Withdraw all pool shares
			MinAmountA:      "1",
			MinAmountB:      "1",
			SourceAccount:   f.PrimaryAccountKP.Address(),
		},

		// Remove trustline to the liquidity pool
		&txnbuild.ChangeTrust{
			Line:          poolAsset,
			Limit:         "0",
			SourceAccount: f.PrimaryAccountKP.Address(),
		},
	}

	b64OpsXDRs, err := ConvertOperationsToBase64XDR(operations)
	if err != nil {
		return nil, nil, fmt.Errorf("encoding operations to base64 XDR: %w", err)
	}

	return b64OpsXDRs, NewSet(f.PrimaryAccountKP), nil
}

// prepareRevokeSponsorshipOps creates revoke sponsorship operations.
// Currently commented out pending further testing.
func (f *Fixtures) prepareRevokeSponsorshipOps() ([]string, *Set[*keypair.Full], error) {
	/*
	Should generate 4 state changes:
		- 1 METADATA/DATA_ENTRY change for creating sponsored data entry on Secondary account
		- 1 RESERVES/SPONSOR change for establishing sponsorship for data entry (for sponsored account)
		- 1 RESERVES/UNSPONSOR change for revoking sponsorship for data entry (for sponsored account)
		- 1 METADATA/DATA_ENTRY change for removing the data entry for secondary account
	*/
	operations := []txnbuild.Operation{
		// Primary begins sponsoring future reserves for Secondary
		&txnbuild.BeginSponsoringFutureReserves{
			SponsoredID:   f.SecondaryAccountKP.Address(),
			SourceAccount: f.PrimaryAccountKP.Address(),
		},

		// Secondary creates a data entry (which will be sponsored)
		&txnbuild.ManageData{
			Name:          "sponsored_data",
			Value:         []byte("test_value"),
			SourceAccount: f.SecondaryAccountKP.Address(),
		},

		// Secondary ends the sponsorship
		&txnbuild.EndSponsoringFutureReserves{
			SourceAccount: f.SecondaryAccountKP.Address(),
		},

		// Primary revokes sponsorship of the data entry
		&txnbuild.RevokeSponsorship{
			SponsorshipType: txnbuild.RevokeSponsorshipTypeData,
			Data: &txnbuild.DataID{
				Account:  f.SecondaryAccountKP.Address(),
				DataName: "sponsored_data",
			},
			SourceAccount: f.PrimaryAccountKP.Address(),
		},

		// Clean up: Remove the data entry
		&txnbuild.ManageData{
			Name:          "sponsored_data",
			Value:         nil, // nil value removes the entry
			SourceAccount: f.SecondaryAccountKP.Address(),
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
		Should generate 5 state changes:
		- 1 ACCOUNT/MERGE change for the destination account (PrimaryAccountKP) receiving the merge
		  Note: The source account (SponsoredNewAccountKP) is deleted by Stellar, so we track the destination's state change
		- 1 BALANCE/CREDIT change for the destination account (PrimaryAccountKP) receiving the merged balance
		- 1 BALANCE/DEBIT change for the source account (SponsoredNewAccountKP) transferring its balance before deletion
		- 2 RESERVES/UNSPONSOR changes for unwinding the sponsorship relationship:
		  - 1 for the sponsored account (SponsoredNewAccountKP) losing its sponsor
		  - 1 for the sponsor account (PrimaryAccountKP) no longer sponsoring
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
	timeout                time.Duration = 120 * time.Second
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
			DelayTime:            2 * time.Second,
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
			DelayTime:            2 * time.Second,
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
			DelayTime:            2 * time.Second,
			RequestedTransaction: types.Transaction{TransactionXdr: txXDR},
		})
	}

	// AuthRequiredIssuerSetupOps - Transaction 1: Set up issuer with auth flags
	authRequiredIssuerSetupOps, txSigners, err := f.prepareAuthRequiredIssuerSetupOps()
	if err != nil {
		return nil, fmt.Errorf("preparing auth required issuer setup operations: %w", err)
	}
	txXDR, txErr := f.buildTransactionXDR(authRequiredIssuerSetupOps, timeoutSeconds)
	if txErr != nil {
		return nil, fmt.Errorf("building transaction XDR for authRequiredIssuerSetupOps: %w", txErr)
	}
	useCases = append(useCases, &UseCase{
		name:                 "authRequiredIssuerSetupOps",
		category:             categoryStellarClassic,
		TxSigners:            txSigners,
		DelayTime:            2 * time.Second,
		RequestedTransaction: types.Transaction{TransactionXdr: txXDR},
	})

	// AuthRequiredAssetOps - Transaction 2: Perform trustline and payment operations
	// This must execute after the issuer setup to ensure flags are persisted
	authRequiredAssetOps, txSigners, err := f.prepareAuthRequiredAssetOps()
	if err != nil {
		return nil, fmt.Errorf("preparing auth required asset operations: %w", err)
	}
	txXDR, txErr = f.buildTransactionXDR(authRequiredAssetOps, timeoutSeconds)
	if txErr != nil {
		return nil, fmt.Errorf("building transaction XDR for authRequiredAssetOps: %w", txErr)
	}
	useCases = append(useCases, &UseCase{
		name:                 "authRequiredAssetOps",
		category:             categoryStellarClassic,
		TxSigners:            txSigners,
		DelayTime:            2 * time.Second,
		RequestedTransaction: types.Transaction{TransactionXdr: txXDR},
	})

	// ClaimableBalanceOps
	createClaimableBalanceOps, txSigners, err := f.prepareCreateClaimableBalanceOps()
	if err != nil {
		return nil, fmt.Errorf("preparing create claimable balance operations: %w", err)
	}
	if txXDR, txErr := f.buildTransactionXDR(createClaimableBalanceOps, timeoutSeconds); txErr != nil {
		return nil, fmt.Errorf("building transaction XDR for createClaimableBalanceOps: %w", txErr)
	} else {
		useCases = append(useCases, &UseCase{
			name:                 "createClaimableBalanceOps",
			category:             categoryStellarClassic,
			TxSigners:            txSigners,
			DelayTime:            2 * time.Second,
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
			DelayTime:            2 * time.Second,
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
			DelayTime:            2 * time.Second,
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
			DelayTime:            2 * time.Second,
			RequestedTransaction: types.Transaction{TransactionXdr: txXDR, SimulationResult: simulationResponse},
		})
	}

	// LiquidityPoolOps
	liquidityPoolOps, txSigners, err := f.prepareLiquidityPoolOps()
	if err != nil {
		return nil, fmt.Errorf("preparing liquidity pool operations: %w", err)
	} else {
		txXDR, txErr := f.buildTransactionXDR(liquidityPoolOps, timeoutSeconds)
		if txErr != nil {
			return nil, fmt.Errorf("building transaction XDR for liquidityPoolOps: %w", txErr)
		}
		useCases = append(useCases, &UseCase{
			name:                 "liquidityPoolOps",
			category:             categoryStellarClassic,
			TxSigners:            txSigners,
			DelayTime:            2 * time.Second,
			RequestedTransaction: types.Transaction{TransactionXdr: txXDR},
		})
	}

	// RevokeSponsorshipOps
	revokeSponsorshipOps, txSigners, err := f.prepareRevokeSponsorshipOps()
	if err != nil {
		return nil, fmt.Errorf("preparing revoke sponsorship operations: %w", err)
	} else {
		txXDR, txErr := f.buildTransactionXDR(revokeSponsorshipOps, timeoutSeconds)
		if txErr != nil {
			return nil, fmt.Errorf("building transaction XDR for revokeSponsorshipOps: %w", txErr)
		}
		useCases = append(useCases, &UseCase{
			name:                 "revokeSponsorshipOps",
			category:             categoryStellarClassic,
			TxSigners:            txSigners,
			RequestedTransaction: types.Transaction{TransactionXdr: txXDR},
		})
	}

	return useCases, nil
}

// buildTransactionXDR builds a complete transaction XDR from operation XDR strings
func (f *Fixtures) buildTransactionXDR(operationXDRs []string, timeoutSeconds int64) (string, error) {
	_ = timeoutSeconds // Reserved for future use; currently using NewInfiniteTimeout()

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
			TimeBounds: txnbuild.NewInfiniteTimeout(),
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
