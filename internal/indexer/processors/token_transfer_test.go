// Token transfer processor tests
// Tests the TokenTransferProcessor for extracting state changes from various Stellar operations

package processors

import (
	"math/big"
	"testing"

	"github.com/stellar/go/keypair"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/support/contractevents"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/indexer/types"
)

func TestTokenTransferProcessor_Process(t *testing.T) {
	t.Run("Fee - extracts only fee event for failed txn", func(t *testing.T) {
		createAccountOp := xdr.Operation{
			SourceAccount: &accountA,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypeCreateAccount,
				CreateAccountOp: &xdr.CreateAccountOp{
					Destination:     accountB.ToAccountId(),
					StartingBalance: 100 * oneUnit,
				},
			},
		}
		createAccountResult := &xdr.OperationResult{}
		tx := createTx(createAccountOp, nil, createAccountResult, true)

		processor := NewTokenTransferProcessor(networkPassphrase)
		changes := processTransaction(t, processor, tx)
		requireEventCount(t, changes, 1)
		assertFeeEvent(t, changes[0], "100")
	})

	t.Run("Fee - extracts credit state change for fee refund", func(t *testing.T) {
		tx := createSorobanTx(
			xdr.LedgerEntryChanges{
				generateAccountEntryChangState(accountEntry(someTxAccount, 1000*oneUnit)),
				generateAccountEntryUpdatedChange(accountEntry(someTxAccount, 1000*oneUnit), 700*oneUnit),
			},
			xdr.LedgerEntryChanges{
				generateAccountEntryChangState(accountEntry(someTxAccount, 700*oneUnit)),
				generateAccountEntryUpdatedChange(accountEntry(someTxAccount, 700*oneUnit), 730*oneUnit),
			}, true)

		processor := NewTokenTransferProcessor(networkPassphrase)
		changes := processTransaction(t, processor, tx)
		requireEventCount(t, changes, 2)

		assertDebitEvent(t, changes[0], someTxAccount.ToAccountId().Address(), "3000000000", "")
		assertCreditEvent(t, changes[1], someTxAccount.ToAccountId().Address(), "300000000", "")
	})

	t.Run("CreateAccount - extracts state changes for successful account creation", func(t *testing.T) {
		createAccountOp := xdr.Operation{
			SourceAccount: &accountA,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypeCreateAccount,
				CreateAccountOp: &xdr.CreateAccountOp{
					Destination:     accountB.ToAccountId(),
					StartingBalance: 100 * oneUnit,
				},
			},
		}
		createAccountResult := &xdr.OperationResult{}
		tx := createTx(createAccountOp, nil, createAccountResult, false)

		processor := NewTokenTransferProcessor(networkPassphrase)
		changes := processTransaction(t, processor, tx)
		requireEventCount(t, changes, 3)

		assertFeeEvent(t, changes[0], "100")
		assertDebitEvent(t, changes[1], accountA.ToAccountId().Address(), "1000000000", "")
		assertCreditEvent(t, changes[2], accountB.ToAccountId().Address(), "1000000000", "")
	})

	t.Run("AccountMerge - extracts state changes for successful account merge with balance", func(t *testing.T) {
		accountMergeOp := xdr.Operation{
			SourceAccount: &accountA,
			Body: xdr.OperationBody{
				Type:        xdr.OperationTypeAccountMerge,
				Destination: &accountB,
			},
		}
		hundredUnits := 100 * oneUnit
		accountMergeResult := &xdr.OperationResult{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypeAccountMerge,
				AccountMergeResult: &xdr.AccountMergeResult{
					Code:                 xdr.AccountMergeResultCodeAccountMergeSuccess,
					SourceAccountBalance: &hundredUnits,
				},
			},
		}
		tx := createTx(accountMergeOp, nil, accountMergeResult, false)

		processor := NewTokenTransferProcessor(networkPassphrase)
		changes := processTransaction(t, processor, tx)
		requireEventCount(t, changes, 3)

		assertFeeEvent(t, changes[0], "100")
		assertDebitEvent(t, changes[1], accountA.ToAccountId().Address(), "1000000000", nativeContractAddress)
		assertCreditEvent(t, changes[2], accountB.ToAccountId().Address(), "1000000000", nativeContractAddress)
	})

	t.Run("AccountMerge - no events for empty account merge", func(t *testing.T) {
		accountMergeOp := xdr.Operation{
			SourceAccount: &accountA,
			Body: xdr.OperationBody{
				Type:        xdr.OperationTypeAccountMerge,
				Destination: &accountB,
			},
		}
		accountMergeResult := &xdr.OperationResult{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypeAccountMerge,
				AccountMergeResult: &xdr.AccountMergeResult{
					Code:                 xdr.AccountMergeResultCodeAccountMergeSuccess,
					SourceAccountBalance: nil,
				},
			},
		}
		tx := createTx(accountMergeOp, nil, accountMergeResult, false)

		processor := NewTokenTransferProcessor(networkPassphrase)
		changes := processTransaction(t, processor, tx)
		requireEventCount(t, changes, 1)
		assertFeeEvent(t, changes[0], "100")
	})

	t.Run("Payment - G to G XLM transfer", func(t *testing.T) {
		paymentOp := xdr.Operation{
			SourceAccount: &accountA,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypePayment,
				PaymentOp: &xdr.PaymentOp{
					Destination: accountB,
					Asset:       xdr.Asset{Type: xdr.AssetTypeAssetTypeNative},
					Amount:      50 * oneUnit,
				},
			},
		}
		paymentResult := &xdr.OperationResult{}
		tx := createTx(paymentOp, nil, paymentResult, false)

		processor := NewTokenTransferProcessor(networkPassphrase)
		changes := processTransaction(t, processor, tx)
		requireEventCount(t, changes, 3)

		assertFeeEvent(t, changes[0], "100")
		assertDebitEvent(t, changes[1], accountA.ToAccountId().Address(), "500000000", "")
		assertCreditEvent(t, changes[2], accountB.ToAccountId().Address(), "500000000", "")
	})

	t.Run("Payment - G to G USDC transfer", func(t *testing.T) {
		paymentOp := xdr.Operation{
			SourceAccount: &accountA,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypePayment,
				PaymentOp: &xdr.PaymentOp{
					Destination: accountB,
					Asset:       usdcAsset,
					Amount:      100 * oneUnit,
				},
			},
		}
		paymentResult := &xdr.OperationResult{}
		tx := createTx(paymentOp, nil, paymentResult, false)

		processor := NewTokenTransferProcessor(networkPassphrase)
		changes := processTransaction(t, processor, tx)
		requireEventCount(t, changes, 3)

		assertFeeEvent(t, changes[0], "100")
		assertDebitEvent(t, changes[1], accountA.ToAccountId().Address(), "1000000000", usdcContractAddress)
		assertCreditEvent(t, changes[2], accountB.ToAccountId().Address(), "1000000000", usdcContractAddress)
	})

	t.Run("Payment - G (issuer) to G mints USDC", func(t *testing.T) {
		mintPaymentOp := xdr.Operation{
			SourceAccount: &usdcAccount,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypePayment,
				PaymentOp: &xdr.PaymentOp{
					Destination: accountB,
					Asset:       usdcAsset,
					Amount:      100 * oneUnit,
				},
			},
		}
		mintPaymentResult := &xdr.OperationResult{}
		tx := createTx(mintPaymentOp, nil, mintPaymentResult, false)

		processor := NewTokenTransferProcessor(networkPassphrase)
		changes := processTransaction(t, processor, tx)
		requireEventCount(t, changes, 3)

		assertFeeEvent(t, changes[0], "100")
		assertMintEvent(t, changes[1], usdcIssuer, "1000000000", usdcContractAddress)
		assertCreditEvent(t, changes[2], accountB.ToAccountId().Address(), "1000000000", usdcContractAddress)
	})

	t.Run("Payment - G to G (issuer) burns USDC for the issuer and creates a debit for the sender", func(t *testing.T) {
		burnPaymentOp := xdr.Operation{
			SourceAccount: &accountA,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypePayment,
				PaymentOp: &xdr.PaymentOp{
					Destination: usdcAccount,
					Asset:       usdcAsset,
					Amount:      75 * oneUnit,
				},
			},
		}
		burnPaymentResult := &xdr.OperationResult{}
		tx := createTx(burnPaymentOp, nil, burnPaymentResult, false)

		processor := NewTokenTransferProcessor(networkPassphrase)
		changes := processTransaction(t, processor, tx)
		requireEventCount(t, changes, 3)

		assertFeeEvent(t, changes[0], "100")
		assertBurnEvent(t, changes[1], usdcAccount.ToAccountId().Address(), "750000000", usdcContractAddress)
		assertDebitEvent(t, changes[2], accountA.ToAccountId().Address(), "750000000", usdcContractAddress)
	})

	t.Run("ClaimClaimableBalance - extracts state changes for claiming USDC balance", func(t *testing.T) {
		claimOp := claimCBOp(anotherBalanceID, &accountB)
		claimResult := &xdr.OperationResult{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypeClaimClaimableBalance,
				ClaimClaimableBalanceResult: &xdr.ClaimClaimableBalanceResult{
					Code: xdr.ClaimClaimableBalanceResultCodeClaimClaimableBalanceSuccess,
				},
			},
		}

		cbEntry := cbLedgerEntry(anotherBalanceID, usdcAsset, 50*oneUnit)
		changes := xdr.LedgerEntryChanges{
			generateCBEntryChangeState(cbEntry),
			generateCBEntryRemovedChange(cbEntry),
		}

		tx := createTx(claimOp, changes, claimResult, false)

		processor := NewTokenTransferProcessor(networkPassphrase)
		stateChanges := processTransaction(t, processor, tx)
		requireEventCount(t, stateChanges, 2)

		assertFeeEvent(t, stateChanges[0], "100")
		assertClaimableBalanceEvent(t, stateChanges[1], types.StateChangeCategoryCredit,
			accountB.ToAccountId().Address(), "500000000", usdcContractAddress,
			anotherBalanceID.MustEncodeToStrkey())
	})

	t.Run("ClaimClaimableBalance - extracts state changes for claiming USDC balance by the issuer", func(t *testing.T) {
		claimOp := claimCBOp(anotherBalanceID, &usdcAccount)
		claimResult := &xdr.OperationResult{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypeClaimClaimableBalance,
				ClaimClaimableBalanceResult: &xdr.ClaimClaimableBalanceResult{
					Code: xdr.ClaimClaimableBalanceResultCodeClaimClaimableBalanceSuccess,
				},
			},
		}

		cbEntry := cbLedgerEntry(anotherBalanceID, usdcAsset, 50*oneUnit)
		changes := xdr.LedgerEntryChanges{
			generateCBEntryChangeState(cbEntry),
			generateCBEntryRemovedChange(cbEntry),
		}

		tx := createTx(claimOp, changes, claimResult, false)

		processor := NewTokenTransferProcessor(networkPassphrase)
		stateChanges := processTransaction(t, processor, tx)
		requireEventCount(t, stateChanges, 2)

		assertFeeEvent(t, stateChanges[0], "100")
		assertClaimableBalanceEvent(t, stateChanges[1], types.StateChangeCategoryBurn,
			usdcAccount.ToAccountId().Address(), "500000000", usdcContractAddress,
			anotherBalanceID.MustEncodeToStrkey())
	})

	t.Run("ClaimClaimableBalance - multiple claims in single transaction", func(t *testing.T) {
		claimOp1 := claimCBOp(someBalanceID, &accountA)
		claimOp2 := claimCBOp(anotherBalanceID, &accountC)

		tx := someTx
		tx.Envelope.V1.Tx.Operations = []xdr.Operation{claimOp1, claimOp2}

		cbEntry1 := cbLedgerEntry(someBalanceID, xdr.Asset{Type: xdr.AssetTypeAssetTypeNative}, 100*oneUnit)
		cbEntry2 := cbLedgerEntry(anotherBalanceID, ethAsset, 25*oneUnit)

		tx.UnsafeMeta.V3.Operations = []xdr.OperationMeta{
			{
				Changes: xdr.LedgerEntryChanges{
					generateCBEntryChangeState(cbEntry1),
					generateCBEntryRemovedChange(cbEntry1),
				},
			},
			{
				Changes: xdr.LedgerEntryChanges{
					generateCBEntryChangeState(cbEntry2),
					generateCBEntryRemovedChange(cbEntry2),
				},
			},
		}

		tx.Result.Result.Result.Results = &[]xdr.OperationResult{
			{
				Code: xdr.OperationResultCodeOpInner,
				Tr: &xdr.OperationResultTr{
					Type: xdr.OperationTypeClaimClaimableBalance,
					ClaimClaimableBalanceResult: &xdr.ClaimClaimableBalanceResult{
						Code: xdr.ClaimClaimableBalanceResultCodeClaimClaimableBalanceSuccess,
					},
				},
			},
			{
				Code: xdr.OperationResultCodeOpInner,
				Tr: &xdr.OperationResultTr{
					Type: xdr.OperationTypeClaimClaimableBalance,
					ClaimClaimableBalanceResult: &xdr.ClaimClaimableBalanceResult{
						Code: xdr.ClaimClaimableBalanceResultCodeClaimClaimableBalanceSuccess,
					},
				},
			},
		}

		processor := NewTokenTransferProcessor(networkPassphrase)
		stateChanges := processTransaction(t, processor, tx)
		requireEventCount(t, stateChanges, 3)

		assertFeeEvent(t, stateChanges[0], "100")
		assertClaimableBalanceEvent(t, stateChanges[1], types.StateChangeCategoryCredit,
			accountA.ToAccountId().Address(), "1000000000", nativeContractAddress,
			someBalanceID.MustEncodeToStrkey())
		assertClaimableBalanceEvent(t, stateChanges[2], types.StateChangeCategoryCredit,
			accountC.ToAccountId().Address(), "250000000", ethContractAddress,
			anotherBalanceID.MustEncodeToStrkey())
	})

	t.Run("ClaimClaimableBalance - no source account uses transaction source", func(t *testing.T) {
		claimOp := claimCBOp(someBalanceID, nil)
		claimResult := &xdr.OperationResult{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypeClaimClaimableBalance,
				ClaimClaimableBalanceResult: &xdr.ClaimClaimableBalanceResult{
					Code: xdr.ClaimClaimableBalanceResultCodeClaimClaimableBalanceSuccess,
				},
			},
		}

		cbEntry := cbLedgerEntry(someBalanceID, xdr.Asset{Type: xdr.AssetTypeAssetTypeNative}, 200*oneUnit)
		changes := xdr.LedgerEntryChanges{
			generateCBEntryChangeState(cbEntry),
			generateCBEntryRemovedChange(cbEntry),
		}

		tx := createTx(claimOp, changes, claimResult, false)

		processor := NewTokenTransferProcessor(networkPassphrase)
		stateChanges := processTransaction(t, processor, tx)
		requireEventCount(t, stateChanges, 2)

		assertFeeEvent(t, stateChanges[0], "100")
		assertClaimableBalanceEvent(t, stateChanges[1], types.StateChangeCategoryCredit,
			someTxAccount.ToAccountId().Address(), "2000000000", nativeContractAddress,
			someBalanceID.MustEncodeToStrkey())
	})

	t.Run("CreateClaimableBalance - extracts state changes for creating USDC balance", func(t *testing.T) {
		claimants := []xdr.Claimant{
			{
				Type: xdr.ClaimantTypeClaimantTypeV0,
				V0: &xdr.ClaimantV0{
					Destination: accountB.ToAccountId(),
					Predicate: xdr.ClaimPredicate{
						Type: xdr.ClaimPredicateTypeClaimPredicateUnconditional,
					},
				},
			},
		}

		createOp := createCBOp(usdcAsset, 75*oneUnit, claimants, &accountA)

		createResult := &xdr.OperationResult{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypeCreateClaimableBalance,
				CreateClaimableBalanceResult: &xdr.CreateClaimableBalanceResult{
					Code:      xdr.CreateClaimableBalanceResultCodeCreateClaimableBalanceSuccess,
					BalanceId: &anotherBalanceID,
				},
			},
		}

		newCBEntry := cbLedgerEntry(anotherBalanceID, usdcAsset, 75*oneUnit)
		changes := xdr.LedgerEntryChanges{
			xdr.LedgerEntryChange{
				Type:    xdr.LedgerEntryChangeTypeLedgerEntryCreated,
				Created: &newCBEntry,
			},
		}

		tx := createTx(createOp, changes, createResult, false)

		processor := NewTokenTransferProcessor(networkPassphrase)
		stateChanges := processTransaction(t, processor, tx)
		requireEventCount(t, stateChanges, 2)

		assertFeeEvent(t, stateChanges[0], "100")
		assertClaimableBalanceEvent(t, stateChanges[1], types.StateChangeCategoryDebit,
			accountA.ToAccountId().Address(), "750000000", usdcContractAddress,
			anotherBalanceID.MustEncodeToStrkey())
	})

	t.Run("Clawback - USDC clawback generates debit and burn events", func(t *testing.T) {
		clawbackOperation := clawbackOp(usdcAsset, 50*oneUnit, accountA, &usdcAccount)
		clawbackResult := &xdr.OperationResult{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypeClawback,
				ClawbackResult: &xdr.ClawbackResult{
					Code: xdr.ClawbackResultCodeClawbackSuccess,
				},
			},
		}
		tx := createTx(clawbackOperation, nil, clawbackResult, false)

		processor := NewTokenTransferProcessor(networkPassphrase)
		stateChanges := processTransaction(t, processor, tx)
		requireEventCount(t, stateChanges, 3)

		assertFeeEvent(t, stateChanges[0], "100")
		assertBurnEvent(t, stateChanges[1], usdcAccount.ToAccountId().Address(), "500000000", usdcContractAddress)
		assertDebitEvent(t, stateChanges[2], accountA.ToAccountId().Address(), "500000000", usdcContractAddress)
	})

	t.Run("ClawbackClaimableBalance - extracts state changes for USDC claimable balance clawback", func(t *testing.T) {
		clawbackCBOperation := clawbackClaimableBalanceOp(someBalanceID, &usdcAccount)
		clawbackCBResult := &xdr.OperationResult{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypeClawbackClaimableBalance,
				ClawbackClaimableBalanceResult: &xdr.ClawbackClaimableBalanceResult{
					Code: xdr.ClawbackClaimableBalanceResultCodeClawbackClaimableBalanceSuccess,
				},
			},
		}

		cbEntry := cbLedgerEntry(someBalanceID, usdcAsset, 25*oneUnit)
		changes := xdr.LedgerEntryChanges{
			generateCBEntryChangeState(cbEntry),
			generateCBEntryRemovedChange(cbEntry),
		}

		tx := createTx(clawbackCBOperation, changes, clawbackCBResult, false)

		processor := NewTokenTransferProcessor(networkPassphrase)
		stateChanges := processTransaction(t, processor, tx)
		requireEventCount(t, stateChanges, 2)

		assertFeeEvent(t, stateChanges[0], "100")
		assertClaimableBalanceEvent(t, stateChanges[1], types.StateChangeCategoryBurn,
			usdcAccount.ToAccountId().Address(), "250000000", usdcContractAddress,
			someBalanceID.MustEncodeToStrkey())
	})

	t.Run("LiquidityPoolDeposit - extracts state changes for new LP creation with transfer events", func(t *testing.T) {
		lpDepositOperation := lpDepositOp(lpBtcEthID, 10*oneUnit, 20*oneUnit, 1, 10, &accountA)
		lpDepositResult := &xdr.OperationResult{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypeLiquidityPoolDeposit,
				LiquidityPoolDepositResult: &xdr.LiquidityPoolDepositResult{
					Code: xdr.LiquidityPoolDepositResultCodeLiquidityPoolDepositSuccess,
				},
			},
		}

		lpEntry := lpLedgerEntry(lpBtcEthID, btcAsset, ethAsset, 5*oneUnit, 15*oneUnit)
		changes := xdr.LedgerEntryChanges{
			generateLpEntryCreatedChange(lpEntry),
		}

		tx := createTx(lpDepositOperation, changes, lpDepositResult, false)

		processor := NewTokenTransferProcessor(networkPassphrase)
		stateChanges := processTransaction(t, processor, tx)
		requireEventCount(t, stateChanges, 3)

		assertFeeEvent(t, stateChanges[0], "100")
		assertLiquidityPoolEvent(t, stateChanges[1], types.StateChangeCategoryDebit,
			accountA.ToAccountId().Address(), "50000000", btcContractAddress,
			lpIDToStrkey(lpBtcEthID))
		assertLiquidityPoolEvent(t, stateChanges[2], types.StateChangeCategoryDebit,
			accountA.ToAccountId().Address(), "150000000", ethContractAddress,
			lpIDToStrkey(lpBtcEthID))
	})

	t.Run("LiquidityPoolWithdraw - LP removal generates transfer events", func(t *testing.T) {
		lpWithdrawOperation := lpWithdrawOp(lpBtcEthID, 100*oneUnit, 2*oneUnit, 5*oneUnit, &accountA)
		lpWithdrawResult := &xdr.OperationResult{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypeLiquidityPoolWithdraw,
				LiquidityPoolWithdrawResult: &xdr.LiquidityPoolWithdrawResult{
					Code: xdr.LiquidityPoolWithdrawResultCodeLiquidityPoolWithdrawSuccess,
				},
			},
		}

		lpEntry := lpLedgerEntry(lpBtcEthID, btcAsset, ethAsset, 5*oneUnit, 12*oneUnit)
		changes := xdr.LedgerEntryChanges{
			generateLpEntryChangeState(lpEntry),
			generateLpEntryRemovedChange(lpBtcEthID),
		}

		tx := createTx(lpWithdrawOperation, changes, lpWithdrawResult, false)

		processor := NewTokenTransferProcessor(networkPassphrase)
		stateChanges := processTransaction(t, processor, tx)
		requireEventCount(t, stateChanges, 3)

		assertFeeEvent(t, stateChanges[0], "100")
		assertLiquidityPoolEvent(t, stateChanges[1], types.StateChangeCategoryCredit,
			accountA.ToAccountId().Address(), "50000000", btcContractAddress,
			lpIDToStrkey(lpBtcEthID))
		assertLiquidityPoolEvent(t, stateChanges[2], types.StateChangeCategoryCredit,
			accountA.ToAccountId().Address(), "120000000", ethContractAddress,
			lpIDToStrkey(lpBtcEthID))
	})

	t.Run("LiquidityPoolWithdraw - LP removal by asset issuer generates burn event", func(t *testing.T) {
		lpWithdrawOperation := lpWithdrawOp(lpBtcEthID, 100*oneUnit, 2*oneUnit, 5*oneUnit, &btcAccount)
		lpWithdrawResult := &xdr.OperationResult{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypeLiquidityPoolWithdraw,
				LiquidityPoolWithdrawResult: &xdr.LiquidityPoolWithdrawResult{
					Code: xdr.LiquidityPoolWithdrawResultCodeLiquidityPoolWithdrawSuccess,
				},
			},
		}

		lpEntry := lpLedgerEntry(lpBtcEthID, btcAsset, ethAsset, 5*oneUnit, 12*oneUnit)
		changes := xdr.LedgerEntryChanges{
			generateLpEntryChangeState(lpEntry),
			generateLpEntryRemovedChange(lpBtcEthID),
		}

		tx := createTx(lpWithdrawOperation, changes, lpWithdrawResult, false)

		processor := NewTokenTransferProcessor(networkPassphrase)
		stateChanges := processTransaction(t, processor, tx)
		requireEventCount(t, stateChanges, 3)

		assertFeeEvent(t, stateChanges[0], "100")
		assertLiquidityPoolEvent(t, stateChanges[1], types.StateChangeCategoryBurn,
			btcAccount.ToAccountId().Address(), "50000000", btcContractAddress,
			lpIDToStrkey(lpBtcEthID))
		assertLiquidityPoolEvent(t, stateChanges[2], types.StateChangeCategoryCredit,
			btcAccount.ToAccountId().Address(), "120000000", ethContractAddress,
			lpIDToStrkey(lpBtcEthID))
	})

	t.Run("PathPaymentStrictSend - A (BTC Issuer) sends BTC to B as USDC - 2 LP sweeps (BTC/ETH, ETH/USDC) - Mint and Transfer events", func(t *testing.T) {
		path := []xdr.Asset{ethAsset}

		pathPaymentOp := pathPaymentStrictSendOp(
			btcAsset, oneUnit, accountB, usdcAsset, 10*oneUnit, path, &btcAccount,
		)

		pathPaymentResult := &xdr.OperationResult{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypePathPaymentStrictSend,
				PathPaymentStrictSendResult: &xdr.PathPaymentStrictSendResult{
					Code: xdr.PathPaymentStrictSendResultCodePathPaymentStrictSendSuccess,
					Success: &xdr.PathPaymentStrictSendResultSuccess{
						Offers: []xdr.ClaimAtom{
							generateClaimAtom(xdr.ClaimAtomTypeClaimAtomTypeLiquidityPool, nil, &lpBtcEthID, ethAsset, 5*oneUnit, btcAsset, oneUnit),
							generateClaimAtom(xdr.ClaimAtomTypeClaimAtomTypeLiquidityPool, nil, &lpEthUsdcID, usdcAsset, 10*oneUnit, ethAsset, 3*oneUnit),
						},
						Last: xdr.SimplePaymentResult{
							Destination: accountB.ToAccountId(),
							Asset:       usdcAsset,
							Amount:      10 * oneUnit,
						},
					},
				},
			},
		}

		tx := createTx(pathPaymentOp, nil, pathPaymentResult, false)

		processor := NewTokenTransferProcessor(networkPassphrase)
		stateChanges := processTransaction(t, processor, tx)
		requireEventCount(t, stateChanges, 7)

		assertFeeEvent(t, stateChanges[0], "100")
		assertLiquidityPoolEvent(t, stateChanges[1], types.StateChangeCategoryCredit,
			btcAccount.ToAccountId().Address(), "50000000", ethContractAddress,
			lpIDToStrkey(lpBtcEthID))
		assertMintEvent(t, stateChanges[2], btcIssuer, "10000000", btcContractAddress)
		assertLiquidityPoolEvent(t, stateChanges[3], types.StateChangeCategoryCredit,
			btcAccount.ToAccountId().Address(), "100000000", usdcContractAddress,
			lpIDToStrkey(lpEthUsdcID))
		assertLiquidityPoolEvent(t, stateChanges[4], types.StateChangeCategoryDebit,
			btcAccount.ToAccountId().Address(), "30000000", ethContractAddress,
			lpIDToStrkey(lpEthUsdcID))
		assertDebitEvent(t, stateChanges[5], btcAccount.ToAccountId().Address(), "100000000", usdcContractAddress)
		assertCreditEvent(t, stateChanges[6], accountB.ToAccountId().Address(), "100000000", usdcContractAddress)
	})

	t.Run("PathPaymentStrictReceive - A (BTC Issuer) sends BTC to B (USDC Issuer) as USDC - 2 LP sweeps (BTC/ETH, ETH/USDC) - Mint, Transfer and Burn events", func(t *testing.T) {
		path := []xdr.Asset{ethAsset}

		pathPaymentOp := pathPaymentStrictReceiveOp(
			btcAsset, 1*oneUnit, usdcAccount, usdcAsset, 6*oneUnit, path, &btcAccount,
		)

		pathPaymentResult := &xdr.OperationResult{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypePathPaymentStrictReceive,
				PathPaymentStrictReceiveResult: &xdr.PathPaymentStrictReceiveResult{
					Code: xdr.PathPaymentStrictReceiveResultCodePathPaymentStrictReceiveSuccess,
					Success: &xdr.PathPaymentStrictReceiveResultSuccess{
						Offers: []xdr.ClaimAtom{
							generateClaimAtom(xdr.ClaimAtomTypeClaimAtomTypeLiquidityPool, nil, &lpBtcEthID, ethAsset, 2*oneUnit, btcAsset, 1*oneUnit),
							generateClaimAtom(xdr.ClaimAtomTypeClaimAtomTypeLiquidityPool, nil, &lpEthUsdcID, usdcAsset, 6*oneUnit, ethAsset, 2*oneUnit),
						},
						Last: xdr.SimplePaymentResult{
							Destination: usdcAccount.ToAccountId(),
							Asset:       usdcAsset,
							Amount:      6 * oneUnit,
						},
					},
				},
			},
		}

		tx := createTx(pathPaymentOp, nil, pathPaymentResult, false)

		processor := NewTokenTransferProcessor(networkPassphrase)
		stateChanges := processTransaction(t, processor, tx)
		requireEventCount(t, stateChanges, 7)

		assertFeeEvent(t, stateChanges[0], "100")
		assertLiquidityPoolEvent(t, stateChanges[1], types.StateChangeCategoryCredit,
			btcAccount.ToAccountId().Address(), "20000000", ethContractAddress,
			lpIDToStrkey(lpBtcEthID))
		assertMintEvent(t, stateChanges[2], btcIssuer, "10000000", btcContractAddress)
		assertLiquidityPoolEvent(t, stateChanges[3], types.StateChangeCategoryCredit,
			btcAccount.ToAccountId().Address(), "60000000", usdcContractAddress,
			lpIDToStrkey(lpEthUsdcID))
		assertLiquidityPoolEvent(t, stateChanges[4], types.StateChangeCategoryDebit,
			btcAccount.ToAccountId().Address(), "20000000", ethContractAddress,
			lpIDToStrkey(lpEthUsdcID))
		assertBurnEvent(t, stateChanges[5], usdcAccount.ToAccountId().Address(), "60000000", usdcContractAddress)
		assertDebitEvent(t, stateChanges[6], btcAccount.ToAccountId().Address(), "60000000", usdcContractAddress)
	})

	t.Run("ManageBuyOffer - Buy USDC for XLM (Source is USDC issuer)", func(t *testing.T) {
		manageBuyOp := manageBuyOfferOp(&usdcAccount)

		manageBuyResult := &xdr.OperationResult{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypeManageBuyOffer,
				ManageBuyOfferResult: &xdr.ManageBuyOfferResult{
					Code: xdr.ManageBuyOfferResultCodeManageBuyOfferSuccess,
					Success: &xdr.ManageOfferSuccessResult{
						OffersClaimed: []xdr.ClaimAtom{
							generateClaimAtom(xdr.ClaimAtomTypeClaimAtomTypeOrderBook, &accountA, nil, usdcAsset, oneUnit, xlmAsset, 5*oneUnit),
							generateClaimAtom(xdr.ClaimAtomTypeClaimAtomTypeOrderBook, &accountB, nil, usdcAsset, 2*oneUnit, xlmAsset, 10*oneUnit),
						},
					},
				},
			},
		}

		tx := createTx(manageBuyOp, nil, manageBuyResult, false)

		processor := NewTokenTransferProcessor(networkPassphrase)
		stateChanges := processTransaction(t, processor, tx)
		requireEventCount(t, stateChanges, 9)

		assertFeeEvent(t, stateChanges[0], "100")
		assertBurnEvent(t, stateChanges[1], usdcAccount.ToAccountId().Address(), "10000000", usdcContractAddress)
		assertDebitEvent(t, stateChanges[2], accountA.ToAccountId().Address(), "10000000", usdcContractAddress)
		assertDebitEvent(t, stateChanges[3], usdcAccount.ToAccountId().Address(), "50000000", nativeContractAddress)
		assertCreditEvent(t, stateChanges[4], accountA.ToAccountId().Address(), "50000000", nativeContractAddress)
		assertBurnEvent(t, stateChanges[5], usdcAccount.ToAccountId().Address(), "20000000", usdcContractAddress)
		assertDebitEvent(t, stateChanges[6], accountB.ToAccountId().Address(), "20000000", usdcContractAddress)
		assertDebitEvent(t, stateChanges[7], usdcAccount.ToAccountId().Address(), "100000000", nativeContractAddress)
		assertCreditEvent(t, stateChanges[8], accountB.ToAccountId().Address(), "100000000", nativeContractAddress)
	})

	t.Run("ManageSellOffer - Sell USDC for XLM (Source is USDC issuer)", func(t *testing.T) {
		manageSellOp := manageSellOfferOp(&usdcAccount)

		manageSellResult := &xdr.OperationResult{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypeManageSellOffer,
				ManageSellOfferResult: &xdr.ManageSellOfferResult{
					Code: xdr.ManageSellOfferResultCodeManageSellOfferSuccess,
					Success: &xdr.ManageOfferSuccessResult{
						OffersClaimed: []xdr.ClaimAtom{
							generateClaimAtom(xdr.ClaimAtomTypeClaimAtomTypeOrderBook, &accountA, nil, xlmAsset, 5*oneUnit, usdcAsset, oneUnit),
							generateClaimAtom(xdr.ClaimAtomTypeClaimAtomTypeOrderBook, &accountC, nil, xlmAsset, 10*oneUnit, usdcAsset, 2*oneUnit),
						},
					},
				},
			},
		}

		tx := createTx(manageSellOp, nil, manageSellResult, false)

		processor := NewTokenTransferProcessor(networkPassphrase)
		stateChanges := processTransaction(t, processor, tx)
		requireEventCount(t, stateChanges, 9)

		assertFeeEvent(t, stateChanges[0], "100")
		assertDebitEvent(t, stateChanges[1], accountA.ToAccountId().Address(), "50000000", nativeContractAddress)
		assertCreditEvent(t, stateChanges[2], usdcAccount.ToAccountId().Address(), "50000000", nativeContractAddress)
		assertMintEvent(t, stateChanges[3], usdcIssuer, "10000000", usdcContractAddress)
		assertCreditEvent(t, stateChanges[4], accountA.ToAccountId().Address(), "10000000", usdcContractAddress)

		assertDebitEvent(t, stateChanges[5], accountC.ToAccountId().Address(), "100000000", nativeContractAddress)
		assertCreditEvent(t, stateChanges[6], usdcAccount.ToAccountId().Address(), "100000000", nativeContractAddress)
		assertMintEvent(t, stateChanges[7], usdcIssuer, "20000000", usdcContractAddress)
		assertCreditEvent(t, stateChanges[8], accountC.ToAccountId().Address(), "20000000", usdcContractAddress)
	})

	t.Run("Contract events - state changes generated by SEP-41 transfers", func(t *testing.T) {
		admin := keypair.MustRandom().Address()
		asset := xdr.MustNewCreditAsset("TEST_ASSET", admin)
		assetContractID, err := asset.ContractID(networkPassphrase)
		require.NoError(t, err)
		fromContractBytes, toContractBytes := xdr.Hash{}, xdr.Hash{1}
		fromContract := strkey.MustEncode(strkey.VersionByteContract, fromContractBytes[:])
		toContract := strkey.MustEncode(strkey.VersionByteContract, toContractBytes[:])
		amount := big.NewInt(100)

		opResult := &xdr.OperationResult{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type:                     xdr.OperationTypeInvokeHostFunction,
				InvokeHostFunctionResult: &xdr.InvokeHostFunctionResult{},
			},
		}
		tx := createInvocationTx(fromContract, toContract, admin, asset, amount, opResult, contractevents.EventTypeTransfer)

		processor := NewTokenTransferProcessor(networkPassphrase)
		stateChanges := processTransaction(t, processor, tx)
		requireEventCount(t, stateChanges, 3)
		assertFeeEvent(t, stateChanges[0], "1234")
		assertContractEvent(t, stateChanges[1], types.StateChangeCategoryDebit, fromContract, "100", strkey.MustEncode(strkey.VersionByteContract, assetContractID[:]))
		assertContractEvent(t, stateChanges[2], types.StateChangeCategoryCredit, toContract, "100", strkey.MustEncode(strkey.VersionByteContract, assetContractID[:]))
	})
}
