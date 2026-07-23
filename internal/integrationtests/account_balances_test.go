// Account Balance Integration Tests
//
// This test suite verifies that the wallet backend correctly retrieves different types
// of token balances for Stellar accounts. It tests three types of balances:
//
// 1. Native XLM Balance
//   - The account's XLM balance stored in the account entry
//
// 2. Classic Trustline Balance (TokenTypeClassic)
//   - Traditional Stellar trustlines created with CHANGE_TRUST operations
//   - Example: USDC issued by a specific issuer
//   - Stored as LedgerEntryTypeTrustLine entries
//
// Test Accounts:
//   - balanceTestAccount1: Has native XLM, classic USDC trustline, and EURC trustline
//   - balanceTestAccount2: Has native XLM and classic USDC trustline
//   - These are separate from transaction test accounts to avoid balance drift
//
// References:
//   - SEP-41 Standard: https://stellar.org/protocol/sep-41
//   - Token Contract: https://github.com/stellar/soroban-examples/tree/v22.0.1/token
//   - Setup code: internal/integrationtests/infrastructure/setup.go lines 277-337
package integrationtests

import (
	"context"
	"strconv"

	"github.com/stellar/go-stellar-sdk/amount"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/stellar/wallet-backend/internal/integrationtests/infrastructure"
	"github.com/stellar/wallet-backend/pkg/wbclient/types"
)

// AccountBalancesAfterCheckpointTestSuite validates that balances are correctly calculated
// using tokens populated from the checkpoint ledger before any fixture transactions are submitted.
//
// This suite tests the initial state after checkpoint setup completes but before
// the BuildAndSubmitTransactionsTestSuite executes any transactions.
type AccountBalancesAfterCheckpointTestSuite struct {
	suite.Suite
	testEnv *infrastructure.TestEnvironment
}

// TestCheckpoint_Account1_HasInitialBalances verifies that balance test account 1
// has the expected initial balances from checkpoint setup:
// - Native XLM (~10000)
// - USDC trustline (100)
// - EURC trustline (100)
func (suite *AccountBalancesAfterCheckpointTestSuite) TestCheckpoint_Account1_HasInitialBalances() {
	balances, err := suite.testEnv.WBClient.GetAllAccountBalances(
		context.Background(),
		suite.testEnv.BalanceTestAccount1KP.Address(),
	)
	suite.Require().NoError(err)
	suite.Require().NotEmpty(balances)

	suite.Require().Equal(3, len(balances), "Expected 3 balances: native XLM, USDC trustline, EURC trustline")

	for _, balance := range balances {
		switch b := balance.(type) {
		case *types.NativeBalance:
			suite.Require().Equal("10000.0000000", b.GetBalance())
			suite.Require().Equal(types.TokenTypeNative, b.GetTokenType())
			suite.Require().NotEmpty(b.MinimumBalance, "MinimumBalance should be populated")
			minBal, err := strconv.ParseFloat(b.MinimumBalance, 64)
			suite.Require().NoError(err)
			suite.Require().Greater(minBal, 0.0, "MinimumBalance should be positive")
			suite.Require().Greater(b.LastModifiedLedger, uint32(0), "LastModifiedLedger should be set")

		case *types.TrustlineBalance:
			suite.Require().Equal(types.TokenTypeClassic, b.GetTokenType())
			suite.Require().NotNil(b.Code, "Trustline balance should have a code")
			suite.Require().NotNil(b.Issuer, "Trustline balance should have an issuer")
			suite.Require().Equal(suite.testEnv.MasterAccountAddress, *b.Issuer)

			switch *b.Code {
			case "USDC":
				suite.Require().Equal("100.0000000", b.GetBalance())
				suite.Require().Equal(suite.testEnv.USDCContractAddress, b.GetTokenID())
			case "EURC":
				suite.Require().Equal("100.0000000", b.GetBalance())
				suite.Require().Equal(suite.testEnv.EURCContractAddress, b.GetTokenID())
			default:
				suite.Fail("Unexpected trustline code: %s", *b.Code)
			}

		default:
			suite.Fail("Unexpected balance type: %T", balance)
		}
	}
}

// TestCheckpoint_Account2_HasInitialBalances verifies that balance test account 2
// has the expected initial balances from checkpoint setup:
// - Native XLM (~10000)
// - USDC trustline (100)
func (suite *AccountBalancesAfterCheckpointTestSuite) TestCheckpoint_Account2_HasInitialBalances() {
	balances, err := suite.testEnv.WBClient.GetAllAccountBalances(
		context.Background(),
		suite.testEnv.BalanceTestAccount2KP.Address(),
	)
	suite.Require().NoError(err)
	suite.Require().NotEmpty(balances)

	suite.Require().Equal(2, len(balances), "Expected 2 balances: native XLM and USDC trustline")

	for _, balance := range balances {
		switch b := balance.(type) {
		case *types.NativeBalance:
			suite.Require().Equal("10000.0000000", b.GetBalance())
			suite.Require().Equal(types.TokenTypeNative, b.GetTokenType())
			// Verify new native balance fields are populated
			suite.Require().NotEmpty(b.MinimumBalance, "MinimumBalance should be populated")
			minBal, err := strconv.ParseFloat(b.MinimumBalance, 64)
			suite.Require().NoError(err)
			suite.Require().Greater(minBal, 0.0, "MinimumBalance should be positive")
			suite.Require().Greater(b.LastModifiedLedger, uint32(0), "LastModifiedLedger should be set")

		case *types.TrustlineBalance:
			suite.Require().Equal(types.TokenTypeClassic, b.GetTokenType())
			suite.Require().NotNil(b.Code, "Trustline balance should have a code")
			suite.Require().NotNil(b.Issuer, "Trustline balance should have an issuer")
			suite.Require().Equal("USDC", *b.Code)
			suite.Require().Equal(suite.testEnv.MasterAccountAddress, *b.Issuer)
			suite.Require().Equal("100.0000000", b.GetBalance())
			suite.Require().Equal(suite.testEnv.USDCContractAddress, b.GetTokenID())

		default:
			suite.Fail("Unexpected balance type: %T", balance)
		}
	}
}

// TestCheckpoint_HolderContract_HasInitialBalances verifies that the holder contract
// has the expected initial balances from checkpoint setup:
// - USDC SAC tokens (200)
func (suite *AccountBalancesAfterCheckpointTestSuite) TestCheckpoint_HolderContract_HasInitialBalances() {
	balances, err := suite.testEnv.WBClient.GetAllAccountBalances(
		context.Background(),
		suite.testEnv.HolderContractAddress,
	)
	suite.Require().NoError(err)
	suite.Require().NotEmpty(balances)

	suite.Require().Equal(1, len(balances), "Expected 1 balance: USDC SAC")

	for _, balance := range balances {
		switch b := balance.(type) {
		case *types.SACBalance:
			suite.Require().Equal("200.0000000", b.GetBalance())
			suite.Require().Equal(suite.testEnv.USDCContractAddress, b.GetTokenID())
			suite.Require().Equal(types.TokenTypeSAC, b.GetTokenType())
			suite.Require().Equal("USDC", b.Code)
			suite.Require().Equal(suite.testEnv.MasterAccountAddress, b.Issuer)
			suite.Require().Equal(int32(7), b.Decimals)

		default:
			suite.Fail("Unexpected balance type: %T", balance)
		}
	}
}

// TestCheckpoint_LiquidityPoolAccount_HasPoolShareBalance verifies that the dedicated liquidity-pool
// account — which established a constant-product pool and deposited before the checkpoint snapshot —
// has its pool-share balance hydrated through the checkpoint BatchCopy path. This proves the
// liquidity_pool_balances ⋈ liquidity_pools JOIN is populated from the snapshot:
//   - Native XLM (reduced by the deposited leg + fees)
//   - Liquidity-pool share balance (TestLPShareStroops shares; native + LPTEST reserves)
func (suite *AccountBalancesAfterCheckpointTestSuite) TestCheckpoint_LiquidityPoolAccount_HasPoolShareBalance() {
	balances, err := suite.testEnv.WBClient.GetAllAccountBalances(
		context.Background(),
		suite.testEnv.CheckpointLPAccountKP.Address(),
	)
	suite.Require().NoError(err)
	suite.Require().Equal(2, len(balances), "Expected 2 balances: native XLM and the liquidity-pool share")

	var lp *types.LiquidityPoolBalance
	for _, balance := range balances {
		switch b := balance.(type) {
		case *types.NativeBalance:
			suite.Require().Equal(types.TokenTypeNative, b.GetTokenType())
			suite.Require().Greater(b.LastModifiedLedger, uint32(0), "LastModifiedLedger should be set")
		case *types.LiquidityPoolBalance:
			lp = b
		default:
			suite.Fail("Unexpected balance type: %T", balance)
		}
	}

	suite.Require().NotNil(lp, "expected a liquidity-pool share balance hydrated from the checkpoint")
	assetB := infrastructure.TestCheckpointLPAssetCode + ":" + suite.testEnv.CheckpointLPAccountKP.Address()
	assertLiquidityPoolBalance(suite.Require(), lp,
		suite.testEnv.CheckpointLiquidityPoolID,
		amount.StringFromInt64(infrastructure.TestLPShareStroops),
		"native", amount.StringFromInt64(infrastructure.TestLPReserveStroops),
		assetB, amount.StringFromInt64(infrastructure.TestLPReserveStroops))
}

// TestCheckpoint_Account1_ForwardPagination verifies that GetAccountBalances
// honors the first and after pagination args. Account1 has 3 balances at
// checkpoint, so calling with first=1 must return a single edge with
// HasNextPage=true; passing the returned EndCursor as after on the next
// call must return a different edge.
func (suite *AccountBalancesAfterCheckpointTestSuite) TestCheckpoint_Account1_ForwardPagination() {
	pageSize := int32(1)
	address := suite.testEnv.BalanceTestAccount1KP.Address()

	page1, err := suite.testEnv.WBClient.GetAccountBalances(
		context.Background(), address,
		&pageSize, nil, nil, nil,
	)
	suite.Require().NoError(err)
	suite.Require().NotNil(page1)
	suite.Require().Len(page1.Edges, 1, "Expected exactly 1 edge on the first page when first=1")
	suite.Require().NotNil(page1.PageInfo)
	suite.Require().True(page1.PageInfo.HasNextPage, "HasNextPage should be true with 3 balances and first=1")
	suite.Require().NotNil(page1.PageInfo.EndCursor, "EndCursor should be populated when HasNextPage is true")

	page2, err := suite.testEnv.WBClient.GetAccountBalances(
		context.Background(), address,
		&pageSize, nil, page1.PageInfo.EndCursor, nil,
	)
	suite.Require().NoError(err)
	suite.Require().NotNil(page2)
	suite.Require().Len(page2.Edges, 1, "Expected exactly 1 edge on the second page when first=1")
	suite.Require().NotEqual(page1.Edges[0].Cursor, page2.Edges[0].Cursor,
		"Second page must return a different edge than the first; equal cursors imply the after arg was ignored")
}

// TestCheckpoint_Account1_BackwardPagination verifies that GetAccountBalances
// honors the last and before pagination args. Account1 has 3 balances at
// checkpoint, so calling with last=1 must return a single edge with
// HasPreviousPage=true; passing the returned StartCursor as before on the
// next call must return a different edge.
func (suite *AccountBalancesAfterCheckpointTestSuite) TestCheckpoint_Account1_BackwardPagination() {
	pageSize := int32(1)
	address := suite.testEnv.BalanceTestAccount1KP.Address()

	lastPage, err := suite.testEnv.WBClient.GetAccountBalances(
		context.Background(), address,
		nil, &pageSize, nil, nil,
	)
	suite.Require().NoError(err)
	suite.Require().NotNil(lastPage)
	suite.Require().Len(lastPage.Edges, 1, "Expected exactly 1 edge on the last page when last=1")
	suite.Require().NotNil(lastPage.PageInfo)
	suite.Require().True(lastPage.PageInfo.HasPreviousPage, "HasPreviousPage should be true with 3 balances and last=1")
	suite.Require().NotNil(lastPage.PageInfo.StartCursor, "StartCursor should be populated when HasPreviousPage is true")

	prevPage, err := suite.testEnv.WBClient.GetAccountBalances(
		context.Background(), address,
		nil, &pageSize, nil, lastPage.PageInfo.StartCursor,
	)
	suite.Require().NoError(err)
	suite.Require().NotNil(prevPage)
	suite.Require().Len(prevPage.Edges, 1, "Expected exactly 1 edge on the page before the last")
	suite.Require().NotEqual(lastPage.Edges[0].Cursor, prevPage.Edges[0].Cursor,
		"Previous page must return a different edge than the last; equal cursors imply the before arg was ignored")
}

// AccountBalancesAfterLiveIngestionTestSuite validates that balances are correctly calculated
// after fixture transactions are submitted and processed by the live ingestion pipeline. These new transactions
// will lead to new tokens being inserted into the token cache and new balances being calculated.
//
// This suite tests the final state after BuildAndSubmitTransactionsTestSuite executes
// the fixture transactions and the ingest service processes them.
type AccountBalancesAfterLiveIngestionTestSuite struct {
	suite.Suite
	testEnv *infrastructure.TestEnvironment
}

// TestLiveIngestion_Account1_HasUpdatedBalances verifies that balance test account 1
// has the expected balances after fixture transactions are processed:
// - Native XLM
// - USDC trustline (100) - unchanged
// - EURC trustline (50) - reduced from 100 after transfer to contract
// - SEP-41 token - the remainder of its mint after a partial transfer to account 2
func (suite *AccountBalancesAfterLiveIngestionTestSuite) TestLiveIngestion_Account1_HasUpdatedBalances() {
	balances, err := suite.testEnv.WBClient.GetAllAccountBalances(
		context.Background(),
		suite.testEnv.BalanceTestAccount1KP.Address(),
	)
	suite.Require().NoError(err)
	suite.Require().NotEmpty(balances)

	suite.Require().Equal(4, len(balances), "Expected 4 balances: native XLM, USDC, EURC, and SEP-41")

	for _, balance := range balances {
		switch b := balance.(type) {
		case *types.NativeBalance:
			parsedBalance, err := strconv.ParseFloat(b.GetBalance(), 64)
			suite.Require().NoError(err)
			suite.Require().Equal(10000.0, parsedBalance, "Balance should be the same as initial balance")
			suite.Require().Equal(types.TokenTypeNative, b.GetTokenType())
			suite.Require().NotEmpty(b.MinimumBalance, "MinimumBalance should be populated")
			minBal, err := strconv.ParseFloat(b.MinimumBalance, 64)
			suite.Require().NoError(err)
			suite.Require().Greater(minBal, 0.0, "MinimumBalance should be positive")
			suite.Require().Greater(b.LastModifiedLedger, uint32(0), "LastModifiedLedger should be updated")

		case *types.TrustlineBalance:
			suite.Require().Equal(types.TokenTypeClassic, b.GetTokenType())
			suite.Require().NotNil(b.Code, "Trustline balance should have a code")
			suite.Require().NotNil(b.Issuer, "Trustline balance should have an issuer")
			suite.Require().Equal(suite.testEnv.MasterAccountAddress, *b.Issuer)

			switch *b.Code {
			case "USDC":
				suite.Require().Equal("100.0000000", b.GetBalance())
				suite.Require().Equal(suite.testEnv.USDCContractAddress, b.GetTokenID())
			case "EURC":
				suite.Require().Equal("50.0000000", b.GetBalance(), "EURC balance should be reduced to 50 after transfer")
				suite.Require().Equal(suite.testEnv.EURCContractAddress, b.GetTokenID())
			default:
				suite.Fail("Unexpected trustline code: %s", *b.Code)
			}

		case *types.SEP41Balance:
			// account1 minted 500 and transferred 200 to account2, keeping 300.
			suite.assertSEP41TokenBalance(b,
				strconv.Itoa(infrastructure.TestSEP41MintStroops-infrastructure.TestSEP41TransferStroops))

		default:
			suite.Fail("Unexpected balance type: %T", balance)
		}
	}
}

// TestLiveIngestion_Account2_HasNewBalances verifies that balance test account 2
// has the expected balances after fixture transactions create new token holdings:
//   - Native XLM
//   - USDC trustline (100) - unchanged
//   - EURC trustline (75) - NEW from trustline creation and payment
//   - SEP-41 token (500) - received from account 1's transfer, backfilled by the DataMigrationTestSuite current-state migration
func (suite *AccountBalancesAfterLiveIngestionTestSuite) TestLiveIngestion_Account2_HasNewBalances() {
	balances, err := suite.testEnv.WBClient.GetAllAccountBalances(
		context.Background(),
		suite.testEnv.BalanceTestAccount2KP.Address(),
	)
	suite.Require().NoError(err)
	suite.Require().NotEmpty(balances)

	suite.Require().Equal(4, len(balances), "Expected 4 balances: native XLM, USDC, EURC, and SEP-41")

	for _, balance := range balances {
		switch b := balance.(type) {
		case *types.NativeBalance:
			parsedBalance, err := strconv.ParseFloat(b.GetBalance(), 64)
			suite.Require().NoError(err)
			suite.Require().Equal(10000.0, parsedBalance, "Balance should be the same as initial balance")
			suite.Require().Equal(types.TokenTypeNative, b.GetTokenType())
			suite.Require().NotEmpty(b.MinimumBalance, "MinimumBalance should be populated")
			minBal, err := strconv.ParseFloat(b.MinimumBalance, 64)
			suite.Require().NoError(err)
			suite.Require().Greater(minBal, 0.0, "MinimumBalance should be positive")
			suite.Require().Greater(b.LastModifiedLedger, uint32(0), "LastModifiedLedger should be updated")

		case *types.TrustlineBalance:
			suite.Require().Equal(types.TokenTypeClassic, b.GetTokenType())
			suite.Require().NotNil(b.Code, "Trustline balance should have a code")
			suite.Require().NotNil(b.Issuer, "Trustline balance should have an issuer")
			suite.Require().Equal(suite.testEnv.MasterAccountAddress, *b.Issuer)

			switch *b.Code {
			case "USDC":
				suite.Require().Equal("100.0000000", b.GetBalance())
				suite.Require().Equal(suite.testEnv.USDCContractAddress, b.GetTokenID())
			case "EURC":
				suite.Require().Equal("75.0000000", b.GetBalance(), "EURC balance should be 75 from payment")
				suite.Require().Equal(suite.testEnv.EURCContractAddress, b.GetTokenID())
			default:
				suite.Fail("Unexpected trustline code: %s", *b.Code)
			}

		case *types.SEP41Balance:
			// account2 received the 200 transferred by account1.
			suite.assertSEP41TokenBalance(b, strconv.Itoa(infrastructure.TestSEP41TransferStroops))

		default:
			suite.Fail("Unexpected balance type: %T", balance)
		}
	}
}

// TestLiveIngestion_HolderContract_HasNewEURC verifies that the holder contract
// has the expected balances after fixture transactions add EURC:
// - USDC SAC tokens (200) - unchanged
// - EURC SAC tokens (50) - NEW from transfer from account 1
// - SEP-41 token (500) - backfilled by DataMigrationTestSuite's current-state migration
func (suite *AccountBalancesAfterLiveIngestionTestSuite) TestLiveIngestion_HolderContract_HasNewEURC() {
	balances, err := suite.testEnv.WBClient.GetAllAccountBalances(
		context.Background(),
		suite.testEnv.HolderContractAddress,
	)
	suite.Require().NoError(err)
	suite.Require().NotEmpty(balances)

	suite.Require().Equal(3, len(balances), "Expected 3 balances: USDC SAC, EURC SAC, and SEP-41")

	for _, balance := range balances {
		switch b := balance.(type) {
		case *types.SACBalance:
			suite.Require().Equal(types.TokenTypeSAC, b.GetTokenType())
			suite.Require().Equal(suite.testEnv.MasterAccountAddress, b.Issuer)
			suite.Require().Equal(int32(7), b.Decimals)

			switch b.Code {
			case "USDC":
				suite.Require().Equal("200.0000000", b.GetBalance())
				suite.Require().Equal(suite.testEnv.USDCContractAddress, b.GetTokenID())
			case "EURC":
				suite.Require().Equal("50.0000000", b.GetBalance(), "EURC SAC balance should be 50 from transfer")
				suite.Require().Equal(suite.testEnv.EURCContractAddress, b.GetTokenID())
			default:
				suite.Fail("Unexpected SAC code: %s", b.Code)
			}

		case *types.SEP41Balance:
			// the holder contract kept its full 500 mint.
			suite.assertSEP41TokenBalance(b, strconv.Itoa(infrastructure.TestSEP41MintStroops))

		default:
			suite.Fail("Unexpected balance type: %T", balance)
		}
	}
}

// TestLiveIngestion_FeeBumpFeeSource_NativeBalanceFoldsFee verifies issue #637: an account that
// pays only a fee-bump fee — touched by no operation — has its native balance reduced by that fee,
// because the fee phase is folded into native balances. The fixture funds this account with
// DefaultFundingAmount and charges exactly FeeBumpFixtureFeeStroops, so the indexed balance is the
// funding amount minus that fee.
func (suite *AccountBalancesAfterLiveIngestionTestSuite) TestLiveIngestion_FeeBumpFeeSource_NativeBalanceFoldsFee() {
	balances, err := suite.testEnv.WBClient.GetAllAccountBalances(
		context.Background(),
		suite.testEnv.FeeBumpSourceKP.Address(),
	)
	suite.Require().NoError(err)
	suite.Require().Len(balances, 1, "fee-bump fee source should have exactly one (native) balance")

	fundingStroops, err := amount.ParseInt64(infrastructure.DefaultFundingAmount)
	suite.Require().NoError(err)
	expectedBalance := amount.StringFromInt64(fundingStroops - int64(infrastructure.FeeBumpFixtureFeeStroops))

	nb, ok := balances[0].(*types.NativeBalance)
	suite.Require().True(ok, "expected a native balance, got %T", balances[0])
	suite.Require().Equal(types.TokenTypeNative, nb.GetTokenType())
	suite.Require().Equal(expectedBalance, nb.GetBalance(),
		"native balance must reflect the fee-phase debit (DefaultFundingAmount − FeeBumpFixtureFeeStroops) folded in (#637)")
	suite.Require().Greater(nb.LastModifiedLedger, uint32(0), "LastModifiedLedger should be set")
}

// TestLiveIngestion_SorobanRefundSource_NativeBalanceMatchesChain verifies that a Soroban post-apply
// fee refund is folded into the transaction source's native balance and reflects the operation
// applied before it. SorobanRefundSourceKP sources a Soroban tx that over-declares its resource fee
// (Core refunds the surplus after apply) and is the `from` of the transfer, so its balance moves in
// the fee, operation, and post-apply refund phases of one ledger. The indexed balance must equal the
// on-chain balance; dropping the post-apply refund would leave the indexed balance short by the
// refunded surplus — which is exactly why the sort key ranks the refund phase above operations.
func (suite *AccountBalancesAfterLiveIngestionTestSuite) TestLiveIngestion_SorobanRefundSource_NativeBalanceMatchesChain() {
	address := suite.testEnv.SorobanRefundSourceKP.Address()

	onChainStroops, err := infrastructure.GetOnChainNativeBalance(suite.testEnv.RPCService, address)
	suite.Require().NoError(err)

	balances, err := suite.testEnv.WBClient.GetAllAccountBalances(context.Background(), address)
	suite.Require().NoError(err)

	var nb *types.NativeBalance
	for _, b := range balances {
		if native, ok := b.(*types.NativeBalance); ok {
			nb = native
			break
		}
	}
	suite.Require().NotNil(nb, "expected a native balance for the Soroban refund source")

	suite.Require().Equal(amount.StringFromInt64(onChainStroops), nb.GetBalance(),
		"indexed native balance must equal on-chain balance — fee debit, transfer op, and post-apply refund all folded in")

	// Sanity: the source paid a net fee and transferred XLM out, so its balance dropped below funding.
	fundingStroops, err := amount.ParseInt64(infrastructure.DefaultFundingAmount)
	suite.Require().NoError(err)
	suite.Require().Less(onChainStroops, fundingStroops, "source should have paid a net fee and transferred XLM out")
}

// TestLiveIngestion_LiquidityPoolAccount_HasPoolShareBalance verifies that the dedicated live
// liquidity-pool account — which deposited into a constant-product pool via a submitted use case
// (prepareLiveLiquidityPoolDepositOps) without withdrawing — has its pool-share balance produced by
// the live delta-ingestion path (liquidity_pools + liquidity_pool_balances upserts). This is the
// live counterpart to the checkpoint hydration test:
//   - Native XLM (reduced by the deposited leg + fees)
//   - Liquidity-pool share balance (TestLPShareStroops shares; native + LPLIVE reserves)
func (suite *AccountBalancesAfterLiveIngestionTestSuite) TestLiveIngestion_LiquidityPoolAccount_HasPoolShareBalance() {
	balances, err := suite.testEnv.WBClient.GetAllAccountBalances(
		context.Background(),
		suite.testEnv.LiveLPAccountKP.Address(),
	)
	suite.Require().NoError(err)
	suite.Require().Equal(2, len(balances), "Expected 2 balances: native XLM and the liquidity-pool share")

	var lp *types.LiquidityPoolBalance
	for _, balance := range balances {
		switch b := balance.(type) {
		case *types.NativeBalance:
			suite.Require().Equal(types.TokenTypeNative, b.GetTokenType())
			suite.Require().Greater(b.LastModifiedLedger, uint32(0), "LastModifiedLedger should be set")
		case *types.LiquidityPoolBalance:
			lp = b
		default:
			suite.Fail("Unexpected balance type: %T", balance)
		}
	}

	suite.Require().NotNil(lp, "expected a liquidity-pool share balance produced by live ingestion")
	assetB := infrastructure.TestLiveLPAssetCode + ":" + suite.testEnv.LiveLPAccountKP.Address()
	assertLiquidityPoolBalance(suite.Require(), lp,
		suite.testEnv.LiveLiquidityPoolID,
		amount.StringFromInt64(infrastructure.TestLPShareStroops),
		"native", amount.StringFromInt64(infrastructure.TestLPReserveStroops),
		assetB, amount.StringFromInt64(infrastructure.TestLPReserveStroops))
}

// assertSEP41TokenBalance verifies a SEP-41 balance node matches the custom token
// deployed in setup and migrated by DataMigrationTestSuite. The API returns the
// raw i128 amount (unscaled by decimals), so expectedAmount is the stroop count
// as a decimal string, e.g. "5000000000" for 500 tokens at 7 decimals.
func (suite *AccountBalancesAfterLiveIngestionTestSuite) assertSEP41TokenBalance(b *types.SEP41Balance, expectedAmount string) {
	suite.Require().Equal(types.TokenTypeSEP41, b.GetTokenType())
	suite.Require().Equal(suite.testEnv.SEP41ContractAddress, b.GetTokenID())
	suite.Require().Equal(expectedAmount, b.GetBalance(), "SEP-41 balance should equal the migrated amount")
	suite.Require().Equal(int32(7), b.Decimals, "SEP-41 token was deployed with 7 decimals")
}

// assertLiquidityPoolBalance verifies a liquidity-pool balance node exposes the expected pool id,
// share balance, and both reserve legs (canonical asset + amount, in AssetA/AssetB order). It is a
// free function rather than a suite method because both the checkpoint and live-ingestion suites
// assert pool-share balances; expectedShares/amountA/amountB are decimal strings (e.g. "100.0000000").
func assertLiquidityPoolBalance(r *require.Assertions, b *types.LiquidityPoolBalance, expectedPoolID, expectedShares, assetA, amountA, assetB, amountB string) {
	r.Equal(types.TokenTypeLiquidityPool, b.GetTokenType())
	r.Equal(expectedPoolID, b.GetTokenID(), "LP tokenId should be the pool id")
	r.Equal(expectedPoolID, b.LiquidityPoolID, "LP liquidityPoolId should be the pool id")
	r.Equal(expectedShares, b.GetBalance(), "LP share balance mismatch")
	r.Greater(b.LastModifiedLedger, uint32(0), "LastModifiedLedger should be set")
	r.Len(b.Reserves, 2, "LP should expose both reserve legs")
	r.Equal(assetA, b.Reserves[0].Asset, "reserve[0] asset mismatch")
	r.Equal(amountA, b.Reserves[0].Amount, "reserve[0] amount mismatch")
	r.Equal(assetB, b.Reserves[1].Asset, "reserve[1] asset mismatch")
	r.Equal(amountB, b.Reserves[1].Amount, "reserve[1] amount mismatch")
}
