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
// 3. SEP-41 Contract Token Balance (TokenTypeSEP41)
//   - Custom token contracts that implement the SEP-41 interface
//   - Balances stored as contract data entries with key [Balance, Address]
//   - This test uses the standard token contract from soroban-examples v22.0.1
//
// SEP-41 Token Contract Setup:
//   - Contract: soroban_token_contract.wasm from stellar/soroban-examples
//   - Initialization: __constructor(admin, decimal, name, symbol)
//   - Admin: Master test account
//   - Decimals: 7
//   - Name: "USD Coin"
//   - Symbol: "USDC"
//   - Functions tested: mint(to, amount)
//   - Balance storage: Contract data entry with key [Balance, G-address]
//
// Test Accounts:
//   - balanceTestAccount1: Has native XLM, classic USDC trustline, and SEP-41 tokens
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
// - SEP-41 contract tokens (500)
func (suite *AccountBalancesAfterCheckpointTestSuite) TestCheckpoint_Account1_HasInitialBalances() {
	balances, err := suite.testEnv.WBClient.GetBalancesByAccountAddress(
		context.Background(),
		suite.testEnv.BalanceTestAccount1KP.Address(),
	)
	suite.Require().NoError(err)
	suite.Require().NotNil(balances)
	suite.Require().NotEmpty(balances)

	suite.Require().Equal(4, len(balances), "Expected 4 balances: native XLM, USDC trustline, EURC trustline, and SEP-41 tokens")

	// Verify native XLM balance
	nativeBalance, ok := balances[0].(*types.NativeBalance)
	suite.Require().True(ok, "First balance should be native XLM")
	suite.Require().Equal("10000.0000000", nativeBalance.GetBalance())
	suite.Require().Equal(types.TokenTypeNative, nativeBalance.GetTokenType())

	// Verify EURC trustline balance
	eurcBalance, ok := balances[1].(*types.TrustlineBalance)
	suite.Require().True(ok, "Third balance should be EURC trustline")
	suite.Require().Equal("100.0000000", eurcBalance.GetBalance())
	suite.Require().Equal(suite.testEnv.EURCContractAddress, eurcBalance.GetTokenID())
	suite.Require().Equal(types.TokenTypeClassic, eurcBalance.GetTokenType())

	// Verify USDC trustline balance
	usdcBalance, ok := balances[2].(*types.TrustlineBalance)
	suite.Require().True(ok, "Second balance should be USDC trustline")
	suite.Require().Equal("100.0000000", usdcBalance.GetBalance())
	suite.Require().Equal(suite.testEnv.USDCContractAddress, usdcBalance.GetTokenID())
	suite.Require().Equal(types.TokenTypeClassic, usdcBalance.GetTokenType())

	// Verify SEP-41 contract token balance
	sep41Balance, ok := balances[3].(*types.SEP41Balance)
	suite.Require().True(ok, "Fourth balance should be SEP-41 tokens")
	suite.Require().Equal("500.0000000", sep41Balance.GetBalance())
	suite.Require().Equal(suite.testEnv.SEP41ContractAddress, sep41Balance.GetTokenID())
	suite.Require().Equal(types.TokenTypeSEP41, sep41Balance.GetTokenType())
}

// TestCheckpoint_Account2_HasInitialBalances verifies that balance test account 2
// has the expected initial balances from checkpoint setup:
// - Native XLM (~10000)
// - USDC trustline (100)
func (suite *AccountBalancesAfterCheckpointTestSuite) TestCheckpoint_Account2_HasInitialBalances() {
	balances, err := suite.testEnv.WBClient.GetBalancesByAccountAddress(
		context.Background(),
		suite.testEnv.BalanceTestAccount2KP.Address(),
	)
	suite.Require().NoError(err)
	suite.Require().NotNil(balances)
	suite.Require().NotEmpty(balances)

	suite.Require().Equal(2, len(balances), "Expected 2 balances: native XLM and USDC trustline")

	// Verify native XLM balance
	nativeBalance, ok := balances[0].(*types.NativeBalance)
	suite.Require().True(ok, "First balance should be native XLM")
	suite.Require().Equal("10000.0000000", nativeBalance.GetBalance())
	suite.Require().Equal(types.TokenTypeNative, nativeBalance.GetTokenType())

	// Verify USDC trustline balance
	usdcBalance, ok := balances[1].(*types.TrustlineBalance)
	suite.Require().True(ok, "Second balance should be USDC trustline")
	suite.Require().Equal("100.0000000", usdcBalance.GetBalance())
	suite.Require().Equal(suite.testEnv.USDCContractAddress, usdcBalance.GetTokenID())
	suite.Require().Equal(types.TokenTypeClassic, usdcBalance.GetTokenType())
}

// TestCheckpoint_HolderContract_HasInitialBalances verifies that the holder contract
// has the expected initial balances from checkpoint setup:
// - USDC SAC tokens (200)
// - SEP-41 contract tokens (500)
func (suite *AccountBalancesAfterCheckpointTestSuite) TestCheckpoint_HolderContract_HasInitialBalances() {
	balances, err := suite.testEnv.WBClient.GetBalancesByAccountAddress(
		context.Background(),
		suite.testEnv.HolderContractAddress,
	)
	suite.Require().NoError(err)
	suite.Require().NotNil(balances)
	suite.Require().NotEmpty(balances)

	suite.Require().Equal(2, len(balances), "Expected 2 balances: USDC SAC and SEP-41 tokens")

	for _, balance := range balances {
		switch balance.GetTokenType() {
		case types.TokenTypeSEP41:
			suite.Require().Equal("500.0000000", balance.GetBalance())
			suite.Require().Equal(suite.testEnv.SEP41ContractAddress, balance.GetTokenID())
			suite.Require().Equal(types.TokenTypeSEP41, balance.GetTokenType())
		case types.TokenTypeSAC:
			suite.Require().Equal("200.0000000", balance.GetBalance())
			suite.Require().Equal(suite.testEnv.USDCContractAddress, balance.GetTokenID())
			suite.Require().Equal(types.TokenTypeSAC, balance.GetTokenType())
		}
	}
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
// - SEP-41 contract tokens (0) - reduced from 500 after transfer to account 2
func (suite *AccountBalancesAfterLiveIngestionTestSuite) TestLiveIngestion_Account1_HasUpdatedBalances() {
	balances, err := suite.testEnv.WBClient.GetBalancesByAccountAddress(
		context.Background(),
		suite.testEnv.BalanceTestAccount1KP.Address(),
	)
	suite.Require().NoError(err)
	suite.Require().NotNil(balances)
	suite.Require().NotEmpty(balances)

	suite.Require().Equal(4, len(balances), "Expected 4 balances: native XLM, USDC, EURC, and SEP-41 tokens")

	// Verify native XLM balance (may have changed due to fees)
	nativeBalance, ok := balances[0].(*types.NativeBalance)
	suite.Require().True(ok, "First balance should be native XLM")
	suite.Require().Equal(types.TokenTypeNative, nativeBalance.GetTokenType())

	// Verify EURC trustline balance (reduced from 100 to 50)
	eurcBalance, ok := balances[1].(*types.TrustlineBalance)
	suite.Require().True(ok, "Third balance should be EURC trustline")
	suite.Require().Equal("50.0000000", eurcBalance.GetBalance(), "EURC balance should be reduced to 50 after transfer")
	suite.Require().Equal(suite.testEnv.EURCContractAddress, eurcBalance.GetTokenID())
	suite.Require().Equal(types.TokenTypeClassic, eurcBalance.GetTokenType())

	// Verify USDC trustline balance (unchanged)
	usdcBalance, ok := balances[2].(*types.TrustlineBalance)
	suite.Require().True(ok, "Second balance should be USDC trustline")
	suite.Require().Equal("100.0000000", usdcBalance.GetBalance())
	suite.Require().Equal(suite.testEnv.USDCContractAddress, usdcBalance.GetTokenID())
	suite.Require().Equal(types.TokenTypeClassic, usdcBalance.GetTokenType())

	// Verify SEP-41 contract token balance (reduced from 500 to 400)
	sep41Balance, ok := balances[3].(*types.SEP41Balance)
	suite.Require().True(ok, "Fourth balance should be SEP-41 tokens")
	suite.Require().Equal("0.0000000", sep41Balance.GetBalance(), "SEP-41 balance should be reduced to 0 after transfer")
	suite.Require().Equal(suite.testEnv.SEP41ContractAddress, sep41Balance.GetTokenID())
	suite.Require().Equal(types.TokenTypeSEP41, sep41Balance.GetTokenType())
}

// TestLiveIngestion_Account2_HasNewBalances verifies that balance test account 2
// has the expected balances after fixture transactions create new token holdings:
// - Native XLM
// - USDC trustline (100) - unchanged
// - EURC trustline (75) - NEW from trustline creation and payment
// - SEP-41 contract tokens (500) - NEW from transfer from account 1
func (suite *AccountBalancesAfterLiveIngestionTestSuite) TestLiveIngestion_Account2_HasNewBalances() {
	balances, err := suite.testEnv.WBClient.GetBalancesByAccountAddress(
		context.Background(),
		suite.testEnv.BalanceTestAccount2KP.Address(),
	)
	suite.Require().NoError(err)
	suite.Require().NotNil(balances)
	suite.Require().NotEmpty(balances)

	suite.Require().Equal(4, len(balances), "Expected 4 balances: native XLM, USDC, EURC, and SEP-41 tokens")

	// Verify native XLM balance
	nativeBalance, ok := balances[0].(*types.NativeBalance)
	suite.Require().True(ok, "First balance should be native XLM")
	suite.Require().Equal(types.TokenTypeNative, nativeBalance.GetTokenType())

	// Verify USDC trustline balance (unchanged)
	usdcBalance, ok := balances[1].(*types.TrustlineBalance)
	suite.Require().True(ok, "Second balance should be USDC trustline")
	suite.Require().Equal("100.0000000", usdcBalance.GetBalance())
	suite.Require().Equal(suite.testEnv.USDCContractAddress, usdcBalance.GetTokenID())
	suite.Require().Equal(types.TokenTypeClassic, usdcBalance.GetTokenType())

	// Verify EURC trustline balance (NEW - 75 tokens from payment)
	eurcBalance, ok := balances[2].(*types.TrustlineBalance)
	suite.Require().True(ok, "Third balance should be EURC trustline")
	suite.Require().Equal("75.0000000", eurcBalance.GetBalance(), "EURC balance should be 75 from payment")
	suite.Require().Equal(suite.testEnv.EURCContractAddress, eurcBalance.GetTokenID())
	suite.Require().Equal(types.TokenTypeClassic, eurcBalance.GetTokenType())

	// Verify SEP-41 contract token balance (NEW - 100 tokens from transfer)
	sep41Balance, ok := balances[3].(*types.SEP41Balance)
	suite.Require().True(ok, "Fourth balance should be SEP-41 tokens")
	suite.Require().Equal("500.0000000", sep41Balance.GetBalance(), "SEP-41 balance should be 500 from transfer")
	suite.Require().Equal(suite.testEnv.SEP41ContractAddress, sep41Balance.GetTokenID())
	suite.Require().Equal(types.TokenTypeSEP41, sep41Balance.GetTokenType())
}

// TestLiveIngestion_HolderContract_HasNewEURC verifies that the holder contract
// has the expected balances after fixture transactions add EURC:
// - USDC SAC tokens (200) - unchanged
// - SEP-41 contract tokens (500) - unchanged
// - EURC SAC tokens (50) - NEW from transfer from account 1
func (suite *AccountBalancesAfterLiveIngestionTestSuite) TestLiveIngestion_HolderContract_HasNewEURC() {
	balances, err := suite.testEnv.WBClient.GetBalancesByAccountAddress(
		context.Background(),
		suite.testEnv.HolderContractAddress,
	)
	suite.Require().NoError(err)
	suite.Require().NotNil(balances)
	suite.Require().NotEmpty(balances)

	suite.Require().Equal(3, len(balances), "Expected 3 balances: USDC SAC, SEP-41, and EURC SAC")

	// Verify SEP-41 contract token balance (unchanged)
	sep41Balance, ok := balances[0].(*types.SEP41Balance)
	suite.Require().True(ok, "First balance should be SEP-41 tokens")
	suite.Require().Equal("500.0000000", sep41Balance.GetBalance())
	suite.Require().Equal(suite.testEnv.SEP41ContractAddress, sep41Balance.GetTokenID())
	suite.Require().Equal(types.TokenTypeSEP41, sep41Balance.GetTokenType())

	// Verify USDC SAC balance (unchanged)
	usdcSACBalance, ok := balances[1].(*types.SACBalance)
	suite.Require().True(ok, "Second balance should be USDC SAC")
	suite.Require().Equal("200.0000000", usdcSACBalance.GetBalance())
	suite.Require().Equal(suite.testEnv.USDCContractAddress, usdcSACBalance.GetTokenID())
	suite.Require().Equal(types.TokenTypeSAC, usdcSACBalance.GetTokenType())

	// Verify EURC SAC balance (NEW - 50 tokens from transfer)
	eurcSACBalance, ok := balances[2].(*types.SACBalance)
	suite.Require().True(ok, "Third balance should be EURC SAC")
	suite.Require().Equal("50.0000000", eurcSACBalance.GetBalance(), "EURC SAC balance should be 50 from transfer")
	suite.Require().Equal(suite.testEnv.EURCContractAddress, eurcSACBalance.GetTokenID())
	suite.Require().Equal(types.TokenTypeSAC, eurcSACBalance.GetTokenType())
}
