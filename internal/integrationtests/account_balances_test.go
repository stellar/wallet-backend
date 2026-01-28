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
	"strconv"

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

		case *types.SEP41Balance:
			suite.Require().Equal("500.0000000", b.GetBalance())
			suite.Require().Equal(suite.testEnv.SEP41ContractAddress, b.GetTokenID())
			suite.Require().Equal(types.TokenTypeSEP41, b.GetTokenType())
			suite.Require().Equal("SEP41 Token", b.Name)
			suite.Require().Equal("SEP41", b.Symbol)
			suite.Require().Equal(int32(7), b.Decimals)

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
	balances, err := suite.testEnv.WBClient.GetBalancesByAccountAddress(
		context.Background(),
		suite.testEnv.BalanceTestAccount2KP.Address(),
	)
	suite.Require().NoError(err)
	suite.Require().NotNil(balances)
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
		switch b := balance.(type) {
		case *types.SEP41Balance:
			suite.Require().Equal("500.0000000", b.GetBalance())
			suite.Require().Equal(suite.testEnv.SEP41ContractAddress, b.GetTokenID())
			suite.Require().Equal(types.TokenTypeSEP41, b.GetTokenType())
			suite.Require().Equal("SEP41 Token", b.Name)
			suite.Require().Equal("SEP41", b.Symbol)
			suite.Require().Equal(int32(7), b.Decimals)

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

// TestCheckpoint_MultiAccount_FetchesAllBalances verifies that multiple accounts can be fetched
// in a single request and returns balances in the same order as input addresses.
func (suite *AccountBalancesAfterCheckpointTestSuite) TestCheckpoint_MultiAccount_FetchesAllBalances() {
	addresses := []string{
		suite.testEnv.BalanceTestAccount1KP.Address(),
		suite.testEnv.BalanceTestAccount2KP.Address(),
		suite.testEnv.HolderContractAddress,
	}

	results, err := suite.testEnv.WBClient.GetBalancesByAccountAddresses(
		context.Background(),
		addresses,
	)
	suite.Require().NoError(err)
	suite.Require().NotNil(results)
	suite.Require().Len(results, 3, "Expected 3 results, one for each address")

	// Verify results are in the same order as input addresses
	suite.Require().Equal(addresses[0], results[0].Address)
	suite.Require().Equal(addresses[1], results[1].Address)
	suite.Require().Equal(addresses[2], results[2].Address)

	// Verify no errors for any account
	suite.Require().Nil(results[0].Error, "Account 1 should not have an error")
	suite.Require().Nil(results[1].Error, "Account 2 should not have an error")
	suite.Require().Nil(results[2].Error, "Holder contract should not have an error")

	// Verify Account 1 has 4 balances (native, USDC, EURC, SEP-41)
	suite.Require().Len(results[0].Balances, 4, "Account 1 should have 4 balances")

	// Verify Account 2 has 2 balances (native, USDC)
	suite.Require().Len(results[1].Balances, 2, "Account 2 should have 2 balances")

	// Verify Holder Contract has 2 balances (USDC SAC, SEP-41)
	suite.Require().Len(results[2].Balances, 2, "Holder contract should have 2 balances")
}

// TestCheckpoint_MultiAccount_EmptyAddressesReturnsError verifies that an empty addresses array
// returns an appropriate error.
func (suite *AccountBalancesAfterCheckpointTestSuite) TestCheckpoint_MultiAccount_EmptyAddressesReturnsError() {
	_, err := suite.testEnv.WBClient.GetBalancesByAccountAddresses(
		context.Background(),
		[]string{},
	)
	suite.Require().Error(err)
	suite.Require().Contains(err.Error(), "empty")
}

// TestCheckpoint_MultiAccount_ExceedsMaxLimitReturnsError verifies that exceeding the maximum
// number of addresses returns an appropriate error.
func (suite *AccountBalancesAfterCheckpointTestSuite) TestCheckpoint_MultiAccount_ExceedsMaxLimitReturnsError() {
	// Create 101 addresses to exceed the typical max limit
	addresses := make([]string, 101)
	for i := range addresses {
		addresses[i] = suite.testEnv.BalanceTestAccount1KP.Address()
	}

	_, err := suite.testEnv.WBClient.GetBalancesByAccountAddresses(
		context.Background(),
		addresses,
	)
	suite.Require().Error(err)
	suite.Require().Contains(err.Error(), "maximum")
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
			suite.Require().Equal("0.0000000", b.GetBalance(), "SEP-41 balance should be reduced to 0 after transfer")
			suite.Require().Equal(suite.testEnv.SEP41ContractAddress, b.GetTokenID())
			suite.Require().Equal(types.TokenTypeSEP41, b.GetTokenType())
			suite.Require().Equal("SEP41 Token", b.Name)
			suite.Require().Equal("SEP41", b.Symbol)
			suite.Require().Equal(int32(7), b.Decimals)

		default:
			suite.Fail("Unexpected balance type: %T", balance)
		}
	}
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
			suite.Require().Equal("500.0000000", b.GetBalance(), "SEP-41 balance should be 500 from transfer")
			suite.Require().Equal(suite.testEnv.SEP41ContractAddress, b.GetTokenID())
			suite.Require().Equal(types.TokenTypeSEP41, b.GetTokenType())
			suite.Require().Equal("SEP41 Token", b.Name)
			suite.Require().Equal("SEP41", b.Symbol)
			suite.Require().Equal(int32(7), b.Decimals)

		default:
			suite.Fail("Unexpected balance type: %T", balance)
		}
	}
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

	for _, balance := range balances {
		switch b := balance.(type) {
		case *types.SEP41Balance:
			suite.Require().Equal("500.0000000", b.GetBalance())
			suite.Require().Equal(suite.testEnv.SEP41ContractAddress, b.GetTokenID())
			suite.Require().Equal(types.TokenTypeSEP41, b.GetTokenType())
			suite.Require().Equal("SEP41 Token", b.Name)
			suite.Require().Equal("SEP41", b.Symbol)
			suite.Require().Equal(int32(7), b.Decimals)

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

		default:
			suite.Fail("Unexpected balance type: %T", balance)
		}
	}
}

// TestLiveIngestion_MultiAccount_FetchesUpdatedBalances verifies that multiple accounts
// can be fetched in a single request and returns the updated balances after fixture transactions.
func (suite *AccountBalancesAfterLiveIngestionTestSuite) TestLiveIngestion_MultiAccount_FetchesUpdatedBalances() {
	addresses := []string{
		suite.testEnv.BalanceTestAccount1KP.Address(),
		suite.testEnv.BalanceTestAccount2KP.Address(),
		suite.testEnv.HolderContractAddress,
	}

	results, err := suite.testEnv.WBClient.GetBalancesByAccountAddresses(
		context.Background(),
		addresses,
	)
	suite.Require().NoError(err)
	suite.Require().NotNil(results)
	suite.Require().Len(results, 3, "Expected 3 results, one for each address")

	// Verify results are in the same order as input addresses
	suite.Require().Equal(addresses[0], results[0].Address)
	suite.Require().Equal(addresses[1], results[1].Address)
	suite.Require().Equal(addresses[2], results[2].Address)

	// Verify no errors for any account
	suite.Require().Nil(results[0].Error, "Account 1 should not have an error")
	suite.Require().Nil(results[1].Error, "Account 2 should not have an error")
	suite.Require().Nil(results[2].Error, "Holder contract should not have an error")

	// Verify Account 1 has 4 balances (native, USDC, EURC, SEP-41)
	suite.Require().Len(results[0].Balances, 4, "Account 1 should have 4 balances")

	// Verify Account 2 has 4 balances after fixture transactions (native, USDC, EURC, SEP-41)
	suite.Require().Len(results[1].Balances, 4, "Account 2 should have 4 balances after fixture transactions")

	// Verify Holder Contract has 3 balances after fixture transactions (USDC SAC, SEP-41, EURC SAC)
	suite.Require().Len(results[2].Balances, 3, "Holder contract should have 3 balances after fixture transactions")
}
