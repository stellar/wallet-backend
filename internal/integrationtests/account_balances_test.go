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
	balances, err := suite.testEnv.WBClient.GetAccountBalances(
		context.Background(),
		suite.testEnv.BalanceTestAccount1KP.Address(),
	)
	suite.Require().NoError(err)
	suite.Require().NotNil(balances)
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
	balances, err := suite.testEnv.WBClient.GetAccountBalances(
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
func (suite *AccountBalancesAfterCheckpointTestSuite) TestCheckpoint_HolderContract_HasInitialBalances() {
	balances, err := suite.testEnv.WBClient.GetAccountBalances(
		context.Background(),
		suite.testEnv.HolderContractAddress,
	)
	suite.Require().NoError(err)
	suite.Require().NotNil(balances)
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
func (suite *AccountBalancesAfterLiveIngestionTestSuite) TestLiveIngestion_Account1_HasUpdatedBalances() {
	balances, err := suite.testEnv.WBClient.GetAccountBalances(
		context.Background(),
		suite.testEnv.BalanceTestAccount1KP.Address(),
	)
	suite.Require().NoError(err)
	suite.Require().NotNil(balances)
	suite.Require().NotEmpty(balances)

	suite.Require().Equal(3, len(balances), "Expected 3 balances: native XLM, USDC, and EURC")

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
func (suite *AccountBalancesAfterLiveIngestionTestSuite) TestLiveIngestion_Account2_HasNewBalances() {
	balances, err := suite.testEnv.WBClient.GetAccountBalances(
		context.Background(),
		suite.testEnv.BalanceTestAccount2KP.Address(),
	)
	suite.Require().NoError(err)
	suite.Require().NotNil(balances)
	suite.Require().NotEmpty(balances)

	suite.Require().Equal(3, len(balances), "Expected 3 balances: native XLM, USDC, and EURC")

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

		default:
			suite.Fail("Unexpected balance type: %T", balance)
		}
	}
}

// TestLiveIngestion_HolderContract_HasNewEURC verifies that the holder contract
// has the expected balances after fixture transactions add EURC:
// - USDC SAC tokens (200) - unchanged
// - EURC SAC tokens (50) - NEW from transfer from account 1
func (suite *AccountBalancesAfterLiveIngestionTestSuite) TestLiveIngestion_HolderContract_HasNewEURC() {
	balances, err := suite.testEnv.WBClient.GetAccountBalances(
		context.Background(),
		suite.testEnv.HolderContractAddress,
	)
	suite.Require().NoError(err)
	suite.Require().NotNil(balances)
	suite.Require().NotEmpty(balances)

	suite.Require().Equal(2, len(balances), "Expected 2 balances: USDC SAC and EURC SAC")

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

		default:
			suite.Fail("Unexpected balance type: %T", balance)
		}
	}
}
