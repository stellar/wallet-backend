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

type AccountBalancesTestSuite struct {
	suite.Suite
	testEnv *infrastructure.TestEnvironment
}

func (suite *AccountBalancesTestSuite) TestAccountBalancesForClassicTrustlines() {
	balances, err := suite.testEnv.WBClient.GetBalancesByAccountAddress(context.Background(), suite.testEnv.BalanceTestAccount2KP.Address())
	suite.Require().NoError(err)
	suite.Require().NotNil(balances)
	suite.Require().NotEmpty(balances)

	suite.Require().Len(balances, 2)

	nativeBalance, ok := balances[0].(*types.NativeBalance)
	suite.Require().True(ok)
	suite.Require().Equal("10000.0000000", nativeBalance.GetBalance())
	suite.Require().Equal(types.TokenTypeNative, nativeBalance.GetTokenType())

	usdcBalance, ok := balances[1].(*types.TrustlineBalance)
	suite.Require().True(ok)
	suite.Require().Equal("100.0000000", usdcBalance.GetBalance())
	suite.Require().Equal(suite.testEnv.USDCContractAddress, usdcBalance.GetTokenID())
	suite.Require().Equal(types.TokenTypeClassic, usdcBalance.GetTokenType())
}

// TestSEP41BalanceForGAddress verifies that the wallet backend correctly retrieves
// SEP-41 token balances for account addresses (G...).
//
// This tests the scenario where:
// 1. A custom SEP-41 token contract is deployed (not a SAC)
// 2. Tokens are minted to a G-address
// 3. A contract data entry is created with key [Balance, G-address]
// 4. The wallet backend reads this entry from the checkpoint ledger cache
// 5. The balance is returned via GetBalancesByAccountAddress API
//
// This is different from classic trustlines - it's a pure contract token balance.
func (suite *AccountBalancesTestSuite) TestSEP41BalanceForGAddress() {
	// Query balances for balanceTestAccount1 which has SEP-41 tokens
	balances, err := suite.testEnv.WBClient.GetBalancesByAccountAddress(
		context.Background(),
		suite.testEnv.BalanceTestAccount1KP.Address(),
	)
	suite.Require().NoError(err)
	suite.Require().NotNil(balances)
	suite.Require().NotEmpty(balances)

	// Should have native balance, classic USDC trustline, and SEP-41 contract balance
	suite.Require().GreaterOrEqual(len(balances), 3, "Expected at least 3 balances: native, USDC, and SEP-41")

	// Find the SEP-41 balance
	var foundSEP41 bool
	for _, balance := range balances {
		if contractBalance, ok := balance.(*types.SEP41Balance); ok {
			if contractBalance.GetTokenID() == suite.testEnv.SEP41ContractAddress {
				foundSEP41 = true
				suite.Require().Equal("500.0000000", contractBalance.GetBalance(), "SEP-41 balance should be 500 tokens")
				suite.Require().Equal(types.TokenTypeSEP41, contractBalance.GetTokenType(), "Token type should be SEP41")
				break
			}
		}
	}
	suite.Require().True(foundSEP41, "SEP-41 balance not found for G-address")
}

// TestSACBalanceForCAddress verifies that the wallet backend correctly retrieves
// SAC token balances for contract addresses (C...).
//
// This tests the scenario where:
// 1. A SAC token (USDC) is deployed for a classic asset
// 2. Tokens are transferred to a C-address (contract)
// 3. A contract data entry is created with key [Balance, C-address]
// 4. The wallet backend reads this entry from the checkpoint ledger cache
// 5. The balance is returned when querying the contract's address
//
// This demonstrates that contracts can hold SAC token balances.
func (suite *AccountBalancesTestSuite) TestSACBalanceForCAddress() {
	// Query balances for the holder contract which has USDC SAC tokens
	balances, err := suite.testEnv.WBClient.GetBalancesByAccountAddress(
		context.Background(),
		suite.testEnv.HolderContractAddress,
	)
	suite.Require().NoError(err)
	suite.Require().NotNil(balances)
	suite.Require().NotEmpty(balances)

	// Should have at least USDC SAC balance
	suite.Require().GreaterOrEqual(len(balances), 1, "Expected at least 1 balance: USDC SAC")

	// Find the USDC SAC balance
	var foundUSDC bool
	for _, balance := range balances {
		if contractBalance, ok := balance.(*types.SACBalance); ok {
			if contractBalance.GetTokenID() == suite.testEnv.USDCContractAddress {
				foundUSDC = true
				suite.Require().Equal("200.0000000", contractBalance.GetBalance(), "USDC SAC balance should be 200 tokens")
				suite.Require().Equal(types.TokenTypeSAC, contractBalance.GetTokenType(), "Token type should be SAC")
				break
			}
		}
	}
	suite.Require().True(foundUSDC, "USDC SAC balance not found for C-address")
}

// TestSEP41BalanceForCAddress verifies that the wallet backend correctly retrieves
// SEP-41 token balances for contract addresses (C...).
//
// This tests the scenario where:
// 1. A custom SEP-41 token contract is deployed
// 2. Tokens are transferred to a C-address (contract)
// 3. A contract data entry is created with key [Balance, C-address]
// 4. The wallet backend reads this entry from the checkpoint ledger cache
// 5. The balance is returned when querying the contract's address
//
// This demonstrates that contracts can hold multiple token types simultaneously.
func (suite *AccountBalancesTestSuite) TestSEP41BalanceForCAddress() {
	// Query balances for the holder contract which has SEP-41 tokens
	balances, err := suite.testEnv.WBClient.GetBalancesByAccountAddress(
		context.Background(),
		suite.testEnv.HolderContractAddress,
	)
	suite.Require().NoError(err)
	suite.Require().NotNil(balances)
	suite.Require().NotEmpty(balances)

	// Should have both USDC SAC and SEP-41 balances
	suite.Require().Equal(len(balances), 2, "Expected 2 balances: USDC SAC and SEP-41")

	// Find the SEP-41 balance
	var foundSEP41 bool
	for _, balance := range balances {
		if contractBalance, ok := balance.(*types.SEP41Balance); ok {
			if contractBalance.GetTokenID() == suite.testEnv.SEP41ContractAddress {
				foundSEP41 = true
				suite.Require().Equal("300.0000000", contractBalance.GetBalance(), "SEP-41 balance should be 300 tokens")
				suite.Require().Equal(types.TokenTypeSEP41, contractBalance.GetTokenType(), "Token type should be SEP41")
				break
			}
		}
	}
	suite.Require().True(foundSEP41, "SEP-41 balance not found for C-address")
}

// TestEURCTransferToContract verifies that EURC SAC tokens can be transferred from a G-address
// to a C-address (holder contract) and the balance is correctly tracked via live ingestion.
//
// This tests the scenario where:
// 1. Balance test account 1 already has EURC balance (setup in checkpoint)
// 2. EURC is transferred to holder contract address (G→C transfer)
// 3. Token transfer processor picks up the transfer event
// 4. Redis cache is updated with holder contract's EURC balance
// 5. Balance query returns correct EURC SAC balance for holder contract
func (suite *AccountBalancesTestSuite) TestEURCTransferToContract() {
	// Query balances for the holder contract which should now have EURC after the transfer
	balances, err := suite.testEnv.WBClient.GetBalancesByAccountAddress(
		context.Background(),
		suite.testEnv.HolderContractAddress,
	)
	suite.Require().NoError(err)
	suite.Require().NotNil(balances)
	suite.Require().NotEmpty(balances)

	// Should have USDC SAC, SEP-41, and now EURC SAC balances
	suite.Require().GreaterOrEqual(len(balances), 3, "Expected at least 3 balances: USDC SAC, SEP-41, and EURC SAC")

	// Find the EURC SAC balance
	var foundEURC bool
	for _, balance := range balances {
		if contractBalance, ok := balance.(*types.SACBalance); ok {
			if contractBalance.GetTokenID() == suite.testEnv.EURCContractAddress {
				foundEURC = true
				suite.Require().Equal("50.0000000", contractBalance.GetBalance(), "EURC SAC balance should be 50 tokens")
				suite.Require().Equal(types.TokenTypeSAC, contractBalance.GetTokenType(), "Token type should be SAC")
				break
			}
		}
	}
	suite.Require().True(foundEURC, "EURC SAC balance not found for holder contract C-address")
}

// TestEURCTrustlineCreation verifies that EURC trustline creation and funding for balance test
// account 2 is correctly tracked via live ingestion through the effects processor.
//
// This tests the scenario where:
// 1. Balance test account 2 does not have EURC trustline initially
// 2. ChangeTrust operation creates EURC trustline for account 2
// 3. Payment operation mints 75 EURC to account 2
// 4. Effects processor picks up the trustline creation
// 5. Token transfer processor picks up the payment
// 6. Redis cache is updated with account 2's EURC trustline and balance
// 7. Balance query returns correct EURC balance for account 2
func (suite *AccountBalancesTestSuite) TestEURCTrustlineCreation() {
	// Query balances for balance test account 2 which should now have EURC trustline
	balances, err := suite.testEnv.WBClient.GetBalancesByAccountAddress(
		context.Background(),
		suite.testEnv.BalanceTestAccount2KP.Address(),
	)
	suite.Require().NoError(err)
	suite.Require().NotNil(balances)
	suite.Require().NotEmpty(balances)

	// Should have native XLM, USDC, and now EURC balances
	suite.Require().GreaterOrEqual(len(balances), 3, "Expected at least 3 balances: native, USDC, and EURC")

	// Find the EURC trustline balance
	var foundEURC bool
	for _, balance := range balances {
		if trustlineBalance, ok := balance.(*types.TrustlineBalance); ok {
			if trustlineBalance.GetTokenID() == suite.testEnv.EURCContractAddress {
				foundEURC = true
				suite.Require().Equal("75.0000000", trustlineBalance.GetBalance(), "EURC trustline balance should be 75 tokens")
				suite.Require().Equal(types.TokenTypeClassic, trustlineBalance.GetTokenType(), "Token type should be Classic")
				break
			}
		}
	}
	suite.Require().True(foundEURC, "EURC trustline balance not found for balance test account 2")
}

// TestSEP41Transfer verifies that SEP-41 tokens can be transferred from one G-address to another
// and the balance is correctly tracked via live ingestion through the token transfer processor.
//
// This tests the scenario where:
// 1. Balance test account 1 has SEP-41 tokens (setup in checkpoint)
// 2. SEP-41 tokens are transferred from account 1 to account 2
// 3. Token transfer processor picks up the transfer event
// 4. Redis cache is updated with account 2's SEP-41 balance
// 5. Balance query returns correct SEP-41 balance for account 2
func (suite *AccountBalancesTestSuite) TestSEP41Transfer() {
	// Query balances for balance test account 2 which should now have SEP-41 tokens
	balances, err := suite.testEnv.WBClient.GetBalancesByAccountAddress(
		context.Background(),
		suite.testEnv.BalanceTestAccount2KP.Address(),
	)
	suite.Require().NoError(err)
	suite.Require().NotNil(balances)
	suite.Require().NotEmpty(balances)

	// Should have native XLM, USDC, EURC, and now SEP-41 balances
	suite.Require().GreaterOrEqual(len(balances), 4, "Expected at least 4 balances: native, USDC, EURC, and SEP-41")

	// Find the SEP-41 balance
	var foundSEP41 bool
	for _, balance := range balances {
		if contractBalance, ok := balance.(*types.SEP41Balance); ok {
			if contractBalance.GetTokenID() == suite.testEnv.SEP41ContractAddress {
				foundSEP41 = true
				suite.Require().Equal("100.0000000", contractBalance.GetBalance(), "SEP-41 balance should be 100 tokens")
				suite.Require().Equal(types.TokenTypeSEP41, contractBalance.GetTokenType(), "Token type should be SEP41")
				break
			}
		}
	}
	suite.Require().True(foundSEP41, "SEP-41 balance not found for balance test account 2")
}
