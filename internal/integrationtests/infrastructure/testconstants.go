// Package infrastructure provides test constants for integration testing.
package infrastructure

import "time"

const (
	// Transaction amounts (in XLM or token units as strings)
	DefaultPaymentAmount          = "10"
	DefaultFundingAmount          = "10000"
	DefaultTrustlineLimit         = "1000000"
	TestAccountCreationAmount     = "5"
	TestCustomAssetAmount         = "3000"
	TestAuthRequiredPaymentAmount = "1000"
	TestClaimableBalanceAmount    = "1"
	TestLiquidityPoolAmount       = "100"
	TestEURCPaymentAmount         = "75"
	TestUSDCPaymentAmount         = "100"

	// Liquidity-pool balance test fixtures.
	//
	// Two dedicated accounts each issue their own credit asset, pair it with native XLM in a
	// constant-product pool, and deposit equal legs of TestLiquidityPoolAmount. The initial
	// deposit into an empty constant-product pool mints sqrt(amountA*amountB) shares, so equal
	// 100-unit legs mint exactly 100 shares and leave 100 of each asset in reserve. One account
	// establishes its pool BEFORE the checkpoint snapshot (exercising checkpoint BatchCopy
	// hydration of liquidity_pool_balances JOINed with liquidity_pools); the other deposits live
	// AFTER the snapshot (exercising the live delta-ingestion upsert path).
	TestCheckpointLPAssetCode = "LPTEST"      // credit asset issued by the checkpoint LP account
	TestLiveLPAssetCode       = "LPLIVE"      // credit asset issued by the live LP account
	TestLPReserveStroops      = 1_000_000_000 // 100.0000000 per reserve leg after the deposit
	TestLPShareStroops        = 1_000_000_000 // 100.0000000 pool shares minted by the initial deposit

	// Soroban contract amounts (in stroops with 7 decimals)
	TestEURCTransferStroops  = 500000000  // 50 EURC
	TestSEP41TransferStroops = 2000000000 // 200 SEP41 (partial: account1 keeps 300 of its 500 mint)
	TestUSDCTransferStroops  = 2000000000 // 200 USDC
	TestSEP41MintStroops     = 5000000000 // 500 SEP41
	TestXLMTransferStroops   = 100000000  // 10 XLM (with 7 decimals)

	// Timeouts and intervals
	DefaultHTTPTimeout        = 30 * time.Second
	TransactionConfirmTimeout = 10 * time.Second
	CheckpointWaitDuration    = 10 * time.Second
	TransactionPollInterval   = 500 * time.Millisecond
	RPCHealthTimeout          = 120 * time.Second

	// Retry configuration
	DefaultConfirmationRetries  = 20
	ExtendedConfirmationRetries = 60

	// Ledger configuration
	LedgerValidityBuffer = 100 // Buffer for auth entry validity (ledgers)

	// Protocol configuration
	DefaultProtocolVersion = 24

	// Transaction timeouts (in seconds)
	DefaultTransactionTimeout  = 300
	InfiniteTransactionTimeout = 0 // Used for infinite timeout transactions
)
