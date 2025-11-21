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

	// Soroban contract amounts (in stroops with 7 decimals)
	TestEURCTransferStroops  = 500000000  // 50 EURC
	TestSEP41TransferStroops = 1000000000 // 100 SEP41
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
