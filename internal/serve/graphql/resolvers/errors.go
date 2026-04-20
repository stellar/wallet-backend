// GraphQL resolver error message constants
package resolvers

// Error message constants
const (
	// Account balance errors
	ErrMsgSingleInvalidAddress = "invalid address format: must be a valid Stellar account (G...) or contract (C...) address"

	// TransactionByHash / StateChanges hash filter errors
	ErrMsgInvalidTransactionHash = "invalid transaction hash format: must be a 64-character hex string"

	// Balance query processing errors
	ErrMsgBalancesFetchFailed = "failed to process account balances"
)
