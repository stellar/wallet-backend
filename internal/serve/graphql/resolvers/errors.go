// GraphQL resolver error message constants
package resolvers

// Error message constants
const (
	// CreateFeeBumpTransaction errors
	ErrMsgCouldNotParseTransactionEnvelope = "Could not parse transaction envelope."
	ErrMsgCannotWrapFeeBumpTransaction     = "Cannot wrap a fee-bump transaction into another fee-bump transaction"
	ErrMsgInvalidTransaction               = "Transaction is not a valid transaction"
	ErrMsgFeeBumpCreationFailed            = "Failed to create fee bump transaction: %s"

	// Account balance errors
	ErrMsgSingleInvalidAddress = "invalid address format: must be a valid Stellar account (G...) or contract (C...) address"

	// TransactionByHash / StateChanges hash filter errors
	ErrMsgInvalidTransactionHash = "invalid transaction hash format: must be a 64-character hex string"

	// Balance query processing errors
	ErrMsgBalancesFetchFailed = "failed to process account balances"
)
