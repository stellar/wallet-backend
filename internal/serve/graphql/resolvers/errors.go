// GraphQL resolver error message constants
package resolvers

// Error message constants
const (
	// RegisterAccount errors
	ErrMsgAccountAlreadyExists      = "Account is already registered"
	ErrMsgInvalidAddress            = "Invalid address: must be a valid Stellar public key or contract address"
	ErrMsgAccountRegistrationFailed = "Failed to register account: %s"

	// DeregisterAccount errors
	ErrMsgAccountNotFound             = "Account not found"
	ErrMsgAccountDeregistrationFailed = "Failed to deregister account: %s"
	ErrMsgAccountDeregisteredSuccess  = "Account deregistered successfully"

	// CreateFeeBumpTransaction errors
	ErrMsgCouldNotParseTransactionEnvelope = "Could not parse transaction envelope."
	ErrMsgCannotWrapFeeBumpTransaction     = "Cannot wrap a fee-bump transaction into another fee-bump transaction"
	ErrMsgInvalidTransaction               = "Transaction is not a valid transaction"
	ErrMsgFeeBumpCreationFailed            = "Failed to create fee bump transaction: %s"

	// BalancesByAccountAddress errors (single account)
	ErrMsgSingleInvalidAddress = "invalid address format: must be a valid Stellar account (G...) or contract (C...) address"

	// BalancesByAccountAddresses errors (multiple accounts)
	ErrMsgEmptyAddresses      = "addresses array cannot be empty"
	ErrMsgTooManyAddresses    = "maximum %d addresses allowed per query, got %d"
	ErrMsgInvalidAddressAt    = "invalid address format at index %d: %s"
	ErrMsgRPCUnavailable      = "failed to fetch ledger entries from RPC"
	ErrMsgBalancesFetchFailed = "failed to process account balances"
)
