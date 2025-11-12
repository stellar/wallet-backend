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
	ErrMsgCouldNotParseTransactionEnvelope = "Could not parse transaction envelope: %s"
	ErrMsgCannotWrapFeeBumpTransaction     = "Cannot wrap a fee-bump transaction into another fee-bump transaction"
	ErrMsgInvalidTransaction               = "Transaction is not a valid transaction"
	ErrMsgFeeBumpCreationFailed            = "Failed to create fee bump transaction: %s"
)
