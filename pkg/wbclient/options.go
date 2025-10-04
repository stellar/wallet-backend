// Package wbclient provides query options for field selection in GraphQL queries
package wbclient

// QueryOptions allows clients to specify which fields to fetch for each entity type
type QueryOptions struct {
	// TransactionFields specifies which transaction fields to fetch
	// If nil or empty, all default fields are fetched
	TransactionFields []string

	// OperationFields specifies which operation fields to fetch
	// If nil or empty, all default fields are fetched
	OperationFields []string

	// AccountFields specifies which account fields to fetch
	// If nil or empty, all default fields are fetched
	AccountFields []string

	// Note: StateChange fields are not configurable due to polymorphic types
	// All state change queries use GraphQL fragments to handle different types
}

// TransactionFieldSet provides commonly used transaction field sets
type TransactionFieldSet struct{}

// AllFields returns all available transaction fields
func (TransactionFieldSet) AllFields() []string {
	return []string{
		"hash",
		"envelopeXdr",
		"resultXdr",
		"metaXdr",
		"ledgerNumber",
		"ledgerCreatedAt",
		"ingestedAt",
	}
}

// OperationFieldSet provides commonly used operation field sets
type OperationFieldSet struct{}

// AllFields returns all available operation fields
func (OperationFieldSet) AllFields() []string {
	return []string{
		"id",
		"operationType",
		"operationXdr",
		"ledgerNumber",
		"ledgerCreatedAt",
		"ingestedAt",
	}
}

// AccountFieldSet provides commonly used account field sets
type AccountFieldSet struct{}

// AllFields returns all available account fields
func (AccountFieldSet) AllFields() []string {
	return []string{
		"address",
		"createdAt",
	}
}

// MinimalFields returns minimal account fields (just address)
func (AccountFieldSet) MinimalFields() []string {
	return []string{
		"address",
	}
}

// Global field set helpers for convenient access
var (
	TransactionFields TransactionFieldSet
	OperationFields   OperationFieldSet
	AccountFields     AccountFieldSet
)
