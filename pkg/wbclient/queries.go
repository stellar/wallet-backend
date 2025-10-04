// Package wbclient provides GraphQL query definitions and builders
package wbclient

import (
	"fmt"
	"strings"
)

const (
	graphqlPath = "/graphql/query"

	// Default field sets for each type
	defaultTransactionFields = `
		hash
		envelopeXdr
		resultXdr
		metaXdr
		ledgerNumber
		ledgerCreatedAt
		ingestedAt
	`

	defaultOperationFields = `
		id
		operationType
		operationXdr
		ledgerNumber
		ledgerCreatedAt
		ingestedAt
	`

	defaultAccountFields = `
		address
		createdAt
	`

	// State change fragments for all concrete types
	stateChangeFragments = `
		__typename
		... on StandardBalanceChange {
			type
			reason
			ingestedAt
			ledgerCreatedAt
			ledgerNumber
			tokenId
			amount
		}
		... on AccountChange {
			type
			reason
			ingestedAt
			ledgerCreatedAt
			ledgerNumber
			tokenId
			amount
		}
		... on SignerChange {
			type
			reason
			ingestedAt
			ledgerCreatedAt
			ledgerNumber
			signerAddress
			signerWeights
		}
		... on SignerThresholdsChange {
			type
			reason
			ingestedAt
			ledgerCreatedAt
			ledgerNumber
			thresholds
		}
		... on MetadataChange {
			type
			reason
			ingestedAt
			ledgerCreatedAt
			ledgerNumber
			keyValue
		}
		... on FlagsChange {
			type
			reason
			ingestedAt
			ledgerCreatedAt
			ledgerNumber
			flags
		}
		... on TrustlineChange {
			type
			reason
			ingestedAt
			ledgerCreatedAt
			ledgerNumber
			limit
		}
		... on ReservesChange {
			type
			reason
			ingestedAt
			ledgerCreatedAt
			ledgerNumber
			sponsoredAddress
			sponsorAddress
		}
		... on BalanceAuthorizationChange {
			type
			reason
			ingestedAt
			ledgerCreatedAt
			ledgerNumber
			flags
			keyValue
		}
	`
)

// buildTransactionQuery builds the GraphQL mutation for building a transaction
func buildTransactionQuery() string {
	return `
		mutation BuildTransaction($input: BuildTransactionInput!) {
			buildTransaction(input: $input) {
				success
				transactionXdr
			}
		}
	`
}

// createFeeBumpTransactionQuery builds the GraphQL mutation for creating a fee bump transaction
func createFeeBumpTransactionQuery() string {
	return `
		mutation CreateFeeBumpTransaction($input: CreateFeeBumpTransactionInput!) {
			createFeeBumpTransaction(input: $input) {
				success
				transaction
				networkPassphrase
			}
		}
	`
}

// registerAccountQuery builds the GraphQL mutation for registering an account
func registerAccountQuery() string {
	return `
		mutation RegisterAccount($input: RegisterAccountInput!) {
			registerAccount(input: $input) {
				success
				account {
					address
				}
			}
		}
	`
}

// deregisterAccountQuery builds the GraphQL mutation for deregistering an account
func deregisterAccountQuery() string {
	return `
		mutation DeregisterAccount($input: DeregisterAccountInput!) {
			deregisterAccount(input: $input) {
				success
				message
			}
		}
	`
}

// buildTransactionByHashQuery builds the GraphQL query for fetching a transaction by hash
func buildTransactionByHashQuery(fields []string) string {
	fieldList := buildFieldList(fields, defaultTransactionFields)
	return fmt.Sprintf(`
		query TransactionByHash($hash: String!) {
			transactionByHash(hash: $hash) {
				%s
			}
		}
	`, fieldList)
}

// buildTransactionsQuery builds the GraphQL query for fetching transactions with pagination
func buildTransactionsQuery(fields []string) string {
	fieldList := buildFieldList(fields, defaultTransactionFields)
	return fmt.Sprintf(`
		query Transactions($first: Int, $after: String, $last: Int, $before: String) {
			transactions(first: $first, after: $after, last: $last, before: $before) {
				edges {
					node {
						%s
					}
					cursor
				}
				pageInfo {
					startCursor
					endCursor
					hasNextPage
					hasPreviousPage
				}
			}
		}
	`, fieldList)
}

// buildAccountByAddressQuery builds the GraphQL query for fetching an account by address
func buildAccountByAddressQuery(fields []string) string {
	fieldList := buildFieldList(fields, defaultAccountFields)
	return fmt.Sprintf(`
		query AccountByAddress($address: String!) {
			accountByAddress(address: $address) {
				%s
			}
		}
	`, fieldList)
}

// buildOperationsQuery builds the GraphQL query for fetching operations with pagination
func buildOperationsQuery(fields []string) string {
	fieldList := buildFieldList(fields, defaultOperationFields)
	return fmt.Sprintf(`
		query Operations($first: Int, $after: String, $last: Int, $before: String) {
			operations(first: $first, after: $after, last: $last, before: $before) {
				edges {
					node {
						%s
					}
					cursor
				}
				pageInfo {
					startCursor
					endCursor
					hasNextPage
					hasPreviousPage
				}
			}
		}
	`, fieldList)
}

// buildOperationByIDQuery builds the GraphQL query for fetching an operation by ID
func buildOperationByIDQuery(fields []string) string {
	fieldList := buildFieldList(fields, defaultOperationFields)
	return fmt.Sprintf(`
		query OperationByID($id: Int64!) {
			operationById(id: $id) {
				%s
			}
		}
	`, fieldList)
}

// buildStateChangesQuery builds the GraphQL query for fetching state changes with pagination
func buildStateChangesQuery() string {
	// For state changes, we always use fragments to handle polymorphic types
	// Individual field selection is not supported for state changes due to their polymorphic nature
	return fmt.Sprintf(`
		query StateChanges($first: Int, $after: String, $last: Int, $before: String) {
			stateChanges(first: $first, after: $after, last: $last, before: $before) {
				edges {
					node {
						%s
					}
					cursor
				}
				pageInfo {
					startCursor
					endCursor
					hasNextPage
					hasPreviousPage
				}
			}
		}
	`, stateChangeFragments)
}

// buildAccountTransactionsQuery builds the GraphQL query for fetching an account's transactions with pagination
func buildAccountTransactionsQuery(fields []string) string {
	fieldList := buildFieldList(fields, defaultTransactionFields)
	return fmt.Sprintf(`
		query AccountTransactions($address: String!, $first: Int, $after: String, $last: Int, $before: String) {
			accountByAddress(address: $address) {
				transactions(first: $first, after: $after, last: $last, before: $before) {
					edges {
						node {
							%s
						}
						cursor
					}
					pageInfo {
						startCursor
						endCursor
						hasNextPage
						hasPreviousPage
					}
				}
			}
		}
	`, fieldList)
}

// buildAccountOperationsQuery builds the GraphQL query for fetching an account's operations with pagination
func buildAccountOperationsQuery(fields []string) string {
	fieldList := buildFieldList(fields, defaultOperationFields)
	return fmt.Sprintf(`
		query AccountOperations($address: String!, $first: Int, $after: String, $last: Int, $before: String) {
			accountByAddress(address: $address) {
				operations(first: $first, after: $after, last: $last, before: $before) {
					edges {
						node {
							%s
						}
						cursor
					}
					pageInfo {
						startCursor
						endCursor
						hasNextPage
						hasPreviousPage
					}
				}
			}
		}
	`, fieldList)
}

// buildAccountStateChangesQuery builds the GraphQL query for fetching an account's state changes with pagination
func buildAccountStateChangesQuery() string {
	return fmt.Sprintf(`
		query AccountStateChanges($address: String!, $first: Int, $after: String, $last: Int, $before: String) {
			accountByAddress(address: $address) {
				stateChanges(first: $first, after: $after, last: $last, before: $before) {
					edges {
						node {
							%s
						}
						cursor
					}
					pageInfo {
						startCursor
						endCursor
						hasNextPage
						hasPreviousPage
					}
				}
			}
		}
	`, stateChangeFragments)
}

// buildTransactionOperationsQuery builds the GraphQL query for fetching a transaction's operations with pagination
func buildTransactionOperationsQuery(fields []string) string {
	fieldList := buildFieldList(fields, defaultOperationFields)
	return fmt.Sprintf(`
		query TransactionOperations($hash: String!, $first: Int, $after: String, $last: Int, $before: String) {
			transactionByHash(hash: $hash) {
				operations(first: $first, after: $after, last: $last, before: $before) {
					edges {
						node {
							%s
						}
						cursor
					}
					pageInfo {
						startCursor
						endCursor
						hasNextPage
						hasPreviousPage
					}
				}
			}
		}
	`, fieldList)
}

// buildTransactionStateChangesQuery builds the GraphQL query for fetching a transaction's state changes with pagination
func buildTransactionStateChangesQuery() string {
	return fmt.Sprintf(`
		query TransactionStateChanges($hash: String!, $first: Int, $after: String, $last: Int, $before: String) {
			transactionByHash(hash: $hash) {
				stateChanges(first: $first, after: $after, last: $last, before: $before) {
					edges {
						node {
							%s
						}
						cursor
					}
					pageInfo {
						startCursor
						endCursor
						hasNextPage
						hasPreviousPage
					}
				}
			}
		}
	`, stateChangeFragments)
}

// buildOperationStateChangesQuery builds the GraphQL query for fetching an operation's state changes with pagination
func buildOperationStateChangesQuery() string {
	return fmt.Sprintf(`
		query OperationStateChanges($id: Int64!, $first: Int, $after: String, $last: Int, $before: String) {
			operationById(id: $id) {
				stateChanges(first: $first, after: $after, last: $last, before: $before) {
					edges {
						node {
							%s
						}
						cursor
					}
					pageInfo {
						startCursor
						endCursor
						hasNextPage
						hasPreviousPage
					}
				}
			}
		}
	`, stateChangeFragments)
}

// buildFieldList constructs a field list string from a slice of field names
// If fields is nil or empty, returns the defaultFields
func buildFieldList(fields []string, defaultFields string) string {
	if len(fields) == 0 {
		return strings.TrimSpace(defaultFields)
	}

	return strings.Join(fields, "\n\t\t\t\t")
}
