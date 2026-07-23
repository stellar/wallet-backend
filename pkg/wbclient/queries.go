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
		resultCode
		feeCharged
		ledgerNumber
		ledgerCreatedAt
		isFeeBump
		ingestedAt
	`

	defaultOperationFields = `
		id
		operationType
		operationXdr
		resultCode
		successful
		ledgerNumber
		ledgerCreatedAt
		ingestedAt
	`

	defaultAccountFields = `
		address
	`

	// State change fragments for all concrete types
	stateChangeFragments = `
		__typename
		type
		reason
		ingestedAt
		ledgerCreatedAt
		ledgerNumber
		
		... on StandardBalanceChange {
			standardBalanceTokenId: tokenId
			amount
		}
		... on AccountChange {
			funderAddress
			deployerAddress
			destinationAddress
		}
		... on SignerChange {
			signerAddress
			signerWeights
		}
		... on SignerThresholdsChange {
			thresholds
		}
		... on MetadataChange {
			metadataKeyValue: keyValue
		}
		... on FlagsChange {
			flags
		}
		... on TrustlineChange {
			trustlineTokenId: tokenId
			limit
			trustlineLiquidityPoolId: liquidityPoolId
		}
		... on ReservesChange {
			sponsoredAddress
			sponsorAddress
			sponsoredData
			sponsoredTrustline
			claimableBalanceId
			liquidityPoolId
		}
		... on BalanceAuthorizationChange {
			balanceAuthTokenId: tokenId
			balanceAuthLiquidityPoolId: liquidityPoolId
			flags
		}
	`
)

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

// buildAccountTransactionsQuery builds the GraphQL query for fetching an account's transactions with pagination
func buildAccountTransactionsQuery(fields []string) string {
	fieldList := buildFieldList(fields, defaultTransactionFields)
	return fmt.Sprintf(`
		query AccountTransactions($address: String!, $since: Time, $until: Time, $first: Int, $after: String, $last: Int, $before: String) {
			accountByAddress(address: $address) {
				transactions(since: $since, until: $until, first: $first, after: $after, last: $last, before: $before) {
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
		query AccountOperations($address: String!, $since: Time, $until: Time, $first: Int, $after: String, $last: Int, $before: String) {
			accountByAddress(address: $address) {
				operations(since: $since, until: $until, first: $first, after: $after, last: $last, before: $before) {
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
// Supports optional filtering by transaction hash and/or operation ID
func buildAccountStateChangesQuery() string {
	return fmt.Sprintf(`
		query AccountStateChanges($address: String!, $filter: AccountStateChangeFilterInput, $since: Time, $until: Time, $first: Int, $after: String, $last: Int, $before: String) {
			accountByAddress(address: $address) {
				stateChanges(filter: $filter, since: $since, until: $until, first: $first, after: $after, last: $last, before: $before) {
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

// buildAccountTransactionsWithOpsAndStateChangesQuery fetches an account's transactions with that account's
// operations and state changes embedded per edge, in one call.
func buildAccountTransactionsWithOpsAndStateChangesQuery() string {
	return fmt.Sprintf(`
		query AccountTransactionsWithOpsAndStateChanges($address: String!, $since: Time, $until: Time, $first: Int, $after: String, $last: Int, $before: String) {
			accountByAddress(address: $address) {
				transactions(since: $since, until: $until, first: $first, after: $after, last: $last, before: $before) {
					edges {
						node {
							%s
						}
						operations {
							%s
						}
						stateChanges {
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
	`, strings.TrimSpace(defaultTransactionFields), strings.TrimSpace(defaultOperationFields), stateChangeFragments)
}

// balanceFragments contains GraphQL fragments for all concrete balance types
const balanceFragments = `
		__typename
		balance
		tokenId
		tokenType
		... on NativeBalance {
			minimumBalance
			buyingLiabilities
			sellingLiabilities
			numSubentries
			lastModifiedLedger
		}
		... on TrustlineBalance {
			code
			issuer
			type
			limit
			buyingLiabilities
			sellingLiabilities
			lastModifiedLedger
			isAuthorized
			isAuthorizedToMaintainLiabilities
		}
		... on SACBalance {
			code
			issuer
			decimals
			isAuthorized
			isClawbackEnabled
		}
		... on SEP41Balance {
			name
			symbol
			decimals
			lastModifiedLedger
		}
		... on LiquidityPoolBalance {
			liquidityPoolId
			reserves {
				asset
				amount
			}
			lastModifiedLedger
		}
	`

// buildAccountBalancesQuery builds the GraphQL query for fetching account balances.
func buildAccountBalancesQuery() string {
	return fmt.Sprintf(`
		query GetAccountBalances($address: String!, $first: Int, $after: String, $last: Int, $before: String) {
			accountByAddress(address: $address) {
				balances(first: $first, after: $after, last: $last, before: $before) {
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
	`, balanceFragments)
}

// buildFieldList constructs a field list string from a slice of field names
// If fields is nil or empty, returns the defaultFields
func buildFieldList(fields []string, defaultFields string) string {
	if len(fields) == 0 {
		return strings.TrimSpace(defaultFields)
	}

	return strings.Join(fields, "\n\t\t\t\t")
}
