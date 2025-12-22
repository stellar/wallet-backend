// query_utils_test.go tests utility functions for building SQL queries.
// Includes tests for column preparation, prefix handling, and pagination query building.
package data

import (
	"sort"
	"strings"
	"testing"

	set "github.com/deckarep/golang-set/v2"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testModel is a sample struct used for testing getDBColumns and prepareColumnsWithID
type testModel struct {
	ID              int64  `db:"id"`
	Name            string `db:"name"`
	LedgerCreatedAt string `db:"ledger_created_at"`
	IgnoredField    string `db:"-"`
	NoTagField      string
}

func TestGetDBColumns(t *testing.T) {
	columns := getDBColumns(testModel{})

	assert.True(t, columns.Contains("id"))
	assert.True(t, columns.Contains("name"))
	assert.True(t, columns.Contains("ledger_created_at"))
	assert.False(t, columns.Contains("-"))
	assert.False(t, columns.Contains("IgnoredField"))
	assert.False(t, columns.Contains("NoTagField"))
	assert.Equal(t, 3, columns.Cardinality())
}

func TestPrepareColumnsWithID(t *testing.T) {
	tests := []struct {
		name           string
		columns        string
		model          any
		prefix         string
		idColumns      []string
		wantContains   []string
		wantNotContain []string
	}{
		{
			name:         "empty columns uses model struct tags",
			columns:      "",
			model:        testModel{},
			prefix:       "",
			idColumns:    []string{"id"},
			wantContains: []string{"id", "name", "ledger_created_at"},
		},
		{
			name:         "empty columns with prefix",
			columns:      "",
			model:        testModel{},
			prefix:       "t",
			idColumns:    []string{"id"},
			wantContains: []string{"t.id", "t.name", "t.ledger_created_at"},
		},
		{
			name:         "comma-separated columns are split correctly",
			columns:      "id, name, ledger_created_at",
			model:        testModel{},
			prefix:       "",
			idColumns:    []string{"id"},
			wantContains: []string{"id", "name", "ledger_created_at"},
		},
		{
			name:         "comma-separated columns with prefix - each column gets prefixed",
			columns:      "id, name, ledger_created_at",
			model:        testModel{},
			prefix:       "o",
			idColumns:    []string{"id"},
			wantContains: []string{"o.id", "o.name", "o.ledger_created_at"},
		},
		{
			name:           "columns without spaces",
			columns:        "id,name,ledger_created_at",
			model:          testModel{},
			prefix:         "o",
			idColumns:      []string{"id"},
			wantContains:   []string{"o.id", "o.name", "o.ledger_created_at"},
			wantNotContain: []string{"id", "name", "ledger_created_at"},
		},
		{
			name:         "single column",
			columns:      "name",
			model:        testModel{},
			prefix:       "t",
			idColumns:    []string{"id"},
			wantContains: []string{"t.name", "t.id"},
		},
		{
			name:         "id column added if not in columns",
			columns:      "name, ledger_created_at",
			model:        testModel{},
			prefix:       "o",
			idColumns:    []string{"id"},
			wantContains: []string{"o.name", "o.ledger_created_at", "o.id"},
		},
		{
			name:         "multiple id columns",
			columns:      "name",
			model:        testModel{},
			prefix:       "sc",
			idColumns:    []string{"id", "name"},
			wantContains: []string{"sc.name", "sc.id"},
		},
		{
			name:         "columns with extra whitespace",
			columns:      "  id  ,  name  ,  ledger_created_at  ",
			model:        testModel{},
			prefix:       "o",
			idColumns:    []string{"id"},
			wantContains: []string{"o.id", "o.name", "o.ledger_created_at"},
		},
		{
			name:           "columns not in model are filtered out",
			columns:        "id, name, tx_hash, unknown_column",
			model:          testModel{},
			prefix:         "o",
			idColumns:      []string{"id"},
			wantContains:   []string{"o.id", "o.name"},
			wantNotContain: []string{"o.tx_hash", "o.unknown_column", "tx_hash", "unknown_column"},
		},
		{
			name:         "empty string elements are ignored",
			columns:      "id,,name,",
			model:        testModel{},
			prefix:       "t",
			idColumns:    []string{"id"},
			wantContains: []string{"t.id", "t.name"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := prepareColumnsWithID(tc.columns, tc.model, tc.prefix, tc.idColumns...)

			// Split result to check individual columns
			resultColumns := strings.Split(result, ", ")
			sort.Strings(resultColumns)

			for _, want := range tc.wantContains {
				assert.Contains(t, resultColumns, want, "result should contain %s", want)
			}

			for _, notWant := range tc.wantNotContain {
				assert.NotContains(t, resultColumns, notWant, "result should not contain %s", notWant)
			}
		})
	}
}

func TestAddPrefixToColumns(t *testing.T) {
	tests := []struct {
		name     string
		columns  []string
		prefix   string
		expected []string
	}{
		{
			name:     "adds prefix to all columns",
			columns:  []string{"id", "name", "ledger_created_at"},
			prefix:   "o",
			expected: []string{"o.id", "o.name", "o.ledger_created_at"},
		},
		{
			name:     "single column",
			columns:  []string{"id"},
			prefix:   "t",
			expected: []string{"t.id"},
		},
		{
			name:     "empty columns",
			columns:  []string{},
			prefix:   "x",
			expected: []string{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			input := createSet(tc.columns)
			result := addPrefixToColumns(input, tc.prefix)

			assert.Equal(t, len(tc.expected), result.Cardinality())
			for _, exp := range tc.expected {
				assert.True(t, result.Contains(exp), "result should contain %s", exp)
			}
		})
	}
}

func TestAddIDColumn(t *testing.T) {
	tests := []struct {
		name       string
		columns    []string
		prefix     string
		idColumn   string
		shouldAdd  bool
		expectedID string
	}{
		{
			name:       "adds id column without prefix",
			columns:    []string{"name"},
			prefix:     "",
			idColumn:   "id",
			shouldAdd:  true,
			expectedID: "id",
		},
		{
			name:       "adds id column with prefix",
			columns:    []string{"o.name"},
			prefix:     "o",
			idColumn:   "id",
			shouldAdd:  true,
			expectedID: "o.id",
		},
		{
			name:       "does not add duplicate id column",
			columns:    []string{"id", "name"},
			prefix:     "",
			idColumn:   "id",
			shouldAdd:  false,
			expectedID: "id",
		},
		{
			name:       "does not add duplicate prefixed id column",
			columns:    []string{"o.id", "o.name"},
			prefix:     "o",
			idColumn:   "id",
			shouldAdd:  false,
			expectedID: "o.id",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			input := createSet(tc.columns)
			initialSize := input.Cardinality()

			result := addIDColumn(input, tc.prefix, tc.idColumn)

			assert.True(t, result.Contains(tc.expectedID))
			if tc.shouldAdd {
				assert.Equal(t, initialSize+1, result.Cardinality())
			} else {
				assert.Equal(t, initialSize, result.Cardinality())
			}
		})
	}
}

func TestBuildGetByAccountAddressQuery(t *testing.T) {
	limit := int32(10)
	cursor := int64(100)
	// Use a valid Stellar address for testing
	testAddress := "GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF"

	tests := []struct {
		name           string
		config         paginatedQueryConfig
		wantContains   []string
		wantArgsLen    int
		wantNotContain []string
	}{
		{
			name: "basic query without cursor or limit",
			config: paginatedQueryConfig{
				TableName:      "operations",
				CursorColumn:   "id",
				JoinTable:      "operations_accounts",
				JoinCondition:  "operations_accounts.operation_id = operations.id",
				Columns:        "operations.id, operations.operation_type",
				AccountAddress: testAddress,
				OrderBy:        ASC,
			},
			wantContains: []string{
				"SELECT operations.id, operations.operation_type",
				"FROM operations",
				"INNER JOIN operations_accounts",
				"WHERE operations_accounts.account_id = $1",
				"ORDER BY operations.id ASC",
			},
			wantArgsLen: 1,
		},
		{
			name: "query with limit",
			config: paginatedQueryConfig{
				TableName:      "operations",
				CursorColumn:   "id",
				JoinTable:      "operations_accounts",
				JoinCondition:  "operations_accounts.operation_id = operations.id",
				Columns:        "operations.id",
				AccountAddress: testAddress,
				Limit:          &limit,
				OrderBy:        ASC,
			},
			wantContains: []string{
				"LIMIT $2",
			},
			wantArgsLen: 2,
		},
		{
			name: "query with cursor ASC",
			config: paginatedQueryConfig{
				TableName:      "operations",
				CursorColumn:   "id",
				JoinTable:      "operations_accounts",
				JoinCondition:  "operations_accounts.operation_id = operations.id",
				Columns:        "operations.id",
				AccountAddress: testAddress,
				Cursor:         &cursor,
				OrderBy:        ASC,
			},
			wantContains: []string{
				"operations.id > $2",
			},
			wantArgsLen: 2,
		},
		{
			name: "query with cursor DESC",
			config: paginatedQueryConfig{
				TableName:      "operations",
				CursorColumn:   "id",
				JoinTable:      "operations_accounts",
				JoinCondition:  "operations_accounts.operation_id = operations.id",
				Columns:        "operations.id",
				AccountAddress: testAddress,
				Cursor:         &cursor,
				OrderBy:        DESC,
			},
			wantContains: []string{
				"operations.id < $2",
				"ORDER BY operations.id DESC",
				"ORDER BY operations.cursor ASC",
			},
			wantArgsLen: 2,
		},
		{
			name: "DESC order wraps query for reverse ordering",
			config: paginatedQueryConfig{
				TableName:      "transactions",
				CursorColumn:   "to_id",
				JoinTable:      "transactions_accounts",
				JoinCondition:  "transactions_accounts.tx_id = transactions.to_id",
				Columns:        "transactions.to_id",
				AccountAddress: testAddress,
				OrderBy:        DESC,
			},
			wantContains: []string{
				"SELECT * FROM (",
				"ORDER BY transactions.cursor ASC",
			},
			wantArgsLen: 1,
		},
	}

	// Expected first arg is a StellarAddress (driver.Valuer will convert to BYTEA)
	expectedFirstArg := types.StellarAddress(testAddress)

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			query, args := buildGetByAccountAddressQuery(tc.config)

			for _, want := range tc.wantContains {
				assert.Contains(t, query, want, "query should contain: %s", want)
			}

			for _, notWant := range tc.wantNotContain {
				assert.NotContains(t, query, notWant, "query should not contain: %s", notWant)
			}

			require.Len(t, args, tc.wantArgsLen)
			// First argument should always be the account address as BYTEA
			assert.Equal(t, expectedFirstArg, args[0], "first argument should be BYTEA representation of account address")
		})
	}
}

// Helper function to create a set from a slice
func createSet(items []string) set.Set[string] {
	s := set.NewSet[string]()
	for _, item := range items {
		s.Add(item)
	}
	return s
}
