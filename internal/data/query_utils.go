package data

import (
	"fmt"
	"reflect"
	"strings"

	set "github.com/deckarep/golang-set/v2"

	"github.com/stellar/wallet-backend/internal/indexer/types"
)

type SortOrder string

const (
	ASC  SortOrder = "ASC"
	DESC SortOrder = "DESC"
)

// PaginatedQueryConfig contains configuration for building paginated queries
type paginatedQueryConfig struct {
	// Base table configuration
	TableName    string // e.g., "operations" or "transactions"
	CursorColumn string // e.g., "id" or "to_id"

	// Join configuration
	JoinTable     string // e.g., "operations_accounts" or "transactions_accounts"
	JoinCondition string // e.g., "operations_accounts.operation_id = operations.id"

	// Query parameters
	Columns        string
	AccountAddress string
	Limit          *int32
	Cursor         *int64
	OrderBy        SortOrder
}

// BuildPaginatedQuery constructs a paginated SQL query with cursor-based pagination
func buildGetByAccountAddressQuery(config paginatedQueryConfig) (string, []any) {
	var queryBuilder strings.Builder
	var args []any
	argIndex := 1

	// Base query with join
	queryBuilder.WriteString(fmt.Sprintf(`
		SELECT %s, %s.%s as cursor
		FROM %s
		INNER JOIN %s 
		ON %s 
		WHERE %s.account_id = $%d`,
		config.Columns,
		config.TableName,
		config.CursorColumn,
		config.TableName,
		config.JoinTable,
		config.JoinCondition,
		config.JoinTable,
		argIndex))
	args = append(args, config.AccountAddress)
	argIndex++

	// Add cursor condition if provided
	if config.Cursor != nil {
		// When paginating in descending order, we are going from greater cursor id to smaller cursor id
		if config.OrderBy == DESC {
			queryBuilder.WriteString(fmt.Sprintf(` AND %s.%s < $%d`, config.TableName, config.CursorColumn, argIndex))
		} else {
			queryBuilder.WriteString(fmt.Sprintf(` AND %s.%s > $%d`, config.TableName, config.CursorColumn, argIndex))
		}
		args = append(args, *config.Cursor)
		argIndex++
	}

	// Add ordering
	if config.OrderBy == DESC {
		queryBuilder.WriteString(fmt.Sprintf(" ORDER BY %s.%s DESC", config.TableName, config.CursorColumn))
	} else {
		queryBuilder.WriteString(fmt.Sprintf(" ORDER BY %s.%s ASC", config.TableName, config.CursorColumn))
	}

	// Add limit if provided
	if config.Limit != nil {
		queryBuilder.WriteString(fmt.Sprintf(` LIMIT $%d`, argIndex))
		args = append(args, *config.Limit)
	}

	query := queryBuilder.String()

	// For backward pagination, wrap query to reverse the final order
	// This ensures we always display the oldest items first in the output
	if config.OrderBy == DESC {
		query = fmt.Sprintf(`SELECT * FROM (%s) AS %s ORDER BY %s.cursor ASC`,
			query, config.TableName, config.TableName)
	}

	return query, args
}

func getDBColumns(model any) set.Set[string] {
	modelType := reflect.TypeOf(model)
	dbColumns := set.NewSet[string]()
	for i := 0; i < modelType.NumField(); i++ {
		field := modelType.Field(i)
		dbTag := field.Tag.Get("db")

		if dbTag != "" && dbTag != "-" {
			dbColumns.Add(dbTag)
		}
	}
	return dbColumns
}

// PrepareColumnsWithID ensures that the specified ID column is always included in the column list.
// When columns is provided, it filters out any columns that don't exist in the model to prevent
// invalid column references when joining tables (e.g., tx_hash from OperationWithCursor which
// comes from a JOIN, not from the operations table itself).
func prepareColumnsWithID(columns string, model any, prefix string, idColumns ...string) string {
	modelColumns := getDBColumns(model)
	var dbColumns set.Set[string]
	if columns == "" {
		dbColumns = modelColumns
	} else {
		dbColumns = set.NewSet[string]()
		for _, col := range strings.Split(columns, ",") {
			trimmed := strings.TrimSpace(col)
			// Only include columns that exist in the model
			if trimmed != "" && modelColumns.Contains(trimmed) {
				dbColumns.Add(trimmed)
			}
		}
	}

	if prefix != "" {
		dbColumns = addPrefixToColumns(dbColumns, prefix)
	}
	// State changes has both to_id and state_change_order as id columns
	for _, idColumn := range idColumns {
		dbColumns = addIDColumn(dbColumns, prefix, idColumn)
	}
	return strings.Join(dbColumns.ToSlice(), ", ")
}

func addPrefixToColumns(columns set.Set[string], prefix string) set.Set[string] {
	result := set.NewSet[string]()
	for _, column := range columns.ToSlice() {
		result.Add(fmt.Sprintf("%s.%s", prefix, column))
	}
	return result
}

func addIDColumn(columns set.Set[string], prefix string, idColumn string) set.Set[string] {
	var columnToAdd string
	if prefix == "" {
		columnToAdd = idColumn
	} else {
		columnToAdd = fmt.Sprintf("%s.%s", prefix, idColumn)
	}
	if !columns.Contains(columnToAdd) {
		columns.Add(columnToAdd)
	}
	return columns
}

// bytesFromStellarAddress converts StellarAddress to []byte for pgx CopyFrom.
// Returns 33 bytes: [version_byte][32-byte_public_key].
func bytesFromStellarAddress(s types.StellarAddress) []byte {
	if s == "" {
		return nil
	}
	val, err := s.Value()
	if err != nil || val == nil {
		return nil
	}
	return val.([]byte)
}

// bytesFromNullableStellarAddress converts NullableStellarAddress to []byte for pgx CopyFrom.
// Returns 33 bytes or nil if not valid.
func bytesFromNullableStellarAddress(n types.NullableStellarAddress) []byte {
	if !n.Valid {
		return nil
	}
	return bytesFromStellarAddress(types.StellarAddress(n.String))
}

// bytesFromAddressString converts a string address (G.../C...) to []byte for pgx CopyFrom.
// Returns 33 bytes: [version_byte][32-byte_public_key].
func bytesFromAddressString(address string) []byte {
	return bytesFromStellarAddress(types.StellarAddress(address))
}
