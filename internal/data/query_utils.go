package data

import (
	"fmt"
	"strings"
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

// PrepareColumnsWithID ensures that the specified ID column is always included in the column list
func prepareColumnsWithID(columns string, model any, tableName string, idColumn string) string {
	if columns == "" {
		dbColumns := getDBColumns(model)
		if tableName == "" {
			return strings.Join(dbColumns, ", ")
		}
		result := make([]string, len(dbColumns))
		for i, dbColumn := range dbColumns {
			result[i] = fmt.Sprintf("%s.%s", tableName, dbColumn)
		}
		return strings.Join(result, ", ")
	}
	// Always return the ID column as it is the primary key and can be used
	// to build further queries
	if tableName == "" {
		return fmt.Sprintf("%s, %s", columns, idColumn)
	}
	return fmt.Sprintf("%s, %s.%s", columns, tableName, idColumn)
}
