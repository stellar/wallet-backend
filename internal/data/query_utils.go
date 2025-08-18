package data

import (
	"context"
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"

	"github.com/stellar/wallet-backend/internal/db"
)

type SortOrder string

const (
	ASC  SortOrder = "ASC"
	DESC SortOrder = "DESC"
)

func (o SortOrder) IsValid() bool {
	return o == ASC || o == DESC
}

// PrepareNamedQuery prepares the given query replacing the named parameters with Postgres' bindvars.
// It returns an SQL Injection-safe query and the arguments array to be used alongside it.
func PrepareNamedQuery(ctx context.Context, connectionPool db.ConnectionPool, namedQuery string, argsMap map[string]interface{}) (string, []interface{}, error) {
	query, args, err := sqlx.Named(namedQuery, argsMap)
	if err != nil {
		return "", nil, fmt.Errorf("replacing attributes with bindvars: %w", err)
	}
	query, args, err = sqlx.In(query, args...)
	if err != nil {
		return "", nil, fmt.Errorf("expanding slice arguments: %w", err)
	}
	query = connectionPool.Rebind(query)

	return query, args, nil
}

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
	IsDescending   bool
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
		if config.IsDescending {
			queryBuilder.WriteString(fmt.Sprintf(` AND %s.%s < $%d`, config.TableName, config.CursorColumn, argIndex))
		} else {
			queryBuilder.WriteString(fmt.Sprintf(` AND %s.%s > $%d`, config.TableName, config.CursorColumn, argIndex))
		}
		args = append(args, *config.Cursor)
		argIndex++
	}

	// Add ordering
	if config.IsDescending {
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
	// This ensures we always display the latest items first in the output
	if !config.IsDescending {
		query = fmt.Sprintf(`SELECT * FROM (%s) AS %s ORDER BY %s.cursor DESC`,
			query, config.TableName, config.TableName)
	}

	return query, args
}

// PrepareColumnsWithID ensures that the specified ID column is always included in the column list
func prepareColumnsWithID(columns string, tableName string, idColumn string) string {
	if columns == "" {
		return fmt.Sprintf("%s.*", tableName)
	}
	// Always return the ID column as it is the primary key and can be used
	// to build further queries
	return fmt.Sprintf("%s, %s.%s", columns, tableName, idColumn)
}
