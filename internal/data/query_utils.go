package data

import (
	"context"
	"fmt"

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
