package db

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// QueryOne executes a query that returns exactly one row and scans it into a struct of type T.
// Uses pgx.RowToAddrOfStructByNameLax which reads `db` struct tags and tolerates missing columns.
func QueryOne[T any](ctx context.Context, pool *pgxpool.Pool, query string, args ...any) (*T, error) {
	rows, err := pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("querying row: %w", err)
	}
	result, err := pgx.CollectExactlyOneRow(rows, pgx.RowToAddrOfStructByNameLax[T])
	if err != nil {
		return nil, fmt.Errorf("collecting row: %w", err)
	}
	return result, nil
}

// QueryAll executes a query that returns multiple rows and scans them into structs of type T.
// Uses pgx.RowToAddrOfStructByNameLax which reads `db` struct tags and tolerates missing columns.
func QueryAll[T any](ctx context.Context, pool *pgxpool.Pool, query string, args ...any) ([]*T, error) {
	rows, err := pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("querying rows: %w", err)
	}
	result, err := pgx.CollectRows(rows, pgx.RowToAddrOfStructByNameLax[T])
	if err != nil {
		return nil, fmt.Errorf("collecting rows: %w", err)
	}
	return result, nil
}
