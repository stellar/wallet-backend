package db

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type querierKey struct{}

// RunInReadSnapshot runs fn with a context whose reads (through QuerierFrom)
// share one read-only REPEATABLE READ transaction, so every query in fn sees
// the same committed state. fn must query from one goroutine at a time: a
// pgx.Tx is not safe for concurrent use.
func RunInReadSnapshot(ctx context.Context, pool *pgxpool.Pool, fn func(ctx context.Context) error) error {
	return pgx.BeginTxFunc(ctx, pool, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}, func(tx pgx.Tx) error { //nolint:wrapcheck // fn's errors pass through unchanged; callers wrap
		return fn(context.WithValue(ctx, querierKey{}, Querier(tx)))
	})
}

// QuerierFrom returns the transaction RunInReadSnapshot put in ctx, or
// fallback when ctx carries none.
func QuerierFrom(ctx context.Context, fallback Querier) Querier {
	if q, ok := ctx.Value(querierKey{}).(Querier); ok {
		return q
	}
	return fallback
}
