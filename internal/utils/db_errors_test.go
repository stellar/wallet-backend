package utils

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
)

func TestGetDBErrorType(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "nil error",
			err:  nil,
			want: "",
		},
		{
			name: "sql.ErrNoRows",
			err:  sql.ErrNoRows,
			want: "no_rows",
		},
		{
			name: "sql.ErrConnDone",
			err:  sql.ErrConnDone,
			want: "connection_closed",
		},
		{
			name: "sql.ErrTxDone",
			err:  sql.ErrTxDone,
			want: "transaction_done",
		},
		{
			name: "context.Canceled",
			err:  context.Canceled,
			want: "context_canceled",
		},
		{
			name: "context.DeadlineExceeded",
			err:  context.DeadlineExceeded,
			want: "context_deadline_exceeded",
		},
		{
			name: "pgconn unique_violation",
			err:  &pgconn.PgError{Code: "23505"},
			want: "unique_violation",
		},
		{
			name: "pgconn foreign_key_violation",
			err:  &pgconn.PgError{Code: "23503"},
			want: "foreign_key_violation",
		},
		{
			name: "pgconn not_null_violation",
			err:  &pgconn.PgError{Code: "23502"},
			want: "not_null_violation",
		},
		{
			name: "pgconn check_violation",
			err:  &pgconn.PgError{Code: "23514"},
			want: "check_violation",
		},
		{
			name: "pgconn serialization_failure",
			err:  &pgconn.PgError{Code: "40001"},
			want: "serialization_failure",
		},
		{
			name: "pgconn deadlock_detected",
			err:  &pgconn.PgError{Code: "40P01"},
			want: "deadlock",
		},
		{
			name: "pgconn query_canceled",
			err:  &pgconn.PgError{Code: "57014"},
			want: "query_canceled",
		},
		{
			name: "pgconn admin_shutdown",
			err:  &pgconn.PgError{Code: "57P01"},
			want: "admin_shutdown",
		},
		{
			name: "pgconn connection error 08000",
			err:  &pgconn.PgError{Code: "08000"},
			want: "connection_error",
		},
		{
			name: "pgconn connection error 08003",
			err:  &pgconn.PgError{Code: "08003"},
			want: "connection_error",
		},
		{
			name: "pgconn connection error 08006",
			err:  &pgconn.PgError{Code: "08006"},
			want: "connection_error",
		},
		{
			name: "pgconn other error",
			err:  &pgconn.PgError{Code: "99999"},
			want: "postgres_error",
		},
		// pgx.ErrNoRows should behave the same as sql.ErrNoRows
		{
			name: "pgx.ErrNoRows",
			err:  pgx.ErrNoRows,
			want: "no_rows",
		},
		{
			name: "wrapped sql.ErrNoRows",
			err:  errors.Join(sql.ErrNoRows, errors.New("some context")),
			want: "no_rows",
		},
		{
			name: "wrapped context.Canceled",
			err:  errors.Join(context.Canceled, errors.New("some context")),
			want: "context_canceled",
		},
		{
			name: "unknown error",
			err:  errors.New("some random error"),
			want: "unknown",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := GetDBErrorType(tc.err)
			assert.Equal(t, tc.want, got)
		})
	}
}
