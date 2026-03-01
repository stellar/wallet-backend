package utils

import (
	"context"
	"errors"
	"testing"

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
			name: "postgres unique_violation",
			err:  &pgconn.PgError{Code: "23505"},
			want: "unique_violation",
		},
		{
			name: "postgres foreign_key_violation",
			err:  &pgconn.PgError{Code: "23503"},
			want: "foreign_key_violation",
		},
		{
			name: "postgres not_null_violation",
			err:  &pgconn.PgError{Code: "23502"},
			want: "not_null_violation",
		},
		{
			name: "postgres check_violation",
			err:  &pgconn.PgError{Code: "23514"},
			want: "check_violation",
		},
		{
			name: "postgres serialization_failure",
			err:  &pgconn.PgError{Code: "40001"},
			want: "serialization_failure",
		},
		{
			name: "postgres deadlock_detected",
			err:  &pgconn.PgError{Code: "40P01"},
			want: "deadlock",
		},
		{
			name: "postgres query_canceled",
			err:  &pgconn.PgError{Code: "57014"},
			want: "query_canceled",
		},
		{
			name: "postgres admin_shutdown",
			err:  &pgconn.PgError{Code: "57P01"},
			want: "admin_shutdown",
		},
		{
			name: "postgres connection error 08000",
			err:  &pgconn.PgError{Code: "08000"},
			want: "connection_error",
		},
		{
			name: "postgres connection error 08003",
			err:  &pgconn.PgError{Code: "08003"},
			want: "connection_error",
		},
		{
			name: "postgres connection error 08006",
			err:  &pgconn.PgError{Code: "08006"},
			want: "connection_error",
		},
		{
			name: "postgres other error",
			err:  &pgconn.PgError{Code: "99999"},
			want: "postgres_error",
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
