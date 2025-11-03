package utils

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/lib/pq"
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
			name: "postgres unique_violation",
			err:  &pq.Error{Code: "23505"},
			want: "unique_violation",
		},
		{
			name: "postgres foreign_key_violation",
			err:  &pq.Error{Code: "23503"},
			want: "foreign_key_violation",
		},
		{
			name: "postgres not_null_violation",
			err:  &pq.Error{Code: "23502"},
			want: "not_null_violation",
		},
		{
			name: "postgres check_violation",
			err:  &pq.Error{Code: "23514"},
			want: "check_violation",
		},
		{
			name: "postgres serialization_failure",
			err:  &pq.Error{Code: "40001"},
			want: "serialization_failure",
		},
		{
			name: "postgres deadlock_detected",
			err:  &pq.Error{Code: "40P01"},
			want: "deadlock",
		},
		{
			name: "postgres query_canceled",
			err:  &pq.Error{Code: "57014"},
			want: "query_canceled",
		},
		{
			name: "postgres admin_shutdown",
			err:  &pq.Error{Code: "57P01"},
			want: "admin_shutdown",
		},
		{
			name: "postgres connection error 08000",
			err:  &pq.Error{Code: "08000"},
			want: "connection_error",
		},
		{
			name: "postgres connection error 08003",
			err:  &pq.Error{Code: "08003"},
			want: "connection_error",
		},
		{
			name: "postgres connection error 08006",
			err:  &pq.Error{Code: "08006"},
			want: "connection_error",
		},
		{
			name: "postgres other error",
			err:  &pq.Error{Code: "99999"},
			want: "postgres_error",
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
