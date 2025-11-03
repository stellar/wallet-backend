package utils

import (
	"context"
	"database/sql"
	"errors"

	"github.com/lib/pq"
)

// GetDBErrorType categorizes database errors into types for metrics
func GetDBErrorType(err error) string {
	if err == nil {
		return ""
	}

	// Check for standard SQL errors
	if errors.Is(err, sql.ErrNoRows) {
		return "no_rows"
	}
	if errors.Is(err, sql.ErrConnDone) {
		return "connection_closed"
	}
	if errors.Is(err, sql.ErrTxDone) {
		return "transaction_done"
	}

	// Check for PostgreSQL errors
	var pqErr *pq.Error
	if errors.As(err, &pqErr) {
		switch pqErr.Code {
		case "23505": // unique_violation
			return "unique_violation"
		case "23503": // foreign_key_violation
			return "foreign_key_violation"
		case "23502": // not_null_violation
			return "not_null_violation"
		case "23514": // check_violation
			return "check_violation"
		case "40001": // serialization_failure
			return "serialization_failure"
		case "40P01": // deadlock_detected
			return "deadlock"
		case "57014": // query_canceled
			return "query_canceled"
		case "57P01": // admin_shutdown
			return "admin_shutdown"
		case "08000", "08003", "08006": // connection errors
			return "connection_error"
		default:
			return "postgres_error"
		}
	}

	// Check for context errors
	if errors.Is(err, context.Canceled) {
		return "context_canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "context_deadline_exceeded"
	}

	return "unknown"
}
