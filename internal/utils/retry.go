// Package utils provides utility functions including retry logic with exponential backoff.
// This file contains retry utilities for handling transient database errors like deadlocks.
package utils

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/lib/pq"
	"github.com/stellar/go/support/log"
)

// RetryConfig holds configuration for retry operations.
type RetryConfig struct {
	MaxRetries int
	BaseDelay  time.Duration
	MaxDelay   time.Duration
}

// DefaultDeadlockRetryConfig provides sensible defaults for deadlock retry handling.
var DefaultDeadlockRetryConfig = RetryConfig{
	MaxRetries: 5,
	BaseDelay:  100 * time.Millisecond,
	MaxDelay:   2 * time.Second,
}

// IsDeadlock checks if the error is a PostgreSQL deadlock error (code 40P01).
func IsDeadlock(err error) bool {
	var pqErr *pq.Error
	if errors.As(err, &pqErr) {
		return pqErr.Code == "40P01"
	}
	return false
}

// IsRetryableDBError checks if the error is a transient database error that can be retried.
// This includes deadlocks and serialization failures.
func IsRetryableDBError(err error) bool {
	var pqErr *pq.Error
	if errors.As(err, &pqErr) {
		switch pqErr.Code {
		case "40P01": // deadlock_detected
			return true
		case "40001": // serialization_failure
			return true
		}
	}
	return false
}

// RetryOnDeadlock executes the given function and retries on deadlock errors
// using exponential backoff with jitter.
func RetryOnDeadlock(ctx context.Context, fn func() error) error {
	return RetryWithConfig(ctx, DefaultDeadlockRetryConfig, IsRetryableDBError, fn)
}

// RetryWithConfig executes the given function and retries based on the provided
// configuration and retryable error check function.
func RetryWithConfig(ctx context.Context, config RetryConfig, isRetryable func(error) bool, fn func() error) error {
	var lastErr error

	for attempt := 0; attempt <= config.MaxRetries; attempt++ {
		// Check context before attempting
		if ctx.Err() != nil {
			return fmt.Errorf("context error before retry attempt: %w", ctx.Err())
		}

		err := fn()
		if err == nil {
			return nil
		}

		lastErr = err

		// Check if error is retryable
		if !isRetryable(err) {
			return err
		}

		// Don't sleep after the last attempt
		if attempt == config.MaxRetries {
			break
		}

		// Calculate delay with exponential backoff and jitter
		delay := calculateBackoffDelay(attempt, config.BaseDelay, config.MaxDelay)

		log.Ctx(ctx).Warnf("Retryable error (attempt %d/%d), retrying in %v: %v",
			attempt+1, config.MaxRetries+1, delay, err)

		// Sleep with context cancellation support
		select {
		case <-ctx.Done():
			return fmt.Errorf("context canceled during retry backoff: %w", ctx.Err())
		case <-time.After(delay):
		}
	}

	return lastErr
}

// calculateBackoffDelay computes the delay for a given attempt using exponential backoff with jitter.
func calculateBackoffDelay(attempt int, baseDelay, maxDelay time.Duration) time.Duration {
	// Exponential backoff: baseDelay * 2^attempt
	delay := baseDelay << attempt

	// Cap at maxDelay
	if delay > maxDelay {
		delay = maxDelay
	}

	// Add jitter (0-50% of delay) to prevent thundering herd
	jitter := time.Duration(rand.Int63n(int64(delay / 2)))
	delay += jitter

	return delay
}
