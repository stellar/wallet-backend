// Package utils provides utility functions including retry logic with exponential backoff.
// This file contains tests for the retry utilities.
package utils

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsDeadlock(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
		{
			name:     "deadlock error",
			err:      &pq.Error{Code: "40P01"},
			expected: true,
		},
		{
			name:     "serialization failure",
			err:      &pq.Error{Code: "40001"},
			expected: false,
		},
		{
			name:     "unique violation",
			err:      &pq.Error{Code: "23505"},
			expected: false,
		},
		{
			name:     "wrapped deadlock error",
			err:      errors.Join(errors.New("outer"), &pq.Error{Code: "40P01"}),
			expected: true,
		},
		{
			name:     "non-pq error",
			err:      errors.New("some error"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsDeadlock(tt.err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsRetryableDBError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
		{
			name:     "deadlock error",
			err:      &pq.Error{Code: "40P01"},
			expected: true,
		},
		{
			name:     "serialization failure",
			err:      &pq.Error{Code: "40001"},
			expected: true,
		},
		{
			name:     "unique violation - not retryable",
			err:      &pq.Error{Code: "23505"},
			expected: false,
		},
		{
			name:     "foreign key violation - not retryable",
			err:      &pq.Error{Code: "23503"},
			expected: false,
		},
		{
			name:     "non-pq error",
			err:      errors.New("some error"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsRetryableDBError(tt.err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestRetryOnDeadlock_Success(t *testing.T) {
	ctx := context.Background()
	callCount := 0

	err := RetryOnDeadlock(ctx, func() error {
		callCount++
		return nil
	})

	require.NoError(t, err)
	assert.Equal(t, 1, callCount, "function should be called exactly once on success")
}

func TestRetryOnDeadlock_NonRetryableError(t *testing.T) {
	ctx := context.Background()
	callCount := 0
	expectedErr := errors.New("non-retryable error")

	err := RetryOnDeadlock(ctx, func() error {
		callCount++
		return expectedErr
	})

	require.Error(t, err)
	assert.Equal(t, expectedErr, err)
	assert.Equal(t, 1, callCount, "function should be called exactly once for non-retryable error")
}

func TestRetryOnDeadlock_RetryThenSuccess(t *testing.T) {
	ctx := context.Background()
	callCount := 0

	err := RetryOnDeadlock(ctx, func() error {
		callCount++
		if callCount < 3 {
			return &pq.Error{Code: "40P01"} // deadlock
		}
		return nil
	})

	require.NoError(t, err)
	assert.Equal(t, 3, callCount, "function should be called 3 times (2 failures + 1 success)")
}

func TestRetryOnDeadlock_MaxRetriesExceeded(t *testing.T) {
	ctx := context.Background()
	callCount := 0
	deadlockErr := &pq.Error{Code: "40P01"}

	// Use a shorter config for testing
	config := RetryConfig{
		MaxRetries: 2,
		BaseDelay:  1 * time.Millisecond,
		MaxDelay:   10 * time.Millisecond,
	}

	err := RetryWithConfig(ctx, config, IsRetryableDBError, func() error {
		callCount++
		return deadlockErr
	})

	require.Error(t, err)
	assert.True(t, IsDeadlock(err))
	// MaxRetries=2 means initial attempt + 2 retries = 3 total calls
	assert.Equal(t, 3, callCount, "function should be called MaxRetries+1 times")
}

func TestRetryOnDeadlock_ContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	callCount := 0

	// Cancel after first call
	err := RetryWithConfig(ctx, RetryConfig{
		MaxRetries: 5,
		BaseDelay:  100 * time.Millisecond,
		MaxDelay:   1 * time.Second,
	}, IsRetryableDBError, func() error {
		callCount++
		if callCount == 1 {
			cancel() // Cancel context after first attempt
		}
		return &pq.Error{Code: "40P01"}
	})

	require.Error(t, err)
	// The error wraps context.Canceled
	assert.ErrorIs(t, err, context.Canceled)
	assert.Contains(t, err.Error(), "context canceled during retry backoff")
}

func TestRetryOnDeadlock_ContextAlreadyCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	callCount := 0
	err := RetryOnDeadlock(ctx, func() error {
		callCount++
		return nil
	})

	require.Error(t, err)
	// The error wraps context.Canceled
	assert.ErrorIs(t, err, context.Canceled)
	assert.Contains(t, err.Error(), "context error before retry attempt")
	assert.Equal(t, 0, callCount, "function should not be called if context is already canceled")
}

func TestCalculateBackoffDelay(t *testing.T) {
	baseDelay := 100 * time.Millisecond
	maxDelay := 2 * time.Second

	// Test that delay increases exponentially
	delay0 := calculateBackoffDelay(0, baseDelay, maxDelay)
	delay1 := calculateBackoffDelay(1, baseDelay, maxDelay)
	delay2 := calculateBackoffDelay(2, baseDelay, maxDelay)

	// Due to jitter, we can only check ranges
	// Attempt 0: base = 100ms, jitter up to 50ms, so 100-150ms
	assert.GreaterOrEqual(t, delay0, baseDelay)
	assert.LessOrEqual(t, delay0, baseDelay+baseDelay/2)

	// Attempt 1: base = 200ms, jitter up to 100ms, so 200-300ms
	assert.GreaterOrEqual(t, delay1, 2*baseDelay)
	assert.LessOrEqual(t, delay1, 2*baseDelay+baseDelay)

	// Attempt 2: base = 400ms, jitter up to 200ms, so 400-600ms
	assert.GreaterOrEqual(t, delay2, 4*baseDelay)
	assert.LessOrEqual(t, delay2, 4*baseDelay+2*baseDelay)
}

func TestCalculateBackoffDelay_CappedAtMax(t *testing.T) {
	baseDelay := 100 * time.Millisecond
	maxDelay := 500 * time.Millisecond

	// At attempt 10, exponential would be 100ms * 2^10 = 102.4s
	// But it should be capped at maxDelay
	delay := calculateBackoffDelay(10, baseDelay, maxDelay)

	// Max delay is 500ms, jitter adds up to 250ms, so max is 750ms
	assert.LessOrEqual(t, delay, maxDelay+maxDelay/2)
}

func TestRetryWithConfig_SerializationFailure(t *testing.T) {
	ctx := context.Background()
	callCount := 0

	config := RetryConfig{
		MaxRetries: 3,
		BaseDelay:  1 * time.Millisecond,
		MaxDelay:   10 * time.Millisecond,
	}

	err := RetryWithConfig(ctx, config, IsRetryableDBError, func() error {
		callCount++
		if callCount < 2 {
			return &pq.Error{Code: "40001"} // serialization failure
		}
		return nil
	})

	require.NoError(t, err)
	assert.Equal(t, 2, callCount, "should retry on serialization failure")
}
