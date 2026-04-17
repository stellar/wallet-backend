package utils

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRetryWithBackoff_SucceedsFirstAttempt(t *testing.T) {
	result, err := RetryWithBackoff(context.Background(), 3, 10*time.Second,
		func(ctx context.Context) (string, error) {
			return "ok", nil
		}, nil)
	require.NoError(t, err)
	assert.Equal(t, "ok", result)
}

func TestRetryWithBackoff_SucceedsAfterRetries(t *testing.T) {
	attempts := 0
	result, err := RetryWithBackoff(context.Background(), 5, 1*time.Second,
		func(ctx context.Context) (int, error) {
			attempts++
			if attempts < 3 {
				return 0, errors.New("not yet")
			}
			return 42, nil
		}, nil)
	require.NoError(t, err)
	assert.Equal(t, 42, result)
	assert.Equal(t, 3, attempts)
}

func TestRetryWithBackoff_ExhaustsRetries(t *testing.T) {
	sentinel := errors.New("persistent failure")
	attempts := 0
	_, err := RetryWithBackoff(context.Background(), 3, 1*time.Second,
		func(ctx context.Context) (string, error) {
			attempts++
			return "", sentinel
		}, nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, sentinel)
	assert.Contains(t, err.Error(), "failed after 3 attempts")
	assert.Equal(t, 3, attempts)
}

func TestRetryWithBackoff_RespectsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := RetryWithBackoff(ctx, 5, 10*time.Second,
		func(ctx context.Context) (string, error) {
			return "", errors.New("should not reach")
		}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "context cancelled")
}

func TestRetryWithBackoff_CallsOnRetry(t *testing.T) {
	var retryAttempts []int
	sentinel := errors.New("fail")

	_, err := RetryWithBackoff(context.Background(), 3, 1*time.Second,
		func(ctx context.Context) (string, error) {
			return "", sentinel
		},
		func(attempt int, err error, backoff time.Duration) {
			retryAttempts = append(retryAttempts, attempt)
			assert.ErrorIs(t, err, sentinel)
			assert.Greater(t, backoff, time.Duration(0))
		})
	require.Error(t, err)
	assert.Equal(t, []int{0, 1}, retryAttempts)
}

func TestRetryWithBackoff_CapsBackoff(t *testing.T) {
	maxBackoff := 2 * time.Second
	var observedBackoffs []time.Duration

	_, err := RetryWithBackoff(context.Background(), 5, maxBackoff,
		func(ctx context.Context) (string, error) {
			return "", errors.New("fail")
		},
		func(attempt int, err error, backoff time.Duration) {
			observedBackoffs = append(observedBackoffs, backoff)
		})
	require.Error(t, err)
	assert.Equal(t, []time.Duration{time.Second, maxBackoff, maxBackoff, maxBackoff}, observedBackoffs)
}

func TestRetryWithBackoff_RejectsZeroMaxRetries(t *testing.T) {
	_, err := RetryWithBackoff(context.Background(), 0, 5*time.Second,
		func(ctx context.Context) (string, error) {
			return "", errors.New("should not be called")
		}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "maxRetries must be > 0")
}

func TestRetryWithBackoff_RejectsZeroMaxBackoff(t *testing.T) {
	_, err := RetryWithBackoff(context.Background(), 3, 0,
		func(ctx context.Context) (string, error) {
			return "", errors.New("should not be called")
		}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "maxBackoff must be > 0")
}
