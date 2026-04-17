package utils

import (
	"context"
	"fmt"
	"time"
)

// RetryWithBackoff calls fn up to maxRetries times with exponential backoff
// capped at maxBackoff. It respects context cancellation between attempts.
// onRetry, if non-nil, is called before each backoff wait with the attempt
// number (0-indexed), the error, and the backoff duration.
func RetryWithBackoff[T any](
	ctx context.Context,
	maxRetries int,
	maxBackoff time.Duration,
	fn func(ctx context.Context) (T, error),
	onRetry func(attempt int, err error, backoff time.Duration),
) (T, error) {
	var zero T
	if maxRetries <= 0 {
		return zero, fmt.Errorf("RetryWithBackoff: maxRetries must be > 0, got %d", maxRetries)
	}
	if maxBackoff <= 0 {
		return zero, fmt.Errorf("RetryWithBackoff: maxBackoff must be > 0, got %s", maxBackoff)
	}
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		select {
		case <-ctx.Done():
			return zero, fmt.Errorf("context cancelled: %w", ctx.Err())
		default:
		}

		result, err := fn(ctx)
		if err == nil {
			return result, nil
		}
		lastErr = err

		backoff := time.Duration(1<<attempt) * time.Second
		if backoff > maxBackoff {
			backoff = maxBackoff
		}

		if onRetry != nil {
			onRetry(attempt, err, backoff)
		}

		select {
		case <-ctx.Done():
			return zero, fmt.Errorf("context cancelled during backoff: %w", ctx.Err())
		case <-time.After(backoff):
		}
	}
	return zero, fmt.Errorf("failed after %d attempts: %w", maxRetries, lastErr)
}
