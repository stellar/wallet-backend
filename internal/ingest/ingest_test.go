package ingest

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsShutdownRequested(t *testing.T) {
	genuineErr := errors.New("genuine failure")
	wrappedCanceled := fmt.Errorf("fetching ledger 5: %w", context.Canceled)

	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	testCases := []struct {
		name string
		ctx  context.Context
		err  error
		want bool
	}{
		{
			name: "cancelled_ctx_with_genuine_error",
			ctx:  cancelledCtx,
			err:  genuineErr,
			want: true,
		},
		{
			name: "live_ctx_with_wrapped_context_canceled",
			ctx:  context.Background(),
			err:  wrappedCanceled,
			want: true,
		},
		{
			name: "live_ctx_with_genuine_error",
			ctx:  context.Background(),
			err:  genuineErr,
			want: false,
		},
		{
			name: "cancelled_ctx_with_nil_error",
			ctx:  cancelledCtx,
			err:  nil,
			want: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, isShutdownRequested(tc.ctx, tc.err))
		})
	}
}
