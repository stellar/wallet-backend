package ingest

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/stellar/wallet-backend/internal/services"
)

func TestIsCleanShutdown(t *testing.T) {
	genuineErr := errors.New("genuine failure")
	wrappedCanceled := fmt.Errorf("fetching ledger 5: %w", context.Canceled)

	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	testCases := []struct {
		name          string
		ctx           context.Context
		err           error
		ingestionMode string
		want          bool
	}{
		{
			name:          "live_cancelled_ctx_with_genuine_error",
			ctx:           cancelledCtx,
			err:           genuineErr,
			ingestionMode: services.IngestionModeLive,
			want:          true,
		},
		{
			name:          "live_ctx_with_wrapped_context_canceled",
			ctx:           context.Background(),
			err:           wrappedCanceled,
			ingestionMode: services.IngestionModeLive,
			want:          true,
		},
		{
			name:          "live_ctx_with_genuine_error",
			ctx:           context.Background(),
			err:           genuineErr,
			ingestionMode: services.IngestionModeLive,
			want:          false,
		},
		{
			name:          "live_cancelled_ctx_with_nil_error",
			ctx:           cancelledCtx,
			err:           nil,
			ingestionMode: services.IngestionModeLive,
			want:          true,
		},
		{
			name:          "backfill_ctx_with_wrapped_context_canceled",
			ctx:           cancelledCtx,
			err:           wrappedCanceled,
			ingestionMode: services.IngestionModeBackfill,
			want:          false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, isCleanShutdown(tc.ctx, tc.err, tc.ingestionMode))
		})
	}
}
