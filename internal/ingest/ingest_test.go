package ingest

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/db"
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

func TestValidateIngestPoolConfig(t *testing.T) {
	testCases := []struct {
		name     string
		maxConns int32
		wantErr  bool
	}{
		{name: "below_floor_is_rejected", maxConns: db.MinIngestMaxConns - 1, wantErr: true},
		{name: "at_floor_is_accepted", maxConns: db.MinIngestMaxConns},
		{name: "default_is_accepted", maxConns: db.DefaultMaxConns},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateIngestPoolConfig(db.PoolConfig{MaxConns: tc.maxConns})
			if tc.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "db-max-conns")
				return
			}
			require.NoError(t, err)
		})
	}
}

// An unset flag must not trip the floor: BuildPoolConfig supplies the default.
func TestValidateIngestPoolConfig_UnsetFlagUsesDefault(t *testing.T) {
	require.NoError(t, validateIngestPoolConfig(Configs{}.BuildPoolConfig()))
}
