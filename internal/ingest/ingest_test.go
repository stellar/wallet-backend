package ingest

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/db"
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

func TestValidateIngestPoolConfig(t *testing.T) {
	testCases := []struct {
		name     string
		maxConns int32
		wantErr  bool
	}{
		{
			name:     "below_floor_is_rejected",
			maxConns: db.MinIngestMaxConns - 1,
			wantErr:  true,
		},
		{
			name:     "at_floor_is_accepted",
			maxConns: db.MinIngestMaxConns,
			wantErr:  false,
		},
		{
			name:     "default_is_accepted",
			maxConns: db.DefaultMaxConns,
			wantErr:  false,
		},
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
