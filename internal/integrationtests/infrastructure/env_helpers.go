package infrastructure

import (
	"context"
	"testing"
	"time"

	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stretchr/testify/require"
)

// WaitForLedgers waits for a number of ledgers to close (approximated by sleep)
func (e *TestEnvironment) WaitForLedgers(ctx context.Context, ledgers int) {
	// Assume 1 second per ledger for standalone + buffer
	duration := time.Duration(ledgers) * 2 * time.Second
	log.Ctx(ctx).Infof("⏳ Waiting for %d ledgers (%s)...", ledgers, duration)
	time.Sleep(duration)
}

// RestartIngestContainer restarts the ingest container with extra environment variables
func (e *TestEnvironment) RestartIngestContainer(ctx context.Context, extraEnv map[string]string) error {
	return e.Containers.RestartIngestContainer(ctx, extraEnv)
}

// RestartAPI restarts the API container with extra environment variables and re-points
// e.WBClient at it. The replacement container is published on a fresh host port, so the client
// built at NewTestEnvironment time would otherwise keep dialing the terminated container's port.
func (e *TestEnvironment) RestartAPI(ctx context.Context, t *testing.T, extraEnv map[string]string) {
	t.Helper()
	require.NoError(t, e.Containers.RestartAPIContainer(ctx, extraEnv), "restarting API container")

	client, err := createWalletBackendClient(ctx, e.Containers)
	require.NoError(t, err, "re-creating wallet-backend client after API restart")
	e.WBClient = client

	log.Ctx(ctx).Info("🔄 Restarted wallet-backend API container and re-pointed the client at it")
}
