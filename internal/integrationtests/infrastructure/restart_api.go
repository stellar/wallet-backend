package infrastructure

import (
	"context"
	"fmt"
	"sync/atomic"
)

// apiRestartCounter generates unique container names for restarted API containers.
var apiRestartCounter atomic.Int64

// RestartAPIContainer stops the current API container and starts a new one with extra environment
// variables, mirroring RestartIngestContainer. The replacement container gets a fresh host port
// mapping, so callers holding a wallet-backend client must re-resolve its URL — see
// TestEnvironment.RestartAPI, which does that.
func (s *SharedContainers) RestartAPIContainer(ctx context.Context, extraEnv map[string]string) error {
	// Terminate existing container
	if s.WalletBackendContainer.API != nil {
		if err := s.WalletBackendContainer.API.Terminate(ctx); err != nil {
			return fmt.Errorf("terminating API container: %w", err)
		}
	}

	// Rebuild or verify wallet-backend Docker image (reuses existing check)
	walletBackendImage, err := ensureWalletBackendImage(ctx, walletBackendContainerTag)
	if err != nil {
		return fmt.Errorf("ensuring wallet backend image: %w", err)
	}

	// Use a unique container name to avoid reusing the terminated container
	counter := apiRestartCounter.Add(1)
	containerName := fmt.Sprintf("%s-restart-%d", walletBackendAPIContainerName, counter)

	// Start new API container
	s.WalletBackendContainer.API, err = createWalletBackendAPIContainer(ctx, containerName,
		walletBackendImage, s.TestNetwork, s.clientAuthKeyPair, extraEnv)
	if err != nil {
		return fmt.Errorf("creating wallet backend API container: %w", err)
	}

	return nil
}
