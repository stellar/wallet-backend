package infrastructure

import (
	"context"
	"fmt"
	"sync/atomic"
)

// restartCounter is used to generate unique container names for restarted ingest containers
var restartCounter atomic.Int64

// RestartIngestContainer stops the current ingest container and starts a new one with extra environment variables
func (s *SharedContainers) RestartIngestContainer(ctx context.Context, extraEnv map[string]string) error {
	// Terminate existing container
	if s.WalletBackendContainer.Ingest != nil {
		if err := s.WalletBackendContainer.Ingest.Terminate(ctx); err != nil {
			return fmt.Errorf("terminating ingest container: %w", err)
		}
	}

	// Rebuild or verify wallet-backend Docker image (reuses existing check)
	walletBackendImage, err := ensureWalletBackendImage(ctx, walletBackendContainerTag)
	if err != nil {
		return fmt.Errorf("ensuring wallet backend image: %w", err)
	}

	// Use a unique container name to avoid reusing the terminated container
	counter := restartCounter.Add(1)
	containerName := fmt.Sprintf("%s-restart-%d", walletBackendIngestContainerName, counter)

	// Start new ingest container
	s.WalletBackendContainer.Ingest, err = createWalletBackendIngestContainer(ctx, containerName,
		walletBackendImage, s.TestNetwork, s.clientAuthKeyPair, s.distributionAccountKeyPair, extraEnv)
	if err != nil {
		return fmt.Errorf("creating wallet backend ingest container: %w", err)
	}

	return nil
}
