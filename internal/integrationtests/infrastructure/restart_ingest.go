package infrastructure

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/stellar/go/support/log"

	"github.com/stellar/wallet-backend/internal/services"
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

// StopIngestContainer stops the ingest container without terminating it.
// This allows the network to advance while the container is stopped.
func (s *SharedContainers) StopIngestContainer(ctx context.Context) error {
	if s.WalletBackendContainer.Ingest == nil {
		return fmt.Errorf("ingest container is not running")
	}

	if err := s.WalletBackendContainer.Ingest.Stop(ctx, nil); err != nil {
		return fmt.Errorf("stopping ingest container: %w", err)
	}

	log.Ctx(ctx).Info("🛑 Stopped ingest container")
	return nil
}

// GetIngestContainerLogs returns the logs from the ingest container.
func (s *SharedContainers) GetIngestContainerLogs(ctx context.Context) (string, error) {
	if s.WalletBackendContainer.Ingest == nil {
		return "", fmt.Errorf("ingest container is not running")
	}

	logsReader, err := s.WalletBackendContainer.Ingest.Logs(ctx)
	if err != nil {
		return "", fmt.Errorf("getting ingest container logs: %w", err)
	}
	defer logsReader.Close() //nolint:errcheck

	logBytes, err := io.ReadAll(logsReader)
	if err != nil {
		return "", fmt.Errorf("reading ingest container logs: %w", err)
	}

	return string(logBytes), nil
}

// WaitForNetworkAdvance polls the RPC service until the network reaches the target ledger.
func (s *SharedContainers) WaitForNetworkAdvance(ctx context.Context, rpcService services.RPCService, targetLedger uint32, timeout time.Duration) error {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	timeoutChan := time.After(timeout)

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled: %w", ctx.Err())
		case <-timeoutChan:
			return fmt.Errorf("network did not reach ledger %d within %v", targetLedger, timeout)
		case <-ticker.C:
			health, err := rpcService.GetHealth()
			if err != nil {
				log.Ctx(ctx).Warnf("Error getting RPC health during network advance wait: %v", err)
				continue
			}

			currentLedger := health.LatestLedger
			log.Ctx(ctx).Infof("Network advance progress: current=%d, target=%d", currentLedger, targetLedger)

			if currentLedger >= targetLedger {
				log.Ctx(ctx).Infof("Network reached target ledger %d", targetLedger)
				return nil
			}
		}
	}
}

// WaitForLatestLedgerToReach polls the database until the latest_ingest_ledger cursor reaches the target.
func (s *SharedContainers) WaitForLatestLedgerToReach(ctx context.Context, targetLedger uint32, timeout time.Duration) error {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	timeoutChan := time.After(timeout)

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled: %w", ctx.Err())
		case <-timeoutChan:
			return fmt.Errorf("latest_ingest_ledger did not reach %d within %v", targetLedger, timeout)
		case <-ticker.C:
			latest, err := s.GetIngestCursor(ctx, "latest_ingest_ledger")
			if err != nil {
				log.Ctx(ctx).Warnf("Error getting latest cursor during catchup wait: %v", err)
				continue
			}

			log.Ctx(ctx).Infof("Catchup progress: latest=%d, target=%d", latest, targetLedger)

			if latest >= targetLedger {
				log.Ctx(ctx).Infof("Catchup completed: latest_ingest_ledger=%d reached target=%d", latest, targetLedger)
				return nil
			}
		}
	}
}
