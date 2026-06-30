package infrastructure

import (
	"context"
	"fmt"
	"io"
	"maps"
	"time"

	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// commandContainerExitTimeout bounds how long we wait for a one-shot
// wallet-backend command container to finish.
const commandContainerExitTimeout = 2 * time.Minute

// RunWalletBackendCommand runs a one-shot wallet-backend subcommand (e.g.
// "protocol-setup --protocol-id SEP41") in its own named container against the
// shared network, waits for it to exit, and returns the exit code and combined
// logs. It mirrors the ingest container's image and environment so the command
// sees the same DB and RPC as live ingestion.
//
// The container is intentionally NOT terminated (like the backfill container) so
// it remains visible to `docker logs <name>` for post-run inspection.
func (s *SharedContainers) RunWalletBackendCommand(ctx context.Context, name, command string, extraEnv map[string]string) (int, string, error) {
	env := map[string]string{
		"RPC_URL":                 "http://stellar-rpc:8000",
		"DATABASE_URL":            "postgres://postgres@wallet-backend-db:5432/wallet-backend?sslmode=disable",
		"LOG_LEVEL":               "DEBUG",
		"NETWORK":                 "standalone",
		"NETWORK_PASSPHRASE":      networkPassphrase,
		"CLIENT_AUTH_PUBLIC_KEYS": s.clientAuthKeyPair.Address(),
		"STELLAR_ENVIRONMENT":     "integration-test",
	}
	maps.Copy(env, extraEnv)

	containerRequest := testcontainers.ContainerRequest{
		Name:  name,
		Image: s.walletBackendImage,
		Labels: map[string]string{
			"org.testcontainers.session-id": "wallet-backend-integration-tests",
		},
		Entrypoint: []string{"sh", "-c"},
		Cmd:        []string{"./wallet-backend " + command},
		Env:        env,
		Networks:   []string{s.TestNetwork.Name},
		WaitingFor: wait.ForExit().WithExitTimeout(commandContainerExitTimeout),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: containerRequest,
		Started:          true,
	})
	if err != nil {
		return -1, "", fmt.Errorf("running wallet-backend command container %q: %w", command, err)
	}

	logs := readContainerLogs(ctx, container)

	state, err := container.State(ctx)
	if err != nil {
		return -1, logs, fmt.Errorf("getting state of command container %q: %w", command, err)
	}

	return state.ExitCode, logs, nil
}

// readContainerLogs reads all logs from a container, returning an empty string on error.
func readContainerLogs(ctx context.Context, container testcontainers.Container) string {
	logsReader, err := container.Logs(ctx)
	if err != nil {
		log.Ctx(ctx).Warnf("reading command container logs: %v", err)
		return ""
	}
	defer logsReader.Close() //nolint:errcheck

	logBytes, err := io.ReadAll(logsReader)
	if err != nil {
		log.Ctx(ctx).Warnf("reading command container logs: %v", err)
		return ""
	}
	return string(logBytes)
}
