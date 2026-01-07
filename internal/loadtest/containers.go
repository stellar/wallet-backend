// Package loadtest provides synthetic ledger generation for load testing ingestion.
// This package manages self-contained Docker containers for ledger generation.
package loadtest

import (
	"context"
	"fmt"
	"net/http"

	// "strings"
	"time"

	"github.com/stellar/go/keypair"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/txnbuild"
	"github.com/testcontainers/testcontainers-go"

	tcnetwork "github.com/testcontainers/testcontainers-go/network"

	"github.com/stellar/wallet-backend/internal/integrationtests/infrastructure"
)

// LoadTestContainers holds the minimal container set for ledger generation.
// This is a self-contained environment that starts and stops its own containers.
type LoadTestContainers struct {
	TestNetwork   *testcontainers.DockerNetwork
	networkName   string
	CoreDB        *infrastructure.TestContainer
	StellarCore   *infrastructure.TestContainer
	RPCContainer  *infrastructure.TestContainer
	MasterAccount *txnbuild.SimpleAccount
	MasterKeyPair *keypair.Full
	HTTPClient    *http.Client
}

// StartLoadTestContainers starts minimal containers for ledger generation.
// It creates a standalone Stellar network with Core + RPC only (no wallet-backend needed).
func StartLoadTestContainers(ctx context.Context) (*LoadTestContainers, error) {
	log.Info("Starting load test containers...")

	// Create master keypair from standalone network seed
	// Initialize master account for funding
	masterKP := keypair.Root(infrastructure.NetworkPassphrase)
	containers := &LoadTestContainers{
		MasterKeyPair: masterKP,
		HTTPClient:    &http.Client{Timeout: 30 * time.Second},
		MasterAccount: &txnbuild.SimpleAccount{
			AccountID: masterKP.Address(),
			Sequence:  0,
		},
	}

	// Create Docker network with unique timestamp-based name
	networkName := fmt.Sprintf("loadtest-network-%d", time.Now().UnixNano())
	testNetwork, err := tcnetwork.New(ctx, tcnetwork.WithDriver("bridge"))
	if err != nil {
		return nil, fmt.Errorf("creating docker network: %w", err)
	}
	containers.TestNetwork = testNetwork
	containers.networkName = networkName
	log.Infof("Created Docker network: %s", networkName)

	// Container options for loadtest: unique names, no reuse
	opts := &infrastructure.ContainerOptions{
		NamePrefix:   "",
		Reuse:        false,
		SessionLabel: "wallet-backend-loadtest",
	}

	// Start Core DB using infrastructure package
	containers.CoreDB, err = infrastructure.CreateCoreDBContainer(ctx, containers.TestNetwork, opts)
	if err != nil {
		_ = containers.Close() //nolint:errcheck
		return nil, fmt.Errorf("creating core DB container: %w", err)
	}

	// Start Stellar Core using infrastructure package (includes protocol upgrade)
	containers.StellarCore, err = infrastructure.CreateStellarCoreContainer(ctx, containers.TestNetwork, opts)
	if err != nil {
		_ = containers.Close() //nolint:errcheck
		return nil, fmt.Errorf("creating stellar core container: %w", err)
	}

	// Start RPC using infrastructure package
	containers.RPCContainer, err = infrastructure.CreateRPCContainer(ctx, containers.TestNetwork, opts)
	if err != nil {
		_ = containers.Close() //nolint:errcheck
		return nil, fmt.Errorf("creating RPC container: %w", err)
	}

	log.Info("Load test containers started successfully")
	return containers, nil
}

// Close shuts down all containers and cleans up resources.
func (c *LoadTestContainers) Close() error {
	ctx := context.Background()
	var errs []error

	if c.RPCContainer != nil {
		if err := c.RPCContainer.Terminate(ctx); err != nil {
			errs = append(errs, fmt.Errorf("terminating RPC container: %w", err))
		}
	}

	if c.StellarCore != nil {
		if err := c.StellarCore.Terminate(ctx); err != nil {
			errs = append(errs, fmt.Errorf("terminating stellar core container: %w", err))
		}
	}

	if c.CoreDB != nil {
		if err := c.CoreDB.Terminate(ctx); err != nil {
			errs = append(errs, fmt.Errorf("terminating core DB container: %w", err))
		}
	}

	if c.TestNetwork != nil {
		if err := c.TestNetwork.Remove(ctx); err != nil {
			errs = append(errs, fmt.Errorf("removing docker network: %w", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing containers: %v", errs)
	}

	log.Info("Load test containers closed")
	return nil
}
