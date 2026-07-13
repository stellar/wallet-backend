// Package integrationtests provides end-to-end integration tests for wallet-backend
package integrationtests

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stretchr/testify/suite"

	"github.com/stellar/wallet-backend/internal/integrationtests/infrastructure"
)

func TestIntegrationTests(t *testing.T) {
	if os.Getenv("ENABLE_INTEGRATION_TESTS") != "true" {
		t.Skip("Skipping integration tests: ENABLE_INTEGRATION_TESTS is not 'true'")
	}

	// Initialize logger for integration tests
	log.DefaultLogger = log.New()
	log.DefaultLogger.SetLevel(logrus.DebugLevel)

	ctx := context.Background()

	// Initialize shared containers
	containers := infrastructure.NewSharedContainers(t)
	defer containers.Cleanup(ctx)

	// Initialize shared test environment
	testEnv, err := infrastructure.NewTestEnvironment(ctx, containers)
	if err != nil {
		t.Fatalf("Failed to initialize test environment: %v", err)
	}

	// Phase 1: Validate balances from checkpoint before fixture transactions
	t.Run("AccountBalancesAfterCheckpointTestSuite", func(t *testing.T) {
		suite.Run(t, &AccountBalancesAfterCheckpointTestSuite{
			testEnv: testEnv,
		})
	})

	// Only proceed if checkpoint balance validation succeeded
	if t.Failed() {
		t.Fatal("AccountBalancesAfterCheckpointTestSuite failed, skipping remaining tests")
	}

	// Submit fixture transactions directly to RPC for ingestion
	err = infrastructure.SubmitUseCases(ctx, testEnv)
	if err != nil {
		t.Fatalf("Failed to submit use case transactions: %v", err)
	}

	// Wait for ingest service to process all transactions
	log.Ctx(ctx).Info("Waiting for ingest service to process transactions...")
	time.Sleep(5 * time.Second)

	// Test parallel live + backfill ingestion (after use case txns so transactions table has data)
	t.Run("BackfillTestSuite", func(t *testing.T) {
		suite.Run(t, &BackfillTestSuite{
			testEnv: testEnv,
		})
	})

	// Data migration tests — protocol setup plus setup-to-live protocol state production
	t.Run("DataMigrationTestSuite", func(t *testing.T) {
		suite.Run(t, &DataMigrationTestSuite{testEnv: testEnv})
	})

	t.Run("DataValidationTestSuite", func(t *testing.T) {
		suite.Run(t, &DataValidationTestSuite{
			testEnv: testEnv,
		})
	})

	if t.Failed() {
		t.Fatal("DataValidationTestSuite failed, skipping remaining tests")
	}

	// Phase 3: Validate balances after live ingestion processes fixture transactions
	t.Run("AccountBalancesAfterLiveIngestionTestSuite", func(t *testing.T) {
		suite.Run(t, &AccountBalancesAfterLiveIngestionTestSuite{
			testEnv: testEnv,
		})
	})

	if t.Failed() {
		t.Fatal("AccountBalancesAfterLiveIngestionTestSuite failed, skipping remaining tests")
	}

	// Phase 4: Blend v2 — deploy the protocol stack and run phase-1 ops under live
	// ingestion, migrate (current-state + history) via the datastore, then run
	// phase-2 ops under live ingestion and assert over GraphQL.
	blendStack := testEnv.Containers.SetupBlendStack(ctx, t, testEnv.RPCService)
	testEnv.Containers.SubmitBlendPhase1Ops(ctx, t, blendStack)

	log.Ctx(ctx).Info("Waiting for ingest service to process Blend phase-1 transactions...")
	time.Sleep(5 * time.Second)

	t.Run("BlendMigrationTestSuite", func(t *testing.T) {
		suite.Run(t, &BlendMigrationTestSuite{testEnv: testEnv, stack: blendStack})
	})
	if t.Failed() {
		t.Fatal("BlendMigrationTestSuite failed, skipping BlendLiveIngestionTestSuite")
	}
	t.Run("BlendLiveIngestionTestSuite", func(t *testing.T) {
		suite.Run(t, &BlendLiveIngestionTestSuite{testEnv: testEnv, stack: blendStack})
	})
}
