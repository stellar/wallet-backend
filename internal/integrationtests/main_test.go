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

	// Phase 2: Test parallel live + backfill ingestion
	t.Run("BackfillTestSuite", func(t *testing.T) {
		suite.Run(t, &BackfillTestSuite{
			testEnv: testEnv,
		})
	})

	// Test catchup backfilling during live ingestion
	t.Run("CatchupTestSuite", func(t *testing.T) {
		suite.Run(t, &CatchupTestSuite{
			testEnv: testEnv,
		})
	})

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

	t.Run("BuildAndSubmitTransactionsTestSuite", func(t *testing.T) {
		suite.Run(t, &BuildAndSubmitTransactionsTestSuite{
			testEnv: testEnv,
		})
	})

	// Only proceed if build and submit succeeded
	if t.Failed() {
		t.Fatal("BuildAndSubmitTransactionsTestSuite failed, skipping data validation")
	}

	// Wait for ingest service to process all transactions
	log.Ctx(ctx).Info("⏳ Waiting for ingest service to process transactions...")
	time.Sleep(5 * time.Second)

	t.Run("DataValidationTestSuite", func(t *testing.T) {
		suite.Run(t, &DataValidationTestSuite{
			testEnv: testEnv,
		})
	})

	if t.Failed() {
		t.Fatal("BuildAndSubmitTransactionsTestSuite failed, skipping remaining tests")
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
}
