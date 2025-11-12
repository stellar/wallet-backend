// Package integrationtests provides end-to-end integration tests for wallet-backend
package integrationtests

import (
	"context"
	"os"
	"testing"

	// "time"

	"github.com/sirupsen/logrus"
	"github.com/stellar/go/support/log"
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
	testEnv, err := infrastructure.NewTestEnvironment(containers, ctx)
	if err != nil {
		t.Fatalf("Failed to initialize test environment: %v", err)
	}

	t.Run("AccountRegisterTestSuite", func(t *testing.T) {
		suite.Run(t, &AccountRegisterTestSuite{
			testEnv: testEnv,
		})
	})

	// Only proceed if account registration succeeded
	if t.Failed() {
		t.Fatal("AccountRegisterTestSuite failed, skipping remaining tests")
	}

	// t.Run("BuildAndSubmitTransactionsTestSuite", func(t *testing.T) {
	// 	suite.Run(t, &BuildAndSubmitTransactionsTestSuite{
	// 		testEnv: testEnv,
	// 	})
	// })

	// // Only proceed if build and submit succeeded
	// if t.Failed() {
	// 	t.Fatal("BuildAndSubmitTransactionsTestSuite failed, skipping data validation")
	// }

	// // Wait for ingest service to process all transactions
	// log.Ctx(ctx).Info("⏳ Waiting for ingest service to process transactions...")
	// time.Sleep(5 * time.Second)

	// t.Run("DataValidationTestSuite", func(t *testing.T) {
	// 	suite.Run(t, &DataValidationTestSuite{
	// 		testEnv: testEnv,
	// 	})
	// })

	t.Run("AccountBalancesTestSuite", func(t *testing.T) {
		suite.Run(t, &AccountBalancesTestSuite{
			testEnv: testEnv,
		})
	})
}
