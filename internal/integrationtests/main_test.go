// Package integrationtests provides end-to-end integration tests for wallet-backend
package integrationtests

import (
	"context"
	"os"
	"testing"

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

	containers := infrastructure.NewSharedContainers(t)
	defer containers.Cleanup(context.Background())

	t.Run("TransactionTestSuite", func(t *testing.T) {
		suite.Run(t, &TransactionTestSuite{
			containers: containers,
		})
	})
}
