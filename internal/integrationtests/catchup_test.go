// catchup_test.go tests the catchup backfilling during live ingestion.
package integrationtests

import (
	"context"
	"time"

	"github.com/stellar/go/support/log"
	"github.com/stretchr/testify/suite"

	"github.com/stellar/wallet-backend/internal/integrationtests/infrastructure"
)

// CatchupTestSuite tests the automatic catchup backfilling that occurs when
// the ingest service falls behind the network tip by more than the catchup threshold.
type CatchupTestSuite struct {
	suite.Suite
	testEnv *infrastructure.TestEnvironment
}

// TestCatchupDuringLiveIngestion validates that when the ingest service is behind
// the network by more than the catchup threshold, it triggers parallel backfilling
// to catch up efficiently.
func (suite *CatchupTestSuite) TestCatchupDuringLiveIngestion() {
	ctx := context.Background()
	containers := suite.testEnv.Containers

	// Phase 1: Record initial state (live ingest is already running)
	log.Ctx(ctx).Info("Phase 1: Recording initial state")

	initialOldest, err := containers.GetIngestCursor(ctx, "oldest_ingest_ledger")
	suite.Require().NoError(err, "failed to get initial oldest_ingest_ledger cursor")

	initialLatest, err := containers.GetIngestCursor(ctx, "latest_ingest_ledger")
	suite.Require().NoError(err, "failed to get initial latest_ingest_ledger cursor")

	log.Ctx(ctx).Infof("Initial state: oldest=%d, latest=%d", initialOldest, initialLatest)

	// Phase 2: Stop ingest container to simulate falling behind
	log.Ctx(ctx).Info("Phase 2: Stopping ingest container")

	err = containers.StopIngestContainer(ctx)
	suite.Require().NoError(err, "failed to stop ingest container")

	// Phase 3: Wait for network to advance beyond catchup threshold
	// We'll use a low threshold of 5 ledgers for faster testing
	log.Ctx(ctx).Info("Phase 3: Waiting for network to advance")

	targetLedger := initialLatest + 20 // Need to be behind by more than catchup threshold (5)
	err = containers.WaitForNetworkAdvance(ctx, suite.testEnv.RPCService, targetLedger, 2*time.Minute)
	suite.Require().NoError(err, "network did not advance to target ledger")

	log.Ctx(ctx).Infof("Network advanced to ledger %d", targetLedger)

	// Phase 4: Restart ingest container with low catchup threshold
	log.Ctx(ctx).Info("Phase 4: Restarting ingest container with low catchup threshold")

	err = containers.RestartIngestContainer(ctx, map[string]string{
		"CATCHUP_THRESHOLD": "5", // Trigger catchup if behind by 5+ ledgers
	})
	suite.Require().NoError(err, "failed to restart ingest container")

	// Phase 5: Wait for catchup to complete
	log.Ctx(ctx).Info("Phase 5: Waiting for catchup to complete")

	err = containers.WaitForLatestLedgerToReach(ctx, targetLedger, 3*time.Minute)
	suite.Require().NoError(err, "catchup did not complete in time")

	log.Ctx(ctx).Info("Catchup completed")

	// Phase 6: Validate results
	log.Ctx(ctx).Info("Phase 6: Validating results")

	// Verify catchup was triggered by checking logs
	logs, err := containers.GetIngestContainerLogs(ctx)
	suite.Require().NoError(err, "failed to get ingest container logs")
	suite.Assert().Contains(logs, "Wallet backend has fallen behind network tip by 20 ledgers. Doing optimized catchup to the tip: 75",
		"should see catchup triggered log message")

	// Verify oldest cursor stayed the same (catchup doesn't change oldest)
	newOldest, err := containers.GetIngestCursor(ctx, "oldest_ingest_ledger")
	suite.Require().NoError(err, "failed to get new oldest_ingest_ledger cursor")
	suite.Assert().Equal(initialOldest, newOldest,
		"oldest cursor should not change during catchup")

	// Verify latest cursor caught up
	newLatest, err := containers.GetIngestCursor(ctx, "latest_ingest_ledger")
	suite.Require().NoError(err, "failed to get new latest_ingest_ledger cursor")
	suite.Assert().GreaterOrEqual(newLatest, targetLedger,
		"should have caught up to target ledger")

	// Verify no gaps exist in the ledger range we caught up
	gapCount, err := containers.GetLedgerGapCount(ctx, initialLatest, newLatest)
	suite.Require().NoError(err, "failed to get ledger gap count")
	suite.Assert().Equal(0, gapCount,
		"should have no gaps after catchup")

	log.Ctx(ctx).Infof("Validation passed: oldest=%d, latest=%d (was %d), gaps=%d",
		newOldest, newLatest, initialLatest, gapCount)

	log.Ctx(ctx).Info("All catchup validations passed successfully!")
}
