// backfill_test.go tests the parallel execution of live ingestion and backfilling.
package integrationtests

import (
	"context"
	"time"

	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stretchr/testify/suite"

	"github.com/stellar/wallet-backend/internal/integrationtests/infrastructure"
)

// BackfillTestSuite tests that live ingestion and backfilling can run concurrently.
type BackfillTestSuite struct {
	suite.Suite
	testEnv *infrastructure.TestEnvironment
}

// TestParallelLiveAndBackfill validates that backfilling works alongside live ingestion.
func (suite *BackfillTestSuite) TestParallelLiveAndBackfill() {
	ctx := context.Background()
	containers := suite.testEnv.Containers

	// Phase 1: Record initial state (live ingest is already running)
	log.Ctx(ctx).Info("Phase 1: Recording initial state")

	oldestLedger, err := containers.GetIngestCursor(ctx, "oldest_ingest_ledger")
	suite.Require().NoError(err, "failed to get oldest_ingest_ledger cursor")

	latestLedger, err := containers.GetIngestCursor(ctx, "latest_ingest_ledger")
	suite.Require().NoError(err, "failed to get latest_ingest_ledger cursor")

	// In standalone mode, checkpoint frequency is 8
	// Live started from some checkpoint, so oldest_ingest_ledger > 8
	// There are older ledgers (8 to oldest-1) that were never processed
	firstCheckpoint := uint32(8)

	log.Ctx(ctx).Infof("Initial state: oldest=%d, latest=%d, firstCheckpoint=%d",
		oldestLedger, latestLedger, firstCheckpoint)

	// Skip test if there's no backfill range available
	if oldestLedger <= firstCheckpoint {
		suite.T().Skipf("No backfill range available: oldest_ingest_ledger (%d) <= firstCheckpoint (%d)",
			oldestLedger, firstCheckpoint)
		return
	}

	backfillEndLedger := oldestLedger - 1 // Just before where live started
	log.Ctx(ctx).Infof("Backfill range: %d to %d", firstCheckpoint, backfillEndLedger)

	// Phase 2: Start backfill container alongside live
	log.Ctx(ctx).Info("Phase 2: Starting backfill container alongside live ingest")

	backfillContainer, err := containers.StartBackfillContainer(ctx, firstCheckpoint, backfillEndLedger)
	suite.Require().NoError(err, "failed to start backfill container")
	defer func() {
		if err := backfillContainer.Terminate(ctx); err != nil {
			log.Ctx(ctx).Warnf("Failed to terminate backfill container: %v", err)
		}
	}()

	log.Ctx(ctx).Info("Backfill container started alongside live ingest")

	// Phase 3: Wait for backfill completion
	log.Ctx(ctx).Info("Phase 3: Waiting for backfill completion")

	err = containers.WaitForBackfillCompletion(ctx, firstCheckpoint, 5*time.Minute)
	suite.Require().NoError(err, "backfill did not complete successfully")

	log.Ctx(ctx).Info("Backfill completed successfully")

	// Phase 4: Validate both ran successfully
	log.Ctx(ctx).Info("Phase 4: Validating cursors")

	newOldestLedger, err := containers.GetIngestCursor(ctx, "oldest_ingest_ledger")
	suite.Require().NoError(err, "failed to get new oldest_ingest_ledger cursor")
	suite.Assert().LessOrEqual(newOldestLedger, firstCheckpoint,
		"backfill should have updated oldest cursor to first checkpoint")

	newLatestLedger, err := containers.GetIngestCursor(ctx, "latest_ingest_ledger")
	suite.Require().NoError(err, "failed to get new latest_ingest_ledger cursor")
	suite.Assert().GreaterOrEqual(newLatestLedger, latestLedger,
		"live ingest should continue processing")

	log.Ctx(ctx).Infof("Cursor validation passed: oldest=%d (was %d), latest=%d (was %d)",
		newOldestLedger, oldestLedger, newLatestLedger, latestLedger)

	// Phase 5: Validate setup transactions were backfilled
	log.Ctx(ctx).Info("Phase 5: Validating setup transactions were backfilled")

	testAccounts := []string{
		containers.GetClientAuthKeyPair(ctx).Address(),
		containers.GetPrimarySourceAccountKeyPair(ctx).Address(),
		containers.GetSecondarySourceAccountKeyPair(ctx).Address(),
		containers.GetDistributionAccountKeyPair(ctx).Address(),
		containers.GetBalanceTestAccount1KeyPair(ctx).Address(),
		containers.GetBalanceTestAccount2KeyPair(ctx).Address(),
	}

	// Verify transactions exist for funded accounts in the backfilled range
	for _, accountAddr := range testAccounts {
		txCount, err := containers.GetTransactionCountForAccount(ctx, accountAddr, firstCheckpoint, backfillEndLedger)
		suite.Require().NoError(err, "failed to get transaction count for account %s", accountAddr)
		suite.Assert().Greater(txCount, 0,
			"should have transactions for account %s in backfilled range", accountAddr)
	}

	log.Ctx(ctx).Info("Transaction validation passed for all test accounts")

	// Verify CREATE_ACCOUNT operations for funded accounts exist in backfilled range
	for _, accountAddr := range testAccounts {
		hasOp, err := containers.HasOperationForAccount(ctx, accountAddr, "CREATE_ACCOUNT", firstCheckpoint, backfillEndLedger)
		suite.Require().NoError(err, "failed to check CREATE_ACCOUNT operation for account %s", accountAddr)
		suite.Assert().True(hasOp,
			"should have CREATE_ACCOUNT operation for account %s in backfilled range", accountAddr)
	}

	log.Ctx(ctx).Info("CREATE_ACCOUNT operation validation passed for all test accounts")

	// Verify CHANGE_TRUST operations for trustlines exist
	hasTrustOp, err := containers.HasOperationForAccount(ctx,
		containers.GetBalanceTestAccount1KeyPair(ctx).Address(),
		"CHANGE_TRUST", firstCheckpoint, backfillEndLedger)
	suite.Require().NoError(err, "failed to check CHANGE_TRUST operation")
	suite.Assert().True(hasTrustOp, "should have CHANGE_TRUST operation for USDC trustline")

	log.Ctx(ctx).Info("CHANGE_TRUST operation validation passed")

	// Verify state_changes were created for account creations
	stateChangeCount, err := containers.GetStateChangeCountForLedgerRange(ctx, firstCheckpoint, backfillEndLedger)
	suite.Require().NoError(err, "failed to get state change count")
	suite.Assert().Greater(stateChangeCount, 0, "should have state changes in backfilled range")

	log.Ctx(ctx).Infof("State change validation passed: %d state changes in backfilled range", stateChangeCount)

	// Verify participant linking tables were populated
	for _, accountAddr := range testAccounts {
		linkCount, err := containers.GetTransactionAccountLinkCount(ctx, accountAddr, firstCheckpoint, backfillEndLedger)
		suite.Require().NoError(err, "failed to get transaction-account link count for %s", accountAddr)
		suite.Assert().Greater(linkCount, 0,
			"should have transaction-account links for %s", accountAddr)
	}

	log.Ctx(ctx).Info("Transaction-account link validation passed for all test accounts")

	log.Ctx(ctx).Info("All backfill validations passed successfully!")
}
