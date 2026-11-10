// lookup_table_test.go tests the transaction_hashes lookup table end to end: every ingested
// transaction has its lookup row, and hash lookups keep resolving after maintenance compresses
// the lookup chunks.
package integrationtests

import (
	"context"
	"crypto/rand"
	"encoding/hex"

	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stretchr/testify/suite"

	"github.com/stellar/wallet-backend/internal/integrationtests/infrastructure"
	"github.com/stellar/wallet-backend/pkg/wbclient"
)

// LookupTableTestSuite runs after DataValidationTestSuite, on the transactions that live
// ingestion and backfill wrote. Its tests run in order: each maintenance run builds on the
// chunks the previous test left.
type LookupTableTestSuite struct {
	suite.Suite
	testEnv *infrastructure.TestEnvironment
}

// requireComplete fails the test when any transactions row has no lookup row.
func (suite *LookupTableTestSuite) requireComplete(ctx context.Context, when string) {
	missing, err := suite.testEnv.Containers.TransactionsWithoutLookupRow(ctx, 0, 0)
	suite.Require().NoError(err, "failed to count transactions without a lookup row %s", when)
	suite.Require().Zero(missing, "transactions without a transaction_hashes row %s", when)
}

// Test1CompleteBeforeMaintenance checks that ingestion wrote a lookup row for every transaction,
// live and backfilled.
func (suite *LookupTableTestSuite) Test1CompleteBeforeMaintenance() {
	ctx := context.Background()
	log.Ctx(ctx).Info("🔍 Checking every transaction has a lookup row before maintenance...")

	suite.requireComplete(ctx, "before maintenance")

	log.Ctx(ctx).Info("Every transaction has a lookup row")
}

// Test2MaintenanceCompressesEveryChunk runs one maintenance pass with a non-zero job id, the way
// the scheduled job runs, and checks it compresses every chunk and deletes nothing.
func (suite *LookupTableTestSuite) Test2MaintenanceCompressesEveryChunk() {
	ctx := context.Background()
	containers := suite.testEnv.Containers
	log.Ctx(ctx).Info("🔧 Running transaction_hashes maintenance with one worker...")

	err := containers.RunLookupMaintenance(ctx, 1, 1, 0)
	suite.Require().NoError(err, "maintenance run failed")

	total, uncompressed, partial, err := containers.LookupChunkCounts(ctx)
	suite.Require().NoError(err, "failed to count transaction_hashes chunks")
	log.Ctx(ctx).Infof("transaction_hashes chunks after maintenance: total=%d, uncompressed=%d, partial=%d", total, uncompressed, partial)

	suite.Require().Positive(total, "ingestion should have created transaction_hashes chunks")
	suite.Assert().Zero(uncompressed,
		"chunks exist only where rows were written; one maintenance run should compress every one of them")
	suite.Assert().Zero(partial,
		"one maintenance run should fold every chunk's rowstore rows into its columnstore")

	// Job id 1 deletes the periods that closed before the oldest transactions row. On this
	// network every transaction falls in the oldest row's period or later, so nothing goes.
	suite.requireComplete(ctx, "after maintenance")

	log.Ctx(ctx).Info("Maintenance compressed every chunk and kept every lookup row")
}

// Test3LookupsFromCompressedChunks resolves the fixture transactions by hash, now that their lookup
// rows sit in compressed chunks, and checks an unknown hash is not found.
func (suite *LookupTableTestSuite) Test3LookupsFromCompressedChunks() {
	ctx := context.Background()
	log.Ctx(ctx).Info("🔍 Resolving fixture transactions by hash from compressed lookup chunks...")

	useCases := append([]*infrastructure.UseCase{}, suite.testEnv.UseCases...)
	useCases = append(useCases, suite.testEnv.ClaimAndClawbackUseCases...)

	resolved := 0
	for _, uc := range useCases {
		hash := uc.GetTransactionResult.Hash
		if hash == "" {
			continue
		}
		tx, err := suite.testEnv.WBClient.GetTransactionByHash(ctx, hash)
		suite.Require().NoError(err, "failed to get transaction %s for use case %s", hash, uc.Name())
		suite.Require().NotNil(tx, "transaction %s for use case %s should not be nil", hash, uc.Name())
		suite.Assert().Equal(hash, tx.Hash, "transaction hash mismatch for use case %s", uc.Name())
		resolved++
	}
	suite.Require().NotZero(resolved, "no fixture use case has a transaction hash")
	log.Ctx(ctx).Infof("Resolved %d fixture transactions by hash", resolved)

	unknown := make([]byte, 32)
	_, err := rand.Read(unknown)
	suite.Require().NoError(err, "failed to generate a random hash")
	tx, err := suite.testEnv.WBClient.GetTransactionByHash(ctx, hex.EncodeToString(unknown))
	suite.Require().ErrorIs(err, wbclient.ErrTransactionNotFound, "an unknown hash should not be found")
	suite.Assert().Nil(tx, "an unknown hash should return no transaction")

	log.Ctx(ctx).Info("Unknown hash returned not found")
}

// Test4TwoWorkerMaintenance runs maintenance as two workers over disjoint chunks and checks every
// lookup row survives.
func (suite *LookupTableTestSuite) Test4TwoWorkerMaintenance() {
	ctx := context.Background()
	containers := suite.testEnv.Containers
	log.Ctx(ctx).Info("🔧 Running transaction_hashes maintenance as two workers...")

	for worker := range 2 {
		err := containers.RunLookupMaintenance(ctx, 1, 2, worker)
		suite.Require().NoError(err, "maintenance run failed for worker %d of 2", worker)
	}

	suite.requireComplete(ctx, "after the two-worker maintenance run")

	log.Ctx(ctx).Info("Two-worker maintenance kept every lookup row")
}
