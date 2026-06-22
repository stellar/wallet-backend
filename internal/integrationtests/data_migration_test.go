package integrationtests

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/suite"

	"github.com/stellar/wallet-backend/internal/data"
	sep41data "github.com/stellar/wallet-backend/internal/data/sep41"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/integrationtests/infrastructure"
	"github.com/stellar/wallet-backend/internal/metrics"
)

const sep41ProtocolID = "SEP41"

// DataMigrationTestSuite exercises the real protocol-setup → protocol-migrate
// deployment flow against the standalone network: it runs the actual
// wallet-backend subcommands as one-shot containers and asserts the migrated
// SEP-41 state, rather than driving the migration services in-process. The
// per-ledger CAS gating and windowed-coalescing engine mechanics are covered by
// unit tests (services.Test_PersistLedgerData_ProtocolCASGating,
// TestProtocolMigrateEngine*); this suite's unique value is verifying the real
// binaries produce correct end state.
type DataMigrationTestSuite struct {
	suite.Suite
	testEnv *infrastructure.TestEnvironment
}

// NOTE: this suite deliberately has no teardown/reset. The SEP-41 protocol state
// it produces (classified contracts, current-state balances, the now-active
// protocol cursor that live ingestion takes over post-handoff) is left in place
// on purpose: AccountBalancesAfterLiveIngestionTestSuite, which runs later,
// asserts the API surfaces those migrated balances. Resetting here would hide the
// exact behavior we want to verify end-to-end.

func (s *DataMigrationTestSuite) setupDB() (*pgxpool.Pool, func()) {
	ctx := context.Background()
	dbURL, err := s.testEnv.Containers.GetWalletDBConnectionString(ctx)
	s.Require().NoError(err)

	pool, err := db.OpenDBConnectionPool(ctx, dbURL)
	s.Require().NoError(err)

	return pool, func() { pool.Close() }
}

func (s *DataMigrationTestSuite) setupModels(pool *pgxpool.Pool) *data.Models {
	dbMetrics := metrics.NewMetrics(prometheus.NewRegistry()).DB
	models, err := data.NewModels(pool, dbMetrics)
	s.Require().NoError(err)

	return models
}

// TestProtocolSetupThenCurrentStateMigration mirrors the real deployment order:
// pre-check → protocol-setup → protocol-migrate current-state → assert. Each
// phase runs as a one-shot wallet-backend container against the standalone
// network while live ingestion keeps running, so the migration converges and
// hands off exactly as it would in production.
func (s *DataMigrationTestSuite) TestProtocolSetupThenCurrentStateMigration() {
	ctx := context.Background()
	pool, cleanup := s.setupDB()
	defer cleanup()

	models := s.setupModels(pool)

	// Phase 1: pre-checks. Live ingestion populated protocol_wasms with
	// unclassified entries, and no SEP-41 state exists yet (no protocol cursors,
	// so live ingestion soft-skips SEP-41 production).
	unclassifiedBefore, err := models.ProtocolWasms.GetUnclassified(ctx)
	s.Require().NoError(err)
	s.Require().NotEmpty(unclassifiedBefore, "live ingest should have populated protocol_wasms with unclassified entries")

	s.Require().Zero(s.countSEP41Balances(ctx, pool), "sep41_balances must be empty before migration runs")

	startLedger, err := models.IngestStore.Get(ctx, data.OldestLedgerCursorName)
	s.Require().NoError(err)
	s.Require().Greater(startLedger, uint32(0), "oldest_ledger_cursor should be set by live ingestion")

	// Phase 2: protocol-setup classifies the SEP-41 WASM via bytecode analysis.
	exitCode, logs, err := s.testEnv.Containers.RunWalletBackendCommand(ctx,
		"protocol-setup --protocol-id "+sep41ProtocolID, nil)
	s.Require().NoError(err)
	s.Require().Zerof(exitCode, "protocol-setup should exit 0; logs:\n%s", logs)

	classifiedContracts, err := models.ProtocolContracts.GetByProtocolID(ctx, sep41ProtocolID)
	s.Require().NoError(err)
	s.Require().NotEmpty(classifiedContracts, "protocol-setup should classify at least one SEP-41 contract")

	protocols, err := models.Protocols.GetByIDs(ctx, []string{sep41ProtocolID})
	s.Require().NoError(err)
	s.Require().Len(protocols, 1)
	s.Assert().Equal(data.StatusSuccess, protocols[0].ClassificationStatus, "classification status should be success")

	// protocol-setup must not create protocol cursors — only the migration does.
	s.Assert().False(s.ingestStoreKeyExists(ctx, pool, "protocol_SEP41_current_state_cursor"),
		"protocol-setup should not initialize the current-state cursor")

	// Phase 3: protocol-migrate current-state builds SEP-41 balances from start
	// ledger to the tip, coalescing with --window-size 10. It terminates by
	// handing off to the concurrently-running live ingestion at the tip.
	cmd := fmt.Sprintf("protocol-migrate current-state --protocol-id %s --ledger-backend-type rpc --start-ledger %d --window-size 10",
		sep41ProtocolID, startLedger)
	exitCode, logs, err = s.testEnv.Containers.RunWalletBackendCommand(ctx, cmd, nil)
	s.Require().NoError(err)
	s.Require().Zerof(exitCode, "protocol-migrate current-state should exit 0; logs:\n%s", logs)

	// Phase 4: assert the migrated current state. Both holders received a single
	// mint of TestSEP41MintStroops during setup; those are the only custom-SEP-41
	// events, so the migrated balance must equal the minted amount exactly.
	mintAmount := fmt.Sprintf("%d", infrastructure.TestSEP41MintStroops)
	s.assertSEP41Balance(ctx, models, s.testEnv.BalanceTestAccount1KP.Address(), mintAmount)
	s.assertSEP41Balance(ctx, models, s.testEnv.HolderContractAddress, mintAmount)

	protocols, err = models.Protocols.GetByIDs(ctx, []string{sep41ProtocolID})
	s.Require().NoError(err)
	s.Require().Len(protocols, 1)
	s.Assert().Equal(data.StatusSuccess, protocols[0].CurrentStateMigrationStatus, "current-state migration status should be success")

	// The cursor advanced past its init value (startLedger-1). Its exact value is
	// not asserted: live ingestion owns it post-handoff and keeps advancing it.
	currentStateCursor, err := models.IngestStore.Get(ctx, "protocol_SEP41_current_state_cursor")
	s.Require().NoError(err)
	s.Assert().GreaterOrEqual(currentStateCursor, startLedger, "current-state cursor should have advanced past its init value")
}

// assertSEP41Balance asserts the holder has exactly one SEP-41 balance — for the
// custom SEP-41 token deployed in setup — with the expected amount. The custom
// token is the only pure SEP-41 contract; USDC/EURC are SACs tracked separately.
func (s *DataMigrationTestSuite) assertSEP41Balance(ctx context.Context, models *data.Models, holder, expectedAmount string) {
	balances, err := models.SEP41.Balances.GetByAccount(ctx, holder, nil, nil, sep41data.SortASC)
	s.Require().NoError(err)
	s.Require().Len(balances, 1, "expected exactly one SEP-41 balance for %s", holder)
	s.Assert().Equal(s.testEnv.SEP41ContractAddress, balances[0].TokenID, "balance should be for the custom SEP-41 token")
	s.Assert().Equal(expectedAmount, balances[0].Balance, "migrated SEP-41 balance for %s should equal the minted amount", holder)
}

func (s *DataMigrationTestSuite) countSEP41Balances(ctx context.Context, pool *pgxpool.Pool) int {
	var count int
	err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM sep41_balances`).Scan(&count)
	s.Require().NoError(err)
	return count
}

func (s *DataMigrationTestSuite) ingestStoreKeyExists(ctx context.Context, pool *pgxpool.Pool, key string) bool {
	var exists bool
	err := pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM ingest_store WHERE key = $1)`, key).Scan(&exists)
	s.Require().NoError(err)
	return exists
}

func TestDataMigrationTestSuiteStandalone(t *testing.T) {
	t.Skip("Run via TestIntegrationTests")
}
