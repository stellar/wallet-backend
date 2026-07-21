package integrationtests

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/suite"

	"github.com/stellar/wallet-backend/internal/data"
	sep41data "github.com/stellar/wallet-backend/internal/data/sep41"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/integrationtests/infrastructure"
	"github.com/stellar/wallet-backend/internal/metrics"
)

const sep41ProtocolID = "SEP41"

// DataMigrationTestSuite exercises the real protocol-setup → protocol-migrate
// deployment flow against the standalone network: it runs the actual
// wallet-backend subcommands as one-shot containers and asserts the migrated
// SEP-41 state, rather than driving the migration services in-process. The
// per-ledger CAS gating and windowed-coalescing engine mechanics are covered by
// unit tests (services.Test_persistLedgerData_ProtocolCASGating,
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
	// The container persists as "wallet-backend-protocol-setup" for `docker logs`.
	exitCode, logs, err := s.testEnv.Containers.RunWalletBackendCommand(ctx,
		"wallet-backend-protocol-setup", "protocol-setup --protocol-id "+sep41ProtocolID, nil)
	s.Require().NoError(err)
	s.Require().Zerof(exitCode, "protocol-setup should exit 0 (see `docker logs wallet-backend-protocol-setup`); logs:\n%s", logs)

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

	// Phase 3: protocol-migrate current-state builds SEP-41 balances from start ledger to
	// the tip, coalescing with --window-size 10, using the production datastore backend
	// (optimizedStorageBackend reading from the in-test minio object store) — the same path
	// live migrations use. A host-side exporter streams standalone ledgers into minio and
	// keeps following the tip, so the unbounded datastore backend never starves; the
	// migration converges and hands off to live ingestion at the tip exactly as it would
	// with the RPC backend. The migrate container persists as "wallet-backend-protocol-migrate"
	// for `docker logs`.
	rpcURL, err := s.testEnv.Containers.RPCContainer.GetConnectionString(ctx)
	s.Require().NoError(err)
	minioEndpoint, err := s.testEnv.Containers.MinioContainer.GetConnectionString(ctx)
	s.Require().NoError(err)
	stopExporter := infrastructure.StartLedgerExporter(s.T(), rpcURL, minioEndpoint, startLedger)
	defer stopExporter()

	// Datastore config (bucket, endpoint, schema, buffer/worker tuning) arrives via env from
	// DatastoreEnv() below — see internal/integrationtests/infrastructure/ledger_exporter.go.
	cmd := fmt.Sprintf("protocol-migrate current-state --protocol-id %s "+
		"--ledger-backend-type datastore "+
		"--start-ledger %d --window-size 10", sep41ProtocolID, startLedger)
	exitCode, logs, err = s.testEnv.Containers.RunWalletBackendCommand(ctx, "wallet-backend-protocol-migrate", cmd, s.testEnv.Containers.DatastoreEnv())
	s.Require().NoError(err)
	s.Require().Zerof(exitCode, "protocol-migrate current-state should exit 0 (see `docker logs wallet-backend-protocol-migrate`); logs:\n%s", logs)

	// Phase 4: assert the migrated current state, replaying the full custom-SEP-41
	// event sequence from setup + use cases. The mint and the partial transfer land
	// in different coalescing windows, so each holder's balance is correct only if
	// the migration accumulated the signed per-ledger deltas:
	//   - account1: minted TestSEP41MintStroops, then transferred TestSEP41TransferStroops
	//     to account2 (fixtures prepareSEP41TransferOp), so it keeps the difference.
	//   - account2: received the transferred amount.
	//   - holder contract: minted TestSEP41MintStroops and kept it.
	// The three amounts are distinct, so a mis-credited delta cannot pass silently.
	s.assertSEP41Balance(ctx, models, pool, s.testEnv.BalanceTestAccount1KP.Address(),
		fmt.Sprintf("%d", infrastructure.TestSEP41MintStroops-infrastructure.TestSEP41TransferStroops))
	s.assertSEP41Balance(ctx, models, pool, s.testEnv.BalanceTestAccount2KP.Address(),
		fmt.Sprintf("%d", infrastructure.TestSEP41TransferStroops))
	s.assertSEP41Balance(ctx, models, pool, s.testEnv.HolderContractAddress,
		fmt.Sprintf("%d", infrastructure.TestSEP41MintStroops))

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
// On failure it dumps every sep41_balances row (decoded) and the relevant
// addresses so the mismatch is diagnosable from a single run.
func (s *DataMigrationTestSuite) assertSEP41Balance(ctx context.Context, models *data.Models, pool *pgxpool.Pool, holder, expectedAmount string) {
	balances, err := models.SEP41.Balances.GetByAccount(ctx, holder, nil, nil, sep41data.SortASC)
	s.Require().NoError(err)
	s.Require().Lenf(balances, 1,
		"expected exactly one SEP-41 balance for %s\nactual sep41_balances:\n%s(account1=%s holder=%s master=%s sep41token=%s)",
		holder, s.dumpSEP41Balances(ctx, pool),
		s.testEnv.BalanceTestAccount1KP.Address(), s.testEnv.HolderContractAddress,
		s.testEnv.MasterAccountAddress, s.testEnv.SEP41ContractAddress)
	s.Assert().Equal(s.testEnv.SEP41ContractAddress, balances[0].TokenID, "balance should be for the custom SEP-41 token")
	s.Assert().Equal(expectedAmount, balances[0].Balance, "migrated SEP-41 balance for %s should equal the expected amount", holder)
}

// dumpSEP41Balances renders every sep41_balances row with its decoded holder
// address and token C-address — used only in assertion failure messages.
func (s *DataMigrationTestSuite) dumpSEP41Balances(ctx context.Context, pool *pgxpool.Pool) string {
	rows, err := pool.Query(ctx,
		`SELECT b.account_id, b.balance, ct.contract_id
		 FROM sep41_balances b JOIN contract_tokens ct ON ct.id = b.contract_id`)
	s.Require().NoError(err)
	defer rows.Close()

	var sb strings.Builder
	for rows.Next() {
		var account types.AddressBytea
		var balance, token string
		s.Require().NoError(rows.Scan(&account, &balance, &token))
		fmt.Fprintf(&sb, "  holder=%s token=%s balance=%s\n", account, token, balance)
	}
	s.Require().NoError(rows.Err())
	return sb.String()
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
