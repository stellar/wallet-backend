package integrationtests

// SEP-41 current-state repair coverage. The repair engine's iteration and
// retry mechanics are covered by unit tests
// (services.TestProtocolCurrentStateRepair*); this suite's unique value is
// running the real protocol-repair CLI container against real corruption in a
// real database while live ingestion keeps producing, so the two writers' guards
// (ApplyAbsolute's optimistic-concurrency WHERE and BatchApplyDeltas'
// strict-monotone CASE) are exercised against each other on a live row
// rather than a fixture.
//
// It reuses the custom SEP-41 token deployed in setup and classified by
// DataMigrationTestSuite's protocol-setup run, so it must run after that
// suite (and after AccountBalancesAfterLiveIngestionTestSuite, whose API
// assertions pin the fixture balances this suite deliberately moves).
//
// NOTE: like DataMigrationTestSuite, this suite has no teardown. It leaves
// account1 holding an extra sep41RepairMintStroops; nothing downstream
// asserts on the custom SEP-41 token.

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/alitto/pond/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stretchr/testify/suite"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/integrationtests/infrastructure"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/services"

	sep41data "github.com/stellar/wallet-backend/internal/data/sep41"
	sep41svc "github.com/stellar/wallet-backend/internal/services/sep41"
)

const (
	// sep41RepairMintStroops is the amount minted after repair to prove a fold
	// delta still lands on a repaired row. It differs from every other SEP-41
	// fixture amount so a delta credited to the wrong row, or dropped by the
	// strict-monotone guard, cannot produce the expected total by coincidence.
	sep41RepairMintStroops = 7_000_000_000 // 700 SEP41

	// sep41PhantomBalance is the balance given to the fabricated row for a holder
	// that owns nothing on-chain. Any non-zero value works; a distinctive one makes
	// a surviving row obvious in a failure dump.
	sep41PhantomBalance = "424242"

	// sep41CorruptionFactor is what the corrupted holder's balance is multiplied by.
	// A multiplier (rather than an added constant) keeps the corrupted value wrong
	// regardless of what the fixtures leave in the row.
	sep41CorruptionFactor = 3
)

// CurrentStateRepairTestSuite drives the real protocol-repair CLI over the
// custom SEP-41 token: it corrupts current state directly in SQL, repairs it
// from network truth, and then proves live ingestion still folds onto the
// repaired rows.
type CurrentStateRepairTestSuite struct {
	suite.Suite
	testEnv *infrastructure.TestEnvironment
}

func (s *CurrentStateRepairTestSuite) setupDB() (*pgxpool.Pool, func()) {
	ctx := context.Background()
	dbURL, err := s.testEnv.Containers.GetWalletDBConnectionString(ctx)
	s.Require().NoError(err)

	pool, err := db.OpenDBConnectionPool(ctx, dbURL)
	s.Require().NoError(err)

	return pool, func() { pool.Close() }
}

// TestSEP41CurrentStateRepair corrupts sep41_balances two ways — a wrong value on
// a real holder and a fabricated row for a holder that owns nothing — repairs the
// token, and then folds a live mint on top of the repaired row.
func (s *CurrentStateRepairTestSuite) TestSEP41CurrentStateRepair() {
	ctx := context.Background()
	pool, cleanup := s.setupDB()
	defer cleanup()

	m := metrics.NewMetrics(prometheus.NewRegistry())
	models, err := data.NewModels(pool, m.DB)
	s.Require().NoError(err)

	token := s.testEnv.SEP41ContractAddress
	tokenUUID := data.DeterministicContractID(token)

	// account1 holds the fixture remainder: minted TestSEP41MintStroops, then
	// transferred TestSEP41TransferStroops away (fixtures prepareSEP41TransferOp).
	corruptedHolder := s.testEnv.BalanceTestAccount1KP.Address()
	fixtureRemainder := strconv.Itoa(infrastructure.TestSEP41MintStroops - infrastructure.TestSEP41TransferStroops)

	// A fresh keypair no fixture ever touches, so balance() simulates to 0 for it:
	// the row inserted below exists only in the database.
	phantomHolder := keypair.MustRandom().Address()

	// The metadata service supplies the simulation primitive for the assertion
	// oracle below; the repair itself runs as the real CLI container.
	metadataPool := pond.NewPool(0)
	defer metadataPool.StopAndWait()
	metadataService, err := services.NewContractMetadataService(s.testEnv.RPCService, models.Contract, metadataPool)
	s.Require().NoError(err)

	// An independent reader over the same primitive, used to assert what the rows
	// should have converged to. Its values are cross-checked against the fixture
	// amounts below, so an agreeing pair of wrong readings cannot pass.
	truthReader := sep41svc.NewBalanceReader(metadataService)

	// ------------------------------------------------------------------
	// Preconditions: live ingestion is caught up and the migrated rows are
	// correct, so anything this test later observes is its own doing.
	// ------------------------------------------------------------------
	s.Require().NoError(s.testEnv.Containers.WaitForIngestCatchup(ctx))

	balance, found := s.mustReadBalance(ctx, pool, corruptedHolder, tokenUUID)
	s.Require().True(found, "account1 should hold the custom SEP-41 token before corruption")
	s.Require().Equal(fixtureRemainder, balance, "account1's migrated balance should be the fixture remainder")

	_, found = s.mustReadBalance(ctx, pool, phantomHolder, tokenUUID)
	s.Require().False(found, "the phantom holder must not already have a row")

	// ------------------------------------------------------------------
	// Corrupt current state: a wrong value on a real row, and a whole row
	// for a holder with nothing on-chain. Both must stay repairable, which
	// means neither may end up stamped above the ledger repair simulates at:
	// the corrupted row keeps its migration stamp, and the fabricated row
	// borrows that same migration-era stamp — where a bad fold would have
	// left it — rather than a current ledger.
	// ------------------------------------------------------------------
	corruptedAccount, err := types.AddressBytea(corruptedHolder).Value()
	s.Require().NoError(err)

	var staleLedger uint32
	s.Require().NoError(pool.QueryRow(ctx,
		`SELECT last_modified_ledger FROM sep41_balances WHERE account_id = $1 AND contract_id = $2`,
		corruptedAccount, tokenUUID).Scan(&staleLedger))

	tag, err := pool.Exec(ctx,
		`UPDATE sep41_balances SET balance = balance * $3 WHERE account_id = $1 AND contract_id = $2`,
		corruptedAccount, tokenUUID, sep41CorruptionFactor)
	s.Require().NoError(err)
	s.Require().EqualValues(1, tag.RowsAffected(), "corruption should have hit exactly one row")

	phantomAccount, err := types.AddressBytea(phantomHolder).Value()
	s.Require().NoError(err)
	_, err = pool.Exec(ctx,
		`INSERT INTO sep41_balances (account_id, contract_id, balance, last_modified_ledger)
		 VALUES ($1, $2, $3::numeric, $4)`,
		phantomAccount, tokenUUID, sep41PhantomBalance, int32(staleLedger))
	s.Require().NoError(err)

	// ------------------------------------------------------------------
	// Repair via the real CLI container (same pattern as protocol-setup and
	// protocol-migrate in DataMigrationTestSuite), scoped to this token so the
	// SAC and Blend rows sharing the table are untouched. The container persists
	// as "wallet-backend-protocol-repair" for `docker logs`.
	// ------------------------------------------------------------------
	exitCode, repairLogs, err := s.testEnv.Containers.RunWalletBackendCommand(ctx,
		"wallet-backend-protocol-repair",
		fmt.Sprintf("protocol-repair current-state --protocol %s --contract %s", sep41ProtocolID, token), nil)
	s.Require().NoError(err)
	s.Require().Zerof(exitCode,
		"protocol-repair should exit 0 (see `docker logs wallet-backend-protocol-repair`); logs:\n%s", repairLogs)

	s.Run("corrupted row converges to the simulated balance", func() {
		truth, _, truthErr := truthReader.ReadBalance(ctx, token, corruptedHolder)
		s.Require().NoError(truthErr)
		s.Require().Equal(fixtureRemainder, truth,
			"on-chain balance() should still be the fixture remainder — the test only corrupted the database")

		repaired, ok := s.mustReadBalance(ctx, pool, corruptedHolder, tokenUUID)
		s.Require().True(ok, "repair must not have removed a holder with a non-zero on-chain balance")
		s.Assert().Equal(truth, repaired, "repaired row should equal the simulated balance()")
	})

	s.Run("phantom row becomes a permanent zero row hidden from readers", func() {
		// Repair rewrites the fabricated row to the contract's answer (0) and keeps it:
		// the row's ledger stamp is what the fold's strict-monotone guard checks stale
		// deltas against, so zero rows are never deleted. Readers never see it —
		// GetByAccount filters balance <> 0.
		balance, ok := s.mustReadBalance(ctx, pool, phantomHolder, tokenUUID)
		s.Require().True(ok, "the zero row must persist as a stale-delta barrier")
		s.Assert().Equal("0", balance, "the fabricated balance should have been rewritten to 0")

		apiBalances, err := models.SEP41.Balances.GetByAccount(ctx, phantomHolder, nil, nil, sep41data.SortASC)
		s.Require().NoError(err)
		s.Assert().Empty(apiBalances, "readers must not see the zero row")
	})

	s.Run("correct rows are left alone", func() {
		// The holder contract's row was in scope and verified, but never wrong.
		expected := strconv.Itoa(infrastructure.TestSEP41MintStroops)
		balance, ok := s.mustReadBalance(ctx, pool, s.testEnv.HolderContractAddress, tokenUUID)
		s.Require().True(ok, "the holder contract should still hold the custom SEP-41 token")
		s.Assert().Equal(expected, balance, "repair should not have moved an already-correct row")
	})

	s.Run("live fold applies on top of the repaired row", func() {
		// The repaired row carries the simulation's ledger R. A mint lands at some
		// ledger > R, so BatchApplyDeltas' strict-monotone guard admits its delta —
		// the case that distinguishes a repair stamp from a wedged row.
		s.testEnv.Containers.MintSEP41Tokens(ctx, s.T(), token, corruptedHolder, sep41RepairMintStroops)
		s.Require().NoError(s.testEnv.Containers.WaitForIngestCatchup(ctx))

		expected := strconv.Itoa(infrastructure.TestSEP41MintStroops - infrastructure.TestSEP41TransferStroops + sep41RepairMintStroops)

		// The protocol cursor trails the ingest cursor slightly, so poll rather than
		// assert once. Assert (not Require) so that a persistent DB error is reported by
		// mustReadBalance below, on the test goroutine, instead of as a bare poll timeout.
		s.Assert().Eventuallyf(func() bool {
			balance, ok, readErr := s.readBalance(ctx, pool, corruptedHolder, tokenUUID)
			return readErr == nil && ok && balance == expected
		}, 60*time.Second, 2*time.Second,
			"account1's SEP-41 balance never reached %s after the post-repair mint of %d", expected, sep41RepairMintStroops)

		folded, ok := s.mustReadBalance(ctx, pool, corruptedHolder, tokenUUID)
		s.Require().True(ok, "account1's row should still exist after the post-repair mint")
		s.Assert().Equal(expected, folded, "the post-repair mint should have folded onto the repaired row")

		truth, _, truthErr := truthReader.ReadBalance(ctx, token, corruptedHolder)
		s.Require().NoError(truthErr)
		s.Assert().Equal(expected, truth, "the folded balance should match on-chain balance() again")
	})
}

// readBalance returns the holder's balance for the token as a decimal string,
// and whether the row exists at all — zero balances are represented by row
// absence, so the two outcomes are distinct.
//
// It reports errors rather than asserting because it is also called from an
// Eventually condition, which testify runs on its own goroutine (see
// assert.Eventually's `go checkCond()`); a Require failure there would Goexit a
// non-test goroutine, swallowing the real error and surfacing it as an opaque
// poll timeout. Call mustReadBalance from the test goroutine instead.
func (s *CurrentStateRepairTestSuite) readBalance(ctx context.Context, pool *pgxpool.Pool, holder string, tokenUUID uuid.UUID) (string, bool, error) {
	account, err := types.AddressBytea(holder).Value()
	if err != nil {
		return "", false, fmt.Errorf("encoding holder address %s: %w", holder, err)
	}

	var balance string
	err = pool.QueryRow(ctx,
		`SELECT balance::text FROM sep41_balances WHERE account_id = $1 AND contract_id = $2`,
		account, tokenUUID).Scan(&balance)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return "", false, nil
	case err != nil:
		return "", false, fmt.Errorf("reading SEP-41 balance of %s: %w", holder, err)
	}
	return balance, true, nil
}

// mustReadBalance is readBalance for the test goroutine, where failing fast on a
// DB error is what we want.
func (s *CurrentStateRepairTestSuite) mustReadBalance(ctx context.Context, pool *pgxpool.Pool, holder string, tokenUUID uuid.UUID) (string, bool) {
	balance, found, err := s.readBalance(ctx, pool, holder, tokenUUID)
	s.Require().NoError(err)
	return balance, found
}

func TestCurrentStateRepairTestSuiteStandalone(t *testing.T) {
	t.Skip("Run via TestIntegrationTests")
}
