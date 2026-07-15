package integrationtests

// Blend v2 integration coverage: BlendMigrationTestSuite exercises the real
// protocol-setup -> protocol-migrate (current-state + history) flow against a
// deployed Blend v2 stack, mirroring DataMigrationTestSuite's real-binary
// verification strategy (see data_migration_test.go's doc comment) but for a
// protocol whose contracts are uploaded WHILE live ingestion is already
// running, so classification and blend_pools enrichment happen in real time
// rather than requiring an explicit backfill. BlendLiveIngestionTestSuite then
// drives phase-2 ops (withdrawals, backstop queue/dequeue, emissions,
// liquidation) under live ingestion and asserts the result over GraphQL,
// mirroring AccountBalancesAfterLiveIngestionTestSuite's API-surface
// verification strategy.

import (
	"context"
	"crypto/rand"
	"fmt"
	"math"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/stellar/wallet-backend/internal/data"
	blenddata "github.com/stellar/wallet-backend/internal/data/blend"
	"github.com/stellar/wallet-backend/internal/db"
	indexertypes "github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/integrationtests/infrastructure"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/utils"
	wbtypes "github.com/stellar/wallet-backend/pkg/wbclient/types"
)

const blendProtocolID = "BLEND"

// randomContractAddress generates a random, syntactically valid but unknown
// C-address, mirroring resolvers/setup_test.go's helper of the same name (not
// importable from this package).
func randomContractAddress(t *testing.T) string {
	t.Helper()
	var raw [32]byte
	_, err := rand.Read(raw[:])
	require.NoError(t, err)
	addr, err := strkey.Encode(strkey.VersionByteContract, raw[:])
	require.NoError(t, err)
	return addr
}

// parseBigIntStr parses a plain base-10 integer string — the raw on-chain
// amount most Blend GraphQL/DB fields carry (unlike net_supplied/net_borrowed,
// see parseBlendDecimalToBigInt).
func parseBigIntStr(t *testing.T, s string) *big.Int {
	t.Helper()
	n, ok := new(big.Int).SetString(s, 10)
	require.Truef(t, ok, "parsing blend integer %q", s)
	return n
}

// parseBlendDecimalToBigInt parses blend_positions.net_supplied/net_borrowed,
// which — per blend_positions.go's parseBigInt doc comment — may carry a
// decimal point (unlike every other Blend integer column); the fractional
// part is truncated, mirroring internal/services/blend/rates.go's
// ParseDecimalToBigInt.
func parseBlendDecimalToBigInt(t *testing.T, s string) *big.Int {
	t.Helper()
	intPart := s
	if idx := strings.IndexByte(s, '.'); idx >= 0 {
		intPart = s[:idx]
	}
	n, ok := new(big.Int).SetString(intPart, 10)
	require.Truef(t, ok, "parsing blend decimal %q", s)
	return n
}

// findPositionByIndex returns the blend_positions row for reserveIndex, or
// nil if absent.
func findPositionByIndex(positions []blenddata.Position, reserveIndex int32) *blenddata.Position {
	for i := range positions {
		if positions[i].ReserveIndex == reserveIndex {
			return &positions[i]
		}
	}
	return nil
}

// findReserveByAsset returns the BlendReserve entry for assetID, or nil.
func findReserveByAsset(reserves []wbtypes.BlendReserve, assetID string) *wbtypes.BlendReserve {
	for i := range reserves {
		if reserves[i].AssetContractID == assetID {
			return &reserves[i]
		}
	}
	return nil
}

// findReservePositionByAsset returns the BlendReservePosition entry for assetID, or nil.
func findReservePositionByAsset(reserves []wbtypes.BlendReservePosition, assetID string) *wbtypes.BlendReservePosition {
	for i := range reserves {
		if reserves[i].AssetContractID == assetID {
			return &reserves[i]
		}
	}
	return nil
}

// findEarnOptionByAsset returns the BlendEarnOption entry for assetID, or nil.
func findEarnOptionByAsset(options []wbtypes.BlendEarnOption, assetID string) *wbtypes.BlendEarnOption {
	for i := range options {
		if options[i].AssetContractID == assetID {
			return &options[i]
		}
	}
	return nil
}

// ============================================================================
// BlendMigrationTestSuite
// ============================================================================

// BlendMigrationTestSuite exercises the real protocol-setup -> protocol-migrate
// (current-state + history) deployment flow for Blend v2, against a stack
// deployed by SetupBlendStack and driven through phase-1 ops
// (SubmitBlendPhase1Ops) by main_test.go before this suite runs.
//
// Unlike SEP-41 (see DataMigrationTestSuite), the Blend pool/backstop
// contracts are uploaded WHILE live ingestion is already running (main_test.go
// calls SetupBlendStack well after the ingest container starts), so live
// ingestion's per-ledger classification (services.ingestService.
// runClassification) and the Blend validator's blend_pools enrichment both
// happen in real time — protocol-setup's classification pass and this suite's
// pre-checks should find that work already done.
//
// NOTE: like DataMigrationTestSuite, this suite deliberately has no teardown:
// the migrated state it produces is asserted again (over GraphQL, post
// phase-2 ops) by BlendLiveIngestionTestSuite, which runs later.
type BlendMigrationTestSuite struct {
	suite.Suite
	testEnv *infrastructure.TestEnvironment
	stack   *infrastructure.BlendStack
}

func (s *BlendMigrationTestSuite) setupDB() (*pgxpool.Pool, func()) {
	ctx := context.Background()
	dbURL, err := s.testEnv.Containers.GetWalletDBConnectionString(ctx)
	s.Require().NoError(err)

	pool, err := db.OpenDBConnectionPool(ctx, dbURL)
	s.Require().NoError(err)

	return pool, func() { pool.Close() }
}

func (s *BlendMigrationTestSuite) setupModels(pool *pgxpool.Pool) *data.Models {
	dbMetrics := metrics.NewMetrics(prometheus.NewRegistry()).DB
	models, err := data.NewModels(pool, dbMetrics)
	s.Require().NoError(err)

	return models
}

// addressBytes converts a strkey address to its 33-byte BYTEA representation,
// mirroring internal/data/blend's addressToBytes (unexported, so not directly
// reusable from this package).
func (s *BlendMigrationTestSuite) addressBytes(addr string) []byte {
	raw, err := indexertypes.AddressBytea(addr).Value()
	s.Require().NoError(err)
	b, ok := raw.([]byte)
	s.Require().True(ok)
	return b
}

func (s *BlendMigrationTestSuite) ingestStoreKeyExists(ctx context.Context, pool *pgxpool.Pool, key string) bool {
	var exists bool
	err := pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM ingest_store WHERE key = $1)`, key).Scan(&exists)
	s.Require().NoError(err)
	return exists
}

func (s *BlendMigrationTestSuite) countRows(ctx context.Context, pool *pgxpool.Pool, table string) int {
	var count int
	err := pool.QueryRow(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s`, table)).Scan(&count)
	s.Require().NoError(err)
	return count
}

func (s *BlendMigrationTestSuite) countLendingStateChanges(ctx context.Context, pool *pgxpool.Pool) int {
	var count int
	err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM state_changes WHERE state_change_category = 'LENDING'`).Scan(&count)
	s.Require().NoError(err)
	return count
}

func (s *BlendMigrationTestSuite) countLendingReason(ctx context.Context, pool *pgxpool.Pool, address, reason string) int {
	var count int
	err := pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM state_changes WHERE account_id = $1 AND state_change_category = 'LENDING' AND state_change_reason = $2`,
		s.addressBytes(address), reason).Scan(&count)
	s.Require().NoError(err)
	return count
}

// assertAllLendingPoolIDs asserts every LENDING state-change row's
// key_value->>'poolId' equals poolID — the phase-1 fixture only ever produces
// pool-scoped rows (no backstop CLAIM, whose poolId is NULL), so every row at
// this point in the run must agree.
func (s *BlendMigrationTestSuite) assertAllLendingPoolIDs(ctx context.Context, pool *pgxpool.Pool, poolID string) {
	var mismatches int
	err := pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM state_changes WHERE state_change_category = 'LENDING' AND (key_value->>'poolId') IS DISTINCT FROM $1`,
		poolID).Scan(&mismatches)
	s.Require().NoError(err)
	s.Assert().Zero(mismatches, "every phase-1 LENDING state change should carry poolId=%s", poolID)
}

// TestProtocolSetupThenMigration mirrors DataMigrationTestSuite's real
// deployment order: pre-check -> protocol-setup -> protocol-migrate
// current-state -> protocol-migrate history -> assert.
func (s *BlendMigrationTestSuite) TestProtocolSetupThenMigration() {
	ctx := context.Background()
	pool, cleanup := s.setupDB()
	defer cleanup()

	models := s.setupModels(pool)
	stack := s.stack

	// ------------------------------------------------------------------
	// Phase 1: pre-checks. Live ingestion classified the pool/backstop
	// wasms and the Blend validator enriched blend_pools in real time
	// (both contracts were uploaded after the ingest container started —
	// see the suite doc comment), so this state already exists even
	// though protocol-setup/protocol-migrate haven't run yet.
	// ------------------------------------------------------------------
	classifiedContracts, err := models.ProtocolContracts.GetByProtocolID(ctx, blendProtocolID)
	s.Require().NoError(err)
	s.Require().GreaterOrEqualf(len(classifiedContracts), 2,
		"live ingestion should have already classified at least the Blend pool and backstop wasms")

	poolsPre, err := models.Blend.Pools.GetByIDs(ctx, []string{stack.PoolID})
	s.Require().NoError(err)
	s.Require().Len(poolsPre, 1, "the Blend validator should have already enriched blend_pools for the pool")
	s.Assert().Equal(stack.OracleID, string(poolsPre[0].OracleContractID))

	s.Assert().Zero(s.countRows(ctx, pool, "blend_positions"), "blend_positions must be empty before migration runs")
	s.Assert().Zero(s.countRows(ctx, pool, "blend_reserves"), "blend_reserves must be empty before migration runs")
	s.Assert().Zero(s.countRows(ctx, pool, "blend_backstop_positions"), "blend_backstop_positions must be empty before migration runs")
	s.Assert().Zero(s.countLendingStateChanges(ctx, pool), "no LENDING state changes should exist before migration runs")

	s.Assert().False(s.ingestStoreKeyExists(ctx, pool, utils.ProtocolCurrentStateCursorName(blendProtocolID)),
		"current-state cursor should not exist before migration runs")
	s.Assert().False(s.ingestStoreKeyExists(ctx, pool, utils.ProtocolHistoryCursorName(blendProtocolID)),
		"history cursor should not exist before migration runs")

	// ------------------------------------------------------------------
	// Phase 2: protocol-setup marks BLEND's classification status.
	// ------------------------------------------------------------------
	exitCode, logs, err := s.testEnv.Containers.RunWalletBackendCommand(ctx,
		"wallet-backend-blend-protocol-setup", "protocol-setup --protocol-id "+blendProtocolID, nil)
	s.Require().NoError(err)
	s.Require().Zerof(exitCode, "protocol-setup should exit 0 (see `docker logs wallet-backend-blend-protocol-setup`); logs:\n%s", logs)

	// ------------------------------------------------------------------
	// Phase 3: protocol-migrate current-state (from the stack's start
	// ledger) and protocol-migrate history (from oldest_ingest_ledger),
	// both against the datastore backend fed by a host-side exporter —
	// see data_migration_test.go's TestProtocolSetupThenCurrentStateMigration
	// for the full rationale.
	// ------------------------------------------------------------------
	oldestLedger, err := models.IngestStore.Get(ctx, data.OldestLedgerCursorName)
	s.Require().NoError(err)
	s.Require().Greater(oldestLedger, uint32(0), "oldest_ledger_cursor should be set by live ingestion")

	rpcURL, err := s.testEnv.Containers.RPCContainer.GetConnectionString(ctx)
	s.Require().NoError(err)
	minioEndpoint, err := s.testEnv.Containers.MinioContainer.GetConnectionString(ctx)
	s.Require().NoError(err)
	// The exporter must start at the oldest cursor (not stack.StartLedger):
	// protocol-migrate history reads its own start from oldest_ingest_ledger.
	stopExporter := infrastructure.StartLedgerExporter(s.T(), rpcURL, minioEndpoint, oldestLedger)
	defer stopExporter()

	currentStateCmd := fmt.Sprintf("protocol-migrate current-state --protocol-id %s "+
		"--ledger-backend-type datastore --start-ledger %d --window-size 10", blendProtocolID, stack.StartLedger)
	exitCode, logs, err = s.testEnv.Containers.RunWalletBackendCommand(ctx,
		"wallet-backend-blend-protocol-migrate-current-state", currentStateCmd, s.testEnv.Containers.DatastoreEnv())
	s.Require().NoError(err)
	s.Require().Zerof(exitCode, "protocol-migrate current-state should exit 0 (see `docker logs wallet-backend-blend-protocol-migrate-current-state`); logs:\n%s", logs)

	historyCmd := fmt.Sprintf("protocol-migrate history --protocol-id %s "+
		"--ledger-backend-type datastore --window-size 10", blendProtocolID)
	exitCode, logs, err = s.testEnv.Containers.RunWalletBackendCommand(ctx,
		"wallet-backend-blend-protocol-migrate-history", historyCmd, s.testEnv.Containers.DatastoreEnv())
	s.Require().NoError(err)
	s.Require().Zerof(exitCode, "protocol-migrate history should exit 0 (see `docker logs wallet-backend-blend-protocol-migrate-history`); logs:\n%s", logs)

	// ------------------------------------------------------------------
	// Phase 4: post-assertions.
	// ------------------------------------------------------------------
	protocols, err := models.Protocols.GetByIDs(ctx, []string{blendProtocolID})
	s.Require().NoError(err)
	s.Require().Len(protocols, 1)
	s.Assert().Equal(data.StatusSuccess, protocols[0].ClassificationStatus, "classification status should be success")
	s.Assert().Equal(data.StatusSuccess, protocols[0].CurrentStateMigrationStatus, "current-state migration status should be success")
	s.Assert().Equal(data.StatusSuccess, protocols[0].HistoryMigrationStatus, "history migration status should be success")

	s.Assert().True(s.ingestStoreKeyExists(ctx, pool, utils.ProtocolCurrentStateCursorName(blendProtocolID)))
	s.Assert().True(s.ingestStoreKeyExists(ctx, pool, utils.ProtocolHistoryCursorName(blendProtocolID)))
	currentStateCursor, err := models.IngestStore.Get(ctx, utils.ProtocolCurrentStateCursorName(blendProtocolID))
	s.Require().NoError(err)
	s.Assert().GreaterOrEqual(currentStateCursor, stack.StartLedger, "current-state cursor should have advanced past its init value")

	// The phase-1 fixture performs no claims, so the claimed-total accumulators
	// must exist (created by migration) and be empty — a guard that current-state
	// indexing folds nothing spurious into them.
	s.Assert().Zero(s.countRows(ctx, pool, "blend_pool_claimed"))
	s.Assert().Zero(s.countRows(ctx, pool, "blend_backstop_claimed"))

	// blend_pools
	pools, err := models.Blend.Pools.GetByIDs(ctx, []string{stack.PoolID})
	s.Require().NoError(err)
	s.Require().Len(pools, 1)
	poolRow := pools[0]
	s.Require().NotNil(poolRow.Status)
	s.Assert().Equal(int32(0), *poolRow.Status)
	s.Require().NotNil(poolRow.BackstopRate)
	s.Assert().Equal(int32(1_000_000), *poolRow.BackstopRate)
	s.Require().NotNil(poolRow.MaxPositions)
	s.Assert().Equal(int32(4), *poolRow.MaxPositions)
	s.Require().NotNil(poolRow.Name)
	s.Assert().Equal(stack.PoolName, *poolRow.Name)
	s.Assert().Equal(stack.OracleID, string(poolRow.OracleContractID))

	// blend_reserves: exactly 2 rows (USDC index 0, XLM index 1).
	reserves, err := models.Blend.Reserves.GetByPools(ctx, []string{stack.PoolID})
	s.Require().NoError(err)
	s.Require().Len(reserves, 2)
	usdcReserve := findPositionInReserves(reserves, 0)
	xlmReserve := findPositionInReserves(reserves, 1)
	s.Require().NotNil(usdcReserve)
	s.Require().NotNil(xlmReserve)
	s.Assert().Equal(stack.USDCTokenID, string(usdcReserve.AssetContractID))
	s.Assert().Equal(stack.XLMTokenID, string(xlmReserve.AssetContractID))
	s.Assert().True(usdcReserve.Enabled)
	s.Assert().True(xlmReserve.Enabled)
	s.Assert().NotEmpty(usdcReserve.BRate)
	s.Assert().NotEmpty(usdcReserve.DRate)
	s.Assert().NotEmpty(xlmReserve.BRate)
	s.Assert().NotEmpty(xlmReserve.DRate)

	// blend_positions: supplier.
	supplierPositions, err := models.Blend.Positions.GetByAccount(ctx, stack.Supplier.Address())
	s.Require().NoError(err)
	supplierUSDC := findPositionByIndex(supplierPositions, 0)
	supplierXLM := findPositionByIndex(supplierPositions, 1)
	s.Require().NotNil(supplierUSDC)
	s.Require().NotNil(supplierXLM)
	s.Assert().Greater(parseBigIntStr(s.T(), supplierUSDC.SupplyBTokens).Sign(), 0)
	s.Assert().Greater(parseBigIntStr(s.T(), supplierUSDC.CollateralBTokens).Sign(), 0)
	s.Assert().Greater(parseBigIntStr(s.T(), supplierXLM.SupplyBTokens).Sign(), 0)

	expectedSupplierNetSuppliedUSDC := big.NewInt(infrastructure.BlendSupplierSupplyUSDC + infrastructure.BlendSupplierSupplyCollateralUSDC)
	s.Assert().Zero(parseBlendDecimalToBigInt(s.T(), supplierUSDC.NetSupplied).Cmp(expectedSupplierNetSuppliedUSDC),
		"supplier's USDC net_supplied should equal SupplyUSDC + SupplyCollateralUSDC")
	expectedSupplierNetSuppliedXLM := big.NewInt(infrastructure.BlendSupplierSupplyXLM)
	s.Assert().Zero(parseBlendDecimalToBigInt(s.T(), supplierXLM.NetSupplied).Cmp(expectedSupplierNetSuppliedXLM),
		"supplier's XLM net_supplied should equal SupplyXLM")

	// blend_positions: borrower.
	borrowerPositions, err := models.Blend.Positions.GetByAccount(ctx, stack.Borrower.Address())
	s.Require().NoError(err)
	borrowerUSDC := findPositionByIndex(borrowerPositions, 0)
	borrowerXLM := findPositionByIndex(borrowerPositions, 1)
	s.Require().NotNil(borrowerUSDC)
	s.Require().NotNil(borrowerXLM)
	s.Assert().Greater(parseBigIntStr(s.T(), borrowerUSDC.CollateralBTokens).Sign(), 0)
	s.Assert().Greater(parseBigIntStr(s.T(), borrowerXLM.LiabilityDTokens).Sign(), 0)

	expectedBorrowerNetBorrowedXLM := big.NewInt(infrastructure.BlendBorrowerBorrowXLM - infrastructure.BlendBorrowerRepayXLM)
	s.Assert().Zero(parseBlendDecimalToBigInt(s.T(), borrowerXLM.NetBorrowed).Cmp(expectedBorrowerNetBorrowedXLM),
		"borrower's XLM net_borrowed should equal BorrowXLM - RepayXLM")

	// blend_backstop_positions: whale.
	backstopPositions, err := models.Blend.BackstopPositions.GetByAccount(ctx, stack.Whale.Address())
	s.Require().NoError(err)
	s.Require().Len(backstopPositions, 1)
	whaleBackstop := backstopPositions[0]
	s.Assert().Equal(stack.PoolID, string(whaleBackstop.PoolContractID))
	s.Assert().Equal("500000000000", whaleBackstop.Shares, "first-ever backstop deposit mints shares 1:1")
	s.Assert().Empty(whaleBackstop.Q4W, "no queued withdrawals in phase 1")

	// blend_backstop_pools.
	backstopPools, err := models.Blend.BackstopPools.GetByIDs(ctx, []string{stack.PoolID})
	s.Require().NoError(err)
	s.Require().Len(backstopPools, 1)
	s.Assert().Greater(parseBigIntStr(s.T(), backstopPools[0].Shares).Sign(), 0)
	s.Assert().Greater(parseBigIntStr(s.T(), backstopPools[0].Tokens).Sign(), 0)

	// state_changes: LENDING counts by (account, reason).
	s.Assert().Equal(2, s.countLendingReason(ctx, pool, stack.Supplier.Address(), "SUPPLY"))
	s.Assert().Equal(1, s.countLendingReason(ctx, pool, stack.Supplier.Address(), "SUPPLY_COLLATERAL"))
	s.Assert().Equal(1, s.countLendingReason(ctx, pool, stack.Borrower.Address(), "SUPPLY_COLLATERAL"))
	s.Assert().Equal(1, s.countLendingReason(ctx, pool, stack.Borrower.Address(), "BORROW"))
	s.Assert().Equal(1, s.countLendingReason(ctx, pool, stack.Borrower.Address(), "REPAY"))
	s.Assert().Equal(1, s.countLendingReason(ctx, pool, stack.Whale.Address(), "BACKSTOP_DEPOSIT"))

	s.assertAllLendingPoolIDs(ctx, pool, stack.PoolID)
}

// findPositionInReserves returns the blend_reserves row for reserveIndex, or nil.
func findPositionInReserves(reserves []blenddata.Reserve, reserveIndex int32) *blenddata.Reserve {
	for i := range reserves {
		if reserves[i].ReserveIndex == reserveIndex {
			return &reserves[i]
		}
	}
	return nil
}

// ============================================================================
// BlendLiveIngestionTestSuite
// ============================================================================

// BlendLiveIngestionTestSuite drives Blend v2 phase-2 ops (withdrawals,
// backstop queue/dequeue, emissions claims, liquidation) under live ingestion
// and asserts the result primarily over GraphQL (testEnv.WBClient), with a
// handful of DB-level assertions for state the GraphQL surface doesn't expose
// directly (emissions config rows, post-liquidation cost-basis positions).
//
// All steps run inside a single test method — rather than testify's
// alphabetically-ordered separate methods — since the steps are strictly
// sequential (each depends on the previous step's on-chain/DB state).
type BlendLiveIngestionTestSuite struct {
	suite.Suite
	testEnv *infrastructure.TestEnvironment
	stack   *infrastructure.BlendStack
}

func (s *BlendLiveIngestionTestSuite) setupDB() (*pgxpool.Pool, func()) {
	ctx := context.Background()
	dbURL, err := s.testEnv.Containers.GetWalletDBConnectionString(ctx)
	s.Require().NoError(err)

	pool, err := db.OpenDBConnectionPool(ctx, dbURL)
	s.Require().NoError(err)

	return pool, func() { pool.Close() }
}

func (s *BlendLiveIngestionTestSuite) setupModels(pool *pgxpool.Pool) *data.Models {
	dbMetrics := metrics.NewMetrics(prometheus.NewRegistry()).DB
	models, err := data.NewModels(pool, dbMetrics)
	s.Require().NoError(err)

	return models
}

// assertApproxRelative asserts actual is within tolerance (a fraction, e.g.
// 0.01 for 1%) of expected.
func (s *BlendLiveIngestionTestSuite) assertApproxRelative(expected, actual, tolerance float64) {
	diff := math.Abs(actual-expected) / expected
	s.Assert().LessOrEqualf(diff, tolerance, "expected ~%v, got %v (relative diff %v)", expected, actual, diff)
}

// fetchLendingChanges fetches an account's LENDING state changes filtered to
// one reason, type-asserting every node to *wbtypes.LendingChange.
func (s *BlendLiveIngestionTestSuite) fetchLendingChanges(ctx context.Context, address, reason string) []*wbtypes.LendingChange {
	category := "LENDING"
	first := int32(50)
	conn, err := s.testEnv.WBClient.GetAccountStateChanges(ctx, address, nil, nil, &category, &reason, nil, nil, &first, nil, nil, nil)
	s.Require().NoError(err)
	s.Require().NotNil(conn)

	out := make([]*wbtypes.LendingChange, 0, len(conn.Edges))
	for _, e := range conn.Edges {
		s.Require().NotNil(e.Node, "expected a non-null state change node for reason %s", reason)
		lc, ok := e.Node.(*wbtypes.LendingChange)
		s.Require().Truef(ok, "expected *LendingChange node for reason %s, got %T", reason, e.Node)
		out = append(out, lc)
	}
	return out
}

// requireOneLendingChange asserts changes has exactly one entry and returns it.
func (s *BlendLiveIngestionTestSuite) requireOneLendingChange(changes []*wbtypes.LendingChange, reason string) *wbtypes.LendingChange {
	s.Require().Lenf(changes, 1, "expected exactly one LENDING/%s state change", reason)
	return changes[0]
}

// assertTokenPoolAmount asserts lc carries a non-nil TokenID equal to
// wantToken, a non-nil PoolID equal to wantPool, and a positive Amount — the
// shape of WITHDRAW, WITHDRAW_COLLATERAL, and a pool-side CLAIM (see
// internal/services/blend/events.go's decodeWithdrawAmbiguous/
// decodeClaimAmbiguous).
func (s *BlendLiveIngestionTestSuite) assertTokenPoolAmount(lc *wbtypes.LendingChange, wantToken, wantPool string) {
	s.Require().NotNil(lc.TokenID)
	s.Assert().Equal(wantToken, *lc.TokenID)
	s.Require().NotNil(lc.PoolID)
	s.Assert().Equal(wantPool, *lc.PoolID)
	s.Require().NotNil(lc.Amount)
	s.Assert().Greater(parseBigIntStr(s.T(), *lc.Amount).Sign(), 0)
}

func (s *BlendLiveIngestionTestSuite) TestBlendLiveIngestion() {
	ctx := context.Background()
	stack := s.stack

	// ------------------------------------------------------------------
	// Step 1: restart ingest with Blend price-snapshot env wired in.
	// ------------------------------------------------------------------
	err := s.testEnv.RestartIngestContainer(ctx, map[string]string{
		"BLEND_PRICE_INTERVAL":          "5s",
		"BLEND_BACKSTOP_LP_CONTRACT_ID": stack.CometID,
	})
	s.Require().NoError(err)
	s.Require().NoError(s.testEnv.Containers.WaitForIngestCatchup(ctx))

	pool, cleanup := s.setupDB()
	defer cleanup()
	models := s.setupModels(pool)

	// ------------------------------------------------------------------
	// Step 2: oracle prices. Exactly 4 rows: (oracle,USDC), (oracle,XLM),
	// (comet,comet) [LP self-priced], (comet,BLND) [sibling]. BLND is not
	// a reserve, so the SEP-40 leg never snapshots it under the oracle.
	// ------------------------------------------------------------------
	s.Require().Eventually(func() bool {
		prices, priceErr := models.Blend.OraclePrices.GetByOracles(ctx, []string{stack.OracleID, stack.CometID})
		if priceErr != nil || len(prices) != 4 {
			return false
		}
		now := time.Now().Unix()
		for _, p := range prices {
			if p.PriceDecimals != 7 {
				return false
			}
			if p.PriceTimestamp < now-120 {
				return false
			}
		}
		return true
	}, 90*time.Second, 5*time.Second, "expected 4 fresh blend_oracle_prices rows within 90s")

	// ------------------------------------------------------------------
	// Step 3: phase-2 ops, observing the whale's queued backstop position
	// mid-flight (between queue and dequeue).
	// ------------------------------------------------------------------
	var whaleQueuedPositions *wbtypes.BlendAccountPositions
	s.testEnv.Containers.SubmitBlendPhase2Ops(ctx, s.T(), stack, func() {
		s.Require().NoError(s.testEnv.Containers.WaitForIngestCatchup(ctx))
		var posErr error
		whaleQueuedPositions, posErr = s.testEnv.WBClient.GetAccountBlendPositions(ctx, stack.Whale.Address())
		s.Require().NoError(posErr)
	})
	s.Require().NoError(s.testEnv.Containers.WaitForIngestCatchup(ctx))
	// Let the 5s price interval re-snapshot the crashed USDC price.
	time.Sleep(6 * time.Second)

	s.assertQueuedWhalePositions(whaleQueuedPositions, stack)

	// ------------------------------------------------------------------
	// Step 4: blendPools.
	// ------------------------------------------------------------------
	poolsList, err := s.testEnv.WBClient.GetBlendPools(ctx)
	s.Require().NoError(err)
	s.Require().Len(poolsList, 1, "exactly one Blend pool should exist")
	catalogPool := poolsList[0]
	s.assertPoolCatalog(catalogPool, stack)

	// ------------------------------------------------------------------
	// Step 5: blendPool(known) / blendPool(unknown).
	// ------------------------------------------------------------------
	onePool, err := s.testEnv.WBClient.GetBlendPool(ctx, stack.PoolID)
	s.Require().NoError(err)
	s.Require().NotNil(onePool)
	s.Assert().Equal(catalogPool.Address, onePool.Address)
	s.Assert().Equal(catalogPool.Name, onePool.Name)

	unknownPool, err := s.testEnv.WBClient.GetBlendPool(ctx, randomContractAddress(s.T()))
	s.Require().NoError(err)
	s.Assert().Nil(unknownPool, "an unknown but valid pool address should resolve to nil, not an error")

	// ------------------------------------------------------------------
	// Step 6: blendEarnOptions.
	// ------------------------------------------------------------------
	earnOptions, err := s.testEnv.WBClient.GetBlendEarnOptions(ctx)
	s.Require().NoError(err)
	s.assertEarnOptions(earnOptions, stack)

	// ------------------------------------------------------------------
	// Step 7: supplier positions.
	// ------------------------------------------------------------------
	supplierPositions, err := s.testEnv.WBClient.GetAccountBlendPositions(ctx, stack.Supplier.Address())
	s.Require().NoError(err)
	s.assertSupplierPositions(supplierPositions, stack)

	// ------------------------------------------------------------------
	// Step 8: borrower positions.
	// ------------------------------------------------------------------
	borrowerPositions, err := s.testEnv.WBClient.GetAccountBlendPositions(ctx, stack.Borrower.Address())
	s.Require().NoError(err)
	s.assertBorrowerPositions(borrowerPositions, stack)

	// ------------------------------------------------------------------
	// Step 9: whale backstop (post-dequeue final state).
	// ------------------------------------------------------------------
	whalePositions, err := s.testEnv.WBClient.GetAccountBlendPositions(ctx, stack.Whale.Address())
	s.Require().NoError(err)
	s.assertWhaleFinalPositions(whalePositions, stack)

	// ------------------------------------------------------------------
	// Step 10: state changes.
	// ------------------------------------------------------------------
	s.assertStateChanges(ctx, stack)

	// ------------------------------------------------------------------
	// Step 11: emissions DB rows.
	// ------------------------------------------------------------------
	s.assertEmissionsRows(ctx, models, stack)

	// ------------------------------------------------------------------
	// Step 12: post-liquidation positions (DB-level).
	// ------------------------------------------------------------------
	s.assertPostLiquidationPositions(ctx, models, stack)
}

// assertQueuedWhalePositions asserts the whale's backstop position while its
// withdrawal is queued (between BackstopQueueWithdrawal and
// BackstopDequeueWithdrawal).
//
// bp.Shares reflects only the ACTIVE (non-queued) share balance: entries.go's
// decodeBackstopUserBalance decodes it as a straight passthrough of the
// contract's on-chain UserBalance.shares field, and blend_positions.go's
// buildBackstopPosition doc comment states this explicitly ("bp.Shares holds
// only the ACTIVE share balance — queued-for-withdrawal shares live in
// bp.Q4W"). Queuing a withdrawal therefore moves shares out of the active
// balance into Q4W, so the active balance during the queued window is
// 500,000,000,000 (initial) - 50,000,000,000 (queued) = 450,000,000,000.
func (s *BlendLiveIngestionTestSuite) assertQueuedWhalePositions(positions *wbtypes.BlendAccountPositions, stack *infrastructure.BlendStack) {
	s.Require().NotNil(positions)
	s.Require().Len(positions.Backstop, 1)
	bp := positions.Backstop[0]
	s.Assert().Equal(stack.PoolID, bp.PoolAddress)
	s.Assert().Equal("450000000000", bp.Shares,
		"active shares should be reduced by the queued amount while the withdrawal is pending")

	s.Require().Len(bp.Q4W, 1)
	q := bp.Q4W[0]
	s.Assert().Equal("50000000000", q.Amount)
	now := time.Now().Unix()
	s.Assert().GreaterOrEqual(q.Expiration, now+16*86400)
	s.Assert().LessOrEqual(q.Expiration, now+18*86400)
	s.Assert().Greater(parseBigIntStr(s.T(), q.LpTokens).Sign(), 0)
	s.Assert().NotNil(q.UsdValue)
}

// assertPoolCatalog asserts blendPools/blendPool's pool-wide catalog view.
func (s *BlendLiveIngestionTestSuite) assertPoolCatalog(p wbtypes.BlendPool, stack *infrastructure.BlendStack) {
	s.Assert().Equal(stack.PoolID, p.Address)
	s.Require().NotNil(p.Name)
	s.Assert().Equal(stack.PoolName, *p.Name)
	s.Require().NotNil(p.Status)
	s.Assert().Equal(int32(0), *p.Status)
	s.Require().NotNil(p.BackstopRate)
	s.Assert().Equal(int32(1_000_000), *p.BackstopRate)
	s.Require().NotNil(p.MaxPositions)
	s.Assert().Equal(int32(4), *p.MaxPositions)
	s.Require().NotNil(p.OracleContractID)
	s.Assert().Equal(stack.OracleID, *p.OracleContractID)
	s.Assert().NotNil(p.SuppliedUsd)
	s.Assert().NotNil(p.BorrowedUsd)
	s.Assert().NotNil(p.BackstopUsd)
	s.Assert().NotNil(p.NetApy)
	s.Require().Len(p.Reserves, 2)

	usdc := findReserveByAsset(p.Reserves, stack.USDCTokenID)
	xlm := findReserveByAsset(p.Reserves, stack.XLMTokenID)
	s.Require().NotNil(usdc)
	s.Require().NotNil(xlm)

	s.Assert().True(usdc.Enabled)
	s.Assert().True(xlm.Enabled)

	s.Require().NotNil(usdc.CFactor)
	s.Assert().Equal(int32(9_500_000), *usdc.CFactor)
	s.Require().NotNil(usdc.LFactor)
	s.Assert().Equal(int32(9_500_000), *usdc.LFactor)
	s.Require().NotNil(xlm.CFactor)
	s.Assert().Equal(int32(9_000_000), *xlm.CFactor)
	s.Require().NotNil(xlm.LFactor)
	s.Assert().Equal(int32(9_000_000), *xlm.LFactor)

	s.Assert().NotNil(usdc.SupplyApy)
	s.Assert().NotNil(usdc.BorrowApy)
	s.Assert().NotNil(xlm.SupplyApy)
	s.Assert().NotNil(xlm.BorrowApy)

	s.Require().NotNil(usdc.PriceUsd)
	// USDC's oracle price was crashed to $0.005 in the liquidation fixture.
	s.assertApproxRelative(0.005, *usdc.PriceUsd, 0.01)
	s.Require().NotNil(xlm.PriceUsd)
	s.assertApproxRelative(0.10, *xlm.PriceUsd, 0.01)

	s.Assert().Greater(parseBigIntStr(s.T(), usdc.SuppliedTokens).Sign(), 0)
	s.Assert().Greater(parseBigIntStr(s.T(), xlm.SuppliedTokens).Sign(), 0)
	s.Assert().Greater(parseBigIntStr(s.T(), xlm.BorrowedTokens).Sign(), 0)

	s.Require().NotNil(xlm.Utilization)
	s.Assert().Greater(*xlm.Utilization, 0.0)

	s.Assert().NotNil(usdc.EmissionsSupplyApr, "USDC reserve has bToken emissions configured")
}

// assertEarnOptions asserts blendEarnOptions contains USDC and XLM entries
// with at least one pool option each.
func (s *BlendLiveIngestionTestSuite) assertEarnOptions(options []wbtypes.BlendEarnOption, stack *infrastructure.BlendStack) {
	usdcOpt := findEarnOptionByAsset(options, stack.USDCTokenID)
	xlmOpt := findEarnOptionByAsset(options, stack.XLMTokenID)
	s.Require().NotNil(usdcOpt, "expected a blendEarnOptions entry for USDC")
	s.Require().NotNil(xlmOpt, "expected a blendEarnOptions entry for XLM")

	for _, opt := range []*wbtypes.BlendEarnOption{usdcOpt, xlmOpt} {
		s.Require().NotEmpty(opt.Pools)
		var found *wbtypes.BlendEarnPoolOption
		for i := range opt.Pools {
			if opt.Pools[i].PoolAddress == stack.PoolID {
				found = &opt.Pools[i]
			}
		}
		s.Require().NotNilf(found, "expected pool option %s for asset %s", stack.PoolID, opt.AssetContractID)
		s.Assert().NotNil(found.SupplyApy)
	}

	for i := range usdcOpt.Pools {
		if usdcOpt.Pools[i].PoolAddress == stack.PoolID {
			s.Assert().NotNil(usdcOpt.Pools[i].EmissionsSupplyApr)
		}
	}
}

// assertSupplierPositions asserts the supplier's positions after phase-2
// withdrawals and an emissions claim.
func (s *BlendLiveIngestionTestSuite) assertSupplierPositions(positions *wbtypes.BlendAccountPositions, stack *infrastructure.BlendStack) {
	s.Require().NotNil(positions)
	s.Require().Len(positions.Pools, 1)
	pp := positions.Pools[0]
	s.Assert().Equal(stack.PoolID, pp.PoolAddress)
	s.Require().NotNil(pp.PoolName)
	s.Assert().Equal(stack.PoolName, *pp.PoolName)
	s.Assert().NotNil(pp.UsdValue)
	s.Assert().NotNil(pp.SuppliedUsd)
	s.Assert().NotNil(pp.NetApy)
	s.Assert().Greater(parseBigIntStr(s.T(), pp.ClaimedBlnd).Sign(), 0, "supplier claimed pool emissions in phase 2")

	usdcPos := findReservePositionByAsset(pp.Reserves, stack.USDCTokenID)
	xlmPos := findReservePositionByAsset(pp.Reserves, stack.XLMTokenID)
	s.Require().NotNil(usdcPos)
	s.Require().NotNil(xlmPos)

	s.Assert().Greater(parseBigIntStr(s.T(), usdcPos.SuppliedTokens).Sign(), 0)
	s.Assert().Greater(parseBigIntStr(s.T(), usdcPos.CollateralTokens).Sign(), 0)
	s.Assert().NotNil(usdcPos.SuppliedUsd)
	s.Assert().NotNil(usdcPos.PriceUsd)
	s.Assert().GreaterOrEqual(parseBigIntStr(s.T(), usdcPos.InterestEarned).Sign(), 0)
	s.Assert().GreaterOrEqual(parseBigIntStr(s.T(), usdcPos.EmissionsEarnedBlnd).Sign(), 0)

	s.Assert().Greater(parseBigIntStr(s.T(), xlmPos.SuppliedTokens).Sign(), 0)
}

// assertBorrowerPositions asserts the borrower's positions after the
// liquidation auction partially absorbed its debt and collateral.
func (s *BlendLiveIngestionTestSuite) assertBorrowerPositions(positions *wbtypes.BlendAccountPositions, stack *infrastructure.BlendStack) {
	s.Require().NotNil(positions)
	s.Require().Len(positions.Pools, 1)
	pp := positions.Pools[0]

	xlmPos := findReservePositionByAsset(pp.Reserves, stack.XLMTokenID)
	s.Require().NotNil(xlmPos)
	s.Assert().Greater(parseBigIntStr(s.T(), xlmPos.BorrowedTokens).Sign(), 0, "debt remains after a 50% auction fill")
	s.Assert().NotNil(xlmPos.BorrowApy)
	s.Assert().GreaterOrEqual(parseBigIntStr(s.T(), xlmPos.InterestPaid).Sign(), 0)

	usdcPos := findReservePositionByAsset(pp.Reserves, stack.USDCTokenID)
	s.Require().NotNil(usdcPos)
	collateralUSDC := parseBigIntStr(s.T(), usdcPos.CollateralTokens)
	s.Assert().Greater(collateralUSDC.Sign(), 0)
	s.Assert().Less(collateralUSDC.Cmp(big.NewInt(infrastructure.BlendBorrowerSupplyCollateralUSDC)), 0,
		"the liquidation auction should have taken some of the borrower's USDC collateral lot")
}

// assertWhaleFinalPositions asserts the whale's backstop position after the
// queued withdrawal was dequeued (restored) and backstop emissions claimed.
func (s *BlendLiveIngestionTestSuite) assertWhaleFinalPositions(positions *wbtypes.BlendAccountPositions, stack *infrastructure.BlendStack) {
	s.Require().NotNil(positions)
	s.Require().Len(positions.Backstop, 1)
	bp := positions.Backstop[0]
	s.Assert().Equal(stack.PoolID, bp.PoolAddress)
	// The dequeue restores the original 50,000e7-share deposit, and the phase-2 backstop claim
	// re-stakes the claimed LP into the position (backstop.claim deposits rather than pays out),
	// minting a small number of additional shares on top.
	shares := parseBigIntStr(s.T(), bp.Shares)
	s.Assert().Greater(shares.Cmp(big.NewInt(500_000_000_000)), 0,
		"dequeue should have restored the deposit and the backstop claim re-staked LP on top")
	s.Assert().Greater(parseBigIntStr(s.T(), bp.LpTokens).Sign(), 0)
	s.Assert().NotNil(bp.UsdValue)
	s.Assert().Empty(bp.Q4W, "the queued withdrawal was dequeued")
	s.Assert().GreaterOrEqual(parseBigIntStr(s.T(), bp.EmissionsEarnedBlnd).Sign(), 0)

	s.Assert().Greater(parseBigIntStr(s.T(), positions.BackstopClaimedLp).Sign(), 0, "whale claimed backstop emissions in phase 2")
}

// assertStateChanges asserts the phase-2 LENDING state changes over GraphQL.
// Token/Amount/PoolID expectations mirror internal/services/blend/events.go's
// EventRow construction exactly (see decodeWithdrawAmbiguous,
// decodeClaimAmbiguous, decodeBackstopDeposit/WithdrawQueue/WithdrawCancel,
// and the liquidation rows in decodeFillAuction), not a blanket "always BLND"
// assumption: WITHDRAW/WITHDRAW_COLLATERAL carry the underlying reserve asset
// (USDC here) as their token, only a pool-side CLAIM carries BLND, backstop
// queue/cancel/claim rows carry no token at all, and LIQUIDATION rows carry
// neither a token nor an amount.
func (s *BlendLiveIngestionTestSuite) assertStateChanges(ctx context.Context, stack *infrastructure.BlendStack) {
	supplierWithdraw := s.requireOneLendingChange(s.fetchLendingChanges(ctx, stack.Supplier.Address(), "WITHDRAW"), "WITHDRAW")
	s.assertTokenPoolAmount(supplierWithdraw, stack.USDCTokenID, stack.PoolID)

	supplierWithdrawCollateral := s.requireOneLendingChange(s.fetchLendingChanges(ctx, stack.Supplier.Address(), "WITHDRAW_COLLATERAL"), "WITHDRAW_COLLATERAL")
	s.assertTokenPoolAmount(supplierWithdrawCollateral, stack.USDCTokenID, stack.PoolID)

	supplierClaim := s.requireOneLendingChange(s.fetchLendingChanges(ctx, stack.Supplier.Address(), "CLAIM"), "CLAIM")
	s.assertTokenPoolAmount(supplierClaim, stack.BLNDTokenID, stack.PoolID)

	whaleQueue := s.requireOneLendingChange(s.fetchLendingChanges(ctx, stack.Whale.Address(), "BACKSTOP_WITHDRAW_QUEUE"), "BACKSTOP_WITHDRAW_QUEUE")
	s.Assert().Nil(whaleQueue.TokenID, "backstop share rows carry no token_id")
	s.Require().NotNil(whaleQueue.PoolID)
	s.Assert().Equal(stack.PoolID, *whaleQueue.PoolID)
	s.Require().NotNil(whaleQueue.Amount)
	s.Assert().Greater(parseBigIntStr(s.T(), *whaleQueue.Amount).Sign(), 0)

	whaleCancel := s.requireOneLendingChange(s.fetchLendingChanges(ctx, stack.Whale.Address(), "BACKSTOP_WITHDRAW_CANCEL"), "BACKSTOP_WITHDRAW_CANCEL")
	s.Assert().Nil(whaleCancel.TokenID)
	s.Require().NotNil(whaleCancel.PoolID)
	s.Assert().Equal(stack.PoolID, *whaleCancel.PoolID)
	s.Require().NotNil(whaleCancel.Amount)
	s.Assert().Greater(parseBigIntStr(s.T(), *whaleCancel.Amount).Sign(), 0)

	whaleClaim := s.requireOneLendingChange(s.fetchLendingChanges(ctx, stack.Whale.Address(), "CLAIM"), "CLAIM")
	s.Assert().Nil(whaleClaim.TokenID, "a backstop claim pays out Comet LP tokens, tracked with no token_id")
	s.Assert().Nil(whaleClaim.PoolID, "a backstop claim aggregates across every pool, carrying no pool address")
	s.Require().NotNil(whaleClaim.Amount)
	s.Assert().Greater(parseBigIntStr(s.T(), *whaleClaim.Amount).Sign(), 0)

	borrowerLiquidation := s.requireOneLendingChange(s.fetchLendingChanges(ctx, stack.Borrower.Address(), "LIQUIDATION"), "LIQUIDATION")
	s.Assert().Nil(borrowerLiquidation.TokenID)
	s.Assert().Nil(borrowerLiquidation.Amount)
	s.Require().NotNil(borrowerLiquidation.PoolID)
	s.Assert().Equal(stack.PoolID, *borrowerLiquidation.PoolID)

	fillerLiquidation := s.requireOneLendingChange(s.fetchLendingChanges(ctx, stack.Filler.Address(), "LIQUIDATION"), "LIQUIDATION")
	s.Assert().Nil(fillerLiquidation.TokenID)
	s.Assert().Nil(fillerLiquidation.Amount)
	s.Require().NotNil(fillerLiquidation.PoolID)
	s.Assert().Equal(stack.PoolID, *fillerLiquidation.PoolID)
}

// assertEmissionsRows asserts the raw blend_emissions/blend_reserve_emissions
// rows the GraphQL surface derives its emissions figures from.
func (s *BlendLiveIngestionTestSuite) assertEmissionsRows(ctx context.Context, models *data.Models, stack *infrastructure.BlendStack) {
	supplierEmissions, err := models.Blend.Emissions.GetByAccount(ctx, stack.Supplier.Address())
	s.Require().NoError(err)
	hasSupplierBTokenStream := false
	for _, e := range supplierEmissions {
		if e.TokenID == 1 {
			hasSupplierBTokenStream = true
		}
	}
	s.Assert().True(hasSupplierBTokenStream, "expected a token_id=1 (USDC bToken) emission row for the supplier")

	whaleEmissions, err := models.Blend.Emissions.GetByAccount(ctx, stack.Whale.Address())
	s.Require().NoError(err)
	hasWhaleBackstopStream := false
	for _, e := range whaleEmissions {
		if e.TokenID == blenddata.BackstopEmissionTokenID {
			hasWhaleBackstopStream = true
		}
	}
	s.Assert().True(hasWhaleBackstopStream, "expected a token_id=-1 (backstop) emission row for the whale")

	reserveEmissions, err := models.Blend.ReserveEmissions.GetByPools(ctx, []string{stack.PoolID})
	s.Require().NoError(err)
	tokenIDs := map[int32]bool{}
	for _, re := range reserveEmissions {
		tokenIDs[re.ReserveTokenID] = true
	}
	s.Assert().True(tokenIDs[1], "expected reserve emission config for token_id=1 (USDC bToken)")
	s.Assert().True(tokenIDs[2], "expected reserve emission config for token_id=2 (XLM dToken)")

	// Lifetime claimed totals: the accumulator rows that back the resolver's
	// claimedBlnd (per pool) and backstopClaimedLp (account-wide). The supplier
	// claimed pool-reserve emissions and the whale claimed backstop emissions in
	// phase 2, so both must have folded a positive total.
	supplierClaimed, err := models.Blend.PoolClaimed.GetByAccount(ctx, stack.Supplier.Address())
	s.Require().NoError(err)
	s.Require().Len(supplierClaimed, 1, "supplier has one pool-source claimed row")
	s.Assert().Equal(stack.PoolID, string(supplierClaimed[0].PoolContractID))
	s.Assert().Greater(parseBigIntStr(s.T(), supplierClaimed[0].ClaimedBlnd).Sign(), 0,
		"supplier's pool claim folded into blend_pool_claimed")

	whaleClaimed, err := models.Blend.BackstopClaimed.GetByAccount(ctx, stack.Whale.Address())
	s.Require().NoError(err)
	s.Require().NotNil(whaleClaimed, "whale has an account-wide backstop claimed row")
	s.Assert().Greater(parseBigIntStr(s.T(), whaleClaimed.ClaimedLp).Sign(), 0,
		"whale's backstop claim folded into blend_backstop_claimed")
}

// assertPostLiquidationPositions asserts the borrower's collateral shrank and
// the filler picked up a USDC collateral position, at the DB level.
func (s *BlendLiveIngestionTestSuite) assertPostLiquidationPositions(ctx context.Context, models *data.Models, stack *infrastructure.BlendStack) {
	borrowerPositions, err := models.Blend.Positions.GetByAccount(ctx, stack.Borrower.Address())
	s.Require().NoError(err)
	borrowerUSDC := findPositionByIndex(borrowerPositions, 0)
	s.Require().NotNil(borrowerUSDC)
	s.Assert().Less(parseBigIntStr(s.T(), borrowerUSDC.CollateralBTokens).Cmp(big.NewInt(infrastructure.BlendBorrowerSupplyCollateralUSDC)), 0,
		"the liquidation auction should have removed some of the borrower's USDC collateral lot")

	fillerPositions, err := models.Blend.Positions.GetByAccount(ctx, stack.Filler.Address())
	s.Require().NoError(err)
	fillerUSDC := findPositionByIndex(fillerPositions, 0)
	s.Require().NotNil(fillerUSDC)
	s.Assert().Greater(parseBigIntStr(s.T(), fillerUSDC.CollateralBTokens).Sign(), 0)
}
