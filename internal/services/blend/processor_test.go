package blend

import (
	"context"
	"encoding/hex"
	"math/big"
	"testing"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/toid"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
	blenddata "github.com/stellar/wallet-backend/internal/data/blend"
	"github.com/stellar/wallet-backend/internal/indexer"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/services"
)

// test fixtures --------------------------------------------------------------
//
// randomContractAddr, randomAccountAddr, symScVal, u32ScVal, u64ScVal,
// i128ScVal, stringScVal, boolScVal, voidScVal, vecScVal, mapScVal,
// mapValScVal, symEntry, u32Entry, contractAddrScVal, accountAddrScVal,
// instanceKeyScVal, instanceValScVal, createdChange, removedChange,
// contractEvent, and trackedSet are declared in scval_test.go/entries_test.go/
// events_test.go and reused here.

const testNetworkPassphrase = "Test SDF Network ; September 2015"

// testCanonicalBackstopAddr is the canonical Blend v2 backstop C-address for
// testNetworkPassphrase (== network.TestNetworkPassphrase). Backstop-derived
// state is folded only from this address (see canonicalBackstopAddress), so
// tests that exercise the backstop fold must register their backstop under it.
var testCanonicalBackstopAddr = canonicalBackstopAddress(testNetworkPassphrase)

// testMocks bundles every data-layer mock the processor can write to.
type testMocks struct {
	pools             *blenddata.PoolModelMock
	positions         *blenddata.PositionModelMock
	reserves          *blenddata.ReserveModelMock
	backstopPositions *blenddata.BackstopPositionModelMock
	backstopPools     *blenddata.BackstopPoolModelMock
	reserveEmissions  *blenddata.ReserveEmissionModelMock
	emissions         *blenddata.EmissionModelMock
	poolClaimed       *blenddata.PoolClaimedModelMock
	backstopClaimed   *blenddata.BackstopClaimedModelMock
	auctions          *blenddata.AuctionModelMock
	stateChanges      *data.StateChangeWriterMock
	protocolContracts *data.ProtocolContractsModelMock
}

// newTestProcessor builds a bare processor for tests that don't need any
// data-layer dependencies, mirroring sep41's processor_test.go pattern.
func newTestProcessor() *processor {
	return &processor{
		networkPassphrase: testNetworkPassphrase,
		canonicalBackstop: canonicalBackstopAddress(testNetworkPassphrase),
	}
}

// newFullTestProcessor builds a processor wired to fresh mocks for every
// data-layer dependency, for tests that exercise Persist*.
func newFullTestProcessor(t *testing.T) (*processor, *testMocks) {
	t.Helper()
	m := &testMocks{
		pools:             blenddata.NewPoolModelMock(t),
		positions:         blenddata.NewPositionModelMock(t),
		reserves:          blenddata.NewReserveModelMock(t),
		backstopPositions: blenddata.NewBackstopPositionModelMock(t),
		backstopPools:     blenddata.NewBackstopPoolModelMock(t),
		reserveEmissions:  blenddata.NewReserveEmissionModelMock(t),
		emissions:         blenddata.NewEmissionModelMock(t),
		poolClaimed:       blenddata.NewPoolClaimedModelMock(t),
		backstopClaimed:   blenddata.NewBackstopClaimedModelMock(t),
		auctions:          blenddata.NewAuctionModelMock(t),
		stateChanges:      data.NewStateChangeWriterMock(t),
		protocolContracts: data.NewProtocolContractsModelMock(t),
	}
	p := &processor{
		networkPassphrase: testNetworkPassphrase,
		canonicalBackstop: canonicalBackstopAddress(testNetworkPassphrase),
		pools:             m.pools,
		positions:         m.positions,
		reserves:          m.reserves,
		backstopPositions: m.backstopPositions,
		backstopPools:     m.backstopPools,
		reserveEmissions:  m.reserveEmissions,
		emissions:         m.emissions,
		poolClaimed:       m.poolClaimed,
		backstopClaimed:   m.backstopClaimed,
		auctions:          m.auctions,
		stateChanges:      m.stateChanges,
		protocolContracts: m.protocolContracts,
	}
	p.Reset()
	return p, m
}

// protocolContractFor builds a data.ProtocolContracts row classifying addr
// (a C-address) under wasmHashHex, the shape indexContracts expects.
func protocolContractFor(t *testing.T, addr string, wasmHashHex string) data.ProtocolContracts {
	t.Helper()
	raw, err := strkey.Decode(strkey.VersionByteContract, addr)
	require.NoError(t, err)
	return data.ProtocolContracts{
		ContractID: types.HashBytea(hex.EncodeToString(raw)),
		WasmHash:   types.HashBytea(wasmHashHex),
	}
}

// positionsKeyScVal builds a Positions(user) ContractData key.
func positionsKeyScVal(t *testing.T, user string) xdr.ScVal {
	t.Helper()
	return vecScVal(symScVal("Positions"), accountAddrScVal(t, user))
}

// positionsSnapshotScVal builds a Positions ContractData value with the
// given supply/collateral maps (liabilities always empty) keyed by reserve_index.
func positionsSnapshotScVal(supply, collateral map[uint32]int64) xdr.ScVal {
	supplyEntries := make([]xdr.ScMapEntry, 0, len(supply))
	for idx, v := range supply {
		supplyEntries = append(supplyEntries, u32Entry(idx, i128ScVal(v)))
	}
	collateralEntries := make([]xdr.ScMapEntry, 0, len(collateral))
	for idx, v := range collateral {
		collateralEntries = append(collateralEntries, u32Entry(idx, i128ScVal(v)))
	}
	return mapValScVal(mapScVal(
		symEntry("collateral", mapValScVal(mapScVal(collateralEntries...))),
		symEntry("liabilities", mapValScVal(mapScVal())),
		symEntry("supply", mapValScVal(mapScVal(supplyEntries...))),
	))
}

// resConfigScVal builds a ResConfig ContractData value at the given reserve index.
func resConfigScVal(index uint32) xdr.ScVal {
	return mapValScVal(mapScVal(
		symEntry("c_factor", u32ScVal(9000)),
		symEntry("decimals", u32ScVal(7)),
		symEntry("enabled", boolScVal(true)),
		symEntry("index", u32ScVal(index)),
		symEntry("l_factor", u32ScVal(9500)),
		symEntry("max_util", u32ScVal(9500)),
		symEntry("r_base", u32ScVal(100)),
		symEntry("r_one", u32ScVal(200)),
		symEntry("r_three", u32ScVal(400)),
		symEntry("r_two", u32ScVal(300)),
		symEntry("reactivity", u32ScVal(1000)),
		symEntry("supply_cap", i128ScVal(1_000_000_000)),
		symEntry("util", u32ScVal(8000)),
	))
}

// resDataScVal builds a ResData ContractData value.
func resDataScVal() xdr.ScVal {
	return mapValScVal(mapScVal(
		symEntry("b_rate", i128ScVal(1_050_000_000_000)),
		symEntry("b_supply", i128ScVal(100_000_000_000)),
		symEntry("backstop_credit", i128ScVal(500)),
		symEntry("d_rate", i128ScVal(1_020_000_000_000)),
		symEntry("d_supply", i128ScVal(40_000_000_000)),
		symEntry("ir_mod", i128ScVal(1_000_000_000_000)),
		symEntry("last_time", u64ScVal(1_700_000_000)),
	))
}

// emisDataScVal builds an EmisData/BEmisData ContractData value.
func emisDataScVal() xdr.ScVal {
	return mapValScVal(mapScVal(
		symEntry("eps", u64ScVal(1000)),
		symEntry("expiration", u64ScVal(1_800_000_000)),
		symEntry("index", i128ScVal(42)),
		symEntry("last_time", u64ScVal(1_700_000_000)),
	))
}

// userEmisScVal builds a UserEmis/UEmisData ContractData value.
func userEmisScVal() xdr.ScVal {
	return mapValScVal(mapScVal(
		symEntry("accrued", i128ScVal(777)),
		symEntry("index", i128ScVal(88)),
	))
}

// poolInstanceScVal builds a pool instance-storage ContractData value.
func poolInstanceScVal(t *testing.T, name, oracleAddr string) xdr.ScVal {
	t.Helper()
	storage := mapScVal(
		symEntry("Name", stringScVal(name)),
		symEntry("Config", mapValScVal(mapScVal(
			symEntry("oracle", contractAddrScVal(t, oracleAddr)),
			symEntry("bstop_rate", u32ScVal(2500)),
			symEntry("status", u32ScVal(0)),
			symEntry("max_positions", u32ScVal(4)),
			symEntry("min_collateral", i128ScVal(1_000_000)),
		))),
	)
	return instanceValScVal(storage)
}

// auctionKeyScVal builds an Auction(auct_type, user) ContractData key.
func auctionKeyScVal(t *testing.T, auctType uint32, user string) xdr.ScVal {
	t.Helper()
	return vecScVal(symScVal("Auction"), mapValScVal(mapScVal(
		symEntry("auct_type", u32ScVal(auctType)),
		symEntry("user", accountAddrScVal(t, user)),
	)))
}

// auctionValScVal builds an AuctionData ContractData value with a single bid and
// lot asset amount plus a start block.
func auctionValScVal(t *testing.T, bidAsset string, bid int64, lotAsset string, lot int64, block uint32) xdr.ScVal {
	t.Helper()
	return mapValScVal(mapScVal(
		symEntry("bid", mapValScVal(mapScVal(addrMapEntry(t, bidAsset, i128ScVal(bid))))),
		symEntry("block", u32ScVal(block)),
		symEntry("lot", mapValScVal(mapScVal(addrMapEntry(t, lotAsset, i128ScVal(lot))))),
	))
}

// poolInstanceWithAdminScVal builds a pool instance-storage ContractData value
// carrying an Admin address alongside Name and Config.
func poolInstanceWithAdminScVal(t *testing.T, name, oracleAddr, adminAddr string) xdr.ScVal {
	t.Helper()
	storage := mapScVal(
		symEntry("Name", stringScVal(name)),
		symEntry("Admin", accountAddrScVal(t, adminAddr)),
		symEntry("Config", mapValScVal(mapScVal(
			symEntry("oracle", contractAddrScVal(t, oracleAddr)),
			symEntry("bstop_rate", u32ScVal(2500)),
			symEntry("status", u32ScVal(0)),
			symEntry("max_positions", u32ScVal(4)),
			symEntry("min_collateral", i128ScVal(1_000_000)),
		))),
	)
	return instanceValScVal(storage)
}

// tests -----------------------------------------------------------------

func TestProcessLedger_NeedsResetGuard(t *testing.T) {
	ctx := context.Background()
	p := newTestProcessor()
	p.Reset()

	// A Persist* call seals the staged sets. With nothing staged it returns early
	// and never touches the (nil) tx, but still flags that a Reset is required.
	require.NoError(t, p.PersistHistory(ctx, nil))

	// Folding again without an intervening Reset must error rather than
	// re-adding already-committed state.
	err := p.ProcessLedger(ctx, services.ProtocolProcessorInput{LedgerSequence: 1})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Reset")

	// Reset clears the seal and folding is allowed again.
	p.Reset()
	require.NoError(t, p.ProcessLedger(ctx, services.ProtocolProcessorInput{LedgerSequence: 1}))
}

func TestProcessLedger_SkipsUntrackedContracts(t *testing.T) {
	ctx := context.Background()
	p := newTestProcessor()
	p.Reset()

	// otherPoolAddr is tracked so p.blendContracts is non-empty (otherwise
	// ProcessLedger's early-return would trivially skip everything, defeating
	// the point of this test); untrackedAddr is not classified at all.
	otherPoolAddr := randomContractAddr(t)
	untrackedAddr := randomContractAddr(t)
	asset := randomContractAddr(t)
	user := randomAccountAddr(t)

	event := contractEvent(t, untrackedAddr,
		[]xdr.ScVal{symScVal("supply"), contractAddrScVal(t, asset), accountAddrScVal(t, user)},
		vecScVal(i128ScVal(100), i128ScVal(95)),
	)
	entry := createdChange(vecScVal(symScVal("ResConfig"), contractAddrScVal(t, asset)), resConfigScVal(0))

	input := services.ProtocolProcessorInput{
		LedgerSequence:      5,
		ProtocolContracts:   []data.ProtocolContracts{protocolContractFor(t, otherPoolAddr, "aa")},
		ContractEvents:      map[indexer.ContractEventKey][]xdr.ContractEvent{{TxIdx: 0, OpIdx: 0}: {event}},
		ContractDataChanges: map[string][]ingest.Change{untrackedAddr: {entry}},
		StagingMode:         services.StagingModeBoth,
	}
	require.NoError(t, p.ProcessLedger(ctx, input))

	assert.Empty(t, p.stagedStateChanges)
	assert.Empty(t, p.stagedReserves)
	assert.Empty(t, p.stagedNetDeltas)
	assert.Empty(t, p.stagedPositions)
	assert.Empty(t, p.stagedPools)
}

func TestProcessLedger_StagingModes(t *testing.T) {
	ctx := context.Background()
	poolAddr := randomContractAddr(t)
	assetAddr := randomContractAddr(t)
	userAddr := randomAccountAddr(t)
	poolPC := protocolContractFor(t, poolAddr, "aa")

	buildInput := func(mode services.StagingMode) services.ProtocolProcessorInput {
		supplyEvent := contractEvent(t, poolAddr,
			[]xdr.ScVal{symScVal("supply"), contractAddrScVal(t, assetAddr), accountAddrScVal(t, userAddr)},
			vecScVal(i128ScVal(1000), i128ScVal(950)),
		)
		resConfigChange := createdChange(vecScVal(symScVal("ResConfig"), contractAddrScVal(t, assetAddr)), resConfigScVal(0))
		return services.ProtocolProcessorInput{
			LedgerSequence:      10,
			ProtocolContracts:   []data.ProtocolContracts{poolPC},
			ContractEvents:      map[indexer.ContractEventKey][]xdr.ContractEvent{{TxIdx: 0, OpIdx: 0}: {supplyEvent}},
			ContractDataChanges: map[string][]ingest.Change{poolAddr: {resConfigChange}},
			StagingMode:         mode,
		}
	}

	t.Run("history mode stages history but no current state", func(t *testing.T) {
		p, _ := newFullTestProcessor(t)
		require.NoError(t, p.ProcessLedger(ctx, buildInput(services.StagingModeHistory)))
		assert.NotEmpty(t, p.stagedStateChanges)
		assert.Empty(t, p.stagedReserves)
		assert.Empty(t, p.stagedNetDeltas)

		// No current-state model call is expected; PersistCurrentState must be a
		// silent no-op given nothing was staged for it.
		require.NoError(t, p.PersistCurrentState(ctx, nil))
	})

	t.Run("current-state mode stages current state but no history", func(t *testing.T) {
		p, m := newFullTestProcessor(t)
		require.NoError(t, p.ProcessLedger(ctx, buildInput(services.StagingModeCurrentState)))
		assert.Empty(t, p.stagedStateChanges)
		assert.NotEmpty(t, p.stagedReserves)
		assert.NotEmpty(t, p.stagedNetDeltas)

		// BatchCopy must never be called: PersistHistory returns early on an
		// empty staged slice.
		require.NoError(t, p.PersistHistory(ctx, nil))

		// Sanity: the current-state side does reach the data layer.
		m.reserves.On("BatchUpsert", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		m.positions.On("BatchApplyNetDeltas", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		require.NoError(t, p.PersistCurrentState(ctx, nil))
	})
}

func TestProcessLedger_StagesClaims(t *testing.T) {
	ctx := context.Background()
	poolAddr := randomContractAddr(t)
	backstopAddr := testCanonicalBackstopAddr
	userAddr := randomAccountAddr(t)
	poolPC := protocolContractFor(t, poolAddr, "aa")
	backstopPC := protocolContractFor(t, backstopAddr, "bb")

	buildInput := func(mode services.StagingMode) services.ProtocolProcessorInput {
		// Pool claim: Vec data (reserve_token_ids, amount) → source=pool, BLND.
		poolClaim := contractEvent(t, poolAddr,
			[]xdr.ScVal{symScVal("claim"), accountAddrScVal(t, userAddr)},
			vecScVal(vecScVal(u32ScVal(1), u32ScVal(3)), i128ScVal(500)),
		)
		// Backstop claim: bare i128 → source=backstop, Comet LP, no pool address.
		backstopClaim := contractEvent(t, backstopAddr,
			[]xdr.ScVal{symScVal("claim"), accountAddrScVal(t, userAddr)},
			i128ScVal(750),
		)
		return services.ProtocolProcessorInput{
			LedgerSequence:    10,
			ProtocolContracts: []data.ProtocolContracts{poolPC, backstopPC},
			ContractEvents: map[indexer.ContractEventKey][]xdr.ContractEvent{
				{TxIdx: 0, OpIdx: 0}: {poolClaim},
				{TxIdx: 0, OpIdx: 1}: {backstopClaim},
			},
			StagingMode: mode,
		}
	}

	t.Run("current-state mode folds claims into accumulators", func(t *testing.T) {
		p, m := newFullTestProcessor(t)
		require.NoError(t, p.ProcessLedger(ctx, buildInput(services.StagingModeCurrentState)))
		assert.Empty(t, p.stagedStateChanges, "current-state mode stages no history rows")
		require.Len(t, p.stagedPoolClaims, 1)
		require.Len(t, p.stagedBackstopClaims, 1)

		m.poolClaimed.On("BatchApplyDeltas", mock.Anything, mock.Anything, mock.MatchedBy(func(rows []blenddata.PoolClaimedDelta) bool {
			return len(rows) == 1 && rows[0].Pool == poolAddr && rows[0].User == userAddr &&
				rows[0].ClaimedBlnd == "500" && rows[0].LedgerNumber == 10
		})).Return(nil).Once()
		m.backstopClaimed.On("BatchApplyDeltas", mock.Anything, mock.Anything, mock.MatchedBy(func(rows []blenddata.BackstopClaimedDelta) bool {
			return len(rows) == 1 && rows[0].User == userAddr && rows[0].ClaimedLp == "750" && rows[0].LedgerNumber == 10
		})).Return(nil).Once()
		require.NoError(t, p.PersistCurrentState(ctx, nil))
	})

	t.Run("history mode stages CLAIM rows but folds no claim totals", func(t *testing.T) {
		p, _ := newFullTestProcessor(t)
		require.NoError(t, p.ProcessLedger(ctx, buildInput(services.StagingModeHistory)))
		assert.NotEmpty(t, p.stagedStateChanges, "history mode still records the CLAIM feed rows")
		assert.Empty(t, p.stagedPoolClaims)
		assert.Empty(t, p.stagedBackstopClaims)

		// PersistCurrentState must be a silent no-op: no claim-accumulator calls.
		require.NoError(t, p.PersistCurrentState(ctx, nil))
	})
}

func TestProcessLedger_ImpostorBackstopEntriesSkipped(t *testing.T) {
	ctx := context.Background()
	// An impostor deployed from the real backstop WASM: classified as Blend and
	// tracked, but not the canonical backstop for this network. Its
	// backstop-shaped entries key their rows under a real pool/user, so folding
	// them would overwrite the true backstop's rows — the gate must skip them.
	impostorAddr := randomContractAddr(t)
	poolAddr := randomContractAddr(t)
	userAddr := randomAccountAddr(t)
	impostorPC := protocolContractFor(t, impostorAddr, "bb")

	changes := []ingest.Change{
		createdChange(vecScVal(symScVal("UserBalance"), mapValScVal(mapScVal(
			symEntry("pool", contractAddrScVal(t, poolAddr)), symEntry("user", accountAddrScVal(t, userAddr)),
		))), mapValScVal(mapScVal(symEntry("q4w", vecScVal()), symEntry("shares", i128ScVal(300))))),
		createdChange(vecScVal(symScVal("PoolBalance"), contractAddrScVal(t, poolAddr)), mapValScVal(mapScVal(
			symEntry("q4w", i128ScVal(50)), symEntry("shares", i128ScVal(1000)), symEntry("tokens", i128ScVal(2000)),
		))),
		createdChange(symScVal("RZ"), vecScVal(contractAddrScVal(t, poolAddr))),
	}

	// Wired to real mocks: any Persist* call into a backstop model would fail the
	// test, so absence of staging is asserted both directly and via the mocks.
	p, _ := newFullTestProcessor(t)
	require.NoError(t, p.ProcessLedger(ctx, services.ProtocolProcessorInput{
		LedgerSequence:      5,
		ProtocolContracts:   []data.ProtocolContracts{impostorPC},
		ContractDataChanges: map[string][]ingest.Change{impostorAddr: changes},
		StagingMode:         services.StagingModeCurrentState,
	}))

	assert.Empty(t, p.stagedBackstopPositions, "impostor UserBalance must not stage a backstop position")
	assert.Empty(t, p.stagedBackstopPools, "impostor PoolBalance must not stage a backstop pool row")

	// Nothing staged, so PersistCurrentState touches no backstop model.
	require.NoError(t, p.PersistCurrentState(ctx, nil))
}

func TestProcessLedger_ImpostorBackstopEventsSkipped(t *testing.T) {
	ctx := context.Background()
	impostorAddr := randomContractAddr(t)
	poolAddr := randomContractAddr(t)
	userAddr := randomAccountAddr(t)
	impostorPC := protocolContractFor(t, impostorAddr, "bb")
	poolPC := protocolContractFor(t, poolAddr, "aa")

	// deposit is a backstop-only symbol; claim with bare-i128 data is a backstop
	// claim. Both are backstop-shaped decodes emitted by the impostor, so neither
	// a history row nor a claimed-total delta may be staged.
	depositEvent := contractEvent(t, impostorAddr,
		[]xdr.ScVal{symScVal("deposit"), contractAddrScVal(t, poolAddr), accountAddrScVal(t, userAddr)},
		vecScVal(i128ScVal(1000), i128ScVal(950)),
	)
	backstopClaimEvent := contractEvent(t, impostorAddr,
		[]xdr.ScVal{symScVal("claim"), accountAddrScVal(t, userAddr)},
		i128ScVal(750),
	)

	p, _ := newFullTestProcessor(t)
	require.NoError(t, p.ProcessLedger(ctx, services.ProtocolProcessorInput{
		LedgerSequence:    5,
		ProtocolContracts: []data.ProtocolContracts{impostorPC, poolPC},
		ContractEvents: map[indexer.ContractEventKey][]xdr.ContractEvent{
			{TxIdx: 0, OpIdx: 0}: {depositEvent},
			{TxIdx: 0, OpIdx: 1}: {backstopClaimEvent},
		},
		StagingMode: services.StagingModeBoth,
	}))

	assert.Empty(t, p.stagedStateChanges, "impostor backstop events must stage no history rows")
	assert.Empty(t, p.stagedBackstopClaims, "impostor backstop claim must fold no claimed total")

	require.NoError(t, p.PersistHistory(ctx, nil))
	require.NoError(t, p.PersistCurrentState(ctx, nil))
}

func TestProcessLedger_StagesEntries(t *testing.T) {
	ctx := context.Background()
	poolAddr := randomContractAddr(t)
	backstopAddr := testCanonicalBackstopAddr
	assetAddr := randomContractAddr(t)
	oracleAddr := randomContractAddr(t)
	userAddr := randomAccountAddr(t)

	poolPC := protocolContractFor(t, poolAddr, "aabbaabbaabbaabbaabbaabbaabbaabbaabbaabbaabbaabbaabbaabbaabbaabb")
	backstopPC := protocolContractFor(t, backstopAddr, "ccddccddccddccddccddccddccddccddccddccddccddccddccddccddccddcc")

	poolChanges := []ingest.Change{
		createdChange(instanceKeyScVal(), poolInstanceScVal(t, "Fixed Pool v2", oracleAddr)),
		createdChange(vecScVal(symScVal("ResConfig"), contractAddrScVal(t, assetAddr)), resConfigScVal(0)),
		createdChange(vecScVal(symScVal("ResData"), contractAddrScVal(t, assetAddr)), resDataScVal()),
		createdChange(positionsKeyScVal(t, userAddr), positionsSnapshotScVal(map[uint32]int64{0: 500}, nil)),
		createdChange(vecScVal(symScVal("EmisData"), u32ScVal(0)), emisDataScVal()),
		createdChange(vecScVal(symScVal("UserEmis"), mapValScVal(mapScVal(
			symEntry("reserve_id", u32ScVal(0)),
			symEntry("user", accountAddrScVal(t, userAddr)),
		))), userEmisScVal()),
	}
	backstopChanges := []ingest.Change{
		createdChange(vecScVal(symScVal("PoolBalance"), contractAddrScVal(t, poolAddr)), mapValScVal(mapScVal(
			symEntry("q4w", i128ScVal(50)), symEntry("shares", i128ScVal(1000)), symEntry("tokens", i128ScVal(2000)),
		))),
		createdChange(vecScVal(symScVal("BEmisData"), contractAddrScVal(t, poolAddr)), emisDataScVal()),
		createdChange(vecScVal(symScVal("UserBalance"), mapValScVal(mapScVal(
			symEntry("pool", contractAddrScVal(t, poolAddr)), symEntry("user", accountAddrScVal(t, userAddr)),
		))), mapValScVal(mapScVal(symEntry("q4w", vecScVal()), symEntry("shares", i128ScVal(300))))),
		createdChange(vecScVal(symScVal("UEmisData"), mapValScVal(mapScVal(
			symEntry("pool", contractAddrScVal(t, poolAddr)), symEntry("user", accountAddrScVal(t, userAddr)),
		))), userEmisScVal()),
	}

	// A "supply" event alongside the entries above exercises the net-delta path
	// too, so the reserves-before-positions persist ordering can be verified.
	supplyEvent := contractEvent(t, poolAddr,
		[]xdr.ScVal{symScVal("supply"), contractAddrScVal(t, assetAddr), accountAddrScVal(t, userAddr)},
		vecScVal(i128ScVal(1000), i128ScVal(950)),
	)

	input := services.ProtocolProcessorInput{
		LedgerSequence:    200,
		LedgerCloseTime:   1_700_000_000,
		ProtocolContracts: []data.ProtocolContracts{poolPC, backstopPC},
		ContractEvents:    map[indexer.ContractEventKey][]xdr.ContractEvent{{TxIdx: 0, OpIdx: 0}: {supplyEvent}},
		ContractDataChanges: map[string][]ingest.Change{
			poolAddr:     poolChanges,
			backstopAddr: backstopChanges,
		},
		StagingMode: services.StagingModeBoth,
	}

	p, m := newFullTestProcessor(t)
	require.NoError(t, p.ProcessLedger(ctx, input))

	// Sanity: every kind staged something.
	require.Len(t, p.stagedPools, 1)
	require.Len(t, p.stagedReserves, 1)
	require.Len(t, p.stagedPositions, 1)
	require.Len(t, p.stagedReserveEmissions, 1)
	require.Len(t, p.stagedBackstopPools, 1)
	require.Len(t, p.stagedBackstopPositions, 1)
	require.Len(t, p.stagedUserEmissions, 2) // pool UserEmis + backstop UEmisData
	require.Len(t, p.stagedNetDeltas, 1)

	var order []string
	recordOrder := func(name string) func(mock.Arguments) {
		return func(mock.Arguments) { order = append(order, name) }
	}

	m.pools.On("BatchUpsert", mock.Anything, mock.Anything, mock.MatchedBy(func(rows []blenddata.Pool) bool {
		return len(rows) == 1 && rows[0].Name != nil && *rows[0].Name == "Fixed Pool v2"
	})).Run(recordOrder("pools.BatchUpsert")).Return(nil).Once()

	m.protocolContracts.On("BatchInsert", mock.Anything, mock.Anything, mock.MatchedBy(func(rows []data.ProtocolContracts) bool {
		return len(rows) == 1 && rows[0].Name != nil && *rows[0].Name == "Fixed Pool v2" && rows[0].WasmHash == poolPC.WasmHash
	})).Run(recordOrder("protocolContracts.BatchInsert")).Return(nil).Once()

	m.reserves.On("BatchUpsert", mock.Anything, mock.Anything, mock.MatchedBy(func(rows []blenddata.Reserve) bool {
		return len(rows) == 1 && rows[0].ReserveIndex == 0 && rows[0].BRate == "1050000000000"
	})).Run(recordOrder("reserves.BatchUpsert")).Return(nil).Once()

	m.positions.On("ZeroAbsentReserves", mock.Anything, mock.Anything, mock.Anything).
		Run(recordOrder("positions.ZeroAbsentReserves")).Return(nil).Once()
	m.positions.On("BatchUpsertSnapshots", mock.Anything, mock.Anything, mock.MatchedBy(func(rows []blenddata.PositionSnapshot) bool {
		return len(rows) == 1 && rows[0].SupplyBTokens == "500"
	})).Run(recordOrder("positions.BatchUpsertSnapshots")).Return(nil).Once()
	m.positions.On("BatchApplyNetDeltas", mock.Anything, mock.Anything, mock.MatchedBy(func(rows []blenddata.PositionNetDelta) bool {
		return len(rows) == 1 && rows[0].NetSuppliedDelta == "1000"
	})).Run(recordOrder("positions.BatchApplyNetDeltas")).Return(nil).Once()

	m.backstopPositions.On("BatchUpsert", mock.Anything, mock.Anything, mock.MatchedBy(func(rows []blenddata.BackstopPosition) bool {
		return len(rows) == 1 && rows[0].Shares == "300"
	})).Run(recordOrder("backstopPositions.BatchUpsert")).Return(nil).Once()

	m.backstopPools.On("BatchUpsertBalances", mock.Anything, mock.Anything, mock.MatchedBy(func(rows []blenddata.BackstopPool) bool {
		return len(rows) == 1 && rows[0].Shares == "1000"
	})).Run(recordOrder("backstopPools.BatchUpsertBalances")).Return(nil).Once()
	m.backstopPools.On("BatchUpsertEmissions", mock.Anything, mock.Anything, mock.MatchedBy(func(rows []blenddata.BackstopPoolEmission) bool {
		return len(rows) == 1 && rows[0].EmisEps != nil && *rows[0].EmisEps == 1000
	})).Run(recordOrder("backstopPools.BatchUpsertEmissions")).Return(nil).Once()

	m.reserveEmissions.On("BatchUpsert", mock.Anything, mock.Anything, mock.MatchedBy(func(rows []blenddata.ReserveEmission) bool {
		return len(rows) == 1 && rows[0].Eps == 1000
	})).Run(recordOrder("reserveEmissions.BatchUpsert")).Return(nil).Once()
	m.emissions.On("BatchUpsert", mock.Anything, mock.Anything, mock.MatchedBy(func(rows []blenddata.Emission) bool {
		return len(rows) == 2
	})).Run(recordOrder("emissions.BatchUpsert")).Return(nil).Once()

	require.NoError(t, p.PersistCurrentState(ctx, nil))

	// Persist order: reserves must land before positions.BatchApplyNetDeltas —
	// the net-delta SQL resolves asset -> reserve_index via blend_reserves.
	reserveIdx, netDeltaIdx := -1, -1
	for i, name := range order {
		switch name {
		case "reserves.BatchUpsert":
			reserveIdx = i
		case "positions.BatchApplyNetDeltas":
			netDeltaIdx = i
		}
	}
	require.NotEqual(t, -1, reserveIdx)
	require.NotEqual(t, -1, netDeltaIdx)
	assert.Less(t, reserveIdx, netDeltaIdx, "reserves must persist before positions net-deltas are applied")
}

func TestProcessLedger_StagesHistory(t *testing.T) {
	ctx := context.Background()
	poolAddr := randomContractAddr(t)
	backstopAddr := testCanonicalBackstopAddr
	assetAddr := randomContractAddr(t)
	fromAddr := randomAccountAddr(t)
	fillerAddr := randomAccountAddr(t)

	poolPC := protocolContractFor(t, poolAddr, "aa")
	backstopPC := protocolContractFor(t, backstopAddr, "bb")

	supplyEvent := contractEvent(t, poolAddr,
		[]xdr.ScVal{symScVal("supply"), contractAddrScVal(t, assetAddr), accountAddrScVal(t, fromAddr)},
		vecScVal(i128ScVal(1000), i128ScVal(950)),
	)
	borrowEvent := contractEvent(t, poolAddr,
		[]xdr.ScVal{symScVal("borrow"), contractAddrScVal(t, assetAddr), accountAddrScVal(t, fromAddr)},
		vecScVal(i128ScVal(200), i128ScVal(190)),
	)
	// Backstop-emitted "withdraw": addr1 (topic[1]) is the pool address, which
	// IS tracked, disambiguating it from a pool withdraw.
	backstopWithdrawEvent := contractEvent(t, backstopAddr,
		[]xdr.ScVal{symScVal("withdraw"), contractAddrScVal(t, poolAddr), accountAddrScVal(t, fromAddr)},
		vecScVal(i128ScVal(300), i128ScVal(295)),
	)
	// Backstop "claim": data is a bare i128, carries no pool address at all.
	backstopClaimEvent := contractEvent(t, backstopAddr,
		[]xdr.ScVal{symScVal("claim"), accountAddrScVal(t, fromAddr)},
		i128ScVal(77),
	)
	badDebtEvent := contractEvent(t, poolAddr,
		[]xdr.ScVal{symScVal("bad_debt"), accountAddrScVal(t, fromAddr), contractAddrScVal(t, assetAddr)},
		i128ScVal(400),
	)
	fillAuctionEvent := contractEvent(t, poolAddr,
		[]xdr.ScVal{symScVal("fill_auction"), u32ScVal(1), accountAddrScVal(t, fromAddr)},
		vecScVal(
			accountAddrScVal(t, fillerAddr),
			i128ScVal(100),
			mapValScVal(mapScVal(
				symEntry("bid", mapValScVal(mapScVal(addrMapEntry(t, assetAddr, i128ScVal(50))))),
				symEntry("block", u32ScVal(12345)),
				symEntry("lot", mapValScVal(mapScVal())),
			)),
		),
	)

	input := services.ProtocolProcessorInput{
		LedgerSequence:    toidStagesHistoryLedgerSeq,
		LedgerCloseTime:   1_700_000_500,
		ProtocolContracts: []data.ProtocolContracts{poolPC, backstopPC},
		ContractEvents: map[indexer.ContractEventKey][]xdr.ContractEvent{
			{TxIdx: 0, OpIdx: 0}: {supplyEvent, borrowEvent},
			{TxIdx: 1, OpIdx: 0}: {backstopWithdrawEvent},
			{TxIdx: 1, OpIdx: 1}: {backstopClaimEvent},
			{TxIdx: 2, OpIdx: 0}: {badDebtEvent},
			{TxIdx: 3, OpIdx: 2}: {fillAuctionEvent},
		},
		StagingMode: services.StagingModeHistory,
	}

	p, m := newFullTestProcessor(t)
	require.NoError(t, p.ProcessLedger(ctx, input))

	// supply + borrow (tx0) + backstopWithdraw (tx1/op0) + backstopClaim (tx1/op1)
	// + badDebt (tx2) + fillAuction (2 rows, tx3/op2) = 7 rows.
	require.Len(t, p.stagedStateChanges, 7)

	byReason := map[types.StateChangeReason][]types.StateChange{}
	for _, sc := range p.stagedStateChanges {
		assert.Equal(t, types.StateChangeCategoryLending, sc.StateChangeCategory)
		byReason[sc.StateChangeReason] = append(byReason[sc.StateChangeReason], sc)
	}

	requireOne := func(reason types.StateChangeReason) types.StateChange {
		t.Helper()
		require.Len(t, byReason[reason], 1, "expected exactly one %s row", reason)
		return byReason[reason][0]
	}

	// supply: tx0/op0 -> ToID = toid.New(300,0,0), OperationID = toid.New(300,0,1).
	supplySC := requireOne(types.StateChangeReasonSupply)
	wantTxID := toidNew(t, 0, 0)
	wantOpID := toidNew(t, 0, 1)
	assert.Equal(t, wantTxID, supplySC.ToID)
	assert.Equal(t, wantOpID, supplySC.OperationID)
	require.NotNil(t, supplySC.KeyValue)
	assert.Equal(t, poolAddr, supplySC.KeyValue["poolId"])
	assert.True(t, supplySC.TokenID.Valid)
	assert.Equal(t, "1000", supplySC.Amount.String)

	borrowSC := requireOne(types.StateChangeReasonBorrow)
	assert.Equal(t, wantTxID, borrowSC.ToID) // same tx as supply
	assert.Equal(t, wantOpID, borrowSC.OperationID)

	backstopWithdrawSC := requireOne(types.StateChangeReasonBackstopWithdraw)
	assert.Equal(t, toidNew(t, 1, 0), backstopWithdrawSC.ToID)
	assert.Equal(t, toidNew(t, 1, 1), backstopWithdrawSC.OperationID)
	require.NotNil(t, backstopWithdrawSC.KeyValue)
	assert.Equal(t, poolAddr, backstopWithdrawSC.KeyValue["poolId"])
	assert.False(t, backstopWithdrawSC.TokenID.Valid, "backstop withdraw's token column is NULL")

	claimSC := requireOne(types.StateChangeReasonClaim)
	assert.Equal(t, toidNew(t, 1, 0), claimSC.ToID)
	assert.Equal(t, toidNew(t, 1, 2), claimSC.OperationID)
	if claimSC.KeyValue != nil {
		_, hasPoolID := claimSC.KeyValue["poolId"]
		assert.False(t, hasPoolID, "a backstop claim carries no pool address, so poolId must be omitted")
	}

	badDebtSC := requireOne(types.StateChangeReasonBadDebt)
	assert.Equal(t, toidNew(t, 2, 0), badDebtSC.ToID)
	assert.Equal(t, toidNew(t, 2, 1), badDebtSC.OperationID)
	assert.Equal(t, poolAddr, badDebtSC.KeyValue["poolId"])

	require.Len(t, byReason[types.StateChangeReasonLiquidation], 2)
	for _, sc := range byReason[types.StateChangeReasonLiquidation] {
		assert.Equal(t, toidNew(t, 3, 0), sc.ToID)
		assert.Equal(t, toidNew(t, 3, 3), sc.OperationID)
		assert.Equal(t, poolAddr, sc.KeyValue["poolId"])
	}

	// History-only mode must never touch current-state maps.
	assert.Empty(t, p.stagedNetDeltas)
	assert.Empty(t, p.stagedAuctionAdjs)

	m.stateChanges.On("BatchCopy", mock.Anything, mock.Anything, mock.MatchedBy(func(scs []types.StateChange) bool {
		return len(scs) == 7
	})).Return(7, nil).Once()
	require.NoError(t, p.PersistHistory(ctx, nil))
}

// toidStagesHistoryLedgerSeq is the LedgerSequence used throughout
// TestProcessLedger_StagesHistory's toid assertions.
const toidStagesHistoryLedgerSeq = 300

// toidNew computes toid.New(toidStagesHistoryLedgerSeq, txIdx, opOrTxID).ToInt64()
// for assertions, mirroring the processor's own ID computation.
func toidNew(t *testing.T, txIdx, opOrTxID int32) int64 {
	t.Helper()
	return toid.New(toidStagesHistoryLedgerSeq, txIdx, opOrTxID).ToInt64()
}

// posOp is one ordered position-model mutation captured during a test run:
// either a delete of key, or an upsert of snapshot.
type posOp struct {
	isDelete bool
	key      blenddata.PoolUserKey
	snapshot blenddata.PositionSnapshot
}

// captured accumulates the position-model calls and net-delta rows a
// processor run makes, in invocation order, for batch-equivalence comparison.
type captured struct {
	posLog       []posOp
	netDeltaRows []blenddata.PositionNetDelta
}

// wireCaptures stubs m's methods to append into c instead of asserting call
// counts — TestBatchEquivalence needs the actual argument content, not
// call-count expectations.
func wireCaptures(m *blenddata.PositionModelMock, c *captured) {
	m.On("DeleteByPoolUser", mock.Anything, mock.Anything, mock.Anything).Maybe().
		Run(func(args mock.Arguments) {
			for _, k := range args.Get(2).([]blenddata.PoolUserKey) {
				c.posLog = append(c.posLog, posOp{isDelete: true, key: k})
			}
		}).Return(nil)
	m.On("ZeroAbsentReserves", mock.Anything, mock.Anything, mock.Anything).Maybe().Return(nil)
	m.On("BatchUpsertSnapshots", mock.Anything, mock.Anything, mock.Anything).Maybe().
		Run(func(args mock.Arguments) {
			for _, r := range args.Get(2).([]blenddata.PositionSnapshot) {
				c.posLog = append(c.posLog, posOp{snapshot: r})
			}
		}).Return(nil)
	m.On("BatchApplyNetDeltas", mock.Anything, mock.Anything, mock.Anything).Maybe().
		Run(func(args mock.Arguments) {
			c.netDeltaRows = append(c.netDeltaRows, args.Get(2).([]blenddata.PositionNetDelta)...)
		}).Return(nil)
	m.On("ApplyAuctionAdjustments", mock.Anything, mock.Anything, mock.Anything).Maybe().Return(nil)
}

// netState is the reduced (DB-visible) cost-basis state for one netKey after
// sequentially applying a series of PositionNetDelta rows.
type netState struct {
	supplied, borrowed *big.Int
}

// reduceNetDeltas simulates blend_positions.BatchApplyNetDeltas' SQL
// (supplied sums; borrowed sums, or resets to the delta when ZeroBorrowed)
// applied in rows' order, so both a one-window and a two-window run's
// captured rows can be compared for the same DB-visible end state.
func reduceNetDeltas(rows []blenddata.PositionNetDelta) map[netKey]*netState {
	state := map[netKey]*netState{}
	for _, r := range rows {
		key := netKey{Pool: r.Pool, User: r.User, Asset: r.Asset}
		s, ok := state[key]
		if !ok {
			s = &netState{supplied: big.NewInt(0), borrowed: big.NewInt(0)}
			state[key] = s
		}
		if supDelta, parsed := new(big.Int).SetString(r.NetSuppliedDelta, 10); parsed {
			s.supplied.Add(s.supplied, supDelta)
		}
		borDelta, parsed := new(big.Int).SetString(r.NetBorrowedDelta, 10)
		if !parsed {
			continue
		}
		if r.ZeroBorrowed {
			s.borrowed.Set(borDelta)
		} else {
			s.borrowed.Add(s.borrowed, borDelta)
		}
	}
	return state
}

// reducePositions simulates blend_positions' delete/upsert semantics applied
// in log's order: a delete drops the key's entire reserve-index map; an
// upsert replaces (LWW) just that reserve_index's row.
func reducePositions(log []posOp) map[blenddata.PoolUserKey]map[int32]blenddata.PositionSnapshot {
	state := map[blenddata.PoolUserKey]map[int32]blenddata.PositionSnapshot{}
	for _, op := range log {
		if op.isDelete {
			delete(state, op.key)
			continue
		}
		key := blenddata.PoolUserKey{Pool: op.snapshot.Pool, User: op.snapshot.User}
		if state[key] == nil {
			state[key] = map[int32]blenddata.PositionSnapshot{}
		}
		state[key][op.snapshot.ReserveIndex] = op.snapshot
	}
	return state
}

// TestBatchEquivalence verifies the batch-equivalence contract documented on
// services.ProtocolProcessor: folding a window of ledgers and persisting
// once must produce the same DB-visible end state as processing and
// persisting each ledger individually. Three scenarios, folded across two
// ledgers (L1, L2):
//
//	(a) user1 supplies 100 in L1 and withdraws 40 in L2 (net-supplied delta
//	    sums to 60 either way) and is re-snapshotted in L2.
//	(b) user2 borrows 500 in L1 and defaults (bad_debt) in L2 — the
//	    zero-then-add fold must land on the same final (zero) borrowed value
//	    whether folded together or applied via two separate persists.
//	(c) user3's Positions entry is removed in L1 and re-created in L2 — a
//	    single-window persist must never call DeleteByPoolUser for user3 (the
//	    final state is a create, not a delete), while a two-window run does
//	    delete in its L1 persist before L2 recreates the row. Both converge
//	    on the same final snapshot.
func TestBatchEquivalence(t *testing.T) {
	ctx := context.Background()
	poolAddr := randomContractAddr(t)
	asset1 := randomContractAddr(t)
	user1 := randomAccountAddr(t)
	user2 := randomAccountAddr(t)
	user3 := randomAccountAddr(t)
	poolPC := protocolContractFor(t, poolAddr, "aa")

	supplyEvent := contractEvent(t, poolAddr,
		[]xdr.ScVal{symScVal("supply"), contractAddrScVal(t, asset1), accountAddrScVal(t, user1)},
		vecScVal(i128ScVal(100), i128ScVal(95)),
	)
	borrowEvent := contractEvent(t, poolAddr,
		[]xdr.ScVal{symScVal("borrow"), contractAddrScVal(t, asset1), accountAddrScVal(t, user2)},
		vecScVal(i128ScVal(500), i128ScVal(480)),
	)
	withdrawEvent := contractEvent(t, poolAddr,
		[]xdr.ScVal{symScVal("withdraw"), contractAddrScVal(t, asset1), accountAddrScVal(t, user1)},
		vecScVal(i128ScVal(40), i128ScVal(38)),
	)
	badDebtEvent := contractEvent(t, poolAddr,
		[]xdr.ScVal{symScVal("bad_debt"), accountAddrScVal(t, user2), contractAddrScVal(t, asset1)},
		i128ScVal(200),
	)

	l1 := services.ProtocolProcessorInput{
		LedgerSequence:    1,
		ProtocolContracts: []data.ProtocolContracts{poolPC},
		ContractEvents: map[indexer.ContractEventKey][]xdr.ContractEvent{
			{TxIdx: 0, OpIdx: 0}: {supplyEvent},
			{TxIdx: 1, OpIdx: 0}: {borrowEvent},
		},
		ContractDataChanges: map[string][]ingest.Change{
			poolAddr: {
				removedChange(positionsKeyScVal(t, user3), voidScVal()),
				createdChange(positionsKeyScVal(t, user1), positionsSnapshotScVal(map[uint32]int64{0: 100}, nil)),
			},
		},
		StagingMode: services.StagingModeCurrentState,
	}
	l2 := services.ProtocolProcessorInput{
		LedgerSequence:    2,
		ProtocolContracts: []data.ProtocolContracts{poolPC},
		ContractEvents: map[indexer.ContractEventKey][]xdr.ContractEvent{
			{TxIdx: 0, OpIdx: 0}: {withdrawEvent},
			{TxIdx: 1, OpIdx: 0}: {badDebtEvent},
		},
		ContractDataChanges: map[string][]ingest.Change{
			poolAddr: {
				createdChange(positionsKeyScVal(t, user3), positionsSnapshotScVal(nil, map[uint32]int64{0: 999})),
				createdChange(positionsKeyScVal(t, user1), positionsSnapshotScVal(map[uint32]int64{0: 60}, nil)),
			},
		},
		StagingMode: services.StagingModeCurrentState,
	}

	// One window: fold L1+L2, persist once.
	pOne, mOne := newFullTestProcessor(t)
	var oneCap captured
	wireCaptures(mOne.positions, &oneCap)
	require.NoError(t, pOne.ProcessLedger(ctx, l1))
	require.NoError(t, pOne.ProcessLedger(ctx, l2))
	require.NoError(t, pOne.PersistCurrentState(ctx, nil))

	// Two windows: persist and Reset() between L1 and L2.
	pTwo, mTwo := newFullTestProcessor(t)
	var twoCap captured
	wireCaptures(mTwo.positions, &twoCap)
	require.NoError(t, pTwo.ProcessLedger(ctx, l1))
	require.NoError(t, pTwo.PersistCurrentState(ctx, nil))
	pTwo.Reset()
	require.NoError(t, pTwo.ProcessLedger(ctx, l2))
	require.NoError(t, pTwo.PersistCurrentState(ctx, nil))

	// (a) + (b): net-delta end state must match regardless of window split.
	oneNet := reduceNetDeltas(oneCap.netDeltaRows)
	twoNet := reduceNetDeltas(twoCap.netDeltaRows)

	key1 := netKey{Pool: poolAddr, User: user1, Asset: asset1}
	require.Contains(t, oneNet, key1)
	require.Contains(t, twoNet, key1)
	assert.Equal(t, 0, oneNet[key1].supplied.Cmp(twoNet[key1].supplied),
		"(a) net-supplied (100-40=60) must match whether folded in one window or two")
	assert.Equal(t, big.NewInt(60), oneNet[key1].supplied)

	key2 := netKey{Pool: poolAddr, User: user2, Asset: asset1}
	require.Contains(t, oneNet, key2)
	require.Contains(t, twoNet, key2)
	assert.Equal(t, 0, oneNet[key2].borrowed.Cmp(twoNet[key2].borrowed),
		"(b) net-borrowed after an other-user bad_debt reset must match whether folded in one window or two")
	assert.Equal(t, big.NewInt(0), oneNet[key2].borrowed)

	// (c): position end state must match, but the one-window run must never
	// delete user3 (it was recreated later in the same window).
	onePos := reducePositions(oneCap.posLog)
	twoPos := reducePositions(twoCap.posLog)

	user3Key := blenddata.PoolUserKey{Pool: poolAddr, User: user3}
	user1Key := blenddata.PoolUserKey{Pool: poolAddr, User: user1}

	require.Contains(t, onePos, user3Key)
	require.Contains(t, twoPos, user3Key)
	assert.Equal(t, "999", onePos[user3Key][0].CollateralBTokens)
	assert.Equal(t, onePos[user3Key][0].CollateralBTokens, twoPos[user3Key][0].CollateralBTokens)

	require.Contains(t, onePos, user1Key)
	require.Contains(t, twoPos, user1Key)
	assert.Equal(t, "60", onePos[user1Key][0].SupplyBTokens)
	assert.Equal(t, onePos[user1Key][0].SupplyBTokens, twoPos[user1Key][0].SupplyBTokens)

	for _, op := range oneCap.posLog {
		if op.isDelete {
			assert.NotEqual(t, user3Key, op.key,
				"single-window persist must not delete a key that was recreated later in the same window")
		}
	}
	deletedInTwo := false
	for _, op := range twoCap.posLog {
		if op.isDelete && op.key == user3Key {
			deletedInTwo = true
		}
	}
	assert.True(t, deletedInTwo, "sanity: the two-window run's L1 persist does delete user3 before L2 recreates it")
}

func TestProcessLedger_StagesAuctions(t *testing.T) {
	ctx := context.Background()
	poolAddr := randomContractAddr(t)
	user := randomAccountAddr(t)
	assetA := randomContractAddr(t)
	assetB := randomContractAddr(t)
	poolPC := protocolContractFor(t, poolAddr, "aa")

	baseInput := func(ledger uint32, changes ...ingest.Change) services.ProtocolProcessorInput {
		return services.ProtocolProcessorInput{
			LedgerSequence:      ledger,
			ProtocolContracts:   []data.ProtocolContracts{poolPC},
			ContractDataChanges: map[string][]ingest.Change{poolAddr: changes},
			StagingMode:         services.StagingModeCurrentState,
		}
	}
	key := auctionStageKey{Pool: poolAddr, User: user, AuctionType: 0}

	t.Run("created stages a live snapshot", func(t *testing.T) {
		p := newTestProcessor()
		p.Reset()
		require.NoError(t, p.ProcessLedger(ctx, baseInput(1,
			createdChange(auctionKeyScVal(t, 0, user), auctionValScVal(t, assetA, 1000, assetB, 2000, 12345)),
		)))

		require.Len(t, p.stagedAuctions, 1)
		require.Contains(t, p.stagedAuctions, key)
		sa := p.stagedAuctions[key]
		assert.False(t, sa.removed)
		require.NotNil(t, sa.data)
		assert.Equal(t, map[string]string{assetA: "1000"}, sa.data.Bid)
		assert.Equal(t, map[string]string{assetB: "2000"}, sa.data.Lot)
		assert.Equal(t, uint32(12345), sa.data.Block)
		assert.Equal(t, uint32(1), sa.ledger)
	})

	t.Run("created then removed in a later ledger nets to removed", func(t *testing.T) {
		p := newTestProcessor()
		p.Reset()
		require.NoError(t, p.ProcessLedger(ctx, baseInput(1,
			createdChange(auctionKeyScVal(t, 0, user), auctionValScVal(t, assetA, 1000, assetB, 2000, 12345)),
		)))
		p.needsReset = false // same window: no intervening Persist/Reset
		require.NoError(t, p.ProcessLedger(ctx, baseInput(2,
			removedChange(auctionKeyScVal(t, 0, user), mapValScVal(mapScVal())),
		)))

		require.Len(t, p.stagedAuctions, 1)
		sa := p.stagedAuctions[key]
		assert.True(t, sa.removed, "the later removal must win over the earlier create")
		assert.Nil(t, sa.data)
		assert.Equal(t, uint32(2), sa.ledger)
	})

	t.Run("partial-fill rewrite keeps the latest snapshot", func(t *testing.T) {
		p := newTestProcessor()
		p.Reset()
		require.NoError(t, p.ProcessLedger(ctx, baseInput(1,
			createdChange(auctionKeyScVal(t, 0, user), auctionValScVal(t, assetA, 1000, assetB, 2000, 12345)),
		)))
		require.NoError(t, p.ProcessLedger(ctx, baseInput(2,
			createdChange(auctionKeyScVal(t, 0, user), auctionValScVal(t, assetA, 400, assetB, 800, 12345)),
		)))

		require.Len(t, p.stagedAuctions, 1)
		sa := p.stagedAuctions[key]
		assert.False(t, sa.removed)
		require.NotNil(t, sa.data)
		assert.Equal(t, map[string]string{assetA: "400"}, sa.data.Bid, "the later partial-fill snapshot must win")
		assert.Equal(t, uint32(2), sa.ledger)
	})
}

func TestProcessLedger_StagesRewardZone(t *testing.T) {
	ctx := context.Background()
	backstopAddr := testCanonicalBackstopAddr
	poolA := randomContractAddr(t)
	poolB := randomContractAddr(t)
	poolC := randomContractAddr(t)
	backstopPC := protocolContractFor(t, backstopAddr, "bb")

	rzInput := func(ledger uint32, pools ...string) services.ProtocolProcessorInput {
		elems := make([]xdr.ScVal, 0, len(pools))
		for _, pool := range pools {
			elems = append(elems, contractAddrScVal(t, pool))
		}
		return services.ProtocolProcessorInput{
			LedgerSequence:      ledger,
			ProtocolContracts:   []data.ProtocolContracts{backstopPC},
			ContractDataChanges: map[string][]ingest.Change{backstopAddr: {createdChange(symScVal("RZ"), vecScVal(elems...))}},
			StagingMode:         services.StagingModeCurrentState,
		}
	}

	p := newTestProcessor()
	p.Reset()
	require.NoError(t, p.ProcessLedger(ctx, rzInput(1, poolA, poolB)))
	require.NoError(t, p.ProcessLedger(ctx, rzInput(2, poolC)))

	require.NotNil(t, p.stagedRewardZone)
	assert.Equal(t, []string{poolC}, p.stagedRewardZone.pools, "a later reward-zone list overwrites the earlier one")
	assert.Equal(t, uint32(2), p.stagedRewardZone.ledger)
}

func TestProcessLedger_StagesPoolAdmin(t *testing.T) {
	ctx := context.Background()
	poolAddr := randomContractAddr(t)
	oracleAddr := randomContractAddr(t)
	adminAddr := randomAccountAddr(t)
	poolPC := protocolContractFor(t, poolAddr, "aa")

	run := func(t *testing.T, instanceVal xdr.ScVal) *blenddata.Pool {
		t.Helper()
		p := newTestProcessor()
		p.Reset()
		require.NoError(t, p.ProcessLedger(ctx, services.ProtocolProcessorInput{
			LedgerSequence:      3,
			ProtocolContracts:   []data.ProtocolContracts{poolPC},
			ContractDataChanges: map[string][]ingest.Change{poolAddr: {createdChange(instanceKeyScVal(), instanceVal)}},
			StagingMode:         services.StagingModeCurrentState,
		}))
		require.Len(t, p.stagedPools, 1)
		return p.stagedPools[poolAddr]
	}

	t.Run("admin present populates the pool row", func(t *testing.T) {
		row := run(t, poolInstanceWithAdminScVal(t, "Fixed Pool v2", oracleAddr, adminAddr))
		assert.Equal(t, types.AddressBytea(adminAddr), row.Admin)
	})

	t.Run("admin absent leaves an empty admin", func(t *testing.T) {
		row := run(t, poolInstanceScVal(t, "Fixed Pool v2", oracleAddr))
		assert.Equal(t, types.AddressBytea(""), row.Admin)
	})
}

func TestProcessLedger_PersistsAuctionsAndRewardZone(t *testing.T) {
	ctx := context.Background()
	poolAddr := randomContractAddr(t)
	user1 := randomAccountAddr(t)
	user2 := randomAccountAddr(t)
	assetA := randomContractAddr(t)
	assetB := randomContractAddr(t)
	backstopAddr := testCanonicalBackstopAddr
	poolPC := protocolContractFor(t, poolAddr, "aa")
	backstopPC := protocolContractFor(t, backstopAddr, "bb")

	t.Run("delete precedes upsert and reward zone is set last", func(t *testing.T) {
		p, m := newFullTestProcessor(t)
		require.NoError(t, p.ProcessLedger(ctx, services.ProtocolProcessorInput{
			LedgerSequence:    7,
			ProtocolContracts: []data.ProtocolContracts{poolPC, backstopPC},
			ContractDataChanges: map[string][]ingest.Change{
				poolAddr: {
					createdChange(auctionKeyScVal(t, 0, user1), auctionValScVal(t, assetA, 1000, assetB, 2000, 12345)),
					removedChange(auctionKeyScVal(t, 1, user2), mapValScVal(mapScVal())),
				},
				backstopAddr: {createdChange(symScVal("RZ"), vecScVal(contractAddrScVal(t, poolAddr)))},
			},
			StagingMode: services.StagingModeCurrentState,
		}))

		var order []string
		recordOrder := func(name string) func(mock.Arguments) {
			return func(mock.Arguments) { order = append(order, name) }
		}

		m.auctions.On("DeleteByKey", mock.Anything, mock.Anything, mock.MatchedBy(func(keys []blenddata.AuctionKey) bool {
			return len(keys) == 1 && keys[0].Pool == types.AddressBytea(poolAddr) &&
				keys[0].User == types.AddressBytea(user2) && keys[0].AuctionType == 1
		})).Run(recordOrder("auctions.DeleteByKey")).Return(nil).Once()
		m.auctions.On("BatchUpsert", mock.Anything, mock.Anything, mock.MatchedBy(func(rows []blenddata.Auction) bool {
			return len(rows) == 1 && rows[0].Pool == types.AddressBytea(poolAddr) &&
				rows[0].User == types.AddressBytea(user1) && rows[0].AuctionType == 0 &&
				rows[0].Bid[assetA] == "1000" && rows[0].Lot[assetB] == "2000" &&
				rows[0].StartBlock == 12345 && rows[0].LastModifiedLedger == 7
		})).Run(recordOrder("auctions.BatchUpsert")).Return(nil).Once()
		m.pools.On("SetRewardZone", mock.Anything, mock.Anything, mock.MatchedBy(func(poolIDs []types.AddressBytea) bool {
			return len(poolIDs) == 1 && poolIDs[0] == types.AddressBytea(poolAddr)
		}), int32(7)).Run(recordOrder("pools.SetRewardZone")).Return(nil).Once()

		require.NoError(t, p.PersistCurrentState(ctx, nil))
		require.Equal(t, []string{"auctions.DeleteByKey", "auctions.BatchUpsert", "pools.SetRewardZone"}, order)
	})

	t.Run("no staged reward zone makes no SetRewardZone call", func(t *testing.T) {
		p, m := newFullTestProcessor(t)
		require.NoError(t, p.ProcessLedger(ctx, services.ProtocolProcessorInput{
			LedgerSequence:    7,
			ProtocolContracts: []data.ProtocolContracts{poolPC},
			ContractDataChanges: map[string][]ingest.Change{
				poolAddr: {createdChange(auctionKeyScVal(t, 0, user1), auctionValScVal(t, assetA, 1000, assetB, 2000, 12345))},
			},
			StagingMode: services.StagingModeCurrentState,
		}))
		require.Nil(t, p.stagedRewardZone)

		// Only the auction upsert is expected; SetRewardZone has no expectation, so a
		// call would fail AssertExpectations.
		m.auctions.On("BatchUpsert", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		require.NoError(t, p.PersistCurrentState(ctx, nil))
	})
}

// auctionOp is one ordered auction-model mutation captured during a test run:
// either a delete of key, or an upsert of row.
type auctionOp struct {
	isDelete bool
	key      blenddata.AuctionKey
	row      blenddata.Auction
}

// wireAuctionCaptures stubs m to append every DeleteByKey/BatchUpsert argument
// into ops in invocation order, for batch-equivalence comparison.
func wireAuctionCaptures(m *blenddata.AuctionModelMock, ops *[]auctionOp) {
	m.On("DeleteByKey", mock.Anything, mock.Anything, mock.Anything).Maybe().
		Run(func(args mock.Arguments) {
			for _, k := range args.Get(2).([]blenddata.AuctionKey) {
				*ops = append(*ops, auctionOp{isDelete: true, key: k})
			}
		}).Return(nil)
	m.On("BatchUpsert", mock.Anything, mock.Anything, mock.Anything).Maybe().
		Run(func(args mock.Arguments) {
			for _, r := range args.Get(2).([]blenddata.Auction) {
				*ops = append(*ops, auctionOp{row: r})
			}
		}).Return(nil)
}

// reduceAuctions simulates blend_auctions' delete/upsert semantics applied in
// ops' order: a delete drops the keyed row; an upsert replaces (LWW) it.
func reduceAuctions(ops []auctionOp) map[blenddata.AuctionKey]blenddata.Auction {
	state := map[blenddata.AuctionKey]blenddata.Auction{}
	for _, op := range ops {
		if op.isDelete {
			delete(state, op.key)
			continue
		}
		state[blenddata.AuctionKey{Pool: op.row.Pool, User: op.row.User, AuctionType: op.row.AuctionType}] = op.row
	}
	return state
}

// TestBatchEquivalence_AuctionsRewardZone extends the batch-equivalence
// contract to the auction and reward-zone folds. Folded across two ledgers
// (L1, L2):
//
//	(a) auction A (user1) is created in L1 and filled (removed) in L2, while
//	    auction B (user2) is created in L2 — a single-window persist must never
//	    upsert A (it nets to a delete) yet still converge on the same final
//	    {B} row set as a two-window run that upserts then deletes A.
//	(b) the reward zone is set to [poolA] in L1 and [poolA, poolB] in L2 (with
//	    poolB's pool instance created in L2) — the last-writer-wins overwrite
//	    means both runs converge on the same final membership regardless of the
//	    window split or the pool-creation ordering.
func TestBatchEquivalence_AuctionsRewardZone(t *testing.T) {
	ctx := context.Background()
	poolA := randomContractAddr(t)
	poolB := randomContractAddr(t)
	oracleAddr := randomContractAddr(t)
	user1 := randomAccountAddr(t)
	user2 := randomAccountAddr(t)
	assetA := randomContractAddr(t)
	assetB := randomContractAddr(t)
	backstopAddr := testCanonicalBackstopAddr
	poolAPC := protocolContractFor(t, poolA, "aa")
	poolBPC := protocolContractFor(t, poolB, "cc")
	backstopPC := protocolContractFor(t, backstopAddr, "bb")

	l1 := services.ProtocolProcessorInput{
		LedgerSequence:    1,
		ProtocolContracts: []data.ProtocolContracts{poolAPC, poolBPC, backstopPC},
		ContractDataChanges: map[string][]ingest.Change{
			poolA:        {createdChange(auctionKeyScVal(t, 0, user1), auctionValScVal(t, assetA, 1000, assetB, 2000, 100))},
			backstopAddr: {createdChange(symScVal("RZ"), vecScVal(contractAddrScVal(t, poolA)))},
		},
		StagingMode: services.StagingModeCurrentState,
	}
	l2 := services.ProtocolProcessorInput{
		LedgerSequence:    2,
		ProtocolContracts: []data.ProtocolContracts{poolAPC, poolBPC, backstopPC},
		ContractDataChanges: map[string][]ingest.Change{
			poolA: {removedChange(auctionKeyScVal(t, 0, user1), mapValScVal(mapScVal()))},
			poolB: {
				createdChange(instanceKeyScVal(), poolInstanceScVal(t, "", oracleAddr)),
				createdChange(auctionKeyScVal(t, 1, user2), auctionValScVal(t, assetA, 300, assetB, 600, 200)),
			},
			backstopAddr: {createdChange(symScVal("RZ"), vecScVal(contractAddrScVal(t, poolA), contractAddrScVal(t, poolB)))},
		},
		StagingMode: services.StagingModeCurrentState,
	}

	// One window: fold L1+L2, persist once.
	pOne, mOne := newFullTestProcessor(t)
	var oneOps []auctionOp
	var oneRZ []types.AddressBytea
	wireAuctionCaptures(mOne.auctions, &oneOps)
	mOne.protocolContracts.On("BatchInsert", mock.Anything, mock.Anything, mock.Anything).Maybe().Return(nil)
	mOne.pools.On("BatchUpsert", mock.Anything, mock.Anything, mock.Anything).Maybe().Return(nil)
	mOne.pools.On("SetRewardZone", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Maybe().
		Run(func(args mock.Arguments) { oneRZ = args.Get(2).([]types.AddressBytea) }).Return(nil)
	require.NoError(t, pOne.ProcessLedger(ctx, l1))
	require.NoError(t, pOne.ProcessLedger(ctx, l2))
	require.NoError(t, pOne.PersistCurrentState(ctx, nil))

	// Two windows: persist and Reset() between L1 and L2.
	pTwo, mTwo := newFullTestProcessor(t)
	var twoOps []auctionOp
	var twoRZ []types.AddressBytea
	wireAuctionCaptures(mTwo.auctions, &twoOps)
	mTwo.protocolContracts.On("BatchInsert", mock.Anything, mock.Anything, mock.Anything).Maybe().Return(nil)
	mTwo.pools.On("BatchUpsert", mock.Anything, mock.Anything, mock.Anything).Maybe().Return(nil)
	mTwo.pools.On("SetRewardZone", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Maybe().
		Run(func(args mock.Arguments) { twoRZ = args.Get(2).([]types.AddressBytea) }).Return(nil)
	require.NoError(t, pTwo.ProcessLedger(ctx, l1))
	require.NoError(t, pTwo.PersistCurrentState(ctx, nil))
	pTwo.Reset()
	require.NoError(t, pTwo.ProcessLedger(ctx, l2))
	require.NoError(t, pTwo.PersistCurrentState(ctx, nil))

	// (a): final auction row set must match, and neither run may leave A present.
	onePos := reduceAuctions(oneOps)
	twoPos := reduceAuctions(twoOps)
	keyB := blenddata.AuctionKey{Pool: types.AddressBytea(poolB), User: types.AddressBytea(user2), AuctionType: 1}
	keyA := blenddata.AuctionKey{Pool: types.AddressBytea(poolA), User: types.AddressBytea(user1), AuctionType: 0}
	require.Contains(t, onePos, keyB)
	require.Contains(t, twoPos, keyB)
	assert.Equal(t, onePos[keyB], twoPos[keyB], "the surviving auction row must be identical across window splits")
	assert.NotContains(t, onePos, keyA, "auction A nets to a delete and must not survive")
	assert.NotContains(t, twoPos, keyA)

	// One-window run must never upsert A (it was removed later in the same window).
	for _, op := range oneOps {
		if !op.isDelete {
			assert.NotEqual(t, keyA, blenddata.AuctionKey{Pool: op.row.Pool, User: op.row.User, AuctionType: op.row.AuctionType},
				"single-window persist must not upsert an auction removed later in the same window")
		}
	}

	// (b): the final reward-zone membership must match regardless of window split.
	assert.ElementsMatch(t, oneRZ, twoRZ)
	assert.ElementsMatch(t, []types.AddressBytea{types.AddressBytea(poolA), types.AddressBytea(poolB)}, oneRZ)
}

func TestProcessLedger_AuctionsRewardZoneStagingModes(t *testing.T) {
	ctx := context.Background()
	poolAddr := randomContractAddr(t)
	backstopAddr := testCanonicalBackstopAddr
	user := randomAccountAddr(t)
	assetA := randomContractAddr(t)
	assetB := randomContractAddr(t)
	poolPC := protocolContractFor(t, poolAddr, "aa")
	backstopPC := protocolContractFor(t, backstopAddr, "bb")

	buildInput := func(mode services.StagingMode) services.ProtocolProcessorInput {
		return services.ProtocolProcessorInput{
			LedgerSequence:    10,
			ProtocolContracts: []data.ProtocolContracts{poolPC, backstopPC},
			ContractDataChanges: map[string][]ingest.Change{
				poolAddr:     {createdChange(auctionKeyScVal(t, 0, user), auctionValScVal(t, assetA, 1000, assetB, 2000, 12345))},
				backstopAddr: {createdChange(symScVal("RZ"), vecScVal(contractAddrScVal(t, poolAddr)))},
			},
			StagingMode: mode,
		}
	}

	t.Run("history mode stages neither auctions nor reward zone", func(t *testing.T) {
		p := newTestProcessor()
		p.Reset()
		require.NoError(t, p.ProcessLedger(ctx, buildInput(services.StagingModeHistory)))
		assert.Empty(t, p.stagedAuctions)
		assert.Nil(t, p.stagedRewardZone)
	})

	t.Run("current-state mode stages both", func(t *testing.T) {
		p := newTestProcessor()
		p.Reset()
		require.NoError(t, p.ProcessLedger(ctx, buildInput(services.StagingModeCurrentState)))
		assert.Len(t, p.stagedAuctions, 1)
		assert.NotNil(t, p.stagedRewardZone)
	})
}

func TestNewProcessor(t *testing.T) {
	t.Run("nil Models leaves data-layer deps nil but still initializes staged sets", func(t *testing.T) {
		p := newProcessor(services.ProtocolDeps{NetworkPassphrase: testNetworkPassphrase})
		assert.Equal(t, testNetworkPassphrase, p.networkPassphrase)
		assert.Nil(t, p.pools)
		assert.Nil(t, p.positions)
		assert.Nil(t, p.reserves)
		assert.Nil(t, p.backstopPositions)
		assert.Nil(t, p.backstopPools)
		assert.Nil(t, p.reserveEmissions)
		assert.Nil(t, p.emissions)
		assert.Nil(t, p.stateChanges)
		assert.Nil(t, p.protocolContracts)

		// Reset() must already have run so ProcessLedger can fold without an
		// explicit Reset call first.
		assert.NotNil(t, p.stagedPools)
		assert.NotNil(t, p.stagedPositions)
		assert.NotNil(t, p.stagedNetDeltas)
		assert.NotNil(t, p.stagedAuctionAdjs)
		assert.NotNil(t, p.stagedReserves)
		assert.NotNil(t, p.stagedBackstopPositions)
		assert.NotNil(t, p.stagedBackstopPools)
		assert.NotNil(t, p.stagedReserveEmissions)
		assert.NotNil(t, p.stagedUserEmissions)
	})

	t.Run("wires every data-layer dependency from deps.Models", func(t *testing.T) {
		// blend.Models' fields are concrete *XxxModel types (see
		// internal/data/blend/models.go), not interfaces, so wiring is checked
		// against zero-value concrete instances rather than mocks here.
		poolsModel := &blenddata.PoolModel{}
		positionsModel := &blenddata.PositionModel{}
		reservesModel := &blenddata.ReserveModel{}
		backstopPositionsModel := &blenddata.BackstopPositionModel{}
		backstopPoolsModel := &blenddata.BackstopPoolModel{}
		reserveEmissionsModel := &blenddata.ReserveEmissionModel{}
		emissionsModel := &blenddata.EmissionModel{}
		protocolContractsMock := data.NewProtocolContractsModelMock(t)
		stateChangeModel := &data.StateChangeModel{}

		deps := services.ProtocolDeps{
			NetworkPassphrase: testNetworkPassphrase,
			Models: &data.Models{
				Blend: blenddata.Models{
					Pools:             poolsModel,
					Positions:         positionsModel,
					Reserves:          reservesModel,
					BackstopPositions: backstopPositionsModel,
					BackstopPools:     backstopPoolsModel,
					ReserveEmissions:  reserveEmissionsModel,
					Emissions:         emissionsModel,
				},
				StateChanges:      stateChangeModel,
				ProtocolContracts: protocolContractsMock,
			},
		}

		p := newProcessor(deps)
		assert.Equal(t, poolsModel, p.pools)
		assert.Equal(t, positionsModel, p.positions)
		assert.Equal(t, reservesModel, p.reserves)
		assert.Equal(t, backstopPositionsModel, p.backstopPositions)
		assert.Equal(t, backstopPoolsModel, p.backstopPools)
		assert.Equal(t, reserveEmissionsModel, p.reserveEmissions)
		assert.Equal(t, emissionsModel, p.emissions)
		assert.Equal(t, stateChangeModel, p.stateChanges)
		assert.Equal(t, protocolContractsMock, p.protocolContracts)
	})
}
