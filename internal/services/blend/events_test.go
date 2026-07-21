package blend

import (
	"testing"

	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/indexer/types"
)

// test fixtures --------------------------------------------------------------
//
// symScVal, u32ScVal, u64ScVal, i128ScVal, vecScVal, mapScVal, mapValScVal,
// symEntry, contractAddrScVal, accountAddrScVal, randomContractAddr and
// randomAccountAddr are declared in scval_test.go/entries_test.go and reused
// here.

// alwaysUntracked is an isTracked stub reporting every address as not a
// tracked Blend contract — the correct answer for every event except a
// backstop "withdraw", exercised separately in
// TestParseEvent_WithdrawDisambiguation.
func alwaysUntracked(string) bool { return false }

// trackedSet returns an isTracked stub reporting true only for addrs.
func trackedSet(addrs ...string) func(string) bool {
	set := make(map[string]struct{}, len(addrs))
	for _, a := range addrs {
		set[a] = struct{}{}
	}
	return func(addr string) bool {
		_, ok := set[addr]
		return ok
	}
}

// eventContractID decodes a strkey C-address into the raw xdr.ContractId
// an xdr.ContractEvent carries.
func eventContractID(t *testing.T, cAddr string) xdr.ContractId {
	t.Helper()
	raw, err := strkey.Decode(strkey.VersionByteContract, cAddr)
	require.NoError(t, err)
	var cid xdr.ContractId
	copy(cid[:], raw)
	return cid
}

// contractEvent builds a V0 Contract-type event emitted by emitter, mirroring
// sep41's test builder of the same name.
func contractEvent(t *testing.T, emitter string, topics []xdr.ScVal, data xdr.ScVal) xdr.ContractEvent {
	t.Helper()
	cid := eventContractID(t, emitter)
	return xdr.ContractEvent{
		Type:       xdr.ContractEventTypeContract,
		ContractId: &cid,
		Body: xdr.ContractEventBody{
			V: 0,
			V0: &xdr.ContractEventV0{
				Topics: topics,
				Data:   data,
			},
		},
	}
}

// addrMapEntry builds an ScMapEntry keyed by a contract address ScVal, the
// shape of AuctionData's bid/lot maps (Map<Address, i128>).
func addrMapEntry(t *testing.T, addr string, val xdr.ScVal) xdr.ScMapEntry {
	t.Helper()
	return xdr.ScMapEntry{Key: contractAddrScVal(t, addr), Val: val}
}

// tests -----------------------------------------------------------------

func TestParseEvent_Supply(t *testing.T) {
	poolAddr := randomContractAddr(t)
	assetAddr := randomContractAddr(t)
	fromAddr := randomAccountAddr(t)
	event := contractEvent(t, poolAddr,
		[]xdr.ScVal{symScVal("supply"), contractAddrScVal(t, assetAddr), accountAddrScVal(t, fromAddr)},
		vecScVal(i128ScVal(1000), i128ScVal(950)),
	)

	got, err := ParseEvent(event, "", alwaysUntracked)
	require.NoError(t, err)
	require.NotNil(t, got)

	require.Len(t, got.Rows, 1)
	row := got.Rows[0]
	assert.Equal(t, types.StateChangeReasonSupply, row.Reason)
	assert.Equal(t, fromAddr, row.Account)
	assert.Equal(t, assetAddr, row.Token)
	assert.Equal(t, "1000", row.Amount)
	assert.Equal(t, poolAddr, row.PoolID)
	assert.Equal(t, map[string]any{"bTokens": "950"}, row.Extra)

	require.Len(t, got.NetDeltas, 1)
	assert.Equal(t, NetDeltaFold{User: fromAddr, Asset: assetAddr, NetSuppliedDelta: "1000", NetBorrowedDelta: "0"}, got.NetDeltas[0])
	assert.Empty(t, got.AuctionAdjs)
}

func TestParseEvent_SupplyCollateral(t *testing.T) {
	poolAddr := randomContractAddr(t)
	assetAddr := randomContractAddr(t)
	fromAddr := randomAccountAddr(t)
	event := contractEvent(t, poolAddr,
		[]xdr.ScVal{symScVal("supply_collateral"), contractAddrScVal(t, assetAddr), accountAddrScVal(t, fromAddr)},
		vecScVal(i128ScVal(500), i128ScVal(480)),
	)

	got, err := ParseEvent(event, "", alwaysUntracked)
	require.NoError(t, err)
	require.NotNil(t, got)

	require.Len(t, got.Rows, 1)
	row := got.Rows[0]
	assert.Equal(t, types.StateChangeReasonSupplyCollateral, row.Reason)
	assert.Equal(t, "500", row.Amount)
	assert.Equal(t, map[string]any{"bTokens": "480"}, row.Extra)

	require.Len(t, got.NetDeltas, 1)
	assert.Equal(t, "500", got.NetDeltas[0].NetSuppliedDelta)
}

func TestParseEvent_WithdrawCollateral(t *testing.T) {
	poolAddr := randomContractAddr(t)
	assetAddr := randomContractAddr(t)
	fromAddr := randomAccountAddr(t)
	event := contractEvent(t, poolAddr,
		[]xdr.ScVal{symScVal("withdraw_collateral"), contractAddrScVal(t, assetAddr), accountAddrScVal(t, fromAddr)},
		vecScVal(i128ScVal(300), i128ScVal(290)),
	)

	got, err := ParseEvent(event, "", alwaysUntracked)
	require.NoError(t, err)
	require.NotNil(t, got)

	require.Len(t, got.Rows, 1)
	row := got.Rows[0]
	assert.Equal(t, types.StateChangeReasonWithdrawCollateral, row.Reason)
	assert.Equal(t, assetAddr, row.Token)
	assert.Equal(t, "300", row.Amount)
	assert.Equal(t, poolAddr, row.PoolID)
	assert.Equal(t, map[string]any{"bTokens": "290"}, row.Extra)

	require.Len(t, got.NetDeltas, 1)
	assert.Equal(t, "-300", got.NetDeltas[0].NetSuppliedDelta)
}

func TestParseEvent_Borrow(t *testing.T) {
	poolAddr := randomContractAddr(t)
	assetAddr := randomContractAddr(t)
	fromAddr := randomAccountAddr(t)
	event := contractEvent(t, poolAddr,
		[]xdr.ScVal{symScVal("borrow"), contractAddrScVal(t, assetAddr), accountAddrScVal(t, fromAddr)},
		vecScVal(i128ScVal(3000), i128ScVal(2950)),
	)

	got, err := ParseEvent(event, "", alwaysUntracked)
	require.NoError(t, err)
	require.NotNil(t, got)

	require.Len(t, got.Rows, 1)
	row := got.Rows[0]
	assert.Equal(t, types.StateChangeReasonBorrow, row.Reason)
	assert.Equal(t, fromAddr, row.Account)
	assert.Equal(t, assetAddr, row.Token)
	assert.Equal(t, "3000", row.Amount)
	assert.Equal(t, map[string]any{"dTokens": "2950"}, row.Extra)

	require.Len(t, got.NetDeltas, 1)
	assert.Equal(t, NetDeltaFold{User: fromAddr, Asset: assetAddr, NetBorrowedDelta: "3000", NetSuppliedDelta: "0"}, got.NetDeltas[0])
}

func TestParseEvent_Repay(t *testing.T) {
	poolAddr := randomContractAddr(t)
	assetAddr := randomContractAddr(t)
	fromAddr := randomAccountAddr(t)
	event := contractEvent(t, poolAddr,
		[]xdr.ScVal{symScVal("repay"), contractAddrScVal(t, assetAddr), accountAddrScVal(t, fromAddr)},
		vecScVal(i128ScVal(1200), i128ScVal(1150)),
	)

	got, err := ParseEvent(event, "", alwaysUntracked)
	require.NoError(t, err)
	require.NotNil(t, got)

	require.Len(t, got.Rows, 1)
	row := got.Rows[0]
	assert.Equal(t, types.StateChangeReasonRepay, row.Reason)
	assert.Equal(t, "1200", row.Amount)
	assert.Equal(t, map[string]any{"dTokens": "1150"}, row.Extra)

	require.Len(t, got.NetDeltas, 1)
	assert.Equal(t, "-1200", got.NetDeltas[0].NetBorrowedDelta)
}

func TestParseEvent_FlashLoan(t *testing.T) {
	poolAddr := randomContractAddr(t)
	assetAddr := randomContractAddr(t)
	fromAddr := randomAccountAddr(t)
	borrowerContract := randomContractAddr(t)
	event := contractEvent(t, poolAddr,
		[]xdr.ScVal{
			symScVal("flash_loan"), contractAddrScVal(t, assetAddr), accountAddrScVal(t, fromAddr),
			contractAddrScVal(t, borrowerContract),
		},
		vecScVal(i128ScVal(5000), i128ScVal(4900)),
	)

	got, err := ParseEvent(event, "", alwaysUntracked)
	require.NoError(t, err)
	require.NotNil(t, got)

	require.Len(t, got.Rows, 1)
	row := got.Rows[0]
	assert.Equal(t, types.StateChangeReasonFlashLoan, row.Reason)
	assert.Equal(t, fromAddr, row.Account)
	assert.Equal(t, assetAddr, row.Token)
	assert.Equal(t, "5000", row.Amount)
	assert.Equal(t, poolAddr, row.PoolID)
	assert.Equal(t, map[string]any{"contract": borrowerContract, "dTokens": "4900"}, row.Extra)

	require.Len(t, got.NetDeltas, 1)
	assert.Equal(t, NetDeltaFold{User: fromAddr, Asset: assetAddr, NetBorrowedDelta: "5000", NetSuppliedDelta: "0"}, got.NetDeltas[0])
}

func TestParseEvent_BadDebt(t *testing.T) {
	poolAddr := randomContractAddr(t)
	userAddr := randomAccountAddr(t)
	assetAddr := randomContractAddr(t)
	event := contractEvent(t, poolAddr,
		[]xdr.ScVal{symScVal("bad_debt"), accountAddrScVal(t, userAddr), contractAddrScVal(t, assetAddr)},
		i128ScVal(400),
	)

	got, err := ParseEvent(event, "", alwaysUntracked)
	require.NoError(t, err)
	require.NotNil(t, got)

	require.Len(t, got.Rows, 1)
	row := got.Rows[0]
	assert.Equal(t, types.StateChangeReasonBadDebt, row.Reason)
	assert.Equal(t, userAddr, row.Account)
	assert.Equal(t, assetAddr, row.Token)
	assert.Equal(t, "400", row.Amount)
	assert.Equal(t, poolAddr, row.PoolID)
	assert.Equal(t, map[string]any{"units": "d_tokens"}, row.Extra)

	require.Len(t, got.NetDeltas, 1)
	assert.Equal(t, NetDeltaFold{
		User: userAddr, Asset: assetAddr, ZeroBorrowed: true, NetBorrowedDelta: "0", NetSuppliedDelta: "0",
	}, got.NetDeltas[0])
}

func TestParseEvent_DefaultedDebt(t *testing.T) {
	poolAddr := randomContractAddr(t)
	assetAddr := randomContractAddr(t)
	event := contractEvent(t, poolAddr,
		[]xdr.ScVal{symScVal("defaulted_debt"), contractAddrScVal(t, assetAddr)},
		i128ScVal(250),
	)

	got, err := ParseEvent(event, "", alwaysUntracked)
	require.NoError(t, err)
	require.NotNil(t, got)

	require.Len(t, got.Rows, 1)
	row := got.Rows[0]
	assert.Equal(t, types.StateChangeReasonDefaultedDebt, row.Reason)
	assert.Equal(t, poolAddr, row.Account, "defaulted_debt has no user; the emitting pool is the row's account")
	assert.Equal(t, assetAddr, row.Token)
	assert.Equal(t, "250", row.Amount)
	assert.Equal(t, poolAddr, row.PoolID)
	assert.Equal(t, map[string]any{"applies_to": "backstop", "units": "d_tokens"}, row.Extra)
	assert.Empty(t, got.NetDeltas)
}

func TestParseEvent_FillAuction(t *testing.T) {
	poolAddr := randomContractAddr(t)
	userAddr := randomAccountAddr(t)
	fillerAddr := randomAccountAddr(t)
	assetA := randomContractAddr(t)
	assetB := randomContractAddr(t)
	assetC := randomContractAddr(t)

	buildEvent := func(auctionType uint32) xdr.ContractEvent {
		auctionData := mapScVal(
			symEntry("bid", mapValScVal(mapScVal(
				addrMapEntry(t, assetC, i128ScVal(300)),
			))),
			symEntry("block", u32ScVal(999)),
			symEntry("lot", mapValScVal(mapScVal(
				addrMapEntry(t, assetA, i128ScVal(1000)),
				addrMapEntry(t, assetB, i128ScVal(2000)),
			))),
		)
		return contractEvent(t, poolAddr,
			[]xdr.ScVal{symScVal("fill_auction"), u32ScVal(auctionType), accountAddrScVal(t, userAddr)},
			vecScVal(accountAddrScVal(t, fillerAddr), i128ScVal(50), mapValScVal(auctionData)),
		)
	}

	t.Run("auction_type 0 (UserLiquidation) folds both user and filler", func(t *testing.T) {
		got, err := ParseEvent(buildEvent(0), "", alwaysUntracked)
		require.NoError(t, err)
		require.NotNil(t, got)

		require.Len(t, got.Rows, 2)
		userRow, fillerRow := got.Rows[0], got.Rows[1]
		assert.Equal(t, types.StateChangeReasonLiquidation, userRow.Reason)
		assert.Equal(t, userAddr, userRow.Account)
		assert.Equal(t, "", userRow.Token)
		assert.Equal(t, "", userRow.Amount)
		assert.Equal(t, poolAddr, userRow.PoolID)
		assert.Equal(t, fillerAddr, userRow.Extra["counterparty"])
		assert.Equal(t, uint32(0), userRow.Extra["auctionType"])
		assert.Equal(t, 50, userRow.Extra["fillPercent"])
		assert.Equal(t, map[string]string{assetC: "300"}, userRow.Extra["bid"])
		assert.Equal(t, map[string]string{assetA: "1000", assetB: "2000"}, userRow.Extra["lot"])

		assert.Equal(t, types.StateChangeReasonLiquidation, fillerRow.Reason)
		assert.Equal(t, fillerAddr, fillerRow.Account)
		assert.Equal(t, userAddr, fillerRow.Extra["counterparty"])

		want := []AuctionFold{
			{User: userAddr, Asset: assetA, LotBTokensDelta: "-1000", BidDTokensDelta: "0"},
			{User: userAddr, Asset: assetB, LotBTokensDelta: "-2000", BidDTokensDelta: "0"},
			{User: userAddr, Asset: assetC, LotBTokensDelta: "0", BidDTokensDelta: "-300"},
			{User: fillerAddr, Asset: assetA, LotBTokensDelta: "1000", BidDTokensDelta: "0"},
			{User: fillerAddr, Asset: assetB, LotBTokensDelta: "2000", BidDTokensDelta: "0"},
			{User: fillerAddr, Asset: assetC, LotBTokensDelta: "0", BidDTokensDelta: "300"},
		}
		assert.ElementsMatch(t, want, got.AuctionAdjs)
	})

	t.Run("auction_type 1 (BadDebtAuction) folds the bid side for user and filler; lot is not pool state", func(t *testing.T) {
		got, err := ParseEvent(buildEvent(1), "", alwaysUntracked)
		require.NoError(t, err)
		require.NotNil(t, got)

		require.Len(t, got.Rows, 2)
		assert.Equal(t, fillerAddr, got.Rows[1].Account)

		// fill_bad_debt_auction moves the bid dTokens from the backstop's
		// Positions (the auction "user") to the filler's; the lot (backstop
		// LP tokens) is drawn straight to the filler's wallet and never
		// touches pool Positions, so it must not be folded for anyone.
		want := []AuctionFold{
			{User: userAddr, Asset: assetC, LotBTokensDelta: "0", BidDTokensDelta: "-300"},
			{User: fillerAddr, Asset: assetC, LotBTokensDelta: "0", BidDTokensDelta: "300"},
		}
		assert.ElementsMatch(t, want, got.AuctionAdjs)
	})

	t.Run("auction_type 2 (InterestAuction) folds nothing; both rows still emitted", func(t *testing.T) {
		got, err := ParseEvent(buildEvent(2), "", alwaysUntracked)
		require.NoError(t, err)
		require.NotNil(t, got)

		require.Len(t, got.Rows, 2)
		// fill_interest_auction settles entirely outside pool Positions: the
		// bid (backstop tokens) is donated to the backstop and the lot
		// (underlying interest) is paid from the reserves' backstop_credit,
		// which the ResData entry snapshot captures. Any fold here would
		// fabricate a cost-basis adjustment the chain never made.
		assert.Empty(t, got.AuctionAdjs)
	})
}

func TestParseEvent_WithdrawDisambiguation(t *testing.T) {
	poolAddr := randomContractAddr(t)
	otherAddr := randomContractAddr(t) // reserve asset (pool case) or pool address (backstop case)
	fromAddr := randomAccountAddr(t)
	data := vecScVal(i128ScVal(700), i128ScVal(650))
	event := contractEvent(t, poolAddr,
		[]xdr.ScVal{symScVal("withdraw"), contractAddrScVal(t, otherAddr), accountAddrScVal(t, fromAddr)},
		data,
	)

	t.Run("interprets as a pool withdraw when topic[1] is not a tracked Blend contract", func(t *testing.T) {
		got, err := ParseEvent(event, "", alwaysUntracked)
		require.NoError(t, err)
		require.NotNil(t, got)

		require.Len(t, got.Rows, 1)
		row := got.Rows[0]
		assert.Equal(t, types.StateChangeReasonWithdraw, row.Reason)
		assert.Equal(t, otherAddr, row.Token)
		assert.Equal(t, "700", row.Amount, "pool withdraw's data[0] is tokens_out")
		assert.Equal(t, poolAddr, row.PoolID)
		assert.Equal(t, map[string]any{"bTokens": "650"}, row.Extra)

		require.Len(t, got.NetDeltas, 1)
		assert.Equal(t, "-700", got.NetDeltas[0].NetSuppliedDelta)
	})

	t.Run("interprets as a backstop withdraw when topic[1] is a tracked Blend contract", func(t *testing.T) {
		got, err := ParseEvent(event, "", trackedSet(otherAddr))
		require.NoError(t, err)
		require.NotNil(t, got)

		require.Len(t, got.Rows, 1)
		row := got.Rows[0]
		assert.Equal(t, types.StateChangeReasonBackstopWithdraw, row.Reason)
		assert.Equal(t, "", row.Token)
		assert.Equal(t, "650", row.Amount, "backstop withdraw's data[1] is tokens_out")
		assert.Equal(t, otherAddr, row.PoolID)
		assert.Equal(t, map[string]any{"shares": "700"}, row.Extra, "backstop withdraw's data[0] is shares burnt")
		assert.Empty(t, got.NetDeltas, "backstop LP shares don't fold blend_positions cost basis")
	})
}

func TestParseEvent_ClaimDisambiguation(t *testing.T) {
	poolAddr := randomContractAddr(t)
	fromAddr := randomAccountAddr(t)
	const blndToken = "BLND_TEST_TOKEN"

	t.Run("pool claim: 2-tuple (Vec<u32>, i128) data", func(t *testing.T) {
		event := contractEvent(t, poolAddr,
			[]xdr.ScVal{symScVal("claim"), accountAddrScVal(t, fromAddr)},
			vecScVal(vecScVal(u32ScVal(1), u32ScVal(3)), i128ScVal(500)),
		)

		got, err := ParseEvent(event, blndToken, alwaysUntracked)
		require.NoError(t, err)
		require.NotNil(t, got)

		require.Len(t, got.Rows, 1)
		row := got.Rows[0]
		assert.Equal(t, types.StateChangeReasonClaim, row.Reason)
		assert.Equal(t, fromAddr, row.Account)
		assert.Equal(t, blndToken, row.Token)
		assert.Equal(t, "500", row.Amount)
		assert.Equal(t, poolAddr, row.PoolID)
		assert.Equal(t, map[string]any{"source": "pool", "reserveTokenIds": []uint32{1, 3}}, row.Extra)

		require.Len(t, got.ClaimFolds, 1)
		assert.Equal(t, ClaimFold{Account: fromAddr, Source: claimSourcePool, Amount: "500"}, got.ClaimFolds[0])
		assert.Empty(t, got.NetDeltas, "a claim never folds cost basis")
	})

	t.Run("backstop claim: bare i128 data, no pool", func(t *testing.T) {
		event := contractEvent(t, poolAddr,
			[]xdr.ScVal{symScVal("claim"), accountAddrScVal(t, fromAddr)},
			i128ScVal(750),
		)

		got, err := ParseEvent(event, blndToken, alwaysUntracked)
		require.NoError(t, err)
		require.NotNil(t, got)

		require.Len(t, got.Rows, 1)
		row := got.Rows[0]
		assert.Equal(t, types.StateChangeReasonClaim, row.Reason)
		assert.Equal(t, "", row.Token, "backstop claim pays out Comet LP tokens (auto-deposited), never raw BLND")
		assert.Equal(t, "750", row.Amount)
		assert.Equal(t, "", row.PoolID, "backstop claim carries no pool address")
		assert.Equal(t, map[string]any{"source": "backstop", "units": "backstop_lp"}, row.Extra)

		require.Len(t, got.ClaimFolds, 1)
		assert.Equal(t, ClaimFold{Account: fromAddr, Source: claimSourceBackstop, Amount: "750"}, got.ClaimFolds[0])
	})

	t.Run("blndToken empty leaves Token empty on both shapes", func(t *testing.T) {
		event := contractEvent(t, poolAddr,
			[]xdr.ScVal{symScVal("claim"), accountAddrScVal(t, fromAddr)},
			i128ScVal(1),
		)
		got, err := ParseEvent(event, "", alwaysUntracked)
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, "", got.Rows[0].Token)
	})
}

func TestParseEvent_BackstopDeposit(t *testing.T) {
	backstopAddr := randomContractAddr(t)
	poolAddr := randomContractAddr(t)
	fromAddr := randomAccountAddr(t)
	event := contractEvent(t, backstopAddr,
		[]xdr.ScVal{symScVal("deposit"), contractAddrScVal(t, poolAddr), accountAddrScVal(t, fromAddr)},
		vecScVal(i128ScVal(2000), i128ScVal(1800)),
	)

	got, err := ParseEvent(event, "", alwaysUntracked)
	require.NoError(t, err)
	require.NotNil(t, got)

	require.Len(t, got.Rows, 1)
	row := got.Rows[0]
	assert.Equal(t, types.StateChangeReasonBackstopDeposit, row.Reason)
	assert.Equal(t, fromAddr, row.Account)
	assert.Equal(t, "", row.Token)
	assert.Equal(t, "2000", row.Amount)
	assert.Equal(t, poolAddr, row.PoolID)
	assert.Equal(t, map[string]any{"shares": "1800", "units": "backstop_lp"}, row.Extra)
	assert.Empty(t, got.NetDeltas)
}

func TestParseEvent_QueueWithdrawal(t *testing.T) {
	backstopAddr := randomContractAddr(t)
	poolAddr := randomContractAddr(t)
	fromAddr := randomAccountAddr(t)
	event := contractEvent(t, backstopAddr,
		[]xdr.ScVal{symScVal("queue_withdrawal"), contractAddrScVal(t, poolAddr), accountAddrScVal(t, fromAddr)},
		vecScVal(i128ScVal(300), u64ScVal(123456)),
	)

	got, err := ParseEvent(event, "", alwaysUntracked)
	require.NoError(t, err)
	require.NotNil(t, got)

	require.Len(t, got.Rows, 1)
	row := got.Rows[0]
	assert.Equal(t, types.StateChangeReasonBackstopWithdrawQueue, row.Reason)
	assert.Equal(t, fromAddr, row.Account)
	assert.Equal(t, "300", row.Amount)
	assert.Equal(t, poolAddr, row.PoolID)
	assert.Equal(t, map[string]any{"exp": uint64(123456)}, row.Extra)
}

func TestParseEvent_DequeueWithdrawal(t *testing.T) {
	backstopAddr := randomContractAddr(t)
	poolAddr := randomContractAddr(t)
	fromAddr := randomAccountAddr(t)
	event := contractEvent(t, backstopAddr,
		[]xdr.ScVal{symScVal("dequeue_withdrawal"), contractAddrScVal(t, poolAddr), accountAddrScVal(t, fromAddr)},
		i128ScVal(150),
	)

	got, err := ParseEvent(event, "", alwaysUntracked)
	require.NoError(t, err)
	require.NotNil(t, got)

	require.Len(t, got.Rows, 1)
	row := got.Rows[0]
	assert.Equal(t, types.StateChangeReasonBackstopWithdrawCancel, row.Reason)
	assert.Equal(t, fromAddr, row.Account)
	assert.Equal(t, "150", row.Amount)
	assert.Equal(t, poolAddr, row.PoolID)
	assert.Nil(t, row.Extra)
}

func TestParseEvent_IgnoredSymbols(t *testing.T) {
	poolAddr := randomContractAddr(t)
	for _, sym := range []string{"gulp", "gulp_emissions", "donate", "set_admin", "distribute", "new_auction", "draw"} {
		event := contractEvent(t, poolAddr, []xdr.ScVal{symScVal(sym)}, i128ScVal(0))
		got, err := ParseEvent(event, "", alwaysUntracked)
		require.NoError(t, err, sym)
		assert.Nil(t, got, sym)
	}
}

func TestParseEvent_UnknownSymbol(t *testing.T) {
	poolAddr := randomContractAddr(t)
	event := contractEvent(t, poolAddr, []xdr.ScVal{symScVal("totally_unknown_event")}, i128ScVal(0))
	got, err := ParseEvent(event, "", alwaysUntracked)
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestParseEvent_NonContractOrNonV0(t *testing.T) {
	poolAddr := randomContractAddr(t)
	cid := eventContractID(t, poolAddr)

	t.Run("System event type is ignored", func(t *testing.T) {
		event := xdr.ContractEvent{
			Type:       xdr.ContractEventTypeSystem,
			ContractId: &cid,
			Body: xdr.ContractEventBody{V: 0, V0: &xdr.ContractEventV0{
				Topics: []xdr.ScVal{symScVal("supply")}, Data: i128ScVal(0),
			}},
		}
		got, err := ParseEvent(event, "", alwaysUntracked)
		require.NoError(t, err)
		assert.Nil(t, got)
	})

	t.Run("non-V0 body is ignored", func(t *testing.T) {
		event := xdr.ContractEvent{
			Type:       xdr.ContractEventTypeContract,
			ContractId: &cid,
			Body:       xdr.ContractEventBody{V: 1},
		}
		got, err := ParseEvent(event, "", alwaysUntracked)
		require.NoError(t, err)
		assert.Nil(t, got)
	})

	t.Run("empty topics is ignored", func(t *testing.T) {
		event := contractEvent(t, poolAddr, []xdr.ScVal{}, i128ScVal(0))
		got, err := ParseEvent(event, "", alwaysUntracked)
		require.NoError(t, err)
		assert.Nil(t, got)
	})

	t.Run("non-symbol first topic is ignored", func(t *testing.T) {
		event := contractEvent(t, poolAddr, []xdr.ScVal{i128ScVal(1)}, i128ScVal(0))
		got, err := ParseEvent(event, "", alwaysUntracked)
		require.NoError(t, err)
		assert.Nil(t, got)
	})
}

func TestParseEvent_Malformed(t *testing.T) {
	poolAddr := randomContractAddr(t)
	assetAddr := randomContractAddr(t)
	fromAddr := randomAccountAddr(t)

	t.Run("wrong topic count", func(t *testing.T) {
		event := contractEvent(t, poolAddr,
			[]xdr.ScVal{symScVal("supply"), contractAddrScVal(t, assetAddr)}, // missing "from"
			vecScVal(i128ScVal(1), i128ScVal(1)),
		)
		got, err := ParseEvent(event, "", alwaysUntracked)
		assert.Nil(t, got)
		assert.Error(t, err)
	})

	t.Run("non-i128 data", func(t *testing.T) {
		event := contractEvent(t, poolAddr,
			[]xdr.ScVal{symScVal("borrow"), contractAddrScVal(t, assetAddr), accountAddrScVal(t, fromAddr)},
			vecScVal(symScVal("not_an_i128"), i128ScVal(1)),
		)
		got, err := ParseEvent(event, "", alwaysUntracked)
		assert.Nil(t, got)
		assert.Error(t, err)
	})

	t.Run("missing map field", func(t *testing.T) {
		fillerAddr := randomAccountAddr(t)
		userAddr := randomAccountAddr(t)
		assetC := randomContractAddr(t)
		auctionData := mapScVal(
			symEntry("bid", mapValScVal(mapScVal(addrMapEntry(t, assetC, i128ScVal(300))))),
			symEntry("block", u32ScVal(1)),
			// "lot" intentionally omitted.
		)
		event := contractEvent(t, poolAddr,
			[]xdr.ScVal{symScVal("fill_auction"), u32ScVal(0), accountAddrScVal(t, userAddr)},
			vecScVal(accountAddrScVal(t, fillerAddr), i128ScVal(100), mapValScVal(auctionData)),
		)
		got, err := ParseEvent(event, "", alwaysUntracked)
		assert.Nil(t, got)
		assert.Error(t, err)
	})
}
