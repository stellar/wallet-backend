// events.go decodes a Blend v2 pool or backstop contract's ContractEvents
// into LENDING state-change history rows plus cost-basis fold instructions.
// ParseEvent is the only exported entry point; the processor (a later task)
// calls it once per event in a transaction's diagnostic events, stages the
// returned rows, and applies the returned folds via
// blenddata.PositionModelInterface.
package blend

import (
	"fmt"
	"math/big"
	"sort"
	"strconv"

	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/wallet-backend/internal/indexer/types"
)

// ignoredEventSymbols lists Blend pool and backstop event symbols this
// package doesn't model: pool administration (set_admin, update_pool,
// queue/cancel/set_reserve, set_status, reserve_emission_update, gulp,
// gulp_emissions, new_auction, delete_auction) and backstop bookkeeping
// (distribute, rw_zone_add, rw_zone_remove, draw, donate). None carry
// LENDING history or cost-basis semantics.
var ignoredEventSymbols = map[string]struct{}{
	"set_admin":               {},
	"update_pool":             {},
	"queue_set_reserve":       {},
	"cancel_set_reserve":      {},
	"set_reserve":             {},
	"set_status":              {},
	"reserve_emission_update": {},
	"gulp_emissions":          {},
	"gulp":                    {},
	"new_auction":             {},
	"delete_auction":          {},
	"distribute":              {},
	"rw_zone_add":             {},
	"rw_zone_remove":          {},
	"draw":                    {},
	"donate":                  {},
}

// EventRow is one LENDING state-change history row decoded from a Blend
// ContractEvent. Token == "" means the row's token_id column is NULL
// (LIQUIDATION rows, whose amount is carried in Extra's lot/bid maps
// instead, and backstop share-denominated rows). Amount == "" means the
// row's amount column is NULL (LIQUIDATION rows). PoolID == "" means the
// row's poolId key_value entry is omitted (backstop CLAIM, whose event
// carries no pool address at all).
type EventRow struct {
	Reason  types.StateChangeReason
	Account string
	Token   string
	Amount  string
	PoolID  string
	Extra   map[string]any
}

// NetDeltaFold is a signed, additive cost-basis delta for one (user, asset),
// ready to become a blend/data.PositionNetDelta once the processor fills in
// Pool and LedgerNumber from the event's context.
type NetDeltaFold struct {
	User             string
	Asset            string
	NetSuppliedDelta string
	NetBorrowedDelta string
	ZeroBorrowed     bool
}

// AuctionFold is a protocol-token cost-basis adjustment for one (user,
// asset), ready to become a blend/data.PositionAuctionAdjustment once the
// processor fills in Pool and LedgerNumber from the event's context.
type AuctionFold struct {
	User            string
	Asset           string
	LotBTokensDelta string
	BidDTokensDelta string
}

// DecodedEvent is the result of decoding one Blend ContractEvent: the
// LENDING history rows it produces, plus the cost-basis folds (net-delta
// and/or auction) it implies. Either slice may be empty — most events
// produce rows with no fold (deposit, claim, ...), fill_auction produces
// rows with auction folds and no net-delta folds, and most balance-changing
// events produce exactly one row with exactly one net-delta fold.
type DecodedEvent struct {
	Rows        []EventRow
	NetDeltas   []NetDeltaFold
	AuctionAdjs []AuctionFold
}

// ParseEvent decodes one Blend pool or backstop ContractEvent into LENDING
// history rows and cost-basis folds. It never panics.
//
// blndToken is the BLND SAC C-address for the current network (see
// blndTokenAddress); pool CLAIM rows use it as their Token (backstop CLAIM
// rows pay out Comet LP tokens instead and leave Token empty — see
// decodeClaimAmbiguous). It may be "" on an unrecognized network, in which
// case pool CLAIM rows also leave Token "" (their token_id column is then
// NULL rather than misattributed).
//
// isTracked reports whether addr is a contract this run classifies as
// Blend (pool or backstop) — see the withdraw/claim disambiguation notes
// below.
//
// Non-Contract event types, non-V0 event bodies, and event bodies with no
// symbol topic all decode to (nil, nil): there is nothing to classify.
// Symbols in ignoredEventSymbols, and any symbol not otherwise recognized,
// also decode to (nil, nil). A recognized symbol whose topics or data don't
// match its expected shape is reported as an error.
func ParseEvent(event xdr.ContractEvent, blndToken string, isTracked func(addr string) bool) (*DecodedEvent, error) {
	if event.Type != xdr.ContractEventTypeContract {
		return nil, nil
	}
	if event.Body.V != 0 || event.Body.V0 == nil {
		return nil, nil
	}
	topics := event.Body.V0.Topics
	if len(topics) == 0 {
		return nil, nil
	}
	sym, ok := topics[0].GetSym()
	if !ok {
		return nil, nil
	}
	symName := string(sym)
	if _, ignored := ignoredEventSymbols[symName]; ignored {
		return nil, nil
	}

	data := event.Body.V0.Data
	// poolID is the emitting contract's own address. It is the correct
	// PoolID for every pool-emitted event (the pool that emitted the event
	// is the pool the row belongs to); backstop-emitted events instead take
	// their PoolID from a topic (see decodeWithdrawAmbiguous,
	// decodeBackstopDeposit, etc.), since one backstop instance serves many
	// pools. A malformed/absent ContractId degrades to "", which only
	// matters for the pool-emitted events below — they report it as an
	// error rather than silently mis-tagging a row.
	poolID, _ := contractIDAddress(event)

	switch symName {
	case "supply":
		return decodeSupplyLike(symName, topics, data, poolID, types.StateChangeReasonSupply, "bTokens")
	case "supply_collateral":
		return decodeSupplyLike(symName, topics, data, poolID, types.StateChangeReasonSupplyCollateral, "bTokens")
	case "withdraw_collateral":
		return decodeWithdrawLike(symName, topics, data, poolID, types.StateChangeReasonWithdrawCollateral, "bTokens")
	case "borrow":
		return decodeBorrowLike(symName, topics, data, poolID, types.StateChangeReasonBorrow, "dTokens", true)
	case "repay":
		return decodeBorrowLike(symName, topics, data, poolID, types.StateChangeReasonRepay, "dTokens", false)
	case "flash_loan":
		return decodeFlashLoan(topics, data, poolID)
	case "bad_debt":
		return decodeBadDebt(topics, data, poolID)
	case "defaulted_debt":
		return decodeDefaultedDebt(topics, data, poolID)
	case "fill_auction":
		return decodeFillAuction(topics, data, poolID)
	case "withdraw":
		return decodeWithdrawAmbiguous(topics, data, poolID, isTracked)
	case "claim":
		return decodeClaimAmbiguous(topics, data, poolID, blndToken)
	case "deposit":
		return decodeBackstopDeposit(topics, data)
	case "queue_withdrawal":
		return decodeQueueWithdrawal(topics, data)
	case "dequeue_withdrawal":
		return decodeDequeueWithdrawal(topics, data)
	default:
		return nil, nil
	}
}

// decodeTwoAddrTopics validates a [sym, addr1, addr2] topic shape shared by
// every 3-topic Blend event and returns addr1, addr2 decoded to strkey.
// symName is used only to name the event in a shape-mismatch error.
func decodeTwoAddrTopics(topics []xdr.ScVal, symName string) (addr1, addr2 string, err error) {
	if len(topics) != 3 {
		return "", "", fmt.Errorf("blend: %s: expected 3 topics, got %d", symName, len(topics))
	}
	addr1, ok := addrString(topics[1])
	if !ok {
		return "", "", fmt.Errorf("blend: %s: topic[1] is not an address (type %v)", symName, topics[1].Type)
	}
	addr2, ok = addrString(topics[2])
	if !ok {
		return "", "", fmt.Errorf("blend: %s: topic[2] is not an address (type %v)", symName, topics[2].Type)
	}
	return addr1, addr2, nil
}

// decodeI128Pair decodes data as a 2-element ScVec of i128 values — the
// shape shared by every Blend token-movement event's data payload
// (tokens_in/out, protocol_tokens_minted/burnt). symName names the event in
// a shape-mismatch error.
func decodeI128Pair(data xdr.ScVal, symName string) (v1, v2 string, err error) {
	vec, ok := vecVal(data)
	if !ok || len(vec) != 2 {
		return "", "", fmt.Errorf("blend: %s: data must be a 2-element vec, got %v", symName, data.Type)
	}
	v1, ok = i128String(vec[0])
	if !ok {
		return "", "", fmt.Errorf("blend: %s: data[0] is not i128 (type %v)", symName, vec[0].Type)
	}
	v2, ok = i128String(vec[1])
	if !ok {
		return "", "", fmt.Errorf("blend: %s: data[1] is not i128 (type %v)", symName, vec[1].Type)
	}
	return v1, v2, nil
}

// decodeSupplyLike decodes supply/supply_collateral: topics [sym, asset,
// from], data (tokens_in, protocol_tokens_minted). Both credit the user's
// net-supplied cost basis by tokens_in.
func decodeSupplyLike(symName string, topics []xdr.ScVal, data xdr.ScVal, poolID string, reason types.StateChangeReason, protocolField string) (*DecodedEvent, error) {
	asset, from, err := decodeTwoAddrTopics(topics, symName)
	if err != nil {
		return nil, err
	}
	tokensIn, minted, err := decodeI128Pair(data, symName)
	if err != nil {
		return nil, err
	}

	row := EventRow{
		Reason: reason, Account: from, Token: asset, Amount: tokensIn, PoolID: poolID,
		Extra: map[string]any{protocolField: minted},
	}
	fold := NetDeltaFold{User: from, Asset: asset, NetSuppliedDelta: tokensIn, NetBorrowedDelta: "0"}
	return &DecodedEvent{Rows: []EventRow{row}, NetDeltas: []NetDeltaFold{fold}}, nil
}

// decodeWithdrawLike decodes withdraw_collateral (and, once disambiguated,
// pool withdraw): topics [sym, asset, from], data (tokens_out,
// protocol_tokens_burnt). Both debit the user's net-supplied cost basis by
// tokens_out.
func decodeWithdrawLike(symName string, topics []xdr.ScVal, data xdr.ScVal, poolID string, reason types.StateChangeReason, protocolField string) (*DecodedEvent, error) {
	asset, from, err := decodeTwoAddrTopics(topics, symName)
	if err != nil {
		return nil, err
	}
	tokensOut, burnt, err := decodeI128Pair(data, symName)
	if err != nil {
		return nil, err
	}

	row := EventRow{
		Reason: reason, Account: from, Token: asset, Amount: tokensOut, PoolID: poolID,
		Extra: map[string]any{protocolField: burnt},
	}
	fold := NetDeltaFold{User: from, Asset: asset, NetSuppliedDelta: negateDecimalString(tokensOut), NetBorrowedDelta: "0"}
	return &DecodedEvent{Rows: []EventRow{row}, NetDeltas: []NetDeltaFold{fold}}, nil
}

// decodeBorrowLike decodes borrow/repay: topics [sym, asset, from], data
// (amount, protocol_tokens_delta). positive=true (borrow) credits the
// user's net-borrowed cost basis by amount; positive=false (repay) debits
// it.
func decodeBorrowLike(symName string, topics []xdr.ScVal, data xdr.ScVal, poolID string, reason types.StateChangeReason, protocolField string, positive bool) (*DecodedEvent, error) {
	asset, from, err := decodeTwoAddrTopics(topics, symName)
	if err != nil {
		return nil, err
	}
	amount, protocolAmount, err := decodeI128Pair(data, symName)
	if err != nil {
		return nil, err
	}

	delta := amount
	if !positive {
		delta = negateDecimalString(amount)
	}
	row := EventRow{
		Reason: reason, Account: from, Token: asset, Amount: amount, PoolID: poolID,
		Extra: map[string]any{protocolField: protocolAmount},
	}
	fold := NetDeltaFold{User: from, Asset: asset, NetBorrowedDelta: delta, NetSuppliedDelta: "0"}
	return &DecodedEvent{Rows: []EventRow{row}, NetDeltas: []NetDeltaFold{fold}}, nil
}

// decodeFlashLoan decodes flash_loan: topics [sym, asset, from, contract],
// data (tokens_out, d_tokens_minted). Credits the user's net-borrowed cost
// basis by tokens_out, same as borrow.
func decodeFlashLoan(topics []xdr.ScVal, data xdr.ScVal, poolID string) (*DecodedEvent, error) {
	if len(topics) != 4 {
		return nil, fmt.Errorf("blend: flash_loan: expected 4 topics, got %d", len(topics))
	}
	asset, ok := addrString(topics[1])
	if !ok {
		return nil, fmt.Errorf("blend: flash_loan: topic[1] asset is not an address (type %v)", topics[1].Type)
	}
	from, ok := addrString(topics[2])
	if !ok {
		return nil, fmt.Errorf("blend: flash_loan: topic[2] from is not an address (type %v)", topics[2].Type)
	}
	contract, ok := addrString(topics[3])
	if !ok {
		return nil, fmt.Errorf("blend: flash_loan: topic[3] contract is not an address (type %v)", topics[3].Type)
	}

	tokensOut, minted, err := decodeI128Pair(data, "flash_loan")
	if err != nil {
		return nil, err
	}

	row := EventRow{
		Reason: types.StateChangeReasonFlashLoan, Account: from, Token: asset, Amount: tokensOut, PoolID: poolID,
		Extra: map[string]any{"contract": contract, "dTokens": minted},
	}
	fold := NetDeltaFold{User: from, Asset: asset, NetBorrowedDelta: tokensOut, NetSuppliedDelta: "0"}
	return &DecodedEvent{Rows: []EventRow{row}, NetDeltas: []NetDeltaFold{fold}}, nil
}

// decodeBadDebt decodes bad_debt: topics [sym, user, asset], data d_tokens
// (bare i128). Resets (rather than accumulates against) the user's
// net-borrowed cost basis for asset via ZeroBorrowed.
func decodeBadDebt(topics []xdr.ScVal, data xdr.ScVal, poolID string) (*DecodedEvent, error) {
	if len(topics) != 3 {
		return nil, fmt.Errorf("blend: bad_debt: expected 3 topics, got %d", len(topics))
	}
	user, ok := addrString(topics[1])
	if !ok {
		return nil, fmt.Errorf("blend: bad_debt: topic[1] user is not an address (type %v)", topics[1].Type)
	}
	asset, ok := addrString(topics[2])
	if !ok {
		return nil, fmt.Errorf("blend: bad_debt: topic[2] asset is not an address (type %v)", topics[2].Type)
	}
	dTokens, ok := i128String(data)
	if !ok {
		return nil, fmt.Errorf("blend: bad_debt: data is not i128 (type %v)", data.Type)
	}

	row := EventRow{
		Reason: types.StateChangeReasonBadDebt, Account: user, Token: asset, Amount: dTokens, PoolID: poolID,
		Extra: map[string]any{"units": "d_tokens"},
	}
	fold := NetDeltaFold{User: user, Asset: asset, ZeroBorrowed: true, NetBorrowedDelta: "0", NetSuppliedDelta: "0"}
	return &DecodedEvent{Rows: []EventRow{row}, NetDeltas: []NetDeltaFold{fold}}, nil
}

// decodeDefaultedDebt decodes defaulted_debt: topics [sym, asset], data
// d_tokens_burnt (bare i128). The event carries no user — the debt is
// socialized to the backstop — so the row's Account is the emitting pool's
// own address.
func decodeDefaultedDebt(topics []xdr.ScVal, data xdr.ScVal, poolID string) (*DecodedEvent, error) {
	if len(topics) != 2 {
		return nil, fmt.Errorf("blend: defaulted_debt: expected 2 topics, got %d", len(topics))
	}
	asset, ok := addrString(topics[1])
	if !ok {
		return nil, fmt.Errorf("blend: defaulted_debt: topic[1] asset is not an address (type %v)", topics[1].Type)
	}
	dTokensBurnt, ok := i128String(data)
	if !ok {
		return nil, fmt.Errorf("blend: defaulted_debt: data is not i128 (type %v)", data.Type)
	}
	if poolID == "" {
		return nil, fmt.Errorf("blend: defaulted_debt: could not resolve emitting pool contract address")
	}

	row := EventRow{
		Reason: types.StateChangeReasonDefaultedDebt, Account: poolID, Token: asset, Amount: dTokensBurnt, PoolID: poolID,
		Extra: map[string]any{"applies_to": "backstop", "units": "d_tokens"},
	}
	return &DecodedEvent{Rows: []EventRow{row}}, nil
}

// decodeFillAuction decodes fill_auction: topics [sym, auction_type, user],
// data (filler, fill_percent, filled_auction_data). filled_auction_data is
// an AuctionData map {bid: Map<Address,i128>, block: u32, lot:
// Map<Address,i128>}.
//
// Confirmed against blend-contracts-v2 @ ba22b487 (pool/src/auctions/auction.rs,
// fill()/scale_auction()): the pool scales AuctionData's bid/lot amounts down
// to the filled portion (percent_filled applied) before returning it for the
// event, so filled_auction_data already reflects only what this fill moved —
// no additional fill_percent scaling is applied here.
//
// Two rows are emitted (user and filler both experienced a LIQUIDATION
// state change). Cost-basis folds mirror exactly the pool-Positions
// mutations each fill performs on-chain (pool/src/auctions/*.rs @ ba22b487):
//   - auction_type 0 (UserLiquidation): the user loses the lot (collateral
//     bTokens) and the bid (liability dTokens), and the filler inherits
//     both — fold every touched asset for both sides, opposite signs.
//   - auction_type 1 (BadDebtAuction): fill_bad_debt_auction moves the bid
//     dTokens from the backstop's Positions (the auction "user" is always
//     the backstop address) to the filler's; the lot (backstop LP tokens)
//     is drawn straight to the filler's wallet and never touches pool
//     Positions — fold the bid side only, for both sides.
//   - auction_type 2 (InterestAuction): fill_interest_auction settles
//     entirely outside pool Positions (bid donated to the backstop, lot
//     paid from the reserves' backstop_credit, which the ResData entry
//     snapshot captures) — no folds at all.
func decodeFillAuction(topics []xdr.ScVal, data xdr.ScVal, poolID string) (*DecodedEvent, error) {
	if len(topics) != 3 {
		return nil, fmt.Errorf("blend: fill_auction: expected 3 topics, got %d", len(topics))
	}
	auctionType, ok := u32Val(topics[1])
	if !ok {
		return nil, fmt.Errorf("blend: fill_auction: topic[1] auction_type is not a u32 (type %v)", topics[1].Type)
	}
	user, ok := addrString(topics[2])
	if !ok {
		return nil, fmt.Errorf("blend: fill_auction: topic[2] user is not an address (type %v)", topics[2].Type)
	}

	vec, ok := vecVal(data)
	if !ok || len(vec) != 3 {
		return nil, fmt.Errorf("blend: fill_auction: data must be a 3-element vec, got %v", data.Type)
	}
	filler, ok := addrString(vec[0])
	if !ok {
		return nil, fmt.Errorf("blend: fill_auction: data[0] filler is not an address (type %v)", vec[0].Type)
	}
	fillPercentStr, ok := i128String(vec[1])
	if !ok {
		return nil, fmt.Errorf("blend: fill_auction: data[1] fill_percent is not i128 (type %v)", vec[1].Type)
	}
	fillPercent, convErr := strconv.Atoi(fillPercentStr)
	if convErr != nil {
		return nil, fmt.Errorf("blend: fill_auction: fill_percent %q is not an integer: %w", fillPercentStr, convErr)
	}

	auctionMap, ok := vec[2].GetMap()
	if !ok || auctionMap == nil {
		return nil, fmt.Errorf("blend: fill_auction: data[2] filled_auction_data is not a map (type %v)", vec[2].Type)
	}
	r := &mapReader{m: auctionMap, kind: "fill_auction.filled_auction_data"}
	lot := readField(r, "lot", mapAddrI128)
	bid := readField(r, "bid", mapAddrI128)
	if r.err != nil {
		return nil, r.err
	}
	// AuctionData.block is metadata about when the auction was created; it
	// has no LENDING history or cost-basis role and is not decoded.

	extraFor := func(counterparty string) map[string]any {
		return map[string]any{
			"auctionType":  auctionType,
			"fillPercent":  fillPercent,
			"counterparty": counterparty,
			"lot":          lot,
			"bid":          bid,
		}
	}
	rows := []EventRow{
		{Reason: types.StateChangeReasonLiquidation, Account: user, PoolID: poolID, Extra: extraFor(filler)},
		{Reason: types.StateChangeReasonLiquidation, Account: filler, PoolID: poolID, Extra: extraFor(user)},
	}

	foldLot := lot
	switch auctionType {
	case 0: // UserLiquidation: both lot and bid are pool-Positions moves.
	case 1: // BadDebtAuction: only the bid dTokens move through Positions.
		foldLot = nil
	default: // InterestAuction (2) and any future type: no Positions moves.
		return &DecodedEvent{Rows: rows}, nil
	}

	assets := unionAssetKeys(foldLot, bid)
	folds := make([]AuctionFold, 0, 2*len(assets))
	for _, asset := range assets {
		lotDelta, bidDelta := "0", "0"
		if amt, present := foldLot[asset]; present {
			lotDelta = negateDecimalString(amt)
		}
		if amt, present := bid[asset]; present {
			bidDelta = negateDecimalString(amt)
		}
		folds = append(folds, AuctionFold{User: user, Asset: asset, LotBTokensDelta: lotDelta, BidDTokensDelta: bidDelta})
	}
	// The filler inherits every Positions move the user side lost.
	for _, asset := range assets {
		lotDelta, bidDelta := "0", "0"
		if amt, present := foldLot[asset]; present {
			lotDelta = amt
		}
		if amt, present := bid[asset]; present {
			bidDelta = amt
		}
		folds = append(folds, AuctionFold{User: filler, Asset: asset, LotBTokensDelta: lotDelta, BidDTokensDelta: bidDelta})
	}

	return &DecodedEvent{Rows: rows, AuctionAdjs: folds}, nil
}

// decodeWithdrawAmbiguous decodes the "withdraw" symbol, which is emitted
// with structurally identical shape ([sym, Address, Address] topics,
// (i128, i128) data) by both the pool and the backstop contracts.
//
// Disambiguation: topic[1] is a reserve asset for a pool withdraw, and a
// pool address for a backstop withdraw. BLEND only ever tracks pool and
// backstop contracts, so a reserve asset is never isTracked — checking
// isTracked(topic[1]) reliably tells the two apart.
func decodeWithdrawAmbiguous(topics []xdr.ScVal, data xdr.ScVal, poolID string, isTracked func(addr string) bool) (*DecodedEvent, error) {
	addr1, from, err := decodeTwoAddrTopics(topics, "withdraw")
	if err != nil {
		return nil, err
	}

	if isTracked(addr1) {
		// Backstop withdraw: addr1 is the pool address. data = (amount
		// shares_burnt, tokens_out LP-tokens-returned).
		shares, tokensOut, dataErr := decodeI128Pair(data, "withdraw (backstop)")
		if dataErr != nil {
			return nil, dataErr
		}
		row := EventRow{
			Reason: types.StateChangeReasonBackstopWithdraw, Account: from, Token: "", Amount: tokensOut, PoolID: addr1,
			Extra: map[string]any{"shares": shares},
		}
		return &DecodedEvent{Rows: []EventRow{row}}, nil
	}

	// Pool withdraw: addr1 is the reserve asset. data = (tokens_out,
	// b_tokens_burnt).
	tokensOut, burnt, dataErr := decodeI128Pair(data, "withdraw (pool)")
	if dataErr != nil {
		return nil, dataErr
	}
	row := EventRow{
		Reason: types.StateChangeReasonWithdraw, Account: from, Token: addr1, Amount: tokensOut, PoolID: poolID,
		Extra: map[string]any{"bTokens": burnt},
	}
	fold := NetDeltaFold{User: from, Asset: addr1, NetSuppliedDelta: negateDecimalString(tokensOut), NetBorrowedDelta: "0"}
	return &DecodedEvent{Rows: []EventRow{row}, NetDeltas: []NetDeltaFold{fold}}, nil
}

// decodeClaimAmbiguous decodes the "claim" symbol, which both the pool and
// the backstop emit with an identical 2-topic shape ([sym, from]).
// Disambiguation is by data shape instead: a pool claim's data is a
// 2-element vec (Vec<u32> reserve_token_ids, i128 amount_claimed); a
// backstop claim's data is a bare i128 (an aggregate amount across every
// pool the caller claimed from, carrying no pool address at all).
func decodeClaimAmbiguous(topics []xdr.ScVal, data xdr.ScVal, poolID string, blndToken string) (*DecodedEvent, error) {
	if len(topics) != 2 {
		return nil, fmt.Errorf("blend: claim: expected 2 topics, got %d", len(topics))
	}
	from, ok := addrString(topics[1])
	if !ok {
		return nil, fmt.Errorf("blend: claim: topic[1] from is not an address (type %v)", topics[1].Type)
	}

	switch data.Type {
	case xdr.ScValTypeScvVec:
		vec, vecOk := vecVal(data)
		if !vecOk || len(vec) != 2 {
			return nil, fmt.Errorf("blend: claim (pool): data must be a 2-element vec, got %v", data.Type)
		}
		idsVec, idsOk := vecVal(vec[0])
		if !idsOk {
			return nil, fmt.Errorf("blend: claim (pool): data[0] reserve_token_ids is not a vec (type %v)", vec[0].Type)
		}
		ids := make([]uint32, 0, len(idsVec))
		for i, el := range idsVec {
			id, idOk := u32Val(el)
			if !idOk {
				return nil, fmt.Errorf("blend: claim (pool): reserve_token_ids[%d] is not a u32 (type %v)", i, el.Type)
			}
			ids = append(ids, id)
		}
		amount, amtOk := i128String(vec[1])
		if !amtOk {
			return nil, fmt.Errorf("blend: claim (pool): data[1] amount_claimed is not i128 (type %v)", vec[1].Type)
		}
		row := EventRow{
			Reason: types.StateChangeReasonClaim, Account: from, Token: blndToken, Amount: amount, PoolID: poolID,
			Extra: map[string]any{"source": "pool", "reserveTokenIds": ids},
		}
		return &DecodedEvent{Rows: []EventRow{row}}, nil
	case xdr.ScValTypeScvI128:
		amount, amtOk := i128String(data)
		if !amtOk {
			return nil, fmt.Errorf("blend: claim (backstop): data is not i128 (type %v)", data.Type)
		}
		// A backstop claim never pays out raw BLND: execute_claim swaps the
		// claimed BLND into Comet LP tokens and re-deposits them (emitting
		// real deposit events per pool), and the event's amount is the LP
		// tokens minted (backstop/src/contract.rs claim -> execute_claim's
		// lp_tokens_out @ ba22b487). Token stays NULL — the same
		// backstop-LP convention every backstop share row uses.
		row := EventRow{
			Reason: types.StateChangeReasonClaim, Account: from, Token: "", Amount: amount, PoolID: "",
			Extra: map[string]any{"source": "backstop", "units": "backstop_lp"},
		}
		return &DecodedEvent{Rows: []EventRow{row}}, nil
	default:
		return nil, fmt.Errorf("blend: claim: data must be a Vec (pool) or an I128 (backstop), got %v", data.Type)
	}
}

// decodeBackstopDeposit decodes the backstop's "deposit": topics [sym,
// pool_address, from], data (tokens_in, backstop_shares_minted). Backstop
// shares carry no separate underlying-asset column, so this produces no
// cost-basis fold.
func decodeBackstopDeposit(topics []xdr.ScVal, data xdr.ScVal) (*DecodedEvent, error) {
	poolAddr, from, err := decodeTwoAddrTopics(topics, "deposit")
	if err != nil {
		return nil, err
	}
	tokensIn, shares, err := decodeI128Pair(data, "deposit")
	if err != nil {
		return nil, err
	}

	row := EventRow{
		Reason: types.StateChangeReasonBackstopDeposit, Account: from, Token: "", Amount: tokensIn, PoolID: poolAddr,
		Extra: map[string]any{"shares": shares, "units": "backstop_lp"},
	}
	return &DecodedEvent{Rows: []EventRow{row}}, nil
}

// decodeQueueWithdrawal decodes the backstop's "queue_withdrawal": topics
// [sym, pool_address, from], data (amount shares_queued, expiration u64).
func decodeQueueWithdrawal(topics []xdr.ScVal, data xdr.ScVal) (*DecodedEvent, error) {
	poolAddr, from, err := decodeTwoAddrTopics(topics, "queue_withdrawal")
	if err != nil {
		return nil, err
	}
	vec, ok := vecVal(data)
	if !ok || len(vec) != 2 {
		return nil, fmt.Errorf("blend: queue_withdrawal: data must be a 2-element vec, got %v", data.Type)
	}
	amount, ok := i128String(vec[0])
	if !ok {
		return nil, fmt.Errorf("blend: queue_withdrawal: data[0] amount is not i128 (type %v)", vec[0].Type)
	}
	exp, ok := u64Val(vec[1])
	if !ok {
		return nil, fmt.Errorf("blend: queue_withdrawal: data[1] expiration is not u64 (type %v)", vec[1].Type)
	}

	row := EventRow{
		Reason: types.StateChangeReasonBackstopWithdrawQueue, Account: from, Token: "", Amount: amount, PoolID: poolAddr,
		Extra: map[string]any{"exp": exp},
	}
	return &DecodedEvent{Rows: []EventRow{row}}, nil
}

// decodeDequeueWithdrawal decodes the backstop's "dequeue_withdrawal":
// topics [sym, pool_address, from], data amount (bare i128, shares
// dequeued).
func decodeDequeueWithdrawal(topics []xdr.ScVal, data xdr.ScVal) (*DecodedEvent, error) {
	poolAddr, from, err := decodeTwoAddrTopics(topics, "dequeue_withdrawal")
	if err != nil {
		return nil, err
	}
	amount, ok := i128String(data)
	if !ok {
		return nil, fmt.Errorf("blend: dequeue_withdrawal: data is not i128 (type %v)", data.Type)
	}

	row := EventRow{
		Reason: types.StateChangeReasonBackstopWithdrawCancel, Account: from, Token: "", Amount: amount, PoolID: poolAddr,
	}
	return &DecodedEvent{Rows: []EventRow{row}}, nil
}

// negateDecimalString returns the arithmetic negation of s, a base-10
// integer string as produced by i128String (always valid, so the SetString
// failure branch is unreachable in practice and returns s unchanged rather
// than panicking).
func negateDecimalString(s string) string {
	n, ok := new(big.Int).SetString(s, 10)
	if !ok {
		return s
	}
	return n.Neg(n).String()
}

// unionAssetKeys returns the sorted, deduplicated set of keys across maps —
// used to enumerate every asset touched by a fill_auction's lot and/or bid
// maps so both get exactly one AuctionFold each.
func unionAssetKeys(maps ...map[string]string) []string {
	seen := make(map[string]struct{})
	for _, m := range maps {
		for k := range m {
			seen[k] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// contractIDAddress returns the strkey-encoded C-address of the contract
// that emitted event, or ok=false if the event carries no contract id.
func contractIDAddress(event xdr.ContractEvent) (addr string, ok bool) {
	if event.ContractId == nil {
		return "", false
	}
	encoded, err := strkey.Encode(strkey.VersionByteContract, event.ContractId[:])
	if err != nil {
		return "", false
	}
	return encoded, true
}
