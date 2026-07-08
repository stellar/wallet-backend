// processor.go implements services.ProtocolProcessor for the Blend v2
// lending protocol: it folds pool/backstop ContractData entries (current
// state) and ContractEvents (LENDING history) into staged sets, then
// persists them inside the CAS-guarded transaction. See entries.go for the
// ContractData decode and events.go for the event decode this file consumes.
package blend

import (
	"context"
	"encoding/hex"
	"fmt"
	"math/big"

	"github.com/jackc/pgx/v5"
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/toid"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/wallet-backend/internal/data"
	blenddata "github.com/stellar/wallet-backend/internal/data/blend"
	"github.com/stellar/wallet-backend/internal/indexer/processors"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/services"
)

// tokenTriple holds one reserve_index's three token-holding columns, each
// defaulting to "0" for a map a given reserve_index isn't present in (see
// PositionsData's godoc).
type tokenTriple struct {
	supply, collateral, liability string
}

// stagedPositions is the LWW-staged state for one (pool, user) Positions
// entry: either a full snapshot replacement (present, keyed by reserve_index)
// or a removal. A later entry for the same key fully replaces the prior one.
type stagedPositions struct {
	removed bool
	present map[uint32]tokenTriple
	ledger  uint32
}

// netKey identifies one (pool, user, asset) cost-basis net-delta accumulator.
type netKey struct {
	Pool, User, Asset string
}

// stagedNetDelta accumulates signed cost-basis deltas for one netKey across a
// window. ZeroBorrowed folds (bad_debt) reset borrowed to zero before any
// delta in the same fold is added, and any later delta in the same window
// accumulates on top of that zero (zero-then-add).
type stagedNetDelta struct {
	supplied, borrowed *big.Int
	zeroBorrowed       bool
	ledger             uint32
}

// auctionKey identifies one (pool, user, asset) auction-adjustment accumulator.
type auctionKey struct {
	Pool, User, Asset string
}

// stagedAuctionAdj accumulates signed protocol-token lot/bid deltas from
// fill_auction events for one auctionKey across a window.
type stagedAuctionAdj struct {
	lot, bid *big.Int
	ledger   uint32
}

// poolAssetKey identifies one pool reserve by (pool, asset).
type poolAssetKey struct {
	Pool, Asset string
}

// stagedReserve holds the ResConfig and ResData halves of one poolAssetKey's
// reserve row, each LWW independently. config and data both nil never
// happens (routing only creates an entry when at least one is decoded).
type stagedReserve struct {
	config *ReserveConfigData
	data   *ReserveDataData
	ledger uint32
}

// stagedBackstopPosition is the LWW-staged state for one (pool, user)
// backstop UserBalance entry: either a full snapshot replacement or a
// removal, mirroring stagedPositions.
type stagedBackstopPosition struct {
	removed bool
	shares  string
	q4w     []blenddata.Q4W
	ledger  uint32
}

// stagedBackstopPool holds the PoolBalance and BEmisData halves of one pool's
// blend_backstop_pools row, each LWW independently.
type stagedBackstopPool struct {
	balance *BackstopPoolBalanceData
	emis    *EmissionData
	ledger  uint32
}

// poolTokenKey identifies one pool's reserve-token emission stream.
type poolTokenKey struct {
	Pool    string
	TokenID uint32
}

// stagedResEmission is the LWW-staged EmisData for one poolTokenKey.
type stagedResEmission struct {
	data   *EmissionData
	ledger uint32
}

// emisKey identifies one user's raw emission accrual stream: Source is the
// pool contract for a reserve-emission stream, or the backstopped pool's
// address for a backstop-emission stream (TokenID ==
// blenddata.BackstopEmissionTokenID in that case).
type emisKey struct {
	Source, User string
	TokenID      int32
}

// stagedUserEmission is the LWW-staged UserEmissionData for one emisKey.
type stagedUserEmission struct {
	data   *UserEmissionData
	ledger uint32
}

// processor implements services.ProtocolProcessor for the Blend v2 lending
// protocol.
type processor struct {
	networkPassphrase string

	pools             blenddata.PoolModelInterface
	positions         blenddata.PositionModelInterface
	reserves          blenddata.ReserveModelInterface
	backstopPositions blenddata.BackstopPositionModelInterface
	backstopPools     blenddata.BackstopPoolModelInterface
	reserveEmissions  blenddata.ReserveEmissionModelInterface
	emissions         blenddata.EmissionModelInterface
	stateChanges      data.StateChangeWriter
	protocolContracts data.ProtocolContractsModelInterface

	// Contracts (pools and the singleton backstop) classified as Blend.
	// Populated from input.ProtocolContracts each ledger.
	blendContracts map[string]struct{}
	// contractWasmHash maps a tracked contract's C-address to its wasm hash,
	// so a pool-instance Name discovered mid-window can be enriched into
	// protocol_contracts (which requires a known, already-classified wasm_hash).
	contractWasmHash map[string]types.HashBytea

	// Staged sets that accumulate across ProcessLedger calls within a window and
	// are cleared by the caller via Reset().
	ledgerNumber            uint32
	stagedStateChanges      []types.StateChange
	stagedPools             map[string]*blenddata.Pool
	stagedPositions         map[blenddata.PoolUserKey]*stagedPositions
	stagedNetDeltas         map[netKey]*stagedNetDelta
	stagedAuctionAdjs       map[auctionKey]*stagedAuctionAdj
	stagedReserves          map[poolAssetKey]*stagedReserve
	stagedBackstopPositions map[blenddata.PoolUserKey]*stagedBackstopPosition
	stagedBackstopPools     map[string]*stagedBackstopPool
	stagedReserveEmissions  map[poolTokenKey]*stagedResEmission
	stagedUserEmissions     map[emisKey]*stagedUserEmission

	// needsReset is set by Persist* and cleared by Reset(). ProcessLedger refuses
	// to fold while it is set: folding after a Persist without an intervening
	// Reset would re-add already-committed state and silently double-count. Both
	// callers Reset() before folding again, so this only fires on misuse by a
	// future caller.
	needsReset bool
}

// newProcessor constructs a Blend processor from generic ProtocolDeps. The
// data layer paths are pulled from deps.Models.
func newProcessor(deps services.ProtocolDeps) *processor {
	p := &processor{
		networkPassphrase: deps.NetworkPassphrase,
	}
	if deps.Models != nil {
		p.pools = deps.Models.Blend.Pools
		p.positions = deps.Models.Blend.Positions
		p.reserves = deps.Models.Blend.Reserves
		p.backstopPositions = deps.Models.Blend.BackstopPositions
		p.backstopPools = deps.Models.Blend.BackstopPools
		p.reserveEmissions = deps.Models.Blend.ReserveEmissions
		p.emissions = deps.Models.Blend.Emissions
		p.stateChanges = deps.Models.StateChanges
		p.protocolContracts = deps.Models.ProtocolContracts
	}
	p.Reset()
	return p
}

// Compile-time interface check.
var _ services.ProtocolProcessor = (*processor)(nil)

func (p *processor) ProtocolID() string { return ProtocolID }

// RequiresContractData reports true: Blend's truth lives in ContractData
// entries (pool/backstop instance and persistent storage) — events alone
// carry LENDING history rows and cost-basis fold instructions, but never the
// current-state snapshot itself.
func (p *processor) RequiresContractData() bool { return true }

// ProcessLedger folds this ledger's Blend contract events (LENDING history
// and cost-basis folds) and, when current state is needed, ContractData
// entries (pool/reserve/position/backstop/emission snapshots) into the
// staged sets. See services.ProtocolProcessor for the folding/batch-
// equivalence contract.
func (p *processor) ProcessLedger(_ context.Context, input services.ProtocolProcessorInput) error {
	if p.needsReset {
		return fmt.Errorf("blend: ProcessLedger called after Persist without Reset(); staged state would double-count")
	}
	p.ledgerNumber = input.LedgerSequence
	p.indexContracts(input.ProtocolContracts)

	if len(p.blendContracts) == 0 {
		return nil
	}

	blndToken := blndTokenAddress(p.networkPassphrase)
	for key, events := range input.ContractEvents {
		// tx.ID() in stellar/go is toid.New(ledgerSeq, txIdx, 0); recompute it
		// here so PersistHistory's state-change rows carry the same txID a full
		// tx walk would have produced (mirrors sep41's processor).
		txID := toid.New(int32(input.LedgerSequence), int32(key.TxIdx), 0).ToInt64()
		opID := toid.New(int32(input.LedgerSequence), int32(key.TxIdx), int32(key.OpIdx+1)).ToInt64()
		opBuilder := processors.NewStateChangeBuilder(input.LedgerSequence, input.LedgerCloseTime, txID, nil).
			WithOperationID(opID)

		for _, event := range events {
			if err := p.processEvent(event, opBuilder, input.StagingMode, blndToken); err != nil {
				// Log and continue — a malformed event must not abort the whole ledger.
				log.Debugf("blend: skipping malformed event at tx=%d op=%d: %v", key.TxIdx, key.OpIdx, err)
				continue
			}
		}
	}

	if input.StagingMode.NeedsCurrentState() {
		p.processContractDataChanges(input.ContractDataChanges)
	}

	return nil
}

// processEvent decodes one contract event and stages its LENDING history
// rows and/or cost-basis folds per mode. Events from contracts this run
// doesn't track are silently skipped (not an error).
func (p *processor) processEvent(event xdr.ContractEvent, opBuilder *processors.StateChangeBuilder, mode services.StagingMode, blndToken string) error {
	contractStr, ok := contractIDAddress(event)
	if !ok {
		return fmt.Errorf("event has no resolvable contract id")
	}
	if _, tracked := p.blendContracts[contractStr]; !tracked {
		return nil // not a contract we track; silently skip
	}

	decoded, err := ParseEvent(event, blndToken, p.isTracked)
	if err != nil {
		return err
	}
	if decoded == nil {
		return nil // event type this package doesn't model
	}

	if mode.NeedsHistory() {
		for _, row := range decoded.Rows {
			p.stageHistoryRow(opBuilder, row)
		}
	}

	if mode.NeedsCurrentState() && (len(decoded.NetDeltas) > 0 || len(decoded.AuctionAdjs) > 0) {
		// Every event that produces a fold also produces at least one row sharing
		// the same PoolID (see NetDeltaFold/AuctionFold's godoc in events.go).
		poolID := ""
		if len(decoded.Rows) > 0 {
			poolID = decoded.Rows[0].PoolID
		}
		for _, nd := range decoded.NetDeltas {
			p.foldNetDelta(poolID, nd)
		}
		for _, aa := range decoded.AuctionAdjs {
			p.foldAuctionAdj(poolID, aa)
		}
	}

	return nil
}

// isTracked reports whether addr is a contract this run classifies as Blend
// (pool or backstop) — passed to ParseEvent for the withdraw/claim
// disambiguation it needs.
func (p *processor) isTracked(addr string) bool {
	_, ok := p.blendContracts[addr]
	return ok
}

// stageHistoryRow appends one LENDING state-change row built from row onto
// the staged history set. row.Token == "" and row.Amount == "" leave their
// respective columns NULL; row.PoolID == "" omits the poolId key_value entry.
func (p *processor) stageHistoryRow(opBuilder *processors.StateChangeBuilder, row EventRow) {
	b := opBuilder.Clone().
		WithCategory(types.StateChangeCategoryLending).
		WithReason(row.Reason).
		WithAccount(row.Account)
	if row.Token != "" {
		b = b.WithToken(row.Token)
	}
	if row.Amount != "" {
		b = b.WithAmount(row.Amount)
	}

	kv := row.Extra
	if row.PoolID != "" {
		if kv == nil {
			kv = make(map[string]any, 1)
		}
		kv["poolId"] = row.PoolID
	}
	if kv != nil {
		b = b.WithKeyValue(kv)
	}

	p.stagedStateChanges = append(p.stagedStateChanges, b.Build())
}

// foldNetDelta accumulates fold's signed cost-basis deltas into the staged
// net-delta accumulator for (pool, fold.User, fold.Asset). See
// stagedNetDelta's godoc for the zero-then-add ZeroBorrowed semantics.
func (p *processor) foldNetDelta(pool string, fold NetDeltaFold) {
	key := netKey{Pool: pool, User: fold.User, Asset: fold.Asset}
	sd, ok := p.stagedNetDeltas[key]
	if !ok {
		sd = &stagedNetDelta{supplied: new(big.Int), borrowed: new(big.Int)}
		p.stagedNetDeltas[key] = sd
	}
	if fold.ZeroBorrowed {
		sd.borrowed.SetInt64(0)
		sd.zeroBorrowed = true
	}
	if delta, parsed := new(big.Int).SetString(fold.NetSuppliedDelta, 10); parsed {
		sd.supplied.Add(sd.supplied, delta)
	} else {
		log.Debugf("blend: skipping unparseable net-supplied delta %q for pool=%s user=%s asset=%s", fold.NetSuppliedDelta, pool, fold.User, fold.Asset)
	}
	if delta, parsed := new(big.Int).SetString(fold.NetBorrowedDelta, 10); parsed {
		sd.borrowed.Add(sd.borrowed, delta)
	} else {
		log.Debugf("blend: skipping unparseable net-borrowed delta %q for pool=%s user=%s asset=%s", fold.NetBorrowedDelta, pool, fold.User, fold.Asset)
	}
	sd.ledger = p.ledgerNumber
}

// foldAuctionAdj accumulates fold's signed protocol-token lot/bid deltas into
// the staged auction-adjustment accumulator for (pool, fold.User, fold.Asset).
func (p *processor) foldAuctionAdj(pool string, fold AuctionFold) {
	key := auctionKey{Pool: pool, User: fold.User, Asset: fold.Asset}
	sa, ok := p.stagedAuctionAdjs[key]
	if !ok {
		sa = &stagedAuctionAdj{lot: new(big.Int), bid: new(big.Int)}
		p.stagedAuctionAdjs[key] = sa
	}
	if delta, parsed := new(big.Int).SetString(fold.LotBTokensDelta, 10); parsed {
		sa.lot.Add(sa.lot, delta)
	} else {
		log.Debugf("blend: skipping unparseable lot delta %q for pool=%s user=%s asset=%s", fold.LotBTokensDelta, pool, fold.User, fold.Asset)
	}
	if delta, parsed := new(big.Int).SetString(fold.BidDTokensDelta, 10); parsed {
		sa.bid.Add(sa.bid, delta)
	} else {
		log.Debugf("blend: skipping unparseable bid delta %q for pool=%s user=%s asset=%s", fold.BidDTokensDelta, pool, fold.User, fold.Asset)
	}
	sa.ledger = p.ledgerNumber
}

// processContractDataChanges walks changes for every tracked contract and
// stages the decoded entries. Iteration order within each contract's slice
// is preserved (transaction application order), which is what makes LWW
// staging correct: a later entry in the slice always wins.
func (p *processor) processContractDataChanges(changes map[string][]ingest.Change) {
	for addr, entries := range changes {
		if _, tracked := p.blendContracts[addr]; !tracked {
			continue
		}
		for _, change := range entries {
			decoded, err := DecodeEntry(change)
			if err != nil {
				log.Debugf("blend: skipping malformed contract data entry on %s: %v", addr, err)
				continue
			}
			p.routeEntry(addr, decoded)
		}
	}
}

// routeEntry stages decoded per its Kind. addr is the contract that owns the
// ContractData change (the caller's map key into ContractDataChanges) — the
// correct pool identity for every kind except the backstop kinds, which
// carry their own Pool identity field (see DecodedEntry's godoc).
func (p *processor) routeEntry(addr string, decoded DecodedEntry) {
	switch decoded.Kind {
	case KindPoolInstance:
		p.stagePoolInstance(addr, decoded)
	case KindPositions:
		p.stagePositions(addr, decoded)
	case KindResConfig:
		p.stageResConfig(addr, decoded)
	case KindResData:
		p.stageResData(addr, decoded)
	case KindEmisData:
		p.stageEmisData(addr, decoded)
	case KindUserEmis:
		p.stageUserEmis(addr, decoded)
	case KindBackstopUserBalance:
		p.stageBackstopUserBalance(decoded)
	case KindBackstopPoolBalance:
		p.stageBackstopPoolBalance(decoded)
	case KindBackstopBEmisData:
		p.stageBackstopBEmisData(decoded)
	case KindBackstopUEmisData:
		p.stageBackstopUEmis(decoded)
	case KindIgnored:
		// Nothing to stage.
	}
}

// stagePoolInstance merges a pool's instance-storage snapshot into its
// staged blend_pools row. Name and the PoolConfig fields are LWW
// independently: a later entry only overwrites Name when it decoded one
// (best-effort per entries.go), so a transient miss never clobbers an
// earlier known name. Removal is ignored — a pool's instance entry going
// away mid-window doesn't delete history, unlike Positions/BackstopUserBalance.
func (p *processor) stagePoolInstance(addr string, decoded DecodedEntry) {
	if decoded.PoolInstance == nil {
		return
	}
	sp, ok := p.stagedPools[addr]
	if !ok {
		sp = &blenddata.Pool{PoolContractID: types.AddressBytea(addr)}
		p.stagedPools[addr] = sp
	}
	inst := decoded.PoolInstance
	if inst.Name != nil {
		sp.Name = inst.Name
	}
	if inst.Oracle != "" {
		sp.OracleContractID = types.AddressBytea(inst.Oracle)
	}
	bstopRate := int32(inst.BstopRate)
	sp.BackstopRate = &bstopRate
	status := int32(inst.Status)
	sp.Status = &status
	maxPositions := int32(inst.MaxPositions)
	sp.MaxPositions = &maxPositions
	minCollateral := inst.MinCollateral
	sp.MinCollateral = &minCollateral
	sp.LastModifiedLedger = p.ledgerNumber
}

// stagePositions replaces (LWW) or removes the staged Positions snapshot for
// (pool, decoded.User). A snapshot's present set is the union of reserve
// indexes across Collateral/Liabilities/Supply, each defaulting to "0" in
// whichever of the three maps didn't carry that index.
func (p *processor) stagePositions(pool string, decoded DecodedEntry) {
	key := blenddata.PoolUserKey{Pool: pool, User: decoded.User}
	if decoded.Removed {
		p.stagedPositions[key] = &stagedPositions{removed: true, ledger: p.ledgerNumber}
		return
	}
	if decoded.Positions == nil {
		return
	}

	present := make(map[uint32]tokenTriple)
	ensure := func(idx uint32) tokenTriple {
		if t, ok := present[idx]; ok {
			return t
		}
		return tokenTriple{supply: "0", collateral: "0", liability: "0"}
	}
	for idx, v := range decoded.Positions.Supply {
		t := ensure(idx)
		t.supply = v
		present[idx] = t
	}
	for idx, v := range decoded.Positions.Collateral {
		t := ensure(idx)
		t.collateral = v
		present[idx] = t
	}
	for idx, v := range decoded.Positions.Liabilities {
		t := ensure(idx)
		t.liability = v
		present[idx] = t
	}
	p.stagedPositions[key] = &stagedPositions{present: present, ledger: p.ledgerNumber}
}

// stageResConfig merges (LWW) a reserve's config half into its staged row,
// independently of any data half already staged for the same (pool, asset).
func (p *processor) stageResConfig(pool string, decoded DecodedEntry) {
	if decoded.ResConfig == nil {
		return
	}
	key := poolAssetKey{Pool: pool, Asset: decoded.Asset}
	sr, ok := p.stagedReserves[key]
	if !ok {
		sr = &stagedReserve{}
		p.stagedReserves[key] = sr
	}
	sr.config = decoded.ResConfig
	sr.ledger = p.ledgerNumber
}

// stageResData merges (LWW) a reserve's data half into its staged row,
// independently of any config half already staged for the same (pool, asset).
func (p *processor) stageResData(pool string, decoded DecodedEntry) {
	if decoded.ResData == nil {
		return
	}
	key := poolAssetKey{Pool: pool, Asset: decoded.Asset}
	sr, ok := p.stagedReserves[key]
	if !ok {
		sr = &stagedReserve{}
		p.stagedReserves[key] = sr
	}
	sr.data = decoded.ResData
	sr.ledger = p.ledgerNumber
}

// stageEmisData replaces (LWW) the staged reserve-emission row for one
// pool's reserve token.
func (p *processor) stageEmisData(pool string, decoded DecodedEntry) {
	if decoded.EmisData == nil {
		return
	}
	key := poolTokenKey{Pool: pool, TokenID: decoded.TokenID}
	p.stagedReserveEmissions[key] = &stagedResEmission{data: decoded.EmisData, ledger: p.ledgerNumber}
}

// stageUserEmis replaces (LWW) the staged user reserve-emission accrual,
// keyed with Source = pool (the emitting contract).
func (p *processor) stageUserEmis(pool string, decoded DecodedEntry) {
	if decoded.UserEmis == nil {
		return
	}
	key := emisKey{Source: pool, User: decoded.User, TokenID: int32(decoded.TokenID)}
	p.stagedUserEmissions[key] = &stagedUserEmission{data: decoded.UserEmis, ledger: p.ledgerNumber}
}

// stageBackstopUserBalance replaces (LWW) or removes the staged backstop
// position for (decoded.Pool, decoded.User), mirroring stagePositions.
func (p *processor) stageBackstopUserBalance(decoded DecodedEntry) {
	key := blenddata.PoolUserKey{Pool: decoded.Pool, User: decoded.User}
	if decoded.Removed {
		p.stagedBackstopPositions[key] = &stagedBackstopPosition{removed: true, ledger: p.ledgerNumber}
		return
	}
	if decoded.BackstopUserBalance == nil {
		return
	}
	q4w := make([]blenddata.Q4W, 0, len(decoded.BackstopUserBalance.Q4W))
	for _, q := range decoded.BackstopUserBalance.Q4W {
		q4w = append(q4w, blenddata.Q4W{Amount: q.Amount, Expiration: int64(q.Exp)})
	}
	p.stagedBackstopPositions[key] = &stagedBackstopPosition{
		shares: decoded.BackstopUserBalance.Shares,
		q4w:    q4w,
		ledger: p.ledgerNumber,
	}
}

// stageBackstopPoolBalance merges (LWW) a backstop pool's balance half into
// its staged row, independently of any emission half already staged.
func (p *processor) stageBackstopPoolBalance(decoded DecodedEntry) {
	if decoded.BackstopPoolBalance == nil {
		return
	}
	sb, ok := p.stagedBackstopPools[decoded.Pool]
	if !ok {
		sb = &stagedBackstopPool{}
		p.stagedBackstopPools[decoded.Pool] = sb
	}
	sb.balance = decoded.BackstopPoolBalance
	sb.ledger = p.ledgerNumber
}

// stageBackstopBEmisData merges (LWW) a backstop pool's emission half into
// its staged row, independently of any balance half already staged. A Void
// (Option::None) payload is nil per DecodedEntry's contract and is treated
// as nothing-to-stage, same as a removal.
func (p *processor) stageBackstopBEmisData(decoded DecodedEntry) {
	if decoded.EmisData == nil {
		return
	}
	sb, ok := p.stagedBackstopPools[decoded.Pool]
	if !ok {
		sb = &stagedBackstopPool{}
		p.stagedBackstopPools[decoded.Pool] = sb
	}
	sb.emis = decoded.EmisData
	sb.ledger = p.ledgerNumber
}

// stageBackstopUEmis replaces (LWW) the staged user backstop-emission
// accrual, keyed with Source = the backstopped pool (from decoded.Pool) and
// TokenID = blenddata.BackstopEmissionTokenID. A Void payload is nil and
// treated as nothing-to-stage, same as stageBackstopBEmisData.
func (p *processor) stageBackstopUEmis(decoded DecodedEntry) {
	if decoded.UserEmis == nil {
		return
	}
	key := emisKey{Source: decoded.Pool, User: decoded.User, TokenID: blenddata.BackstopEmissionTokenID}
	p.stagedUserEmissions[key] = &stagedUserEmission{data: decoded.UserEmis, ledger: p.ledgerNumber}
}

// indexContracts rebuilds the per-ledger tracked-contract index (pools and
// the backstop) plus each contract's wasm hash, from input.ProtocolContracts.
func (p *processor) indexContracts(contracts []data.ProtocolContracts) {
	p.blendContracts = make(map[string]struct{}, len(contracts))
	p.contractWasmHash = make(map[string]types.HashBytea, len(contracts))
	for _, c := range contracts {
		// ContractID is a hex-encoded string of the 32 raw bytes (types.HashBytea).
		raw, err := hex.DecodeString(string(c.ContractID))
		if err != nil {
			log.Warnf("blend: skipping contract with invalid hex ID %q: %v", c.ContractID, err)
			continue
		}
		addr, err := strkey.Encode(strkey.VersionByteContract, raw)
		if err != nil {
			log.Warnf("blend: skipping contract with invalid ID (%d bytes): %v", len(raw), err)
			continue
		}
		p.blendContracts[addr] = struct{}{}
		p.contractWasmHash[addr] = c.WasmHash
	}
}

// Reset clears the staged sets for the next window. ledgerNumber is
// intentionally left untouched — ProcessLedger sets it each ledger.
func (p *processor) Reset() {
	p.stagedStateChanges = nil
	p.stagedPools = map[string]*blenddata.Pool{}
	p.stagedPositions = map[blenddata.PoolUserKey]*stagedPositions{}
	p.stagedNetDeltas = map[netKey]*stagedNetDelta{}
	p.stagedAuctionAdjs = map[auctionKey]*stagedAuctionAdj{}
	p.stagedReserves = map[poolAssetKey]*stagedReserve{}
	p.stagedBackstopPositions = map[blenddata.PoolUserKey]*stagedBackstopPosition{}
	p.stagedBackstopPools = map[string]*stagedBackstopPool{}
	p.stagedReserveEmissions = map[poolTokenKey]*stagedResEmission{}
	p.stagedUserEmissions = map[emisKey]*stagedUserEmission{}
	p.needsReset = false
}

// PersistHistory writes staged state changes inside the CAS transaction.
func (p *processor) PersistHistory(ctx context.Context, dbTx pgx.Tx) error {
	p.needsReset = true
	if len(p.stagedStateChanges) == 0 {
		return nil
	}
	if _, err := p.stateChanges.BatchCopy(ctx, dbTx, p.stagedStateChanges); err != nil {
		return fmt.Errorf("persisting %d blend state changes for ledger %d: %w", len(p.stagedStateChanges), p.ledgerNumber, err)
	}
	return nil
}

// PersistCurrentState writes every staged current-state set in the order
// required by cross-table dependencies:
//  1. pools, then reserves — net-delta/auction-adjustment SQL resolves an
//     asset to a reserve_index via blend_reserves, so reserves must exist
//     before positions are applied.
//  2. positions: delete removed groups, then zero/upsert/apply-deltas, so a
//     Positions entry removed and re-created within the same window nets to
//     the recreate rather than a delete-after-upsert race.
//  3. backstop positions, backstop pools, reserve emissions, user emissions.
func (p *processor) PersistCurrentState(ctx context.Context, dbTx pgx.Tx) error {
	p.needsReset = true

	if err := p.persistPools(ctx, dbTx); err != nil {
		return err
	}
	if err := p.persistReserves(ctx, dbTx); err != nil {
		return err
	}
	if err := p.persistPositions(ctx, dbTx); err != nil {
		return err
	}
	if err := p.persistBackstopPositions(ctx, dbTx); err != nil {
		return err
	}
	if err := p.persistBackstopPools(ctx, dbTx); err != nil {
		return err
	}
	if err := p.persistReserveEmissions(ctx, dbTx); err != nil {
		return err
	}
	if err := p.persistUserEmissions(ctx, dbTx); err != nil {
		return err
	}
	return nil
}

// persistPools upserts staged blend_pools rows and, for any pool whose
// instance entry carried a Name, enriches protocol_contracts.name so the
// pool's display name surfaces through the generic contract lookup too. A
// pool with an unknown wasm hash (not present in this ledger's
// ProtocolContracts) is skipped for the enrichment half only — blend_pools
// itself is unaffected.
func (p *processor) persistPools(ctx context.Context, dbTx pgx.Tx) error {
	if len(p.stagedPools) == 0 {
		return nil
	}

	rows := make([]blenddata.Pool, 0, len(p.stagedPools))
	var nameRows []data.ProtocolContracts
	for addr, sp := range p.stagedPools {
		rows = append(rows, *sp)
		if sp.Name == nil {
			continue
		}
		wasmHash, ok := p.contractWasmHash[addr]
		if !ok {
			continue
		}
		raw, err := strkey.Decode(strkey.VersionByteContract, addr)
		if err != nil {
			log.Debugf("blend: skipping protocol_contracts name enrichment for undecodable pool address %q: %v", addr, err)
			continue
		}
		nameRows = append(nameRows, data.ProtocolContracts{
			ContractID: types.HashBytea(hex.EncodeToString(raw)),
			WasmHash:   wasmHash,
			Name:       sp.Name,
		})
	}

	if err := p.pools.BatchUpsert(ctx, dbTx, rows); err != nil {
		return fmt.Errorf("upserting %d blend pools for ledger %d: %w", len(rows), p.ledgerNumber, err)
	}
	if len(nameRows) > 0 && p.protocolContracts != nil {
		if err := p.protocolContracts.BatchInsert(ctx, dbTx, nameRows); err != nil {
			return fmt.Errorf("enriching protocol_contracts names for ledger %d: %w", p.ledgerNumber, err)
		}
	}
	return nil
}

// persistReserves upserts full reserve rows for reserves whose config half
// was staged this window, and updates only the data half for reserves whose
// config wasn't. See stagedReserve's godoc for why config gates the choice.
func (p *processor) persistReserves(ctx context.Context, dbTx pgx.Tx) error {
	if len(p.stagedReserves) == 0 {
		return nil
	}

	var fullRows []blenddata.Reserve
	var dataRows []blenddata.ReserveDataUpdate
	for key, sr := range p.stagedReserves {
		if sr.config != nil {
			row := blenddata.Reserve{
				PoolContractID:     types.AddressBytea(key.Pool),
				AssetContractID:    types.AddressBytea(key.Asset),
				ReserveIndex:       int32(sr.config.Index),
				Decimals:           int32(sr.config.Decimals),
				CFactor:            int32(sr.config.CFactor),
				LFactor:            int32(sr.config.LFactor),
				Util:               int32(sr.config.Util),
				MaxUtil:            int32(sr.config.MaxUtil),
				RBase:              int32(sr.config.RBase),
				ROne:               int32(sr.config.ROne),
				RTwo:               int32(sr.config.RTwo),
				RThree:             int32(sr.config.RThree),
				Reactivity:         int32(sr.config.Reactivity),
				SupplyCap:          sr.config.SupplyCap,
				Enabled:            sr.config.Enabled,
				LastModifiedLedger: sr.ledger,
			}
			if sr.data != nil {
				row.BRate = sr.data.BRate
				row.DRate = sr.data.DRate
				row.BSupply = sr.data.BSupply
				row.DSupply = sr.data.DSupply
				row.IRMod = sr.data.IRMod
				row.BackstopCredit = sr.data.BackstopCredit
				row.LastTime = int64(sr.data.LastTime)
			} else {
				// ResData wasn't observed alongside this window's new/changed
				// ResConfig. In practice set_reserve always writes both entries
				// together, so this is defensive: zero the rate/supply fields
				// rather than sending Go's zero-value "" (an invalid numeric
				// string) on a full-row overwrite.
				row.BRate, row.DRate, row.BSupply, row.DSupply, row.IRMod, row.BackstopCredit = "0", "0", "0", "0", "0", "0"
			}
			fullRows = append(fullRows, row)
			continue
		}
		if sr.data != nil {
			dataRows = append(dataRows, blenddata.ReserveDataUpdate{
				Pool:           key.Pool,
				Asset:          key.Asset,
				BRate:          sr.data.BRate,
				DRate:          sr.data.DRate,
				IRMod:          sr.data.IRMod,
				BSupply:        sr.data.BSupply,
				DSupply:        sr.data.DSupply,
				BackstopCredit: sr.data.BackstopCredit,
				LastTime:       int64(sr.data.LastTime),
				LedgerNumber:   sr.ledger,
			})
		}
	}

	if len(fullRows) > 0 {
		if err := p.reserves.BatchUpsert(ctx, dbTx, fullRows); err != nil {
			return fmt.Errorf("upserting %d blend reserves for ledger %d: %w", len(fullRows), p.ledgerNumber, err)
		}
	}
	if len(dataRows) > 0 {
		if err := p.reserves.BatchUpdateData(ctx, dbTx, dataRows); err != nil {
			return fmt.Errorf("updating %d blend reserve data rows for ledger %d: %w", len(dataRows), p.ledgerNumber, err)
		}
	}
	return nil
}

// persistPositions applies every staged position mutation in the order
// DeleteByPoolUser -> ZeroAbsentReserves -> BatchUpsertSnapshots ->
// BatchApplyNetDeltas -> ApplyAuctionAdjustments. Each staged (pool, user)
// key is either a removal or a snapshot (never both), so delete and
// zero/upsert never target the same key within one window.
func (p *processor) persistPositions(ctx context.Context, dbTx pgx.Tx) error {
	var deleteKeys []blenddata.PoolUserKey
	var presences []blenddata.PositionPresence
	var snapshots []blenddata.PositionSnapshot
	for key, sp := range p.stagedPositions {
		if sp.removed {
			deleteKeys = append(deleteKeys, key)
			continue
		}
		indexes := make([]int32, 0, len(sp.present))
		for idx := range sp.present {
			indexes = append(indexes, int32(idx))
		}
		presences = append(presences, blenddata.PositionPresence{
			Pool: key.Pool, User: key.User, PresentIndexes: indexes, LedgerNumber: sp.ledger,
		})
		for idx, t := range sp.present {
			snapshots = append(snapshots, blenddata.PositionSnapshot{
				Pool: key.Pool, User: key.User, ReserveIndex: int32(idx),
				SupplyBTokens: t.supply, CollateralBTokens: t.collateral, LiabilityDTokens: t.liability,
				LedgerNumber: sp.ledger,
			})
		}
	}

	if len(deleteKeys) > 0 {
		if err := p.positions.DeleteByPoolUser(ctx, dbTx, deleteKeys); err != nil {
			return fmt.Errorf("deleting %d blend positions for ledger %d: %w", len(deleteKeys), p.ledgerNumber, err)
		}
	}
	if len(presences) > 0 {
		if err := p.positions.ZeroAbsentReserves(ctx, dbTx, presences); err != nil {
			return fmt.Errorf("zeroing absent blend reserves for ledger %d: %w", p.ledgerNumber, err)
		}
	}
	if len(snapshots) > 0 {
		if err := p.positions.BatchUpsertSnapshots(ctx, dbTx, snapshots); err != nil {
			return fmt.Errorf("upserting %d blend position snapshots for ledger %d: %w", len(snapshots), p.ledgerNumber, err)
		}
	}

	if len(p.stagedNetDeltas) > 0 {
		netDeltaRows := make([]blenddata.PositionNetDelta, 0, len(p.stagedNetDeltas))
		for key, sd := range p.stagedNetDeltas {
			netDeltaRows = append(netDeltaRows, blenddata.PositionNetDelta{
				Pool: key.Pool, User: key.User, Asset: key.Asset,
				NetSuppliedDelta: sd.supplied.String(), NetBorrowedDelta: sd.borrowed.String(),
				ZeroBorrowed: sd.zeroBorrowed, LedgerNumber: sd.ledger,
			})
		}
		if err := p.positions.BatchApplyNetDeltas(ctx, dbTx, netDeltaRows); err != nil {
			return fmt.Errorf("applying %d blend position net deltas for ledger %d: %w", len(netDeltaRows), p.ledgerNumber, err)
		}
	}

	if len(p.stagedAuctionAdjs) > 0 {
		auctionRows := make([]blenddata.PositionAuctionAdjustment, 0, len(p.stagedAuctionAdjs))
		for key, sa := range p.stagedAuctionAdjs {
			auctionRows = append(auctionRows, blenddata.PositionAuctionAdjustment{
				Pool: key.Pool, User: key.User, Asset: key.Asset,
				LotBTokensDelta: sa.lot.String(), BidDTokensDelta: sa.bid.String(),
				LedgerNumber: sa.ledger,
			})
		}
		if err := p.positions.ApplyAuctionAdjustments(ctx, dbTx, auctionRows); err != nil {
			return fmt.Errorf("applying %d blend position auction adjustments for ledger %d: %w", len(auctionRows), p.ledgerNumber, err)
		}
	}
	return nil
}

// persistBackstopPositions deletes removed backstop positions and upserts
// the rest, mirroring persistPositions' delete-then-upsert split.
func (p *processor) persistBackstopPositions(ctx context.Context, dbTx pgx.Tx) error {
	if len(p.stagedBackstopPositions) == 0 {
		return nil
	}

	var deleteKeys []blenddata.PoolUserKey
	var rows []blenddata.BackstopPosition
	for key, sb := range p.stagedBackstopPositions {
		if sb.removed {
			deleteKeys = append(deleteKeys, key)
			continue
		}
		rows = append(rows, blenddata.BackstopPosition{
			PoolContractID:     types.AddressBytea(key.Pool),
			UserAccountID:      types.AddressBytea(key.User),
			Shares:             sb.shares,
			Q4W:                sb.q4w,
			LastModifiedLedger: sb.ledger,
		})
	}

	if len(deleteKeys) > 0 {
		if err := p.backstopPositions.DeleteByPoolUser(ctx, dbTx, deleteKeys); err != nil {
			return fmt.Errorf("deleting %d blend backstop positions for ledger %d: %w", len(deleteKeys), p.ledgerNumber, err)
		}
	}
	if len(rows) > 0 {
		if err := p.backstopPositions.BatchUpsert(ctx, dbTx, rows); err != nil {
			return fmt.Errorf("upserting %d blend backstop positions for ledger %d: %w", len(rows), p.ledgerNumber, err)
		}
	}
	return nil
}

// persistBackstopPools upserts the balance and emission halves of staged
// blend_backstop_pools rows independently, per stagedBackstopPool's LWW split.
func (p *processor) persistBackstopPools(ctx context.Context, dbTx pgx.Tx) error {
	if len(p.stagedBackstopPools) == 0 {
		return nil
	}

	var balRows []blenddata.BackstopPool
	var emisRows []blenddata.BackstopPoolEmission
	for addr, sb := range p.stagedBackstopPools {
		if sb.balance != nil {
			balRows = append(balRows, blenddata.BackstopPool{
				PoolContractID:     types.AddressBytea(addr),
				Shares:             sb.balance.Shares,
				Tokens:             sb.balance.Tokens,
				Q4W:                sb.balance.Q4W,
				LastModifiedLedger: sb.ledger,
			})
		}
		if sb.emis != nil {
			eps := int64(sb.emis.Eps)
			idx := sb.emis.Index
			exp := int64(sb.emis.Expiration)
			lastTime := int64(sb.emis.LastTime)
			emisRows = append(emisRows, blenddata.BackstopPoolEmission{
				Pool:           addr,
				EmisEps:        &eps,
				EmisIndex:      &idx,
				EmisExpiration: &exp,
				EmisLastTime:   &lastTime,
				LedgerNumber:   sb.ledger,
			})
		}
	}

	if len(balRows) > 0 {
		if err := p.backstopPools.BatchUpsertBalances(ctx, dbTx, balRows); err != nil {
			return fmt.Errorf("upserting %d blend backstop pool balances for ledger %d: %w", len(balRows), p.ledgerNumber, err)
		}
	}
	if len(emisRows) > 0 {
		if err := p.backstopPools.BatchUpsertEmissions(ctx, dbTx, emisRows); err != nil {
			return fmt.Errorf("upserting %d blend backstop pool emissions for ledger %d: %w", len(emisRows), p.ledgerNumber, err)
		}
	}
	return nil
}

// persistReserveEmissions upserts staged blend_reserve_emissions rows.
func (p *processor) persistReserveEmissions(ctx context.Context, dbTx pgx.Tx) error {
	if len(p.stagedReserveEmissions) == 0 {
		return nil
	}

	rows := make([]blenddata.ReserveEmission, 0, len(p.stagedReserveEmissions))
	for key, sr := range p.stagedReserveEmissions {
		rows = append(rows, blenddata.ReserveEmission{
			PoolContractID:     types.AddressBytea(key.Pool),
			ReserveTokenID:     int32(key.TokenID),
			Eps:                int64(sr.data.Eps),
			EmissionIndex:      sr.data.Index,
			Expiration:         int64(sr.data.Expiration),
			LastTime:           int64(sr.data.LastTime),
			LastModifiedLedger: sr.ledger,
		})
	}
	if err := p.reserveEmissions.BatchUpsert(ctx, dbTx, rows); err != nil {
		return fmt.Errorf("upserting %d blend reserve emissions for ledger %d: %w", len(rows), p.ledgerNumber, err)
	}
	return nil
}

// persistUserEmissions upserts staged blend_emissions rows (reserve-emission
// and backstop-emission accrual streams alike).
func (p *processor) persistUserEmissions(ctx context.Context, dbTx pgx.Tx) error {
	if len(p.stagedUserEmissions) == 0 {
		return nil
	}

	rows := make([]blenddata.Emission, 0, len(p.stagedUserEmissions))
	for key, su := range p.stagedUserEmissions {
		rows = append(rows, blenddata.Emission{
			SourceContractID:   types.AddressBytea(key.Source),
			UserAccountID:      types.AddressBytea(key.User),
			TokenID:            key.TokenID,
			EmissionIndex:      su.data.Index,
			Accrued:            su.data.Accrued,
			LastModifiedLedger: su.ledger,
		})
	}
	if err := p.emissions.BatchUpsert(ctx, dbTx, rows); err != nil {
		return fmt.Errorf("upserting %d blend user emissions for ledger %d: %w", len(rows), p.ledgerNumber, err)
	}
	return nil
}
