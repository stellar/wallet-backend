// entries.go decodes a Blend v2 pool or backstop contract's ContractData
// ledger-entry changes into typed payloads. DecodeEntry is the only exported
// entry point; the processor (a later task) calls it once per
// ingest.Change in ProtocolProcessorInput.ContractDataChanges[contractAddr]
// and stages the result.
package blend

import (
	"fmt"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// EntryKind identifies which Blend storage entry a DecodedEntry represents.
// KindIgnored is the zero value so an unrecognized or unmodeled key never
// aliases a real kind.
type EntryKind int

const (
	// KindIgnored marks a ContractData change this package doesn't model:
	// an unrecognized key symbol, a bare-symbol key with no per-entity
	// identity (e.g. ResList, PoolEmis, LastDist), or a backstop instance
	// (no "Config" in its storage).
	KindIgnored EntryKind = iota
	// KindPoolInstance is a pool contract's instance storage (Admin, Name,
	// Backstop, BLNDTkn, Config). Payload: PoolInstance.
	KindPoolInstance
	// KindPositions is a pool's Positions(user) entry. Payload: Positions.
	KindPositions
	// KindResConfig is a pool's ResConfig(asset) entry. Payload: ResConfig.
	KindResConfig
	// KindResData is a pool's ResData(asset) entry. Payload: ResData.
	KindResData
	// KindEmisData is a pool's EmisData(reserveTokenID) entry — reserve
	// emission config/accrual. Payload: EmisData.
	KindEmisData
	// KindUserEmis is a pool's UserEmis(UserReserveKey) entry — a user's
	// accrued reserve emissions. Payload: UserEmis.
	KindUserEmis
	// KindBackstopUserBalance is a backstop UserBalance(PoolUserKey) entry.
	// Payload: BackstopUserBalance.
	KindBackstopUserBalance
	// KindBackstopPoolBalance is a backstop PoolBalance(pool) entry.
	// Payload: BackstopPoolBalance.
	KindBackstopPoolBalance
	// KindBackstopBEmisData is a backstop BEmisData(pool) entry — that
	// pool's backstop emission config/accrual. On-chain this is
	// Option<BackstopEmissionData>; see DecodedEntry's godoc for the Void
	// (None) contract. Payload: EmisData (same shape as KindEmisData's).
	KindBackstopBEmisData
	// KindBackstopUEmisData is a backstop UEmisData(PoolUserKey) entry — a
	// user's accrued backstop emissions for one pool. On-chain this is
	// Option<UserEmissionData>; see DecodedEntry's godoc for the Void (None)
	// contract. Payload: UserEmis (same shape as KindUserEmis's).
	KindBackstopUEmisData
	// KindAuction is a pool's Auction(AuctionKey) TEMPORARY-storage entry —
	// an active liquidation, bad-debt, or interest auction. Payload:
	// AuctionData.
	KindAuction
	// KindRewardZone is the backstop's bare-symbol "RZ" entry — the ordered
	// list of pool addresses currently in the reward zone. Payload: none;
	// see DecodedEntry.RewardZone.
	KindRewardZone
)

// PoolInstanceData is the decoded payload for KindPoolInstance: the pool's
// instance-storage Config (a PoolConfig UDT) plus its Name and Admin. Name is
// nil if the instance storage has no "Name" key or it isn't a String; Admin
// is nil if the instance storage has no "Admin" key or it isn't an Address.
type PoolInstanceData struct {
	Name          *string
	Admin         *string
	Oracle        string
	BstopRate     uint32
	Status        uint32
	MaxPositions  uint32
	MinCollateral string
}

// PositionsData is the decoded payload for KindPositions: a user's lending
// positions on one pool, each map keyed by reserve_index. A reserve index
// absent from a map means the user has no position of that kind in that
// reserve — it is not represented as a zero entry.
type PositionsData struct {
	Collateral  map[uint32]string
	Liabilities map[uint32]string
	Supply      map[uint32]string
}

// ReserveConfigData is the decoded payload for KindResConfig.
type ReserveConfigData struct {
	CFactor    uint32
	Decimals   uint32
	Enabled    bool
	Index      uint32
	LFactor    uint32
	MaxUtil    uint32
	RBase      uint32
	ROne       uint32
	RThree     uint32
	RTwo       uint32
	Reactivity uint32
	SupplyCap  string
	Util       uint32
}

// ReserveDataData is the decoded payload for KindResData: a reserve's live
// rates and supply/debt totals.
type ReserveDataData struct {
	BRate          string
	BSupply        string
	BackstopCredit string
	DRate          string
	DSupply        string
	IRMod          string
	LastTime       uint64
}

// EmissionData is the decoded payload shape shared by KindEmisData (a pool
// reserve's ReserveEmissionData) and KindBackstopBEmisData (a backstop
// pool's BackstopEmissionData) — the two UDTs have identical fields.
type EmissionData struct {
	Eps        uint64
	Expiration uint64
	Index      string
	LastTime   uint64
}

// UserEmissionData is the decoded payload shape shared by KindUserEmis (a
// pool reserve's per-user accrual) and KindBackstopUEmisData (a backstop
// pool's per-user accrual) — both are UserEmissionData on-chain with
// identical fields.
type UserEmissionData struct {
	Accrued string
	Index   string
}

// Q4WData is one queued-withdrawal entry within a backstop UserBalance.
type Q4WData struct {
	Amount string
	Exp    uint64
}

// BackstopUserBalanceData is the decoded payload for
// KindBackstopUserBalance.
type BackstopUserBalanceData struct {
	Q4W    []Q4WData
	Shares string
}

// BackstopPoolBalanceData is the decoded payload for
// KindBackstopPoolBalance. Q4W here is the pool's aggregate queued-withdrawal
// total (an i128), unlike BackstopUserBalanceData.Q4W which is a per-user
// list of individual queued withdrawals.
type BackstopPoolBalanceData struct {
	Q4W    string
	Shares string
	Tokens string
}

// AuctionData is the decoded payload for KindAuction. Bid and Lot map asset
// C-addresses to i128 amounts (decimal strings); Block is the auction start.
type AuctionData struct {
	Bid   map[string]string
	Lot   map[string]string
	Block uint32
}

// DecodedEntry is the typed result of decoding one ContractData
// ledger-entry change. It is a tagged union: Kind selects which single
// payload pointer field (if any) is meaningful.
//
// Identity fields (User, Asset, Pool, TokenID) are populated straight from
// the entry's LedgerKey, which is unaffected by removal, so they are always
// present for a non-ignored Kind regardless of Removed. Pool is populated
// only for backstop keys, whose key explicitly embeds a pool address —
// pool-contract entries (ResConfig, Positions, ...) never populate Pool,
// since the owning pool address is the caller's map key into
// ProtocolProcessorInput.ContractDataChanges, not part of the entry itself.
// TokenID carries EmisData's reserveTokenID or UserEmis's reserve_id.
// AuctionType carries KindAuction's auct_type (0=UserLiquidation,
// 1=BadDebtAuction, 2=InterestAuction) and is meaningful only for that Kind.
//
// Removal contract: Removed reports change.Post == nil (the entry was
// deleted). A removed entry always has a nil payload pointer (and, for
// KindRewardZone, a nil RewardZone slice) — callers must act on Kind plus the
// identity fields alone (e.g. delete all rows for that pool/user) rather than
// reading stale payload data.
//
// Option/Void contract: BEmisData and UEmisData are Option<T> on-chain and
// may be ScvVoid (None) even on a live, non-removed entry. That case reports
// the same Kind and identity fields as the Some case but leaves the payload
// pointer nil, exactly like a removal. Processors must treat a nil payload
// pointer — whether from Removed or from Void — as "nothing to stage" rather
// than assuming a live entry of a known Kind always carries a payload.
type DecodedEntry struct {
	Kind    EntryKind
	Removed bool

	User  string
	Asset string
	Pool  string

	TokenID     uint32
	AuctionType int32

	PoolInstance        *PoolInstanceData
	Positions           *PositionsData
	ResConfig           *ReserveConfigData
	ResData             *ReserveDataData
	EmisData            *EmissionData
	UserEmis            *UserEmissionData
	BackstopUserBalance *BackstopUserBalanceData
	BackstopPoolBalance *BackstopPoolBalanceData
	Auction             *AuctionData

	// RewardZone is KindRewardZone's payload: the ordered list of pool
	// addresses currently in the backstop's reward zone. nil on Removed;
	// non-nil (possibly empty — an emptied reward zone is valid) otherwise.
	RewardZone []string
}

// DecodeEntry decodes one Blend pool or backstop ContractData ledger-entry
// change. It never panics: an unrecognized key shape (a key symbol this
// package doesn't track, an unexpected ScVal type for the LedgerKey) decodes
// to KindIgnored with a nil error. Only a recognized key whose value fails
// to match its expected shape (missing field, wrong ScVal type) is reported
// as an error — the processor is expected to log and skip those.
func DecodeEntry(change ingest.Change) (DecodedEntry, error) {
	entry := change.Post
	removed := false
	if entry == nil {
		entry = change.Pre
		removed = true
	}
	if entry == nil {
		return DecodedEntry{}, fmt.Errorf("blend: change has neither Pre nor Post entry")
	}

	cd, ok := entry.Data.GetContractData()
	if !ok {
		return DecodedEntry{}, fmt.Errorf("blend: change is not a ContractData entry (type %v)", entry.Data.Type)
	}

	switch cd.Key.Type {
	case xdr.ScValTypeScvLedgerKeyContractInstance:
		return decodeInstanceEntry(cd, removed)
	case xdr.ScValTypeScvVec:
		return decodeVecKeyEntry(cd, removed)
	case xdr.ScValTypeScvSymbol:
		return decodeSymbolKeyEntry(cd, removed)
	default:
		// Any other key shape this package doesn't expect.
		return DecodedEntry{Kind: KindIgnored, Removed: removed}, nil
	}
}

// decodeSymbolKeyEntry handles a bare xdr.ScValTypeScvSymbol key. Most of
// these (ResList, PoolEmis, PropAdmin on pools; LastDist, DropList,
// BackfillEmis, Backfill on the backstop) carry no per-entity identity and
// aren't modeled — except the backstop's "RZ" reward-zone entry, which is.
func decodeSymbolKeyEntry(cd xdr.ContractDataEntry, removed bool) (DecodedEntry, error) {
	sym, ok := cd.Key.GetSym()
	if !ok {
		return DecodedEntry{Kind: KindIgnored, Removed: removed}, nil
	}
	if string(sym) == "RZ" {
		return decodeRewardZone(cd, removed)
	}
	return DecodedEntry{Kind: KindIgnored, Removed: removed}, nil
}

// decodeRewardZone decodes the backstop's bare-symbol "RZ" entry: a Vec of
// pool Address elements. An empty vec is a valid, live value — it means the
// reward zone has been emptied, not that decoding failed.
func decodeRewardZone(cd xdr.ContractDataEntry, removed bool) (DecodedEntry, error) {
	const kind = "RZ"
	out := DecodedEntry{Kind: KindRewardZone, Removed: removed}
	if removed {
		return out, nil
	}

	elems, ok := vecVal(cd.Val)
	if !ok {
		return DecodedEntry{}, fmt.Errorf("blend: %s: expected vec value, got %v", kind, cd.Val.Type)
	}

	pools := make([]string, 0, len(elems))
	for i, el := range elems {
		addr, ok := addrString(el)
		if !ok {
			return DecodedEntry{}, fmt.Errorf("blend: %s: element %d is not an address (type %v)", kind, i, el.Type)
		}
		pools = append(pools, addr)
	}

	out.RewardZone = pools
	return out, nil
}

// decodeInstanceEntry handles the ScvLedgerKeyContractInstance key: routes to
// KindPoolInstance only if the instance storage contains "Config" (the
// backstop's instance storage never does), else KindIgnored. This check runs
// even for a removed entry (using change.Pre's storage) so a deleted pool
// instance is still reported as KindPoolInstance rather than KindIgnored.
func decodeInstanceEntry(cd xdr.ContractDataEntry, removed bool) (DecodedEntry, error) {
	instance, ok := cd.Val.GetInstance()
	if !ok {
		return DecodedEntry{}, fmt.Errorf("blend: instance entry: expected ScContractInstance value, got %v", cd.Val.Type)
	}
	if _, hasConfig := mapGet(instance.Storage, "Config"); !hasConfig {
		return DecodedEntry{Kind: KindIgnored, Removed: removed}, nil
	}

	out := DecodedEntry{Kind: KindPoolInstance, Removed: removed}
	if removed {
		return out, nil
	}

	payload, err := decodePoolInstanceData(instance.Storage)
	if err != nil {
		return DecodedEntry{}, err
	}
	out.PoolInstance = payload
	return out, nil
}

func decodePoolInstanceData(storage *xdr.ScMap) (*PoolInstanceData, error) {
	instR := &mapReader{m: storage, kind: "PoolInstance"}
	cfgMap := readField(instR, "Config", xdr.ScVal.GetMap)
	if instR.err != nil {
		return nil, instR.err
	}

	cfgR := &mapReader{m: cfgMap, kind: "PoolInstance.Config"}
	payload := &PoolInstanceData{
		Oracle:        readField(cfgR, "oracle", addrString),
		BstopRate:     readField(cfgR, "bstop_rate", u32Val),
		Status:        readField(cfgR, "status", u32Val),
		MaxPositions:  readField(cfgR, "max_positions", u32Val),
		MinCollateral: readField(cfgR, "min_collateral", i128String),
	}
	if cfgR.err != nil {
		return nil, cfgR.err
	}

	// Name and Admin are metadata-only and best-effort: a missing or
	// wrong-typed value leaves the field nil rather than failing the whole
	// decode.
	if nameVal, ok := mapGet(storage, "Name"); ok {
		if s, ok := stringVal(nameVal); ok {
			payload.Name = &s
		}
	}
	if adminVal, ok := mapGet(storage, "Admin"); ok {
		if s, ok := addrString(adminVal); ok {
			payload.Admin = &s
		}
	}
	return payload, nil
}

// decodeVecKeyEntry handles the 2-elem-ScVec-key shape ([Symbol, arg]) used
// by every Blend persistent (and Auction's TEMPORARY) storage key. The
// symbol selects the kind; unrecognized symbols (ResInit, RzEmis, PoolUSDC,
// and any future variant) decode to KindIgnored, never an error.
func decodeVecKeyEntry(cd xdr.ContractDataEntry, removed bool) (DecodedEntry, error) {
	vec, ok := cd.Key.GetVec()
	if !ok || vec == nil || len(*vec) == 0 {
		return DecodedEntry{Kind: KindIgnored, Removed: removed}, nil
	}
	elems := []xdr.ScVal(*vec)

	sym, ok := elems[0].GetSym()
	if !ok {
		return DecodedEntry{Kind: KindIgnored, Removed: removed}, nil
	}

	switch string(sym) {
	case "Auction":
		return decodeAuction(cd, elems, removed)
	case "ResConfig":
		return decodeResConfig(cd, elems, removed)
	case "ResData":
		return decodeResData(cd, elems, removed)
	case "EmisData":
		return decodeEmisData(cd, elems, removed)
	case "Positions":
		return decodePositions(cd, elems, removed)
	case "UserEmis":
		return decodeUserEmis(cd, elems, removed)
	case "UserBalance":
		return decodeBackstopUserBalance(cd, elems, removed)
	case "PoolBalance":
		return decodeBackstopPoolBalance(cd, elems, removed)
	case "BEmisData":
		return decodeBackstopBEmisData(cd, elems, removed)
	case "UEmisData":
		return decodeBackstopUEmisData(cd, elems, removed)
	default:
		return DecodedEntry{Kind: KindIgnored, Removed: removed}, nil
	}
}

// decodeAddressArg extracts a 2-elem key vec's second element ([Symbol,
// Address]) as a strkey address, wrapping any shape mismatch with kind.
func decodeAddressArg(elems []xdr.ScVal, kind string) (string, error) {
	if len(elems) < 2 {
		return "", fmt.Errorf("blend: %s: key vec has %d element(s), want 2", kind, len(elems))
	}
	addr, ok := addrString(elems[1])
	if !ok {
		return "", fmt.Errorf("blend: %s: key arg is not an address (type %v)", kind, elems[1].Type)
	}
	return addr, nil
}

// decodeU32Arg extracts a 2-elem key vec's second element ([Symbol, u32]) as
// a u32, wrapping any shape mismatch with kind.
func decodeU32Arg(elems []xdr.ScVal, kind string) (uint32, error) {
	if len(elems) < 2 {
		return 0, fmt.Errorf("blend: %s: key vec has %d element(s), want 2", kind, len(elems))
	}
	u, ok := u32Val(elems[1])
	if !ok {
		return 0, fmt.Errorf("blend: %s: key arg is not a u32 (type %v)", kind, elems[1].Type)
	}
	return u, nil
}

// decodeMapArg extracts a 2-elem key vec's second element ([Symbol, Map]) as
// an ScMap, wrapping any shape mismatch with kind. Used for the UserReserveKey
// and PoolUserKey struct arguments.
func decodeMapArg(elems []xdr.ScVal, kind string) (*xdr.ScMap, error) {
	if len(elems) < 2 {
		return nil, fmt.Errorf("blend: %s: key vec has %d element(s), want 2", kind, len(elems))
	}
	m, ok := elems[1].GetMap()
	if !ok {
		return nil, fmt.Errorf("blend: %s: key arg is not a map (type %v)", kind, elems[1].Type)
	}
	return m, nil
}

// decodePoolUserKey decodes a PoolUserKey argument {pool Address, user
// Address} out of a 2-elem key vec.
func decodePoolUserKey(elems []xdr.ScVal, kind string) (pool, user string, err error) {
	m, err := decodeMapArg(elems, kind)
	if err != nil {
		return "", "", err
	}
	r := &mapReader{m: m, kind: kind}
	pool = readField(r, "pool", addrString)
	user = readField(r, "user", addrString)
	if r.err != nil {
		return "", "", r.err
	}
	return pool, user, nil
}

// decodeUserReserveKey decodes a UserReserveKey argument {reserve_id u32,
// user Address} out of a 2-elem key vec.
func decodeUserReserveKey(elems []xdr.ScVal, kind string) (reserveID uint32, user string, err error) {
	m, err := decodeMapArg(elems, kind)
	if err != nil {
		return 0, "", err
	}
	r := &mapReader{m: m, kind: kind}
	reserveID = readField(r, "reserve_id", u32Val)
	user = readField(r, "user", addrString)
	if r.err != nil {
		return 0, "", r.err
	}
	return reserveID, user, nil
}

// decodeAuctionKey decodes an AuctionKey argument {auct_type u32, user
// Address} out of a 2-elem key vec. The sorted ScMap orders fields
// auct_type, user (symbol-byte order) — the reverse of the Rust struct
// declaration order (AuctionKey{user, auct_type}).
func decodeAuctionKey(elems []xdr.ScVal, kind string) (auctType uint32, user string, err error) {
	m, err := decodeMapArg(elems, kind)
	if err != nil {
		return 0, "", err
	}
	r := &mapReader{m: m, kind: kind}
	auctType = readField(r, "auct_type", u32Val)
	user = readField(r, "user", addrString)
	if r.err != nil {
		return 0, "", r.err
	}
	return auctType, user, nil
}

// decodeEmissionData decodes a ReserveEmissionData/BackstopEmissionData
// map (identical shape on pool and backstop contracts) out of v.
func decodeEmissionData(v xdr.ScVal, kind string) (*EmissionData, error) {
	m, ok := v.GetMap()
	if !ok {
		return nil, fmt.Errorf("blend: %s: expected map value, got %v", kind, v.Type)
	}
	r := &mapReader{m: m, kind: kind}
	data := &EmissionData{
		Eps:        readField(r, "eps", u64Val),
		Expiration: readField(r, "expiration", u64Val),
		Index:      readField(r, "index", i128String),
		LastTime:   readField(r, "last_time", u64Val),
	}
	if r.err != nil {
		return nil, r.err
	}
	return data, nil
}

// decodeUserEmissionData decodes a UserEmissionData map (identical shape on
// pool and backstop contracts) out of v.
func decodeUserEmissionData(v xdr.ScVal, kind string) (*UserEmissionData, error) {
	m, ok := v.GetMap()
	if !ok {
		return nil, fmt.Errorf("blend: %s: expected map value, got %v", kind, v.Type)
	}
	r := &mapReader{m: m, kind: kind}
	data := &UserEmissionData{
		Accrued: readField(r, "accrued", i128String),
		Index:   readField(r, "index", i128String),
	}
	if r.err != nil {
		return nil, r.err
	}
	return data, nil
}

func decodeResConfig(cd xdr.ContractDataEntry, elems []xdr.ScVal, removed bool) (DecodedEntry, error) {
	const kind = "ResConfig"
	asset, err := decodeAddressArg(elems, kind)
	if err != nil {
		return DecodedEntry{}, err
	}

	out := DecodedEntry{Kind: KindResConfig, Removed: removed, Asset: asset}
	if removed {
		return out, nil
	}

	m, ok := cd.Val.GetMap()
	if !ok {
		return DecodedEntry{}, fmt.Errorf("blend: %s(%s): expected map value, got %v", kind, asset, cd.Val.Type)
	}

	r := &mapReader{m: m, kind: kind}
	cfg := &ReserveConfigData{
		CFactor:    readField(r, "c_factor", u32Val),
		Decimals:   readField(r, "decimals", u32Val),
		Enabled:    readField(r, "enabled", boolVal),
		Index:      readField(r, "index", u32Val),
		LFactor:    readField(r, "l_factor", u32Val),
		MaxUtil:    readField(r, "max_util", u32Val),
		RBase:      readField(r, "r_base", u32Val),
		ROne:       readField(r, "r_one", u32Val),
		RThree:     readField(r, "r_three", u32Val),
		RTwo:       readField(r, "r_two", u32Val),
		Reactivity: readField(r, "reactivity", u32Val),
		SupplyCap:  readField(r, "supply_cap", i128String),
		Util:       readField(r, "util", u32Val),
	}
	if r.err != nil {
		return DecodedEntry{}, r.err
	}

	out.ResConfig = cfg
	return out, nil
}

func decodeResData(cd xdr.ContractDataEntry, elems []xdr.ScVal, removed bool) (DecodedEntry, error) {
	const kind = "ResData"
	asset, err := decodeAddressArg(elems, kind)
	if err != nil {
		return DecodedEntry{}, err
	}

	out := DecodedEntry{Kind: KindResData, Removed: removed, Asset: asset}
	if removed {
		return out, nil
	}

	m, ok := cd.Val.GetMap()
	if !ok {
		return DecodedEntry{}, fmt.Errorf("blend: %s(%s): expected map value, got %v", kind, asset, cd.Val.Type)
	}

	r := &mapReader{m: m, kind: kind}
	data := &ReserveDataData{
		BRate:          readField(r, "b_rate", i128String),
		BSupply:        readField(r, "b_supply", i128String),
		BackstopCredit: readField(r, "backstop_credit", i128String),
		DRate:          readField(r, "d_rate", i128String),
		DSupply:        readField(r, "d_supply", i128String),
		IRMod:          readField(r, "ir_mod", i128String),
		LastTime:       readField(r, "last_time", u64Val),
	}
	if r.err != nil {
		return DecodedEntry{}, r.err
	}

	out.ResData = data
	return out, nil
}

func decodeEmisData(cd xdr.ContractDataEntry, elems []xdr.ScVal, removed bool) (DecodedEntry, error) {
	const kind = "EmisData"
	tokenID, err := decodeU32Arg(elems, kind)
	if err != nil {
		return DecodedEntry{}, err
	}

	out := DecodedEntry{Kind: KindEmisData, Removed: removed, TokenID: tokenID}
	if removed {
		return out, nil
	}

	payload, err := decodeEmissionData(cd.Val, kind)
	if err != nil {
		return DecodedEntry{}, err
	}
	out.EmisData = payload
	return out, nil
}

func decodeAuction(cd xdr.ContractDataEntry, elems []xdr.ScVal, removed bool) (DecodedEntry, error) {
	const kind = "Auction"
	auctType, user, err := decodeAuctionKey(elems, kind)
	if err != nil {
		return DecodedEntry{}, err
	}

	out := DecodedEntry{Kind: KindAuction, Removed: removed, User: user, AuctionType: int32(auctType)}
	if removed {
		return out, nil
	}

	m, ok := cd.Val.GetMap()
	if !ok {
		return DecodedEntry{}, fmt.Errorf("blend: %s(%d,%s): expected map value, got %v", kind, auctType, user, cd.Val.Type)
	}

	r := &mapReader{m: m, kind: kind}
	data := &AuctionData{
		Bid:   readField(r, "bid", mapAddrI128),
		Block: readField(r, "block", u32Val),
		Lot:   readField(r, "lot", mapAddrI128),
	}
	if r.err != nil {
		return DecodedEntry{}, r.err
	}

	out.Auction = data
	return out, nil
}

func decodePositions(cd xdr.ContractDataEntry, elems []xdr.ScVal, removed bool) (DecodedEntry, error) {
	const kind = "Positions"
	user, err := decodeAddressArg(elems, kind)
	if err != nil {
		return DecodedEntry{}, err
	}

	out := DecodedEntry{Kind: KindPositions, Removed: removed, User: user}
	if removed {
		return out, nil
	}

	m, ok := cd.Val.GetMap()
	if !ok {
		return DecodedEntry{}, fmt.Errorf("blend: %s(%s): expected map value, got %v", kind, user, cd.Val.Type)
	}

	r := &mapReader{m: m, kind: kind}
	positions := &PositionsData{
		Collateral:  readField(r, "collateral", mapU32I128),
		Liabilities: readField(r, "liabilities", mapU32I128),
		Supply:      readField(r, "supply", mapU32I128),
	}
	if r.err != nil {
		return DecodedEntry{}, r.err
	}

	out.Positions = positions
	return out, nil
}

func decodeUserEmis(cd xdr.ContractDataEntry, elems []xdr.ScVal, removed bool) (DecodedEntry, error) {
	const kind = "UserEmis"
	reserveID, user, err := decodeUserReserveKey(elems, kind)
	if err != nil {
		return DecodedEntry{}, err
	}

	out := DecodedEntry{Kind: KindUserEmis, Removed: removed, TokenID: reserveID, User: user}
	if removed {
		return out, nil
	}

	payload, err := decodeUserEmissionData(cd.Val, kind)
	if err != nil {
		return DecodedEntry{}, err
	}
	out.UserEmis = payload
	return out, nil
}

func decodeBackstopUserBalance(cd xdr.ContractDataEntry, elems []xdr.ScVal, removed bool) (DecodedEntry, error) {
	const kind = "UserBalance"
	pool, user, err := decodePoolUserKey(elems, kind)
	if err != nil {
		return DecodedEntry{}, err
	}

	out := DecodedEntry{Kind: KindBackstopUserBalance, Removed: removed, Pool: pool, User: user}
	if removed {
		return out, nil
	}

	m, ok := cd.Val.GetMap()
	if !ok {
		return DecodedEntry{}, fmt.Errorf("blend: %s(%s,%s): expected map value, got %v", kind, pool, user, cd.Val.Type)
	}

	r := &mapReader{m: m, kind: kind}
	shares := readField(r, "shares", i128String)
	q4wElems := readField(r, "q4w", vecVal)
	if r.err != nil {
		return DecodedEntry{}, r.err
	}

	q4ws := make([]Q4WData, 0, len(q4wElems))
	for i, el := range q4wElems {
		q4wMap, ok := el.GetMap()
		if !ok {
			return DecodedEntry{}, fmt.Errorf("blend: %s(%s,%s): q4w[%d] is not a map (type %v)", kind, pool, user, i, el.Type)
		}
		qr := &mapReader{m: q4wMap, kind: "Q4W"}
		amount := readField(qr, "amount", i128String)
		exp := readField(qr, "exp", u64Val)
		if qr.err != nil {
			return DecodedEntry{}, qr.err
		}
		q4ws = append(q4ws, Q4WData{Amount: amount, Exp: exp})
	}

	out.BackstopUserBalance = &BackstopUserBalanceData{Q4W: q4ws, Shares: shares}
	return out, nil
}

func decodeBackstopPoolBalance(cd xdr.ContractDataEntry, elems []xdr.ScVal, removed bool) (DecodedEntry, error) {
	const kind = "PoolBalance"
	pool, err := decodeAddressArg(elems, kind)
	if err != nil {
		return DecodedEntry{}, err
	}

	out := DecodedEntry{Kind: KindBackstopPoolBalance, Removed: removed, Pool: pool}
	if removed {
		return out, nil
	}

	m, ok := cd.Val.GetMap()
	if !ok {
		return DecodedEntry{}, fmt.Errorf("blend: %s(%s): expected map value, got %v", kind, pool, cd.Val.Type)
	}

	r := &mapReader{m: m, kind: kind}
	data := &BackstopPoolBalanceData{
		Q4W:    readField(r, "q4w", i128String),
		Shares: readField(r, "shares", i128String),
		Tokens: readField(r, "tokens", i128String),
	}
	if r.err != nil {
		return DecodedEntry{}, r.err
	}

	out.BackstopPoolBalance = data
	return out, nil
}

func decodeBackstopBEmisData(cd xdr.ContractDataEntry, elems []xdr.ScVal, removed bool) (DecodedEntry, error) {
	const kind = "BEmisData"
	pool, err := decodeAddressArg(elems, kind)
	if err != nil {
		return DecodedEntry{}, err
	}

	out := DecodedEntry{Kind: KindBackstopBEmisData, Removed: removed, Pool: pool}
	if removed || cd.Val.Type == xdr.ScValTypeScvVoid {
		// Option<BackstopEmissionData>::None (or a removed entry, whose
		// payload is dropped regardless): identity is still reported, no
		// payload to decode.
		return out, nil
	}

	payload, err := decodeEmissionData(cd.Val, kind)
	if err != nil {
		return DecodedEntry{}, err
	}
	out.EmisData = payload
	return out, nil
}

func decodeBackstopUEmisData(cd xdr.ContractDataEntry, elems []xdr.ScVal, removed bool) (DecodedEntry, error) {
	const kind = "UEmisData"
	pool, user, err := decodePoolUserKey(elems, kind)
	if err != nil {
		return DecodedEntry{}, err
	}

	out := DecodedEntry{Kind: KindBackstopUEmisData, Removed: removed, Pool: pool, User: user}
	if removed || cd.Val.Type == xdr.ScValTypeScvVoid {
		// Option<UserEmissionData>::None (or a removed entry): same
		// no-payload contract as decodeBackstopBEmisData above.
		return out, nil
	}

	payload, err := decodeUserEmissionData(cd.Val, kind)
	if err != nil {
		return DecodedEntry{}, err
	}
	out.UserEmis = payload
	return out, nil
}

// mapReader decodes a sequence of required fields out of one ScMap,
// capturing the first missing-or-malformed field as a sticky error (already
// wrapped with which kind and field failed via readField) so callers can
// chain reads — e.g. inside a struct literal — without an if-err-return
// after every single field; check r.err once after the last read.
type mapReader struct {
	m    *xdr.ScMap
	kind string
	err  error
}

// readField reads field's value out of r's map via decode. If r already
// holds an error from an earlier field in the same mapReader, it returns the
// zero value without touching the map, so a chain of readField calls always
// reports the first failure.
func readField[T any](r *mapReader, field string, decode func(xdr.ScVal) (T, bool)) T {
	var zero T
	if r.err != nil {
		return zero
	}
	v, ok := mapGet(r.m, field)
	if !ok {
		r.err = fmt.Errorf("blend: %s: missing field %q", r.kind, field)
		return zero
	}
	out, ok := decode(v)
	if !ok {
		r.err = fmt.Errorf("blend: %s: field %q has unexpected type %v", r.kind, field, v.Type)
		return zero
	}
	return out
}
