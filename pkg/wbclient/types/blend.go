package types

// Blend v2 client types, mirroring internal/serve/graphql/schema/blend.graphqls.
// Conventions carried over from the schema:
//   - USD/APY values are nullable Float: null means "uncomputable" (a missing
//     or >24h-stale oracle price); a genuinely zero balance is 0, not null.
//   - On-chain token amounts are non-null String at full precision.
//   - tokenName/tokenSymbol/tokenDecimals come from the contract_tokens
//     metadata registry and are nullable.

// BlendPoolStatus is a pool's operational status. ADMIN_ACTIVE/ACTIVE/
// ADMIN_ON_ICE/ON_ICE accept supply (deposits); ADMIN_ACTIVE/ACTIVE also
// allow borrowing; ADMIN_FROZEN/FROZEN/SETUP reject both.
type BlendPoolStatus string

const (
	BlendPoolStatusAdminActive BlendPoolStatus = "ADMIN_ACTIVE"
	BlendPoolStatusActive      BlendPoolStatus = "ACTIVE"
	BlendPoolStatusAdminOnIce  BlendPoolStatus = "ADMIN_ON_ICE"
	BlendPoolStatusOnIce       BlendPoolStatus = "ON_ICE"
	BlendPoolStatusAdminFrozen BlendPoolStatus = "ADMIN_FROZEN"
	BlendPoolStatusFrozen      BlendPoolStatus = "FROZEN"
	BlendPoolStatusSetup       BlendPoolStatus = "SETUP"
)

// AcceptsSupply reports whether a pool in this status accepts deposits.
func (s BlendPoolStatus) AcceptsSupply() bool {
	switch s {
	case BlendPoolStatusAdminActive, BlendPoolStatusActive, BlendPoolStatusAdminOnIce, BlendPoolStatusOnIce:
		return true
	}
	return false
}

// AcceptsBorrow reports whether a pool in this status allows borrowing.
func (s BlendPoolStatus) AcceptsBorrow() bool {
	return s == BlendPoolStatusAdminActive || s == BlendPoolStatusActive
}

// BlendAuctionType distinguishes the three Blend v2 Dutch auction kinds.
type BlendAuctionType string

const (
	BlendAuctionTypeUserLiquidation BlendAuctionType = "USER_LIQUIDATION"
	BlendAuctionTypeBadDebt         BlendAuctionType = "BAD_DEBT"
	BlendAuctionTypeInterest        BlendAuctionType = "INTEREST"
)

// BlendAccountPositions aggregates one account's Blend v2 exposure across
// every pool it has touched. BackstopClaimedLp is lifetime backstop-emission
// claims in Comet LP tokens, account-wide (backstop claim events carry no
// pool address).
type BlendAccountPositions struct {
	Pools             []BlendPoolPosition     `json:"pools"`
	Backstop          []BlendBackstopPosition `json:"backstop"`
	BackstopClaimedLp string                  `json:"backstopClaimedLp"`
	ActiveAuctions    []BlendAuction          `json:"activeAuctions"`
}

// BlendPoolPosition rolls up an account's reserve positions within one pool.
// UsdValue is supplied minus borrowed. NetApy nets supply earnings against
// borrow interest over TOTAL SUPPLIED USD (the blend-sdk-js convention the
// Blend UI shows); 0 for a debt-only position, null when any contributing
// reserve is missing a fresh oracle price.
type BlendPoolPosition struct {
	PoolAddress string                 `json:"poolAddress"`
	PoolName    *string                `json:"poolName"`
	UsdValue    *float64               `json:"usdValue"`
	SuppliedUsd *float64               `json:"suppliedUsd"`
	BorrowedUsd *float64               `json:"borrowedUsd"`
	NetApy      *float64               `json:"netApy"`
	ClaimedBlnd string                 `json:"claimedBlnd"`
	Reserves    []BlendReservePosition `json:"reserves"`
}

// BlendReservePosition is an account's position in one reserve of a pool.
// Token amounts are underlying-asset amounts at rates projected to now.
// InterestEarned/InterestPaid are lifetime, token-denominated (raw integers
// at TokenDecimals; multiply by PriceUsd for USD), and survive a full exit.
// EmissionsEarnedBlnd is claimable (uncollected) BLND across the reserve's
// emission streams. EmissionsSupplyApr/EmissionsBorrowApr are the reserve's
// pool-wide per-side emission-stream APRs (0 = no active stream, null =
// stream active but unpriceable).
type BlendReservePosition struct {
	AssetContractID     string   `json:"assetContractId"`
	TokenName           *string  `json:"tokenName"`
	TokenSymbol         *string  `json:"tokenSymbol"`
	TokenDecimals       *int32   `json:"tokenDecimals"`
	SuppliedTokens      string   `json:"suppliedTokens"`
	CollateralTokens    string   `json:"collateralTokens"`
	BorrowedTokens      string   `json:"borrowedTokens"`
	SuppliedUsd         *float64 `json:"suppliedUsd"`
	BorrowedUsd         *float64 `json:"borrowedUsd"`
	SupplyApy           *float64 `json:"supplyApy"`
	BorrowApy           *float64 `json:"borrowApy"`
	EmissionsSupplyApr  *float64 `json:"emissionsSupplyApr"`
	EmissionsBorrowApr  *float64 `json:"emissionsBorrowApr"`
	InterestEarned      string   `json:"interestEarned"`
	InterestPaid        string   `json:"interestPaid"`
	EmissionsEarnedBlnd string   `json:"emissionsEarnedBlnd"`
	EmissionsEarnedUsd  *float64 `json:"emissionsEarnedUsd"`
	PriceUsd            *float64 `json:"priceUsd"`
}

// BlendBackstopPosition is an account's backstop deposit in one pool. Shares
// is the ACTIVE (non-queued) share balance; LpTokens/UsdValue value the
// whole deposit including queued-for-withdrawal shares (queued shares keep
// earning pool interest and remain slashable until withdrawn).
type BlendBackstopPosition struct {
	PoolAddress         string     `json:"poolAddress"`
	PoolName            *string    `json:"poolName"`
	Shares              string     `json:"shares"`
	LpTokens            string     `json:"lpTokens"`
	UsdValue            *float64   `json:"usdValue"`
	Q4w                 []BlendQ4W `json:"q4w"`
	EmissionsEarnedBlnd string     `json:"emissionsEarnedBlnd"`
	EmissionsEarnedUsd  *float64   `json:"emissionsEarnedUsd"`
}

// BlendQ4W is one queued backstop withdrawal, unlocking at Expiration (unix
// seconds). Amount is in backstop shares.
type BlendQ4W struct {
	Amount     string   `json:"amount"`
	Expiration int64    `json:"expiration"`
	LpTokens   string   `json:"lpTokens"`
	UsdValue   *float64 `json:"usdValue"`
}

// BlendAuction is one active Dutch auction on a Blend v2 pool. Bid/Lot
// amounts are raw protocol-token decimal strings, not USD.
type BlendAuction struct {
	PoolAddress string               `json:"poolAddress"`
	PoolName    *string              `json:"poolName"`
	AuctionType BlendAuctionType     `json:"auctionType"`
	Bid         []BlendAuctionAmount `json:"bid"`
	Lot         []BlendAuctionAmount `json:"lot"`
	StartBlock  int32                `json:"startBlock"`
}

// BlendAuctionAmount is one asset's raw protocol-token amount within an
// auction's bid or lot.
type BlendAuctionAmount struct {
	AssetContractID string `json:"assetContractId"`
	Amount          string `json:"amount"`
}

// BlendPool is a pool-wide catalog view of one Blend v2 pool, independent of
// any account. SuppliedUsd/BorrowedUsd are strict-null: a missing price on
// any reserve makes the pool-wide total uncomputable. InterestApy is the
// supplied-USD-weighted supply rate (interest only); NetApy additionally
// folds in each reserve's supply-side BLND emissions — a supply-side yield,
// not netted against the pool's borrow side.
type BlendPool struct {
	Address          string           `json:"address"`
	Name             *string          `json:"name"`
	Status           *BlendPoolStatus `json:"status"`
	OracleContractID *string          `json:"oracleContractId"`
	// BackstopRate is the share of borrower interest routed to the pool's
	// backstop, as 7-decimal fixed point (4750000 = 47.5%).
	BackstopRate *int32         `json:"backstopRate"`
	MaxPositions *int32         `json:"maxPositions"`
	SuppliedUsd  *float64       `json:"suppliedUsd"`
	BorrowedUsd  *float64       `json:"borrowedUsd"`
	BackstopUsd  *float64       `json:"backstopUsd"`
	InterestApy  *float64       `json:"interestApy"`
	NetApy       *float64       `json:"netApy"`
	Reserves     []BlendReserve `json:"reserves"`
	Admin        *string        `json:"admin"`
	// InRewardZone reports whether the pool is in the backstop's reward zone
	// and therefore receives BLND emissions.
	InRewardZone bool `json:"inRewardZone"`
}

// BlendReserve is a pool-wide reserve catalog row: rates, totals, and risk
// parameters as of now, no per-account data. CFactor/LFactor are 7-decimal
// fixed point (9000000 = 0.9).
type BlendReserve struct {
	AssetContractID    string   `json:"assetContractId"`
	TokenName          *string  `json:"tokenName"`
	TokenSymbol        *string  `json:"tokenSymbol"`
	TokenDecimals      *int32   `json:"tokenDecimals"`
	Enabled            bool     `json:"enabled"`
	Utilization        *float64 `json:"utilization"`
	SupplyApy          *float64 `json:"supplyApy"`
	BorrowApy          *float64 `json:"borrowApy"`
	EmissionsSupplyApr *float64 `json:"emissionsSupplyApr"`
	EmissionsBorrowApr *float64 `json:"emissionsBorrowApr"`
	SuppliedTokens     string   `json:"suppliedTokens"`
	BorrowedTokens     string   `json:"borrowedTokens"`
	SuppliedUsd        *float64 `json:"suppliedUsd"`
	BorrowedUsd        *float64 `json:"borrowedUsd"`
	CFactor            *int32   `json:"cFactor"`
	LFactor            *int32   `json:"lFactor"`
	PriceUsd           *float64 `json:"priceUsd"`
}
