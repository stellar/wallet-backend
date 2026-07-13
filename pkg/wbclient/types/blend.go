package types

// BlendPool is a pool-wide catalog view of one Blend v2 pool, independent of any account.
type BlendPool struct {
	Address          string         `json:"address"`
	Name             *string        `json:"name,omitempty"`
	Status           *int32         `json:"status,omitempty"`
	OracleContractID *string        `json:"oracleContractId,omitempty"`
	BackstopRate     *int32         `json:"backstopRate,omitempty"`
	MaxPositions     *int32         `json:"maxPositions,omitempty"`
	SuppliedUsd      *float64       `json:"suppliedUsd,omitempty"`
	BorrowedUsd      *float64       `json:"borrowedUsd,omitempty"`
	BackstopUsd      *float64       `json:"backstopUsd,omitempty"`
	InterestApy      *float64       `json:"interestApy,omitempty"`
	NetApy           *float64       `json:"netApy,omitempty"`
	Reserves         []BlendReserve `json:"reserves"`
	// Admin is the pool admin address (G... or C...), distinguishing owned pools — whose admin
	// can retune parameters — from standard pools whose admin is disabled. Nil when not yet
	// observed.
	Admin *string `json:"admin,omitempty"`
	// InRewardZone reports whether the pool is in the backstop's reward zone and therefore
	// receives BLND emissions.
	InRewardZone bool `json:"inRewardZone"`
}

// BlendReserve is a pool-wide reserve catalog view: utilization, APYs, emissions APRs, and
// pool-wide underlying token amounts, all as of "now".
type BlendReserve struct {
	AssetContractID    string   `json:"assetContractId"`
	TokenName          *string  `json:"tokenName,omitempty"`
	TokenSymbol        *string  `json:"tokenSymbol,omitempty"`
	TokenDecimals      *int32   `json:"tokenDecimals,omitempty"`
	Enabled            bool     `json:"enabled"`
	Utilization        *float64 `json:"utilization,omitempty"`
	SupplyApy          *float64 `json:"supplyApy,omitempty"`
	BorrowApy          *float64 `json:"borrowApy,omitempty"`
	EmissionsSupplyApr *float64 `json:"emissionsSupplyApr,omitempty"`
	EmissionsBorrowApr *float64 `json:"emissionsBorrowApr,omitempty"`
	SuppliedTokens     string   `json:"suppliedTokens"`
	BorrowedTokens     string   `json:"borrowedTokens"`
	SuppliedUsd        *float64 `json:"suppliedUsd,omitempty"`
	BorrowedUsd        *float64 `json:"borrowedUsd,omitempty"`
	CFactor            *int32   `json:"cFactor,omitempty"`
	LFactor            *int32   `json:"lFactor,omitempty"`
	PriceUsd           *float64 `json:"priceUsd,omitempty"`
}

// BlendEarnOption is a "where can I earn this asset" catalog view: one entry per asset with at
// least one enabled reserve in a pool that currently accepts supply.
type BlendEarnOption struct {
	AssetContractID string                `json:"assetContractId"`
	TokenName       *string               `json:"tokenName,omitempty"`
	TokenSymbol     *string               `json:"tokenSymbol,omitempty"`
	TokenDecimals   *int32                `json:"tokenDecimals,omitempty"`
	Pools           []BlendEarnPoolOption `json:"pools"`
}

// BlendEarnPoolOption is one pool's offer for a BlendEarnOption's asset.
type BlendEarnPoolOption struct {
	PoolAddress        string   `json:"poolAddress"`
	PoolName           *string  `json:"poolName,omitempty"`
	SupplyApy          *float64 `json:"supplyApy,omitempty"`
	EmissionsSupplyApr *float64 `json:"emissionsSupplyApr,omitempty"`
	SuppliedUsd        *float64 `json:"suppliedUsd,omitempty"`
}

// BlendAccountPositions aggregates one account's Blend v2 exposure across every pool it has
// touched.
type BlendAccountPositions struct {
	Pools             []BlendPoolPosition     `json:"pools"`
	Backstop          []BlendBackstopPosition `json:"backstop"`
	BackstopClaimedLp string                  `json:"backstopClaimedLp"`
	// ActiveAuctions are the Dutch auctions where this account is the auction owner: being
	// liquidated (USER_LIQUIDATION), or — only when this account IS the backstop address —
	// carrying bad debt (BAD_DEBT) or settling interest (INTEREST). Sorted by
	// (poolAddress, auctionType).
	ActiveAuctions []BlendAuction `json:"activeAuctions"`
}

// BlendAuction is one active Dutch auction on a Blend v2 pool. The amounts in Bid and Lot are raw
// protocol-token integer strings (not USD), at the scale noted per field.
type BlendAuction struct {
	PoolAddress string  `json:"poolAddress"`
	PoolName    *string `json:"poolName,omitempty"`
	// AuctionType is one of USER_LIQUIDATION, BAD_DEBT, INTEREST.
	AuctionType string `json:"auctionType"`
	// Bid holds the assets the filler pays. Units by type: USER_LIQUIDATION/BAD_DEBT dTokens;
	// INTEREST backstop LP tokens.
	Bid []BlendAuctionAmount `json:"bid"`
	// Lot holds the assets the filler receives. Units by type: USER_LIQUIDATION bTokens;
	// BAD_DEBT backstop LP tokens; INTEREST underlying.
	Lot []BlendAuctionAmount `json:"lot"`
	// StartBlock is the ledger the auction started at, anchoring the Dutch-auction lot/bid
	// scaling (0-200 lot ramps up, 200-400 bid ramps down).
	StartBlock int32 `json:"startBlock"`
}

// BlendPoolPosition rolls up an account's reserve positions within one pool.
type BlendPoolPosition struct {
	PoolAddress string                 `json:"poolAddress"`
	PoolName    *string                `json:"poolName,omitempty"`
	UsdValue    *float64               `json:"usdValue,omitempty"`
	SuppliedUsd *float64               `json:"suppliedUsd,omitempty"`
	BorrowedUsd *float64               `json:"borrowedUsd,omitempty"`
	NetApy      *float64               `json:"netApy,omitempty"`
	ClaimedBlnd string                 `json:"claimedBlnd"`
	Reserves    []BlendReservePosition `json:"reserves"`
}

// BlendReservePosition is an account's position in one reserve of a pool.
type BlendReservePosition struct {
	AssetContractID     string   `json:"assetContractId"`
	TokenName           *string  `json:"tokenName,omitempty"`
	TokenSymbol         *string  `json:"tokenSymbol,omitempty"`
	TokenDecimals       *int32   `json:"tokenDecimals,omitempty"`
	SuppliedTokens      string   `json:"suppliedTokens"`
	CollateralTokens    string   `json:"collateralTokens"`
	BorrowedTokens      string   `json:"borrowedTokens"`
	SuppliedUsd         *float64 `json:"suppliedUsd,omitempty"`
	BorrowedUsd         *float64 `json:"borrowedUsd,omitempty"`
	SupplyApy           *float64 `json:"supplyApy,omitempty"`
	BorrowApy           *float64 `json:"borrowApy,omitempty"`
	EmissionsApr        *float64 `json:"emissionsApr,omitempty"`
	InterestEarned      string   `json:"interestEarned"`
	InterestPaid        string   `json:"interestPaid"`
	EmissionsEarnedBlnd string   `json:"emissionsEarnedBlnd"`
	EmissionsEarnedUsd  *float64 `json:"emissionsEarnedUsd,omitempty"`
	PriceUsd            *float64 `json:"priceUsd,omitempty"`
}

// BlendBackstopPosition is an account's backstop deposit in one pool.
type BlendBackstopPosition struct {
	PoolAddress         string     `json:"poolAddress"`
	PoolName            *string    `json:"poolName,omitempty"`
	Shares              string     `json:"shares"`
	LpTokens            string     `json:"lpTokens"`
	UsdValue            *float64   `json:"usdValue,omitempty"`
	Q4W                 []BlendQ4W `json:"q4w"`
	EmissionsEarnedBlnd string     `json:"emissionsEarnedBlnd"`
	EmissionsEarnedUsd  *float64   `json:"emissionsEarnedUsd,omitempty"`
}

// BlendQ4W is one queued backstop withdrawal, unlocking at expiration (unix seconds).
type BlendQ4W struct {
	Amount     string   `json:"amount"`
	Expiration int64    `json:"expiration"`
	LpTokens   string   `json:"lpTokens"`
	UsdValue   *float64 `json:"usdValue,omitempty"`
}
