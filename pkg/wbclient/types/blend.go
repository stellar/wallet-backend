package types

import (
	"encoding/json"
	"fmt"
)

// BlendPoolStatus is a pool's operational status. The on-chain integer status
// (0-6) is surfaced by the GraphQL API as one of these enum names; see the
// BlendPoolStatus enum in the schema for the numeric encoding.
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

// AcceptsSupply reports whether a pool in this status accepts deposits,
// per the schema's status table: ADMIN_ACTIVE/ACTIVE/ADMIN_ON_ICE/ON_ICE
// accept supply; ADMIN_FROZEN/FROZEN/SETUP reject it.
func (s BlendPoolStatus) AcceptsSupply() bool {
	switch s {
	case BlendPoolStatusAdminActive, BlendPoolStatusActive, BlendPoolStatusAdminOnIce, BlendPoolStatusOnIce:
		return true
	case BlendPoolStatusAdminFrozen, BlendPoolStatusFrozen, BlendPoolStatusSetup:
		return false
	}
	return false
}

// AcceptsBorrow reports whether a pool in this status allows borrowing:
// only ADMIN_ACTIVE and ACTIVE do.
func (s BlendPoolStatus) AcceptsBorrow() bool {
	return s == BlendPoolStatusAdminActive || s == BlendPoolStatusActive
}

// BlendPool is a pool-wide catalog view of one Blend v2 pool, independent of any account.
type BlendPool struct {
	Address          string           `json:"address"`
	Name             *string          `json:"name,omitempty"`
	Status           *BlendPoolStatus `json:"status,omitempty"`
	OracleContractID *string          `json:"oracleContractId,omitempty"`
	BackstopRate     *int32           `json:"backstopRate,omitempty"`
	MaxPositions     *int32           `json:"maxPositions,omitempty"`
	SuppliedUsd      *float64         `json:"suppliedUsd,omitempty"`
	BorrowedUsd      *float64         `json:"borrowedUsd,omitempty"`
	BackstopUsd      *float64         `json:"backstopUsd,omitempty"`
	InterestApy      *float64         `json:"interestApy,omitempty"`
	NetApy           *float64         `json:"netApy,omitempty"`
	Reserves         []BlendReserve   `json:"reserves"`
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

// BlendPoolEdge represents an edge in the Blend pool connection.
type BlendPoolEdge struct {
	Node   *BlendPool `json:"node,omitempty"`
	Cursor string     `json:"cursor"`
}

// UnmarshalJSON implements custom JSON unmarshaling for BlendPoolEdge.
// The GraphQL schema declares the edge as `node: BlendPool!` (non-null),
// so a null or missing node is a malformed server response and is rejected
// rather than left as e.Node == nil.
func (e *BlendPoolEdge) UnmarshalJSON(data []byte) error {
	type tempEdge struct {
		Node   json.RawMessage `json:"node"`
		Cursor string          `json:"cursor"`
	}

	var temp tempEdge
	if err := json.Unmarshal(data, &temp); err != nil {
		return fmt.Errorf("unmarshaling blend pool edge: %w", err)
	}

	e.Cursor = temp.Cursor

	if len(temp.Node) == 0 || string(temp.Node) == "null" {
		return fmt.Errorf("blend pool edge missing required node (cursor=%q): the GraphQL schema declares BlendPool as non-null", temp.Cursor)
	}

	var node BlendPool
	if err := json.Unmarshal(temp.Node, &node); err != nil {
		return fmt.Errorf("decoding blend pool edge node: %w", err)
	}
	e.Node = &node
	return nil
}

// BlendPoolConnection represents a paginated page of the Blend v2 pool catalog.
type BlendPoolConnection struct {
	Edges    []*BlendPoolEdge `json:"edges,omitempty"`
	PageInfo *PageInfo        `json:"pageInfo"`
}

// UnmarshalJSON implements custom JSON unmarshaling for BlendPoolConnection
// and enforces the schema's non-null guarantees. The GraphQL schema declares
// edges as [BlendPoolEdge!]! and pageInfo as PageInfo!, so each of the
// following is a server bug and is rejected here:
//   - a missing or null edges field on the connection
//   - a null entry within the edges array
//   - a missing or null pageInfo field on the connection
//
// Null nodes inside an edge object are caught separately by
// BlendPoolEdge.UnmarshalJSON.
func (c *BlendPoolConnection) UnmarshalJSON(data []byte) error {
	edges, pageInfo, err := unmarshalConnection[BlendPoolEdge](data, "blend pool connection", "BlendPoolEdge")
	if err != nil {
		return err
	}
	c.Edges = edges
	c.PageInfo = pageInfo
	return nil
}

// Pools returns the connection's pool nodes as a flat slice. Returns nil if the
// receiver is nil or has no edges. Combined with the strict UnmarshalJSON on
// BlendPoolEdge and BlendPoolConnection, callers using this helper can trust
// that every returned BlendPool is non-nil.
//
// The defensive nil-edge skip protects against connections constructed directly
// in Go code (not via JSON decode); JSON-derived connections never reach this
// branch because UnmarshalJSON rejects null edges.
func (c *BlendPoolConnection) Pools() []BlendPool {
	if c == nil || len(c.Edges) == 0 {
		return nil
	}
	pools := make([]BlendPool, 0, len(c.Edges))
	for _, edge := range c.Edges {
		if edge == nil || edge.Node == nil {
			continue
		}
		pools = append(pools, *edge.Node)
	}
	return pools
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
	EmissionsSupplyApr  *float64 `json:"emissionsSupplyApr,omitempty"`
	EmissionsBorrowApr  *float64 `json:"emissionsBorrowApr,omitempty"`
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
