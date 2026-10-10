package types

import (
	"math/big"
	"time"
)

// TradeVenue identifies where a fill executed. The values are stored in trades.venue.
type TradeVenue int16

const (
	TradeVenueSDEXOrderbook TradeVenue = 1
	TradeVenueSDEXPool      TradeVenue = 2
	TradeVenueSoroswap      TradeVenue = 3
	TradeVenueAquarius      TradeVenue = 4
)

// Trade is one executed fill, oriented for pricing: BaseToken is the token the fill prices and
// CounterToken is the anchor side (USDC before XLM before anything else). Tokens are contract
// C-addresses; classic assets appear as their Stellar Asset Contract. Amounts are raw token units.
//
// Decimals are set by the processor when it knows them (classic assets always have 7) and
// resolved from contract_tokens otherwise. BaseQty and USDValue are filled by the persist path
// from the decimals and the oracle anchor rate, and stay nil when either is unknown.
type Trade struct {
	OperationID int64
	// Taker is the operation's source account (a G-address, muxed ids dropped): the account that
	// crossed the offer or invoked the swap. Price confidence counts distinct takers, not fills.
	Taker         string
	FillIndex     int16
	LedgerNumber  uint32
	LedgerClosed  time.Time
	BaseToken     string
	CounterToken  string
	BaseAmount    *big.Int
	CounterAmount *big.Int
	Venue         TradeVenue

	BaseDecimals    *int32
	CounterDecimals *int32
	BaseQty         *float64
	USDValue        *float64
}

// AMMPool is a Soroban AMM pool whose swap events the trades processor trusts, keyed by the pool
// contract C-address. Token0 and Token1 are the pool's tokens in the venue's own order.
type AMMPool struct {
	Pool          string
	Venue         TradeVenue
	Token0        string
	Token1        string
	CreatedLedger uint32
}
