package resolvers

import (
	"time"

	graphql1 "github.com/stellar/wallet-backend/internal/serve/graphql/generated"
	"github.com/stellar/wallet-backend/internal/services/prices"
)

// Bounds for the token price queries. Spot lookups are in-memory, so the id cap only bounds the
// response size; the history caps keep every candle query to one bounded aggregate scan.
const (
	maxTokenPriceIDs     = 200
	maxCandleBuckets     = 1000
	maxSpanOneMinute     = 24 * time.Hour
	maxSpanOneHour       = 60 * 24 * time.Hour
	maxSpanOneDay        = 5 * 365 * 24 * time.Hour
	errMsgTokenIDInvalid = "invalid tokenId: must be a valid contract (C...) address"
)

// priceWindow maps a snapshot window to its GraphQL value; WindowNone has none.
func priceWindow(w prices.Window) *graphql1.PriceWindow {
	var out graphql1.PriceWindow
	switch w {
	case prices.Window1H:
		out = graphql1.PriceWindowOneHour
	case prices.Window24H:
		out = graphql1.PriceWindowOneDay
	case prices.WindowNone:
		return nil
	default:
		return nil
	}
	return &out
}

// priceSource maps a snapshot price source to its GraphQL value.
func priceSource(s prices.PriceSource) *graphql1.TokenPriceSource {
	var out graphql1.TokenPriceSource
	switch s {
	case prices.PriceSourceVWAP1H:
		out = graphql1.TokenPriceSourceVwap1h
	case prices.PriceSourceVWAP24H:
		out = graphql1.TokenPriceSourceVwap24h
	case prices.PriceSourceOracle:
		out = graphql1.TokenPriceSourceOracle
	default:
		return nil
	}
	return &out
}
