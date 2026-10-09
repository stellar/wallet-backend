package resolvers

import "time"

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
