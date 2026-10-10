package prices

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/stellar/wallet-backend/internal/indexer/types"
)

// DefaultMaxError is the largest estimated relative error at which a price is served.
const DefaultMaxError = 0.05

// PriceSource says which number a TokenPrice's PriceUSD is.
type PriceSource int16

const (
	PriceSourceVWAP1H  PriceSource = 1
	PriceSourceVWAP24H PriceSource = 2
	// PriceSourceOracle is the anchor oracle's own reading, used for the anchor tokens the fills
	// never price: an anchor is always the counter side of a fill, so it has no base rows.
	PriceSourceOracle PriceSource = 3
)

// Window is the trailing period a price was estimated over.
type Window int16

const (
	WindowNone Window = 0
	Window1H   Window = 1
	Window24H  Window = 2
)

// oracleMaxAge is how old an oracle reading may be and still be served as a price; the oracle
// poller already refuses older readings, so this is a second guard on the read path.
const oracleMaxAge = 24 * time.Hour

// TokenPrice is one token's spot price with the evidence behind it.
//
// PriceUSD is the VWAP of the trailing hour when that hour's fills pin the price within the
// tolerance, else the VWAP of the trailing 24 hours under the same test. A token that passes
// neither is still described, by its 24-hour window, but is not Publishable. ErrorPct is the
// estimated relative standard error in percent; it is nil when fewer than two effective takers
// leave no way to estimate it.
type TokenPrice struct {
	Token           string
	PriceUSD        float64
	Source          PriceSource
	Window          Window
	ErrorPct        *float64
	EffectiveTakers float64
	Publishable     bool
	Price24hAgoUSD  *float64
	Volume24hUSD    float64
	LastTradeAt     time.Time
}

// PercentChange24h returns the change against the price 24 hours ago, or nil without one.
func (p TokenPrice) PercentChange24h() *float64 {
	if p.Price24hAgoUSD == nil || *p.Price24hAgoUSD <= 0 {
		return nil
	}
	v := (p.PriceUSD/(*p.Price24hAgoUSD) - 1) * 100
	return &v
}

// Snapshot is every token's current price, as of one instant.
type Snapshot struct {
	AsOf   time.Time
	Prices map[string]TokenPrice
}

// windowStats are one token's sums over a window, after its fills were grouped by taker: USD
// weight w, base quantity q, the weighted first and second moments of log price, and the sum of
// squared per-taker weights.
type windowStats struct {
	w, q, wl, wll, takerW2 float64
	lastAt                 time.Time
}

// effectiveTakers is Kish's effective sample size over takers: (Σw)² / Σ_k W_k².
func (s windowStats) effectiveTakers() float64 {
	if s.takerW2 <= 0 {
		return 0
	}
	return s.w * s.w / s.takerW2
}

// relativeError is the standard error of the weighted mean log price, which approximates the
// relative error of the VWAP. ok is false below two effective takers.
func (s windowStats) relativeError() (float64, bool) {
	n := s.effectiveTakers()
	if n < 2 || s.w <= 0 {
		return 0, false
	}
	mean := s.wl / s.w
	variance := math.Max(s.wll/s.w-mean*mean, 0)
	return math.Sqrt(variance / n), true
}

// passes reports whether the window pins the price within maxError.
func (s windowStats) passes(maxError float64) bool {
	e, ok := s.relativeError()
	return ok && s.q > 0 && e <= maxError
}

// windowQuery sums a taker aggregate over [$1, $2), grouping by taker before token so the sum of
// squared per-taker weights is taken over each taker's whole window.
const windowQuery = `
	SELECT base_token, sum(w), sum(q), sum(wl), sum(wll), sum(w * w), max(last_at)
	FROM (
		SELECT base_token, taker, sum(w) AS w, sum(q) AS q, sum(wl) AS wl, sum(wll) AS wll, max(last_at) AS last_at
		FROM %s WHERE bucket >= $1 AND bucket < $2
		GROUP BY base_token, taker
	) per_taker
	GROUP BY base_token`

// LoadSnapshot assembles the snapshot from four bounded reads: the trailing hour from the
// 1-minute taker aggregate, the trailing 24 hours from the 1-hour taker aggregate, the VWAP of
// the hour 24 hours ago, and the oracle readings for the anchor tokens. A price is publishable
// when its window has at least two effective takers and an estimated error within maxError.
func LoadSnapshot(ctx context.Context, db *pgxpool.Pool, asOf time.Time, maxError float64) (*Snapshot, error) {
	hour, err := loadWindow(ctx, db, "trades_takers_1m", asOf.Add(-time.Hour), asOf)
	if err != nil {
		return nil, fmt.Errorf("reading trailing hour: %w", err)
	}
	dayStart := asOf.Add(-24 * time.Hour).Truncate(time.Hour)
	day, err := loadWindow(ctx, db, "trades_takers_1h", dayStart, asOf)
	if err != nil {
		return nil, fmt.Errorf("reading trailing day: %w", err)
	}

	snap := &Snapshot{AsOf: asOf, Prices: make(map[string]TokenPrice, len(day))}
	for token, d := range day {
		tp := TokenPrice{Token: token, Volume24hUSD: d.w, LastTradeAt: d.lastAt}
		h, inHour := hour[token]
		switch {
		case inHour && h.passes(maxError):
			describe(&tp, h, Window1H, PriceSourceVWAP1H)
			tp.Publishable = true
		case d.passes(maxError):
			describe(&tp, d, Window24H, PriceSourceVWAP24H)
			tp.Publishable = true
		default:
			describe(&tp, d, Window24H, PriceSourceVWAP24H)
		}
		snap.Prices[token] = tp
	}

	if err := loadPrice24hAgo(ctx, db, dayStart, snap); err != nil {
		return nil, err
	}
	if err := loadAnchors(ctx, db, asOf, snap); err != nil {
		return nil, err
	}
	return snap, nil
}

func describe(tp *TokenPrice, s windowStats, w Window, src PriceSource) {
	tp.Window, tp.Source = w, src
	if s.q > 0 {
		tp.PriceUSD = s.w / s.q
	}
	tp.EffectiveTakers = s.effectiveTakers()
	if e, ok := s.relativeError(); ok {
		pct := e * 100
		tp.ErrorPct = &pct
	}
}

func loadWindow(ctx context.Context, db *pgxpool.Pool, view string, from, to time.Time) (map[string]windowStats, error) {
	var q string
	switch view {
	case "trades_takers_1m":
		q = fmt.Sprintf(windowQuery, "trades_takers_1m")
	case "trades_takers_1h":
		q = fmt.Sprintf(windowQuery, "trades_takers_1h")
	default:
		return nil, fmt.Errorf("unknown taker aggregate %q", view)
	}
	rows, err := db.Query(ctx, q, from, to)
	if err != nil {
		return nil, fmt.Errorf("querying %s: %w", view, err)
	}
	defer rows.Close()
	out := make(map[string]windowStats)
	for rows.Next() {
		var token types.AddressBytea
		var s windowStats
		if err := rows.Scan(&token, &s.w, &s.q, &s.wl, &s.wll, &s.takerW2, &s.lastAt); err != nil {
			return nil, fmt.Errorf("scanning %s: %w", view, err)
		}
		out[string(token)] = s
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating %s: %w", view, err)
	}
	return out, nil
}

// loadPrice24hAgo sets each token's reference price: the VWAP of the hour that starts 24 hours
// before the snapshot. A VWAP, unlike the hour's close, cannot be set by one late dust fill.
func loadPrice24hAgo(ctx context.Context, db *pgxpool.Pool, hourStart time.Time, snap *Snapshot) error {
	rows, err := db.Query(ctx, `
		SELECT base_token, usd_volume / base_volume
		FROM trades_1h WHERE bucket = $1 AND base_volume > 0`, hourStart)
	if err != nil {
		return fmt.Errorf("reading price 24h ago: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var token types.AddressBytea
		var price float64
		if err := rows.Scan(&token, &price); err != nil {
			return fmt.Errorf("scanning price 24h ago: %w", err)
		}
		if tp, ok := snap.Prices[string(token)]; ok && price > 0 {
			tp.Price24hAgoUSD = &price
			snap.Prices[string(token)] = tp
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterating price 24h ago: %w", err)
	}
	return nil
}

// loadAnchors serves the anchor tokens from the oracle whenever their fills do not publish a
// price, which for USDC is always. An oracle reading is publishable while it is fresh.
func loadAnchors(ctx context.Context, db *pgxpool.Pool, asOf time.Time, snap *Snapshot) error {
	rows, err := db.Query(ctx, `SELECT asset, price_usd, price_timestamp FROM oracle_prices`)
	if err != nil {
		return fmt.Errorf("reading oracle prices: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var asset types.AddressBytea
		var price float64
		var ts int64
		if err := rows.Scan(&asset, &price, &ts); err != nil {
			return fmt.Errorf("scanning oracle prices: %w", err)
		}
		existing, priced := snap.Prices[string(asset)]
		if (priced && existing.Publishable) || price <= 0 {
			continue
		}
		readAt := time.Unix(ts, 0)
		tp := TokenPrice{
			Token:       string(asset),
			PriceUSD:    price,
			Source:      PriceSourceOracle,
			Publishable: asOf.Sub(readAt) <= oracleMaxAge,
			LastTradeAt: readAt,
		}
		if priced {
			tp.Volume24hUSD, tp.Price24hAgoUSD = existing.Volume24hUSD, existing.Price24hAgoUSD
		}
		snap.Prices[string(asset)] = tp
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterating oracle prices: %w", err)
	}
	return nil
}
