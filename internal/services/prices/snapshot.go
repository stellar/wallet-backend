package prices

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/stellar/wallet-backend/internal/indexer/types"
)

// PriceSource says which number a TokenPrice's PriceUSD is.
type PriceSource int16

const (
	PriceSourceVWAP1H    PriceSource = 1
	PriceSourceLastTrade PriceSource = 2
	// PriceSourceOracle is the anchor oracle's own reading, used for the anchor tokens the fills
	// never price: an anchor is always the counter side of a fill, so it has no base rows.
	PriceSourceOracle PriceSource = 3
)

// oracleMaxAge is how old an oracle reading may be and still be served as a price; the oracle
// poller already refuses older readings, so this is a second guard on the read path.
const oracleMaxAge = 24 * time.Hour

// TokenPrice is one token's spot price with the windowed statistics the publish rule needs.
// PriceUSD is the trailing-hour VWAP when the token traded in that hour, else its last fill.
type TokenPrice struct {
	Token          string
	PriceUSD       float64
	Source         PriceSource
	Price24hAgoUSD *float64
	Volume24hUSD   float64
	LastTradeAt    time.Time
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

// LoadSnapshot assembles the snapshot from four bounded reads: the last fill per token, the
// trailing-hour VWAP from the real-time 1-minute aggregate, the 24-hour statistics from the
// real-time 1-hour aggregate, and the oracle readings for the anchor tokens the fills never
// price. Nothing here depends on how long ago a token last traded.
func LoadSnapshot(ctx context.Context, db *pgxpool.Pool, asOf time.Time) (*Snapshot, error) {
	snap := &Snapshot{AsOf: asOf, Prices: make(map[string]TokenPrice)}

	rows, err := db.Query(ctx, `SELECT token, price_usd, ledger_created_at FROM token_last_trades`)
	if err != nil {
		return nil, fmt.Errorf("reading last trades: %w", err)
	}
	for rows.Next() {
		var token types.AddressBytea
		var tp TokenPrice
		if err := rows.Scan(&token, &tp.PriceUSD, &tp.LastTradeAt); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scanning last trades: %w", err)
		}
		tp.Token = string(token)
		tp.Source = PriceSourceLastTrade
		snap.Prices[tp.Token] = tp
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating last trades: %w", err)
	}

	rows, err = db.Query(ctx, `
		SELECT base_token, sum(usd_volume) / sum(base_volume)
		FROM trades_1m
		WHERE bucket >= $1::timestamptz - INTERVAL '1 hour' AND bucket < $1::timestamptz
		GROUP BY base_token
		HAVING sum(base_volume) > 0`, asOf)
	if err != nil {
		return nil, fmt.Errorf("reading 1h VWAP: %w", err)
	}
	for rows.Next() {
		var token types.AddressBytea
		var vwap float64
		if err := rows.Scan(&token, &vwap); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scanning 1h VWAP: %w", err)
		}
		if tp, ok := snap.Prices[string(token)]; ok && vwap > 0 {
			tp.PriceUSD, tp.Source = vwap, PriceSourceVWAP1H
			snap.Prices[string(token)] = tp
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating 1h VWAP: %w", err)
	}

	rows, err = db.Query(ctx, `
		SELECT base_token,
		       sum(usd_volume),
		       max(close) FILTER (WHERE bucket = time_bucket('1 hour', $1::timestamptz - INTERVAL '24 hours'))
		FROM trades_1h
		WHERE bucket >= time_bucket('1 hour', $1::timestamptz - INTERVAL '24 hours') AND bucket < $1::timestamptz
		GROUP BY base_token`, asOf)
	if err != nil {
		return nil, fmt.Errorf("reading 24h stats: %w", err)
	}
	for rows.Next() {
		var token types.AddressBytea
		var volume, ago *float64
		if err := rows.Scan(&token, &volume, &ago); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scanning 24h stats: %w", err)
		}
		if tp, ok := snap.Prices[string(token)]; ok {
			if volume != nil {
				tp.Volume24hUSD = *volume
			}
			tp.Price24hAgoUSD = ago
			snap.Prices[string(token)] = tp
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating 24h stats: %w", err)
	}

	rows, err = db.Query(ctx, `SELECT asset, price_usd, price_timestamp FROM oracle_prices`)
	if err != nil {
		return nil, fmt.Errorf("reading oracle prices: %w", err)
	}
	for rows.Next() {
		var asset types.AddressBytea
		var price float64
		var ts int64
		if err := rows.Scan(&asset, &price, &ts); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scanning oracle prices: %w", err)
		}
		if _, priced := snap.Prices[string(asset)]; priced || price <= 0 {
			continue
		}
		snap.Prices[string(asset)] = TokenPrice{
			Token:       string(asset),
			PriceUSD:    price,
			Source:      PriceSourceOracle,
			LastTradeAt: time.Unix(ts, 0),
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating oracle prices: %w", err)
	}
	return snap, nil
}

// PublishRule decides whether a token's price is served.
type PublishRule struct {
	MinVolume24hUSD float64
	MaxStaleness    time.Duration
}

// Publishable reports whether tp passes the rule as of asOf. An oracle-sourced price has no
// fill volume to judge; it is served while the reading is fresh.
func (r PublishRule) Publishable(tp TokenPrice, asOf time.Time) bool {
	if tp.PriceUSD <= 0 {
		return false
	}
	if tp.Source == PriceSourceOracle {
		return asOf.Sub(tp.LastTradeAt) <= oracleMaxAge
	}
	return tp.Volume24hUSD >= r.MinVolume24hUSD && asOf.Sub(tp.LastTradeAt) <= r.MaxStaleness
}
