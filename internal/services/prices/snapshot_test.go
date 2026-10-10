package prices

import (
	"context"
	"math"
	"math/big"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/indexer/processors"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
)

var (
	tokenT = contractAddress(0x54)
	tokenU = contractAddress(0x55)
)

// contractAddress builds a valid C-address whose key bytes are all fill.
func contractAddress(fill byte) string {
	var raw [32]byte
	for i := range raw {
		raw[i] = fill
	}
	return strkey.MustEncode(strkey.VersionByteContract, raw[:])
}

// accountAddress builds a valid G-address whose key bytes are all fill.
func accountAddress(fill byte) string {
	var raw [32]byte
	for i := range raw {
		raw[i] = fill
	}
	return strkey.MustEncode(strkey.VersionByteAccountID, raw[:])
}

// pricedFill is a fill whose quantity and USD value are already known, so a test can state the
// price evidence directly: qty units of base for usd dollars, taken by taker at at.
func pricedFill(opID int64, at time.Time, base, taker string, qty, usd float64, counter string) types.Trade {
	return types.Trade{
		OperationID: opID, LedgerNumber: uint32(opID >> 32), LedgerClosed: at, Taker: taker,
		BaseToken: base, BaseAmount: big.NewInt(1), CounterToken: counter, CounterAmount: big.NewInt(1),
		Venue: types.TradeVenueSDEXOrderbook, BaseQty: &qty, USDValue: &usd,
	}
}

// expectedStats computes VWAP, effective takers and relative error straight from fills, as an
// independent reference for the aggregate-based loader.
func expectedStats(fills []types.Trade) (vwap, neff, rse float64) {
	var w, q, wl, wll float64
	perTaker := map[string]float64{}
	for _, f := range fills {
		lp := math.Log(*f.USDValue / *f.BaseQty)
		w += *f.USDValue
		q += *f.BaseQty
		wl += *f.USDValue * lp
		wll += *f.USDValue * lp * lp
		perTaker[f.Taker] += *f.USDValue
	}
	var w2 float64
	for _, v := range perTaker {
		w2 += v * v
	}
	neff = w * w / w2
	mean := wl / w
	return w / q, neff, math.Sqrt(math.Max(wll/w-mean*mean, 0) / neff)
}

// TestLoadSnapshot_ConfidenceRule drives the publish rule end to end: fills are copied, every
// aggregate refreshes, and LoadSnapshot picks a window, or none, for each token.
func TestLoadSnapshot_ConfidenceRule(t *testing.T) {
	ctx := context.Background()
	dbt := dbtest.Open(t)
	defer dbt.Close()
	pool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer pool.Close()
	models, err := data.NewModels(pool, metrics.NewMetrics(prometheus.NewRegistry()).DB)
	require.NoError(t, err)
	venues, err := processors.TradeVenuesFor(network.PublicNetworkPassphrase)
	require.NoError(t, err)

	asOf := time.Date(2026, 10, 9, 12, 30, 0, 0, time.UTC)
	hourAgo24 := asOf.Add(-24 * time.Hour).Truncate(time.Hour) // the reference hour for the 24h change
	k1, k2, k3 := accountAddress(1), accountAddress(2), accountAddress(3)
	agree, bot, dayOnly, spread := contractAddress(0x61), contractAddress(0x62), contractAddress(0x63), contractAddress(0x64)
	usdc := venues.USDCSAC

	var opID int64
	next := func() int64 { opID++; return opID<<32 | 1 }
	agreeHour := []types.Trade{
		pricedFill(next(), asOf.Add(-40*time.Minute), agree, k1, 100, 10.0, usdc),
		pricedFill(next(), asOf.Add(-30*time.Minute), agree, k2, 100, 10.1, usdc),
		pricedFill(next(), asOf.Add(-20*time.Minute), agree, k3, 100, 9.95, usdc),
	}
	fills := append([]types.Trade{
		// The hour 24h ago sets the reference for agree's 24h change.
		pricedFill(next(), hourAgo24.Add(10*time.Minute), agree, k1, 100, 8, usdc),
	}, agreeHour...)
	// One account trading 50 times is one observation.
	for i := 0; i < 50; i++ {
		fills = append(fills, pricedFill(next(), asOf.Add(-time.Duration(50-i)*time.Minute), bot, k1, 10, 1.0+0.001*float64(i%3), usdc))
	}
	// Three takers 3-20 hours ago, nothing in the trailing hour.
	dayOnlyFills := []types.Trade{
		pricedFill(next(), asOf.Add(-20*time.Hour), dayOnly, k1, 50, 100, usdc),
		pricedFill(next(), asOf.Add(-10*time.Hour), dayOnly, k2, 50, 101, usdc),
		pricedFill(next(), asOf.Add(-3*time.Hour), dayOnly, k3, 50, 99, usdc),
	}
	fills = append(fills, dayOnlyFills...)
	// Three equal-weight takers, one 30% above the others: enough takers for an error estimate,
	// and the estimate is above 5%.
	fills = append(fills,
		pricedFill(next(), asOf.Add(-25*time.Minute), spread, k1, 100, 10, usdc),
		pricedFill(next(), asOf.Add(-15*time.Minute), spread, k2, 100/1.3, 10, usdc),
		pricedFill(next(), asOf.Add(-5*time.Minute), spread, k3, 100, 10, usdc),
	)

	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(ctx) }()
	require.NoError(t, models.Trades.BatchCopy(ctx, tx, fills))
	require.NoError(t, tx.Commit(ctx))
	for _, view := range []string{"trades_1m", "trades_1h", "trades_1d", "trades_takers_1m", "trades_takers_1h"} {
		_, err = pool.Exec(ctx, "CALL refresh_continuous_aggregate($1, NULL, NULL)", view)
		require.NoError(t, err, view)
	}
	require.NoError(t, models.OraclePrices.Upsert(ctx, []data.OraclePrice{
		{Asset: usdc, PriceUSD: 1.0004, PriceTimestamp: asOf.Add(-5 * time.Minute).Unix()},
		{Asset: venues.XLMSAC, PriceUSD: 0.2, PriceTimestamp: asOf.Add(-5 * time.Minute).Unix()},
	}))

	snap, err := LoadSnapshot(ctx, pool, asOf, DefaultMaxError)
	require.NoError(t, err)

	t.Run("takers who agree publish the trailing hour", func(t *testing.T) {
		tp := snap.Prices[agree]
		vwap, neff, rse := expectedStats(agreeHour)
		assert.True(t, tp.Publishable)
		assert.Equal(t, Window1H, tp.Window)
		assert.Equal(t, PriceSourceVWAP1H, tp.Source)
		assert.InDelta(t, vwap, tp.PriceUSD, 1e-9)
		assert.InDelta(t, neff, tp.EffectiveTakers, 1e-9)
		require.NotNil(t, tp.ErrorPct)
		assert.InDelta(t, rse*100, *tp.ErrorPct, 1e-9)
		require.NotNil(t, tp.Price24hAgoUSD)
		assert.InDelta(t, 0.08, *tp.Price24hAgoUSD, 1e-9, "the VWAP of the hour 24h ago")
		assert.InDelta(t, (vwap/0.08-1)*100, *tp.PercentChange24h(), 1e-9)
		assert.Equal(t, asOf.Add(-20*time.Minute), tp.LastTradeAt.UTC())
	})

	t.Run("one account trading many times is not published", func(t *testing.T) {
		tp := snap.Prices[bot]
		assert.False(t, tp.Publishable)
		assert.InDelta(t, 1, tp.EffectiveTakers, 1e-9)
		assert.Nil(t, tp.ErrorPct, "no error estimate below two effective takers")
		assert.InDelta(t, 50*1.001, tp.Volume24hUSD, 0.05, "volume is still reported")
	})

	t.Run("takers only earlier in the day publish the 24h window", func(t *testing.T) {
		tp := snap.Prices[dayOnly]
		vwap, neff, rse := expectedStats(dayOnlyFills)
		assert.True(t, tp.Publishable)
		assert.Equal(t, Window24H, tp.Window)
		assert.Equal(t, PriceSourceVWAP24H, tp.Source)
		assert.InDelta(t, vwap, tp.PriceUSD, 1e-9)
		assert.InDelta(t, neff, tp.EffectiveTakers, 1e-9)
		require.NotNil(t, tp.ErrorPct)
		assert.InDelta(t, rse*100, *tp.ErrorPct, 1e-9)
	})

	t.Run("takers 30% apart are not published", func(t *testing.T) {
		tp := snap.Prices[spread]
		assert.False(t, tp.Publishable)
		assert.InDelta(t, 3, tp.EffectiveTakers, 1e-9)
		require.NotNil(t, tp.ErrorPct)
		assert.Greater(t, *tp.ErrorPct, 5.0)
	})

	t.Run("anchors without fills come from the oracle while it is fresh", func(t *testing.T) {
		tp := snap.Prices[usdc]
		assert.True(t, tp.Publishable)
		assert.Equal(t, PriceSourceOracle, tp.Source)
		assert.InDelta(t, 1.0004, tp.PriceUSD, 1e-9)

		later, err := LoadSnapshot(ctx, pool, asOf.Add(25*time.Hour), DefaultMaxError)
		require.NoError(t, err)
		assert.False(t, later.Prices[usdc].Publishable, "a reading older than 24h is not served")
	})
}

// TestEnricher_ResolvesDecimalsFromContractTokens covers AMM fills, whose decimals come from
// contract_tokens; a token without a row is persisted without quantities.
func TestEnricher_ResolvesDecimalsFromContractTokens(t *testing.T) {
	ctx := context.Background()
	dbt := dbtest.Open(t)
	defer dbt.Close()
	pool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer pool.Close()
	models, err := data.NewModels(pool, metrics.NewMetrics(prometheus.NewRegistry()).DB)
	require.NoError(t, err)
	venues, err := processors.TradeVenuesFor(network.PublicNetworkPassphrase)
	require.NoError(t, err)

	_, err = pool.Exec(ctx, `INSERT INTO contract_tokens (id, contract_id, type, decimals, code, issuer)
		VALUES ($1, $2, 'SEP41', 18, NULL, NULL), ($3, $4, 'SAC', 7, 'USDC', 'GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN')`,
		data.DeterministicContractID(tokenU), tokenU, data.DeterministicContractID(venues.USDCSAC), venues.USDCSAC)
	require.NoError(t, err)

	anchor := &Anchor{}
	anchor.Set(AnchorRates{XLMUSD: 0.2, USDCUSD: 1.0, AsOf: time.Now()})
	enricher := NewEnricher(venues, anchor, models.Contract)

	wei := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
	known := types.Trade{
		OperationID: 1, LedgerClosed: time.Now(), BaseToken: tokenU, BaseAmount: new(big.Int).Mul(big.NewInt(3), wei),
		CounterToken: venues.USDCSAC, CounterAmount: big.NewInt(60_000_000), Venue: types.TradeVenueSoroswap,
	}
	unknown := types.Trade{
		OperationID: 2, LedgerClosed: time.Now(), BaseToken: tokenT, BaseAmount: big.NewInt(5),
		CounterToken: venues.USDCSAC, CounterAmount: big.NewInt(60_000_000), Venue: types.TradeVenueSoroswap,
	}
	trades := []types.Trade{known, unknown}

	require.NoError(t, enricher.Enrich(ctx, trades))
	require.NotNil(t, trades[0].BaseQty)
	assert.InDelta(t, 3, *trades[0].BaseQty, 1e-9)
	require.NotNil(t, trades[0].USDValue)
	assert.InDelta(t, 6, *trades[0].USDValue, 1e-9)
	assert.Nil(t, trades[1].BaseQty)
	assert.Nil(t, trades[1].USDValue)

	// The native SAC has no contract_tokens row; an AMM fill against XLM is still priced.
	xlmFill := types.Trade{
		OperationID: 3, LedgerClosed: time.Now(), BaseToken: tokenU, BaseAmount: new(big.Int).Mul(big.NewInt(2), wei),
		CounterToken: venues.XLMSAC, CounterAmount: big.NewInt(100_000_000), Venue: types.TradeVenueAquarius,
	}
	trades = []types.Trade{xlmFill}
	require.NoError(t, enricher.Enrich(ctx, trades))
	require.NotNil(t, trades[0].USDValue, "XLM decimals are known without a lookup")
	assert.InDelta(t, 2, *trades[0].USDValue, 1e-9, "10 XLM at $0.2")

	// An UNKNOWN row's placeholder decimals are never used.
	tokenV := contractAddress(0x56)
	_, err = pool.Exec(ctx, `INSERT INTO contract_tokens (id, contract_id, type, decimals) VALUES ($1, $2, 'UNKNOWN', 0)`,
		data.DeterministicContractID(tokenV), tokenV)
	require.NoError(t, err)
	trades = []types.Trade{{
		OperationID: 4, LedgerClosed: time.Now(), BaseToken: tokenV, BaseAmount: big.NewInt(5),
		CounterToken: venues.USDCSAC, CounterAmount: big.NewInt(60_000_000), Venue: types.TradeVenueSoroswap,
	}}
	require.NoError(t, enricher.Enrich(ctx, trades))
	assert.Nil(t, trades[0].BaseQty)
	assert.Nil(t, trades[0].USDValue)

	// No anchor yet: quantities resolve, USD does not.
	enricher = NewEnricher(venues, &Anchor{}, models.Contract)
	trades = []types.Trade{known}
	require.NoError(t, enricher.Enrich(ctx, trades))
	require.NotNil(t, trades[0].BaseQty)
	assert.Nil(t, trades[0].USDValue)
}
