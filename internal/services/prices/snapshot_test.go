package prices

import (
	"context"
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

func seven() *int32 { d := int32(7); return &d }

func classicFill(opID int64, at time.Time, base string, baseAmt int64, counter string, counterAmt int64) types.Trade {
	return types.Trade{
		OperationID: opID, LedgerNumber: uint32(opID >> 32), LedgerClosed: at,
		BaseToken: base, BaseAmount: big.NewInt(baseAmt),
		CounterToken: counter, CounterAmount: big.NewInt(counterAmt),
		Venue: types.TradeVenueSDEXOrderbook, BaseDecimals: seven(), CounterDecimals: seven(),
	}
}

// TestSnapshot_EndToEnd proves the data path: enriched fills are copied, the last fill per token
// is recorded, the aggregates refresh, and LoadSnapshot derives spot, VWAP, 24h volume and change.
func TestSnapshot_EndToEnd(t *testing.T) {
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
	anchor := &Anchor{}
	anchor.Set(AnchorRates{XLMUSD: 0.2, USDCUSD: 1.0, AsOf: time.Now()})
	enricher := NewEnricher(venues, anchor, models.Contract)

	asOf := time.Date(2026, 10, 9, 12, 30, 0, 0, time.UTC)
	// Valid 24-hour windows need fills in the 1h bucket that starts 24h before asOf.
	dayAgo := asOf.Add(-24 * time.Hour).Truncate(time.Hour) // 2026-10-08 11:00
	const xlm = 10_000_000
	trades := []types.Trade{
		// T 24h ago: 1000 T for 400 XLM → $80 → 0.08/T
		classicFill(1<<32|1, dayAgo.Add(15*time.Minute), tokenT, 1000*xlm, venues.XLMSAC, 400*xlm),
		// U 3.5h ago: 10 U for 20 USDC → $20 → 2.0/U; no fill in the last hour
		classicFill(2<<32|1, asOf.Add(-210*time.Minute), tokenU, 10*xlm, venues.USDCSAC, 20*xlm),
		// T in the last hour: 1000 T for 500 XLM → $100 → 0.10/T, then 1000 T for 600 XLM → $120 → 0.12/T
		classicFill(3<<32|1, asOf.Add(-30*time.Minute), tokenT, 1000*xlm, venues.XLMSAC, 500*xlm),
		classicFill(4<<32|1, asOf.Add(-10*time.Minute), tokenT, 1000*xlm, venues.XLMSAC, 600*xlm),
		// a non-anchor pair stays unpriced in USD
		classicFill(5<<32|1, asOf.Add(-5*time.Minute), tokenU, 1*xlm, tokenT, 20*xlm),
	}

	last, err := enricher.Enrich(ctx, trades)
	require.NoError(t, err)
	require.Len(t, last, 2)
	require.Nil(t, trades[4].USDValue, "non-anchor counter is not priced")
	require.NotNil(t, trades[3].USDValue)
	assert.InDelta(t, 120, *trades[3].USDValue, 1e-9)
	assert.InDelta(t, 1000, *trades[3].BaseQty, 1e-9)

	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(ctx) }()
	require.NoError(t, models.Trades.BatchCopy(ctx, tx, trades))
	require.NoError(t, models.Trades.UpsertLastTrades(ctx, tx, last))
	require.NoError(t, tx.Commit(ctx))

	for _, view := range []string{"trades_1m", "trades_1h", "trades_1d"} {
		_, err = pool.Exec(ctx, "CALL refresh_continuous_aggregate($1, NULL, NULL)", view)
		require.NoError(t, err, view)
	}

	require.NoError(t, models.OraclePrices.Upsert(ctx, []data.OraclePrice{
		{Asset: venues.USDCSAC, PriceUSD: 1.0004, PriceTimestamp: asOf.Add(-5 * time.Minute).Unix()},
		{Asset: venues.XLMSAC, PriceUSD: 0.2, PriceTimestamp: asOf.Add(-5 * time.Minute).Unix()},
	}))

	snap, err := LoadSnapshot(ctx, pool, asOf)
	require.NoError(t, err)
	require.Len(t, snap.Prices, 4, "two traded tokens plus the two anchors from the oracle")

	usdc := snap.Prices[venues.USDCSAC]
	assert.Equal(t, PriceSourceOracle, usdc.Source, "USDC is always the counter, so it comes from the oracle")
	assert.InDelta(t, 1.0004, usdc.PriceUSD, 1e-9)
	assert.True(t, PublishRule{MinVolume24hUSD: 100, MaxStaleness: time.Hour}.Publishable(usdc, asOf), "oracle prices need no volume")
	assert.False(t, PublishRule{}.Publishable(usdc, asOf.Add(25*time.Hour)), "but must be fresh")

	tp := snap.Prices[tokenT]
	assert.Equal(t, PriceSourceVWAP1H, tp.Source)
	assert.InDelta(t, 0.11, tp.PriceUSD, 1e-9, "trailing-hour VWAP = (100+120)/(1000+1000)")
	assert.InDelta(t, 300, tp.Volume24hUSD, 1e-9, "24h volume includes the bucket 24h ago")
	require.NotNil(t, tp.Price24hAgoUSD)
	assert.InDelta(t, 0.08, *tp.Price24hAgoUSD, 1e-9)
	assert.InDelta(t, 37.5, *tp.PercentChange24h(), 1e-6)
	assert.Equal(t, asOf.Add(-10*time.Minute), tp.LastTradeAt.UTC())

	up := snap.Prices[tokenU]
	assert.Equal(t, PriceSourceLastTrade, up.Source, "no fill in the trailing hour falls back to the last fill")
	assert.InDelta(t, 2.0, up.PriceUSD, 1e-9)
	assert.InDelta(t, 20, up.Volume24hUSD, 1e-9)
	assert.Nil(t, up.Price24hAgoUSD)
	assert.Nil(t, up.PercentChange24h())

	rule := PublishRule{MinVolume24hUSD: 100, MaxStaleness: 7 * 24 * time.Hour}
	assert.True(t, rule.Publishable(tp, asOf))
	assert.False(t, rule.Publishable(up, asOf), "below the volume floor")
	assert.False(t, PublishRule{MinVolume24hUSD: 0, MaxStaleness: time.Minute}.Publishable(tp, asOf), "stale")

	// A replayed older fill never moves the last trade backwards.
	tx, err = pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(ctx) }()
	require.NoError(t, models.Trades.UpsertLastTrades(ctx, tx, []data.LastTrade{{Token: tokenT, PriceUSD: 0.01, LedgerCreatedAt: dayAgo, OperationID: 1<<32 | 1}}))
	require.NoError(t, tx.Commit(ctx))
	all, err := models.Trades.GetAllLastTrades(ctx)
	require.NoError(t, err)
	for _, l := range all {
		if l.Token == tokenT {
			assert.InDelta(t, 0.12, l.PriceUSD, 1e-9)
		}
	}
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

	last, err := enricher.Enrich(ctx, trades)
	require.NoError(t, err)
	require.NotNil(t, trades[0].BaseQty)
	assert.InDelta(t, 3, *trades[0].BaseQty, 1e-9)
	require.NotNil(t, trades[0].USDValue)
	assert.InDelta(t, 6, *trades[0].USDValue, 1e-9)
	assert.Nil(t, trades[1].BaseQty)
	assert.Nil(t, trades[1].USDValue)
	require.Len(t, last, 1)
	assert.Equal(t, tokenU, last[0].Token)
	assert.InDelta(t, 2, last[0].PriceUSD, 1e-9)

	// The native SAC has no contract_tokens row; an AMM fill against XLM is still priced.
	xlmFill := types.Trade{
		OperationID: 3, LedgerClosed: time.Now(), BaseToken: tokenU, BaseAmount: new(big.Int).Mul(big.NewInt(2), wei),
		CounterToken: venues.XLMSAC, CounterAmount: big.NewInt(100_000_000), Venue: types.TradeVenueAquarius,
	}
	trades = []types.Trade{xlmFill}
	_, err = enricher.Enrich(ctx, trades)
	require.NoError(t, err)
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
	_, err = enricher.Enrich(ctx, trades)
	require.NoError(t, err)
	assert.Nil(t, trades[0].BaseQty)
	assert.Nil(t, trades[0].USDValue)

	// No anchor yet: quantities resolve, USD does not.
	enricher = NewEnricher(venues, &Anchor{}, models.Contract)
	trades = []types.Trade{known}
	last, err = enricher.Enrich(ctx, trades)
	require.NoError(t, err)
	assert.Empty(t, last)
	require.NotNil(t, trades[0].BaseQty)
	assert.Nil(t, trades[0].USDValue)
}
