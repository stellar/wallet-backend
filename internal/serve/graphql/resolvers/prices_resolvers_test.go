package resolvers

import (
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/metrics"
	graphql1 "github.com/stellar/wallet-backend/internal/serve/graphql/generated"
	"github.com/stellar/wallet-backend/internal/services/prices"
)

func testContractAddress(t *testing.T, seed byte) string {
	t.Helper()
	raw := make([]byte, 32)
	for i := range raw {
		raw[i] = seed
	}
	addr, err := strkey.Encode(strkey.VersionByteContract, raw)
	require.NoError(t, err)
	return addr
}

func TestQueryResolver_TokenPrices(t *testing.T) {
	asOf := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	ago := 2.0
	good := testContractAddress(t, 1)
	thin := testContractAddress(t, 2)
	unknown := testContractAddress(t, 3)

	holder := prices.NewSnapshotHolder(nil, time.Second, nil)
	newResolver := func(h *prices.SnapshotHolder) *queryResolver {
		return &queryResolver{&Resolver{config: ResolverConfig{
			Prices:      h,
			PublishRule: prices.PublishRule{MinVolume24hUSD: 100, MaxStaleness: 168 * time.Hour},
		}}}
	}

	t.Run("nil snapshot returns nulls", func(t *testing.T) {
		got, err := newResolver(holder).TokenPrices(t.Context(), []string{good})
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, good, got[0].TokenID)
		assert.Nil(t, got[0].PriceUsd)
		assert.Nil(t, got[0].PercentChange24h)
		assert.Nil(t, got[0].Volume24hUsd)
	})

	t.Run("nil holder returns nulls", func(t *testing.T) {
		got, err := newResolver(nil).TokenPrices(t.Context(), []string{good})
		require.NoError(t, err)
		assert.Nil(t, got[0].PriceUsd)
	})

	holder.Set(&prices.Snapshot{AsOf: asOf, Prices: map[string]prices.TokenPrice{
		good: {Token: good, PriceUSD: 3, Source: prices.PriceSourceVWAP1H, Price24hAgoUSD: &ago, Volume24hUSD: 5000, LastTradeAt: asOf.Add(-time.Minute)},
		thin: {Token: thin, PriceUSD: 1, Source: prices.PriceSourceLastTrade, Volume24hUSD: 10, LastTradeAt: asOf.Add(-time.Minute)},
	}})

	t.Run("publishable token, input order, unknown token", func(t *testing.T) {
		got, err := newResolver(holder).TokenPrices(t.Context(), []string{unknown, good})
		require.NoError(t, err)
		require.Len(t, got, 2)
		assert.Equal(t, unknown, got[0].TokenID)
		assert.Nil(t, got[0].PriceUsd)
		assert.Nil(t, got[0].Volume24hUsd)

		require.NotNil(t, got[1].PriceUsd)
		assert.InDelta(t, 3.0, *got[1].PriceUsd, 1e-9)
		require.NotNil(t, got[1].PercentChange24h)
		assert.InDelta(t, 50.0, *got[1].PercentChange24h, 1e-9)
		require.NotNil(t, got[1].PriceSource)
		assert.Equal(t, graphql1.TokenPriceSourceVwap1h, *got[1].PriceSource)
		assert.InDelta(t, 5000.0, *got[1].Volume24hUsd, 1e-9)
		require.NotNil(t, got[1].LastTradeAt)
	})

	t.Run("below volume threshold keeps volume but nulls price", func(t *testing.T) {
		got, err := newResolver(holder).TokenPrices(t.Context(), []string{thin})
		require.NoError(t, err)
		assert.Nil(t, got[0].PriceUsd)
		assert.Nil(t, got[0].PercentChange24h)
		assert.Nil(t, got[0].PriceSource)
		require.NotNil(t, got[0].Volume24hUsd)
		assert.InDelta(t, 10.0, *got[0].Volume24hUsd, 1e-9)
		require.NotNil(t, got[0].LastTradeAt)
	})

	t.Run("invalid id", func(t *testing.T) {
		for _, id := range []string{"nope", "GAAZI4TCR3TY5OJHCTJC2A4QSY6CJWJH5IAJTGKIN2ER7LBNVKOCCWN7"} {
			_, err := newResolver(holder).TokenPrices(t.Context(), []string{good, id})
			requireBadUserInput(t, err)
		}
	})

	t.Run("empty and too many ids", func(t *testing.T) {
		_, err := newResolver(holder).TokenPrices(t.Context(), nil)
		requireBadUserInput(t, err)

		ids := make([]string, 201)
		for i := range ids {
			ids[i] = good
		}
		_, err = newResolver(holder).TokenPrices(t.Context(), ids)
		requireBadUserInput(t, err)

		_, err = newResolver(holder).TokenPrices(t.Context(), ids[:200])
		require.NoError(t, err)
	})
}

func TestQueryResolver_TokenPriceHistory_Validation(t *testing.T) {
	resolver := &queryResolver{&Resolver{}}
	token := testContractAddress(t, 1)
	from := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name       string
		token      string
		resolution graphql1.CandleResolution
		from, to   time.Time
	}{
		{"bad address", "nope", graphql1.CandleResolutionOneHour, from, from.Add(time.Hour)},
		{"from not before to", token, graphql1.CandleResolutionOneHour, from, from},
		{"from after to", token, graphql1.CandleResolutionOneHour, from.Add(time.Hour), from},
		{"1m span over 1 day", token, graphql1.CandleResolutionOneMinute, from, from.Add(25 * time.Hour)},
		{"1h span over 60 days", token, graphql1.CandleResolutionOneHour, from, from.Add(61 * 24 * time.Hour)},
		{"1d span over 5 years", token, graphql1.CandleResolutionOneDay, from, from.AddDate(5, 0, 2)},
		{"unknown resolution", token, graphql1.CandleResolution("WEEK"), from, from.Add(time.Hour)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := resolver.TokenPriceHistory(t.Context(), tc.token, tc.resolution, tc.from, tc.to)
			requireBadUserInput(t, err)
		})
	}
}

func TestQueryResolver_TokenPriceHistory_Candles(t *testing.T) {
	m := metrics.NewMetrics(prometheus.NewRegistry())
	resolver := &queryResolver{&Resolver{models: &data.Models{
		Trades: &data.TradesModel{DB: testDBConnectionPool, Metrics: m.DB},
	}}}
	token := testContractAddress(t, 9)
	counter := testContractAddress(t, 10)
	base := time.Now().UTC().Truncate(time.Hour).Add(-3 * time.Hour)

	t.Cleanup(func() {
		execTestDB(t, `DELETE FROM trades WHERE base_token = $1`, mustAddressBytes(t, token))
	})
	for i, fill := range []struct {
		offset time.Duration
		qty    float64
		usd    float64
	}{
		{0, 10, 20},
		{10 * time.Second, 10, 30},
		{2 * time.Minute, 5, 5},
	} {
		execTestDB(t, `INSERT INTO trades (ledger_created_at, operation_id, fill_index, ledger_number, base_token, counter_token,
			base_amount, counter_amount, base_qty, usd_value, venue) VALUES ($1, $2, 0, 1, $3, $4, 1, 1, $5, $6, 1)`,
			base.Add(fill.offset), int64(9_000_000+i), mustAddressBytes(t, token), mustAddressBytes(t, counter), fill.qty, fill.usd)
	}
	for _, view := range []string{"trades_1m", "trades_1h", "trades_1d"} {
		execTestDB(t, fmt.Sprintf(`CALL refresh_continuous_aggregate('%s', NULL, NULL)`, view))
	}

	to := base.Add(time.Hour)
	got, err := resolver.TokenPriceHistory(t.Context(), token, graphql1.CandleResolutionOneMinute, base, to)
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.True(t, got[0].Bucket.Before(got[1].Bucket))
	assert.Equal(t, int32(2), got[0].Trades)
	assert.InDelta(t, 2.0, got[0].Open, 1e-9)
	assert.InDelta(t, 3.0, got[0].High, 1e-9)
	assert.InDelta(t, 3.0, got[0].Close, 1e-9)
	assert.InDelta(t, 50.0, got[0].UsdVolume, 1e-9)
	assert.InDelta(t, 2.5, got[0].Vwap, 1e-9)

	hourly, err := resolver.TokenPriceHistory(t.Context(), token, graphql1.CandleResolutionOneHour, base, to)
	require.NoError(t, err)
	require.Len(t, hourly, 1)
	assert.Equal(t, int32(3), hourly[0].Trades)
	assert.InDelta(t, 55.0, hourly[0].UsdVolume, 1e-9)

	none, err := resolver.TokenPriceHistory(t.Context(), testContractAddress(t, 11), graphql1.CandleResolutionOneHour, base, to)
	require.NoError(t, err)
	assert.Empty(t, none)
}
