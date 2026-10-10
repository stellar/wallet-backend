package prices

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
)

type fakeComparisonStore struct {
	mu      sync.Mutex
	samples []data.PriceComparison
	inserts int
	cutoff  time.Time
}

func (f *fakeComparisonStore) Insert(_ context.Context, s []data.PriceComparison) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.inserts++
	f.samples = append(f.samples, s...)
	return nil
}

func (f *fakeComparisonStore) DeleteOlderThan(_ context.Context, c time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cutoff = c
	return nil
}

type fakePriceSource struct {
	mu     sync.Mutex
	calls  []string
	prices map[string]*float64
	errs   map[string]error
	onCall func()
}

func (f *fakePriceSource) AssetPriceUSD(_ context.Context, token string) (*float64, error) {
	f.mu.Lock()
	f.calls = append(f.calls, token)
	f.mu.Unlock()
	if f.onCall != nil {
		f.onCall()
	}
	if err := f.errs[token]; err != nil {
		return nil, err
	}
	return f.prices[token], nil
}

func snapOf(vols map[string]float64) *Snapshot {
	s := &Snapshot{Prices: map[string]TokenPrice{}}
	for tok, v := range vols {
		s.Prices[tok] = TokenPrice{Token: tok, PriceUSD: 1, Volume24hUSD: v, Publishable: true}
	}
	return s
}

func newTestSampler(store *fakeComparisonStore, client *fakePriceSource, topN int) *ComparisonSampler {
	return NewComparisonSampler(ComparisonConfig{
		Store: store, Client: client, XLMSAC: "CXLM", Interval: time.Hour,
		TopN: topN, Spacing: time.Nanosecond, Retention: time.Hour,
	})
}

func TestComparisonSampler_TopNOrderingAndXLM(t *testing.T) {
	snap := snapOf(map[string]float64{"A": 10, "B": 50, "C": 30, "D": 20, "CXLM": 1})
	store, client := &fakeComparisonStore{}, &fakePriceSource{}
	s := newTestSampler(store, client, 3)
	now := time.Now()
	require.NoError(t, s.samplePass(context.Background(), snap, now))

	// The native SAC is asked for as XLM; everything else by its own id.
	assert.Equal(t, []string{"B", "C", "XLM"}, client.calls)
	require.Len(t, store.samples, 3)
	assert.Equal(t, 1, store.inserts)
	assert.Equal(t, now.Add(-time.Hour), store.cutoff)
	assert.Equal(t, now, store.samples[0].SampledAt)
}

func TestComparisonSampler_TopNWithoutXLM(t *testing.T) {
	snap := snapOf(map[string]float64{"A": 10, "B": 50, "C": 30})
	client := &fakePriceSource{}
	s := newTestSampler(&fakeComparisonStore{}, client, 2)
	require.NoError(t, s.samplePass(context.Background(), snap, time.Now()))
	assert.Equal(t, []string{"B", "C"}, client.calls)
}

func TestComparisonSampler_ClientErrorRecordsNil(t *testing.T) {
	snap := snapOf(map[string]float64{"A": 30, "B": 20, "C": 10})
	client := &fakePriceSource{
		errs:   map[string]error{"B": errors.New("boom")},
		prices: map[string]*float64{"A": ptr(2), "C": ptr(3)},
	}
	store := &fakeComparisonStore{}
	s := newTestSampler(store, client, 10)
	require.NoError(t, s.samplePass(context.Background(), snap, time.Now()))

	require.Len(t, store.samples, 3)
	assert.Equal(t, ptr(2), store.samples[0].Theirs)
	assert.Nil(t, store.samples[1].Theirs)
	require.NotNil(t, store.samples[1].Ours)
	assert.Equal(t, ptr(3), store.samples[2].Theirs)
}

func TestComparisonSampler_CancelMidPass(t *testing.T) {
	snap := snapOf(map[string]float64{"A": 30, "B": 20, "C": 10})
	ctx, cancel := context.WithCancel(context.Background())
	client := &fakePriceSource{onCall: cancel}
	store := &fakeComparisonStore{}
	s := NewComparisonSampler(ComparisonConfig{
		Store: store, Client: client, XLMSAC: "CXLM", Interval: time.Hour, Spacing: time.Hour,
	})
	s.cfg.Spacing = time.Nanosecond
	// First call cancels; the second wait must observe it. Use a long spacing after the first call.
	client.onCall = func() { s.cfg.Spacing = time.Hour; cancel() }

	done := make(chan error, 1)
	go func() { done <- s.samplePass(ctx, snap, time.Now()) }()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("samplePass did not return after cancel")
	}
	assert.Equal(t, 0, store.inserts)
	assert.Len(t, client.calls, 1)
}

func TestComparisonSampler_ClassicAssetsUseCodeIssuerIDs(t *testing.T) {
	store := &fakeComparisonStore{}
	client := &fakePriceSource{prices: map[string]*float64{"USDC-GISSUER": ptr(1)}}
	sampler := NewComparisonSampler(ComparisonConfig{
		Store: store, Client: client, XLMSAC: "CXLM", Interval: time.Hour, TopN: 10,
		Spacing: time.Nanosecond, Retention: time.Hour,
		ClassicAssets: func(context.Context) (map[string]string, error) {
			return map[string]string{"CUSDC": "USDC-GISSUER"}, nil
		},
	})
	snap := snapOf(map[string]float64{"CUSDC": 100, "CTOKEN": 50, "CXLM": 10})

	require.NoError(t, sampler.samplePass(context.Background(), snap, time.Now()))
	assert.Equal(t, []string{"USDC-GISSUER", "CTOKEN", "XLM"}, client.calls)
	require.Len(t, store.samples, 3)
	assert.Equal(t, "CUSDC", store.samples[0].Token, "samples keep our token id")
	require.NotNil(t, store.samples[0].Theirs)
}
