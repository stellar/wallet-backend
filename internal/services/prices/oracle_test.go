package prices

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
)

const (
	testOracleID = "CAFJZQWSED6YAWZU3GWRTOCNPPCGBN32L7QV43XX5LZLFTK6JLN34DLN"
	testXLMSAC   = "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA"
	testUSDCSAC  = "CCW67TSZV3SSS2HXMBQ5JFGCKJNXKZM7UQUWUZPUTHXSTZLEO7SJMI75"
)

var testNow = time.Unix(1_760_000_000, 0)

// fakeFetcher answers base(), decimals() and lastprice(Other(sym)) from fixed results and counts calls.
type fakeFetcher struct {
	mu      sync.Mutex
	results map[string]fetchResult
	calls   map[string]int
}

type fetchResult struct {
	val xdr.ScVal
	err error
}

func newFakeFetcher(results map[string]fetchResult) *fakeFetcher {
	return &fakeFetcher{results: results, calls: map[string]int{}}
}

func (f *fakeFetcher) FetchSingleField(_ context.Context, contract, fn string, args ...xdr.ScVal) (xdr.ScVal, error) {
	if contract != testOracleID {
		return xdr.ScVal{}, fmt.Errorf("unexpected contract %s", contract)
	}
	key := fn
	if fn == "lastprice" {
		if len(args) != 1 {
			return xdr.ScVal{}, fmt.Errorf("lastprice: want 1 arg, got %d", len(args))
		}
		sym, ok := otherAssetSymbol(args[0])
		if !ok {
			return xdr.ScVal{}, errors.New("lastprice: arg is not Asset::Other")
		}
		key = fn + ":" + sym
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls[key]++
	r, ok := f.results[key]
	if !ok {
		return xdr.ScVal{}, fmt.Errorf("no result for %s", key)
	}
	return r.val, r.err
}

func (f *fakeFetcher) count(key string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[key]
}

type fakeStore struct {
	mu        sync.Mutex
	rows      []data.OraclePrice
	getErr    error
	upsertErr error
	upserts   [][]data.OraclePrice
}

func (s *fakeStore) Upsert(_ context.Context, prices []data.OraclePrice) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.upserts = append(s.upserts, prices)
	return s.upsertErr
}

func (s *fakeStore) GetAll(context.Context) ([]data.OraclePrice, error) {
	return s.rows, s.getErr
}

func symVal(s string) xdr.ScVal {
	sym := xdr.ScSymbol(s)
	return xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym}
}

func vecVal(vals ...xdr.ScVal) xdr.ScVal {
	vec := xdr.ScVec(vals)
	p := &vec
	return xdr.ScVal{Type: xdr.ScValTypeScvVec, Vec: &p}
}

func u32ScVal(v uint32) xdr.ScVal {
	u := xdr.Uint32(v)
	return xdr.ScVal{Type: xdr.ScValTypeScvU32, U32: &u}
}

func voidVal() xdr.ScVal { return xdr.ScVal{Type: xdr.ScValTypeScvVoid} }

func priceDataVal(price int64, ts int64) xdr.ScVal {
	hi := xdr.Int64(0)
	if price < 0 {
		hi = -1
	}
	i128 := xdr.ScVal{Type: xdr.ScValTypeScvI128, I128: &xdr.Int128Parts{Hi: hi, Lo: xdr.Uint64(uint64(price))}}
	u := xdr.Uint64(uint64(ts))
	tsVal := xdr.ScVal{Type: xdr.ScValTypeScvU64, U64: &u}
	m := xdr.ScMap{
		{Key: symVal("price"), Val: i128},
		{Key: symVal("timestamp"), Val: tsVal},
	}
	pm := &m
	return xdr.ScVal{Type: xdr.ScValTypeScvMap, Map: &pm}
}

// healthy returns results for a USD-based, 14-decimal oracle quoting XLM at 0.12 and USDC at 0.9998.
func healthy() map[string]fetchResult {
	return map[string]fetchResult{
		"base":           {val: vecVal(symVal("Other"), symVal("USD"))},
		"decimals":       {val: u32ScVal(14)},
		"lastprice:XLM":  {val: priceDataVal(12_000_000_000_000, testNow.Unix()-60)},
		"lastprice:USDC": {val: priceDataVal(99_980_000_000_000, testNow.Unix()-30)},
	}
}

func newTestPoller(t *testing.T, f ContractFieldFetcher, s OraclePriceStore, a *Anchor) *OraclePoller {
	t.Helper()
	p, err := NewOraclePoller(OracleConfig{
		OracleContractID: testOracleID,
		Metadata:         f,
		Interval:         time.Hour,
		Anchor:           a,
		OraclePrices:     s,
		XLMSAC:           testXLMSAC,
		USDCSAC:          testUSDCSAC,
	})
	require.NoError(t, err)
	p.now = func() time.Time { return testNow }
	return p
}

func TestOraclePoller_PollOnce(t *testing.T) {
	previous := AnchorRates{XLMUSD: 0.1, USDCUSD: 1, AsOf: testNow.Add(-time.Hour)}
	stale := testNow.Add(-MaxOraclePriceAge - time.Second).Unix()

	tests := []struct {
		name         string
		override     map[string]fetchResult
		wantErr      string
		wantRates    AnchorRates
		wantUpsert   bool
		wantDecimals bool
	}{
		{
			name:         "both readings usable sets anchor and stores rows",
			wantRates:    AnchorRates{XLMUSD: 0.12, USDCUSD: 0.9998, AsOf: time.Unix(testNow.Unix()-60, 0)},
			wantUpsert:   true,
			wantDecimals: true,
		},
		{
			name:      "non-USD base is refused",
			override:  map[string]fetchResult{"base": {val: vecVal(symVal("Other"), symVal("EUR"))}},
			wantErr:   `base asset is not Other("USD")`,
			wantRates: previous,
		},
		{
			name:      "Stellar base is refused",
			override:  map[string]fetchResult{"base": {val: vecVal(symVal("Stellar"), symVal("USD"))}},
			wantErr:   `base asset is not Other("USD")`,
			wantRates: previous,
		},
		{
			name:         "stale XLM reading keeps previous anchor",
			override:     map[string]fetchResult{"lastprice:XLM": {val: priceDataVal(12_000_000_000_000, stale)}},
			wantRates:    previous,
			wantDecimals: true,
		},
		{
			name:         "zero USDC price keeps previous anchor",
			override:     map[string]fetchResult{"lastprice:USDC": {val: priceDataVal(0, testNow.Unix())}},
			wantRates:    previous,
			wantDecimals: true,
		},
		{
			name:         "negative USDC price keeps previous anchor",
			override:     map[string]fetchResult{"lastprice:USDC": {val: priceDataVal(-5, testNow.Unix())}},
			wantRates:    previous,
			wantDecimals: true,
		},
		{
			name:         "missing XLM price keeps previous anchor",
			override:     map[string]fetchResult{"lastprice:XLM": {val: voidVal()}},
			wantRates:    previous,
			wantDecimals: true,
		},
		{
			name:         "XLM fetch error still reads USDC and keeps previous anchor",
			override:     map[string]fetchResult{"lastprice:XLM": {err: errors.New("rpc down")}},
			wantErr:      "rpc down",
			wantRates:    previous,
			wantDecimals: true,
		},
		{
			name:         "malformed USDC value is an error",
			override:     map[string]fetchResult{"lastprice:USDC": {val: u32ScVal(1)}},
			wantErr:      "decoding lastprice(USDC)",
			wantRates:    previous,
			wantDecimals: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			results := healthy()
			for k, v := range tc.override {
				results[k] = v
			}
			f := newFakeFetcher(results)
			s := &fakeStore{}
			a := &Anchor{}
			a.Set(previous)
			p := newTestPoller(t, f, s, a)

			err := p.pollOnce(context.Background())
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}

			got, ok := a.Get()
			require.True(t, ok)
			assert.InDelta(t, tc.wantRates.XLMUSD, got.XLMUSD, 1e-12)
			assert.InDelta(t, tc.wantRates.USDCUSD, got.USDCUSD, 1e-12)
			assert.True(t, tc.wantRates.AsOf.Equal(got.AsOf), "AsOf: want %s, got %s", tc.wantRates.AsOf, got.AsOf)

			if tc.wantUpsert {
				require.Len(t, s.upserts, 1)
				rows := s.upserts[0]
				require.Len(t, rows, 2)
				assert.Equal(t, testXLMSAC, rows[0].Asset)
				assert.InDelta(t, 0.12, rows[0].PriceUSD, 1e-12)
				assert.Equal(t, testNow.Unix()-60, rows[0].PriceTimestamp)
				assert.Equal(t, testUSDCSAC, rows[1].Asset)
				assert.InDelta(t, 0.9998, rows[1].PriceUSD, 1e-12)
				assert.Equal(t, testNow.Unix()-30, rows[1].PriceTimestamp)
			} else {
				assert.Empty(t, s.upserts)
			}

			assert.Equal(t, tc.wantDecimals, p.ready)
			if tc.wantDecimals {
				// One lastprice failing never skips the other.
				assert.Equal(t, 1, f.count("lastprice:XLM"))
				assert.Equal(t, 1, f.count("lastprice:USDC"))
			} else {
				assert.Zero(t, f.count("decimals"))
				assert.Zero(t, f.count("lastprice:XLM"))
			}
		})
	}
}

func TestOraclePoller_ChecksOracleOnce(t *testing.T) {
	f := newFakeFetcher(healthy())
	p := newTestPoller(t, f, &fakeStore{}, &Anchor{})

	require.NoError(t, p.pollOnce(context.Background()))
	require.NoError(t, p.pollOnce(context.Background()))

	assert.Equal(t, 1, f.count("base"))
	assert.Equal(t, 1, f.count("decimals"))
	assert.Equal(t, 2, f.count("lastprice:XLM"))
}

func TestOraclePoller_RetriesRefusedBase(t *testing.T) {
	results := healthy()
	results["base"] = fetchResult{err: errors.New("rpc down")}
	f := newFakeFetcher(results)
	a := &Anchor{}
	p := newTestPoller(t, f, &fakeStore{}, a)

	require.Error(t, p.pollOnce(context.Background()))
	_, ok := a.Get()
	require.False(t, ok)

	f.results["base"] = healthy()["base"]
	require.NoError(t, p.pollOnce(context.Background()))
	_, ok = a.Get()
	assert.True(t, ok)
	assert.Equal(t, 2, f.count("base"))
}

func TestOraclePoller_UpsertErrorStillSetsAnchor(t *testing.T) {
	s := &fakeStore{upsertErr: errors.New("db down")}
	a := &Anchor{}
	p := newTestPoller(t, newFakeFetcher(healthy()), s, a)

	require.ErrorContains(t, p.pollOnce(context.Background()), "db down")
	got, ok := a.Get()
	require.True(t, ok)
	assert.InDelta(t, 0.12, got.XLMUSD, 1e-12)
}

func TestOraclePoller_SeedFromStore(t *testing.T) {
	fresh := testNow.Unix() - 600
	stale := testNow.Add(-MaxOraclePriceAge - time.Second).Unix()

	tests := []struct {
		name     string
		rows     []data.OraclePrice
		getErr   error
		wantSeed bool
	}{
		{
			name: "both fresh rows seed the anchor",
			rows: []data.OraclePrice{
				{Asset: testXLMSAC, PriceUSD: 0.11, PriceTimestamp: fresh},
				{Asset: testUSDCSAC, PriceUSD: 1.0, PriceTimestamp: fresh + 5},
				{Asset: "COTHER", PriceUSD: 3, PriceTimestamp: fresh},
			},
			wantSeed: true,
		},
		{
			name:     "missing USDC row leaves anchor unset",
			rows:     []data.OraclePrice{{Asset: testXLMSAC, PriceUSD: 0.11, PriceTimestamp: fresh}},
			wantSeed: false,
		},
		{
			name: "stale row leaves anchor unset",
			rows: []data.OraclePrice{
				{Asset: testXLMSAC, PriceUSD: 0.11, PriceTimestamp: stale},
				{Asset: testUSDCSAC, PriceUSD: 1.0, PriceTimestamp: fresh},
			},
			wantSeed: false,
		},
		{
			name:     "store error leaves anchor unset",
			getErr:   errors.New("db down"),
			wantSeed: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a := &Anchor{}
			p := newTestPoller(t, newFakeFetcher(nil), &fakeStore{rows: tc.rows, getErr: tc.getErr}, a)

			p.seedFromStore(context.Background())

			got, ok := a.Get()
			require.Equal(t, tc.wantSeed, ok)
			if tc.wantSeed {
				assert.InDelta(t, 0.11, got.XLMUSD, 1e-12)
				assert.InDelta(t, 1.0, got.USDCUSD, 1e-12)
				assert.Equal(t, fresh, got.AsOf.Unix())
			}
		})
	}
}

func TestOraclePoller_RunPollsAtOnceAndStopsOnCancel(t *testing.T) {
	a := &Anchor{}
	p := newTestPoller(t, newFakeFetcher(healthy()), &fakeStore{}, a)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		p.Run(ctx)
		close(done)
	}()

	require.Eventually(t, func() bool {
		_, ok := a.Get()
		return ok
	}, 2*time.Second, 5*time.Millisecond, "first pass should run at start")

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

func TestOraclePriceFloat(t *testing.T) {
	// Hi=1, Lo=0 is 2^64, beyond int64.
	big2to64 := int128ToBigInt(xdr.Int128Parts{Hi: 1, Lo: 0})
	assert.Equal(t, "18446744073709551616", big2to64.String())
	assert.InDelta(t, 184467.44073709551616, oraclePriceFloat(big2to64, 14), 1e-9)
	assert.InDelta(t, 0.12, oraclePriceFloat(big.NewInt(12_000_000_000_000), 14), 1e-15)
	assert.InDelta(t, 7.0, oraclePriceFloat(big.NewInt(7), 0), 0)
}

func TestNewOraclePoller_Validation(t *testing.T) {
	valid := OracleConfig{
		OracleContractID: testOracleID,
		Metadata:         newFakeFetcher(nil),
		Interval:         time.Minute,
		Anchor:           &Anchor{},
		OraclePrices:     &fakeStore{},
		XLMSAC:           testXLMSAC,
		USDCSAC:          testUSDCSAC,
	}
	_, err := NewOraclePoller(valid)
	require.NoError(t, err)

	for name, mutate := range map[string]func(*OracleConfig){
		"no oracle":   func(c *OracleConfig) { c.OracleContractID = "" },
		"no metadata": func(c *OracleConfig) { c.Metadata = nil },
		"no interval": func(c *OracleConfig) { c.Interval = 0 },
		"no anchor":   func(c *OracleConfig) { c.Anchor = nil },
		"no store":    func(c *OracleConfig) { c.OraclePrices = nil },
		"no USDC SAC": func(c *OracleConfig) { c.USDCSAC = "" },
	} {
		t.Run(name, func(t *testing.T) {
			cfg := valid
			mutate(&cfg)
			_, err := NewOraclePoller(cfg)
			require.Error(t, err)
		})
	}
}
