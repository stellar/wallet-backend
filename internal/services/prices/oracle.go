package prices

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/metrics"
)

// MaxOraclePriceAge is how old an oracle reading may be and still set the anchor rates.
const MaxOraclePriceAge = 24 * time.Hour

// The oracle quotes both anchor tokens as SEP-40 Asset::Other symbols against a USD base.
const (
	oracleBaseSymbol = "USD"
	oracleXLMSymbol  = "XLM"
	oracleUSDCSymbol = "USDC"
)

// ContractFieldFetcher calls a read-only contract function through RPC simulation.
type ContractFieldFetcher interface {
	FetchSingleField(ctx context.Context, contractAddress, functionName string, args ...xdr.ScVal) (xdr.ScVal, error)
}

// OraclePriceStore persists the latest oracle reading per asset.
type OraclePriceStore interface {
	Upsert(ctx context.Context, prices []data.OraclePrice) error
	GetAll(ctx context.Context) ([]data.OraclePrice, error)
}

// OracleConfig configures an OraclePoller.
type OracleConfig struct {
	// OracleContractID is the SEP-40 oracle's C-address. Its base asset must be Other("USD").
	OracleContractID string
	Metadata         ContractFieldFetcher
	// Interval is the wait between the end of one pass and the start of the next.
	Interval     time.Duration
	Anchor       *Anchor
	OraclePrices OraclePriceStore
	// XLMSAC and USDCSAC are the SAC addresses the two readings are stored under.
	XLMSAC  string
	USDCSAC string
	// Metrics is optional.
	Metrics *metrics.PricesMetrics
}

// OraclePoller keeps the anchor rates fresh from a SEP-40 oracle. It publishes rates only when
// both the XLM and USDC readings are usable, so the anchor never mixes a fresh rate with a stale one.
type OraclePoller struct {
	cfg OracleConfig
	now func() time.Time

	// decimals is read from the oracle once, after its base asset checks out.
	decimals uint32
	ready    bool
}

// NewOraclePoller validates cfg and builds a poller.
func NewOraclePoller(cfg OracleConfig) (*OraclePoller, error) {
	switch {
	case cfg.OracleContractID == "":
		return nil, errors.New("prices: OracleConfig.OracleContractID is required")
	case cfg.Metadata == nil:
		return nil, errors.New("prices: OracleConfig.Metadata is required")
	case cfg.Interval <= 0:
		return nil, fmt.Errorf("prices: OracleConfig.Interval must be positive, got %s", cfg.Interval)
	case cfg.Anchor == nil:
		return nil, errors.New("prices: OracleConfig.Anchor is required")
	case cfg.OraclePrices == nil:
		return nil, errors.New("prices: OracleConfig.OraclePrices is required")
	case cfg.XLMSAC == "" || cfg.USDCSAC == "":
		return nil, errors.New("prices: OracleConfig.XLMSAC and USDCSAC are required")
	}
	return &OraclePoller{cfg: cfg, now: time.Now}, nil
}

// Run seeds the anchor from the stored readings, then polls the oracle until ctx is done. The first
// pass runs at once; each later pass starts Interval after the previous one ends, so a slow pass
// never causes back-to-back RPC calls. A failed pass is logged and retried on the next tick.
func (p *OraclePoller) Run(ctx context.Context) {
	p.seedFromStore(ctx)

	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		if err := p.pollOnce(ctx); err != nil {
			log.Ctx(ctx).Errorf("prices: oracle poll failed: %v", err)
		}
		timer.Reset(p.cfg.Interval)
	}
}

// seedFromStore sets the anchor from the stored XLM and USDC readings when both exist and are
// younger than MaxOraclePriceAge, so a restarted ingester prices fills before its first poll.
func (p *OraclePoller) seedFromStore(ctx context.Context) {
	rows, err := p.cfg.OraclePrices.GetAll(ctx)
	if err != nil {
		log.Ctx(ctx).Warnf("prices: reading stored oracle prices: %v", err)
		return
	}
	var xlm, usdc *data.OraclePrice
	for i := range rows {
		switch rows[i].Asset {
		case p.cfg.XLMSAC:
			xlm = &rows[i]
		case p.cfg.USDCSAC:
			usdc = &rows[i]
		}
	}
	if xlm == nil || usdc == nil {
		return
	}
	cutoff := p.now().Add(-MaxOraclePriceAge).Unix()
	for _, r := range []*data.OraclePrice{xlm, usdc} {
		if r.PriceUSD <= 0 || r.PriceTimestamp < cutoff {
			return
		}
	}
	p.setAnchor(AnchorRates{
		XLMUSD:  xlm.PriceUSD,
		USDCUSD: usdc.PriceUSD,
		AsOf:    time.Unix(min(xlm.PriceTimestamp, usdc.PriceTimestamp), 0),
	})
}

// setAnchor publishes rates and records how old the reading behind them is.
func (p *OraclePoller) setAnchor(rates AnchorRates) {
	p.cfg.Anchor.Set(rates)
	if m := p.cfg.Metrics; m != nil {
		m.AnchorAgeSeconds.Set(p.now().Sub(rates.AsOf).Seconds())
	}
}

// observe counts one oracle reading under result.
func (p *OraclePoller) observe(result string) {
	if m := p.cfg.Metrics; m != nil {
		m.OracleFetchesTotal.WithLabelValues(result).Inc()
	}
}

// pollOnce runs one pass. The first successful pass also checks the oracle's base asset and
// caches its decimals. A failed XLM call does not skip the USDC call; every failure is joined.
func (p *OraclePoller) pollOnce(ctx context.Context) error {
	if !p.ready {
		if err := p.checkOracle(ctx); err != nil {
			return err
		}
	}

	now := p.now()
	xlm, xlmErr := p.reading(ctx, oracleXLMSymbol, now)
	usdc, usdcErr := p.reading(ctx, oracleUSDCSymbol, now)
	errs := []error{xlmErr, usdcErr}

	if xlm == nil || usdc == nil {
		log.Ctx(ctx).Warnf("prices: oracle readings incomplete (XLM usable=%t, USDC usable=%t); keeping previous anchor rates",
			xlm != nil, usdc != nil)
		return errors.Join(errs...)
	}

	p.setAnchor(AnchorRates{
		XLMUSD:  xlm.PriceUSD,
		USDCUSD: usdc.PriceUSD,
		AsOf:    time.Unix(min(xlm.PriceTimestamp, usdc.PriceTimestamp), 0),
	})
	if err := p.cfg.OraclePrices.Upsert(ctx, []data.OraclePrice{*xlm, *usdc}); err != nil {
		errs = append(errs, fmt.Errorf("storing oracle prices: %w", err))
	}
	return errors.Join(errs...)
}

// checkOracle refuses an oracle whose base asset is not Other("USD"), since every reading is taken
// as a USD rate, then caches decimals().
func (p *OraclePoller) checkOracle(ctx context.Context) error {
	baseVal, err := p.cfg.Metadata.FetchSingleField(ctx, p.cfg.OracleContractID, "base")
	if err != nil {
		return fmt.Errorf("fetching oracle base: %w", err)
	}
	if sym, ok := otherAssetSymbol(baseVal); !ok || sym != oracleBaseSymbol {
		return fmt.Errorf("oracle %s base asset is not Other(%q); refusing to read its prices as USD",
			p.cfg.OracleContractID, oracleBaseSymbol)
	}

	decVal, err := p.cfg.Metadata.FetchSingleField(ctx, p.cfg.OracleContractID, "decimals")
	if err != nil {
		return fmt.Errorf("fetching oracle decimals: %w", err)
	}
	dec, ok := decVal.GetU32()
	if !ok {
		return fmt.Errorf("oracle decimals: expected u32, got %v", decVal.Type)
	}
	p.decimals = uint32(dec)
	p.ready = true
	return nil
}

// reading fetches lastprice(Other(symbol)) and returns it as a row keyed by the symbol's SAC. It
// returns nil with no error when the oracle has no price, the price is not positive, or the price
// is older than MaxOraclePriceAge.
func (p *OraclePoller) reading(ctx context.Context, symbol string, now time.Time) (*data.OraclePrice, error) {
	val, err := p.cfg.Metadata.FetchSingleField(ctx, p.cfg.OracleContractID, "lastprice", otherAssetScVal(symbol))
	if err != nil {
		p.observe("error")
		return nil, fmt.Errorf("fetching lastprice(%s): %w", symbol, err)
	}
	price, ts, found, err := decodeOraclePriceData(val)
	if err != nil {
		p.observe("error")
		return nil, fmt.Errorf("decoding lastprice(%s): %w", symbol, err)
	}
	switch {
	case !found:
		p.observe("none")
		log.Ctx(ctx).Warnf("prices: oracle has no %s price", symbol)
		return nil, nil
	case price.Sign() <= 0:
		p.observe("invalid")
		log.Ctx(ctx).Warnf("prices: oracle %s price %s is not positive", symbol, price)
		return nil, nil
	case int64(ts) < now.Add(-MaxOraclePriceAge).Unix():
		p.observe("stale")
		log.Ctx(ctx).Warnf("prices: oracle %s price is stale (timestamp %d)", symbol, ts)
		return nil, nil
	}

	p.observe("success")
	asset := p.cfg.XLMSAC
	if symbol == oracleUSDCSymbol {
		asset = p.cfg.USDCSAC
	}
	return &data.OraclePrice{
		Asset:          asset,
		PriceUSD:       oraclePriceFloat(price, p.decimals),
		PriceTimestamp: int64(ts),
	}, nil
}

// otherAssetScVal encodes the SEP-40 Asset::Other(symbol) variant: a contracttype tuple enum
// serializes as a vec of the variant name followed by its payload.
func otherAssetScVal(symbol string) xdr.ScVal {
	variant := xdr.ScSymbol("Other")
	payload := xdr.ScSymbol(symbol)
	vec := &xdr.ScVec{
		{Type: xdr.ScValTypeScvSymbol, Sym: &variant},
		{Type: xdr.ScValTypeScvSymbol, Sym: &payload},
	}
	return xdr.ScVal{Type: xdr.ScValTypeScvVec, Vec: &vec}
}

// otherAssetSymbol returns the symbol of an Asset::Other(symbol) value; ok is false for any other shape.
func otherAssetSymbol(v xdr.ScVal) (string, bool) {
	vec, ok := v.GetVec()
	if !ok || vec == nil || len(*vec) != 2 {
		return "", false
	}
	variant, ok := (*vec)[0].GetSym()
	if !ok || variant != "Other" {
		return "", false
	}
	sym, ok := (*vec)[1].GetSym()
	if !ok {
		return "", false
	}
	return string(sym), true
}

// decodeOraclePriceData decodes an Option<PriceData> return value. None (void) reports found=false.
// A present PriceData is a map keyed by field-name symbols; any other shape is an error, so a
// decode failure is never mistaken for an absent price.
func decodeOraclePriceData(v xdr.ScVal) (price *big.Int, timestamp uint64, found bool, err error) {
	if v.Type == xdr.ScValTypeScvVoid {
		return nil, 0, false, nil
	}
	m, ok := v.GetMap()
	if !ok || m == nil {
		return nil, 0, false, fmt.Errorf("PriceData: expected map or void, got %v", v.Type)
	}
	var havePrice, haveTS bool
	for _, e := range *m {
		key, ok := e.Key.GetSym()
		if !ok {
			continue
		}
		switch key {
		case "price":
			parts, ok := e.Val.GetI128()
			if !ok {
				return nil, 0, false, fmt.Errorf("PriceData.price: expected i128, got %v", e.Val.Type)
			}
			price = int128ToBigInt(parts)
			havePrice = true
		case "timestamp":
			ts, ok := e.Val.GetU64()
			if !ok {
				return nil, 0, false, fmt.Errorf("PriceData.timestamp: expected u64, got %v", e.Val.Type)
			}
			timestamp = uint64(ts)
			haveTS = true
		}
	}
	if !havePrice || !haveTS {
		return nil, 0, false, errors.New("PriceData: missing price or timestamp field")
	}
	return price, timestamp, true, nil
}

// int128ToBigInt converts Int128Parts (Hi signed, Lo unsigned) to Hi*2^64 + Lo.
func int128ToBigInt(parts xdr.Int128Parts) *big.Int {
	v := big.NewInt(int64(parts.Hi))
	v.Lsh(v, 64)
	return v.Add(v, new(big.Int).SetUint64(uint64(parts.Lo)))
}

// oraclePriceFloat returns price / 10^decimals.
func oraclePriceFloat(price *big.Int, decimals uint32) float64 {
	scale := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil)
	f, _ := new(big.Float).Quo(new(big.Float).SetInt(price), new(big.Float).SetInt(scale)).Float64()
	return f
}
