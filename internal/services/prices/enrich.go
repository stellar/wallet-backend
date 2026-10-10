package prices

import (
	"context"
	"fmt"
	"math"
	"math/big"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer/processors"
	"github.com/stellar/wallet-backend/internal/indexer/types"
)

// DecimalsSource resolves token decimals for contract addresses.
type DecimalsSource interface {
	GetDecimals(ctx context.Context, contractIDs []string) (map[string]int32, error)
}

// Enricher fills a fill's quantities and USD value before it is persisted.
type Enricher struct {
	Venues   processors.TradeVenues
	Anchor   *Anchor
	Decimals DecimalsSource

	// decimalsCache memoizes resolved decimals; a token's decimals never change.
	decimalsCache map[string]int32
}

// NewEnricher builds an enricher over the given sources. The anchor tokens are classic assets,
// so their decimals are known without a lookup; the native token's SAC has no contract_tokens row.
func NewEnricher(venues processors.TradeVenues, anchor *Anchor, decimals DecimalsSource) *Enricher {
	cache := make(map[string]int32)
	for _, sac := range []string{venues.XLMSAC, venues.USDCSAC} {
		if sac != "" {
			cache[sac] = classicDecimals
		}
	}
	return &Enricher{Venues: venues, Anchor: anchor, Decimals: decimals, decimalsCache: cache}
}

// classicDecimals is the fixed precision of every classic asset and its SAC.
const classicDecimals int32 = 7

// Enrich sets BaseQty and USDValue on every fill it can price and returns each token's last priced
// fill in the batch. A fill whose decimals or anchor rate are unknown is persisted unpriced.
// Enrich is called from the single persist goroutine, so the cache needs no locking.
func (e *Enricher) Enrich(ctx context.Context, trades []types.Trade) ([]data.LastTrade, error) {
	if len(trades) == 0 {
		return nil, nil
	}
	if err := e.resolveDecimals(ctx, trades); err != nil {
		return nil, err
	}
	rates, haveRates := e.Anchor.Get()

	last := make(map[string]data.LastTrade)
	for i := range trades {
		t := &trades[i]
		if t.BaseDecimals == nil || t.CounterDecimals == nil {
			continue
		}
		baseQty := scaled(t.BaseAmount, *t.BaseDecimals)
		if baseQty <= 0 {
			continue
		}
		t.BaseQty = &baseQty
		if !haveRates {
			continue
		}
		rate, ok := e.anchorRate(t.CounterToken, rates)
		if !ok {
			continue
		}
		usd := scaled(t.CounterAmount, *t.CounterDecimals) * rate
		t.USDValue = &usd

		// Fills arrive in (operation, fill) order, so a later fill for the same token wins.
		if prev, seen := last[t.BaseToken]; !seen || t.OperationID >= prev.OperationID {
			last[t.BaseToken] = data.LastTrade{
				Token:           t.BaseToken,
				PriceUSD:        usd / baseQty,
				LedgerCreatedAt: t.LedgerClosed,
				OperationID:     t.OperationID,
			}
		}
	}
	out := make([]data.LastTrade, 0, len(last))
	for _, l := range last {
		out = append(out, l)
	}
	return out, nil
}

func (e *Enricher) resolveDecimals(ctx context.Context, trades []types.Trade) error {
	var missing []string
	seen := make(map[string]struct{})
	need := func(token string, have *int32) {
		if have != nil {
			return
		}
		if _, cached := e.decimalsCache[token]; cached {
			return
		}
		if _, dup := seen[token]; dup {
			return
		}
		seen[token] = struct{}{}
		missing = append(missing, token)
	}
	for i := range trades {
		need(trades[i].BaseToken, trades[i].BaseDecimals)
		need(trades[i].CounterToken, trades[i].CounterDecimals)
	}
	if len(missing) > 0 {
		found, err := e.Decimals.GetDecimals(ctx, missing)
		if err != nil {
			return fmt.Errorf("resolving token decimals: %w", err)
		}
		for token, d := range found {
			e.decimalsCache[token] = d
		}
	}
	for i := range trades {
		t := &trades[i]
		if t.BaseDecimals == nil {
			if d, ok := e.decimalsCache[t.BaseToken]; ok {
				t.BaseDecimals = &d
			}
		}
		if t.CounterDecimals == nil {
			if d, ok := e.decimalsCache[t.CounterToken]; ok {
				t.CounterDecimals = &d
			}
		}
	}
	return nil
}

func (e *Enricher) anchorRate(counter string, rates AnchorRates) (float64, bool) {
	switch counter {
	case e.Venues.USDCSAC:
		return rates.USDCUSD, rates.USDCUSD > 0
	case e.Venues.XLMSAC:
		return rates.XLMUSD, rates.XLMUSD > 0
	default:
		return 0, false
	}
}

// scaled converts a raw amount to whole-token units. Float precision is enough for pricing; the
// exact amount is stored alongside.
func scaled(amount *big.Int, decimals int32) float64 {
	if amount == nil {
		return 0
	}
	f, _ := new(big.Float).SetInt(amount).Float64()
	return f / math.Pow10(int(decimals))
}
