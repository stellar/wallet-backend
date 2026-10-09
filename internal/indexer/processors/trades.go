package processors

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
)

// TradesProcessor extracts executed fills from an operation: classic ClaimAtoms from offer and
// path-payment results, and Soroban AMM swaps from the contract events of trusted pools. Each
// fill is oriented so the priced token is the base and the anchor is the counter.
type TradesProcessor struct {
	networkPassphrase string
	venues            TradeVenues
	registry          *AMMPoolRegistry
	metricsService    *metrics.IngestionMetrics

	// sacByAsset memoizes classic asset → SAC address; the derivation is a hash per call.
	sacByAsset sync.Map
}

// NewTradesProcessor creates a trades processor reading trusted pools from registry.
func NewTradesProcessor(networkPassphrase string, venues TradeVenues, registry *AMMPoolRegistry, metricsService *metrics.IngestionMetrics) *TradesProcessor {
	return &TradesProcessor{
		networkPassphrase: networkPassphrase,
		venues:            venues,
		registry:          registry,
		metricsService:    metricsService,
	}
}

// Name returns the processor name for logging and metrics.
func (p *TradesProcessor) Name() string {
	return "trades"
}

// fill is one executed exchange before orientation: the taker gave AmountIn of TokenIn and
// received AmountOut of TokenOut.
type fill struct {
	TokenIn   string
	AmountIn  *big.Int
	TokenOut  string
	AmountOut *big.Int
	Venue     types.TradeVenue
	// DecimalsKnown is true for classic fills: both sides are classic assets, always 7 decimals.
	DecimalsKnown bool
}

// classicDecimals is the fixed precision of every classic asset and its SAC.
const classicDecimals int32 = 7

// ProcessOperation returns the operation's fills in meta order. Failed transactions have no
// fills: their results carry no claim atoms and their events are not emitted.
func (p *TradesProcessor) ProcessOperation(ctx context.Context, opWrapper *TransactionOperationWrapper) ([]types.Trade, error) {
	startTime := time.Now()
	defer func() {
		if p.metricsService != nil {
			p.metricsService.StateChangeProcessingDuration.WithLabelValues("TradesProcessor").Observe(time.Since(startTime).Seconds())
		}
	}()

	if !opWrapper.Transaction.Result.Successful() {
		return nil, nil
	}

	var fills []fill
	var err error
	switch opWrapper.OperationType() {
	case xdr.OperationTypePathPaymentStrictReceive, xdr.OperationTypePathPaymentStrictSend,
		xdr.OperationTypeManageBuyOffer, xdr.OperationTypeManageSellOffer, xdr.OperationTypeCreatePassiveSellOffer:
		fills, err = p.classicFills(opWrapper)
	case xdr.OperationTypeInvokeHostFunction:
		fills, err = p.ammFills(ctx, opWrapper)
	default:
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	trades := make([]types.Trade, 0, len(fills))
	for _, f := range fills {
		if t, ok := p.orient(f); ok {
			t.OperationID = opWrapper.ID()
			t.FillIndex = int16(len(trades))
			t.LedgerNumber = opWrapper.LedgerSequence
			t.LedgerClosed = opWrapper.LedgerClosed
			trades = append(trades, t)
		}
	}
	return trades, nil
}

// classicFills reads the claim atoms of an offer or path-payment result. Core also uses the
// manage-sell-offer result arm for passive offers, so both arms are accepted.
func (p *TradesProcessor) classicFills(opWrapper *TransactionOperationWrapper) ([]fill, error) {
	results, ok := opWrapper.Transaction.Result.Result.OperationResults()
	if !ok || int(opWrapper.Index) >= len(results) {
		return nil, nil
	}
	tr, ok := results[opWrapper.Index].GetTr()
	if !ok {
		return nil, nil
	}

	var atoms []xdr.ClaimAtom
	switch tr.Type {
	case xdr.OperationTypePathPaymentStrictReceive:
		if s, ok := tr.MustPathPaymentStrictReceiveResult().GetSuccess(); ok {
			atoms = s.Offers
		}
	case xdr.OperationTypePathPaymentStrictSend:
		if s, ok := tr.MustPathPaymentStrictSendResult().GetSuccess(); ok {
			atoms = s.Offers
		}
	case xdr.OperationTypeManageBuyOffer:
		if s, ok := tr.MustManageBuyOfferResult().GetSuccess(); ok {
			atoms = s.OffersClaimed
		}
	case xdr.OperationTypeManageSellOffer:
		if s, ok := tr.MustManageSellOfferResult().GetSuccess(); ok {
			atoms = s.OffersClaimed
		}
	case xdr.OperationTypeCreatePassiveSellOffer:
		if s, ok := tr.MustCreatePassiveSellOfferResult().GetSuccess(); ok {
			atoms = s.OffersClaimed
		}
	default:
		return nil, nil
	}

	fills := make([]fill, 0, len(atoms))
	for _, atom := range atoms {
		// An atom's seller (an offer or a pool) sold AmountSold of AssetSold and received
		// AmountBought of AssetBought, so from the taker's side the sold asset is what came out.
		sold, err := p.sacAddress(atom.AssetSold())
		if err != nil {
			return nil, err
		}
		bought, err := p.sacAddress(atom.AssetBought())
		if err != nil {
			return nil, err
		}
		venue := types.TradeVenueSDEXOrderbook
		if atom.Type == xdr.ClaimAtomTypeClaimAtomTypeLiquidityPool {
			venue = types.TradeVenueSDEXPool
		}
		fills = append(fills, fill{
			TokenIn:       bought,
			AmountIn:      big.NewInt(int64(atom.AmountBought())),
			TokenOut:      sold,
			AmountOut:     big.NewInt(int64(atom.AmountSold())),
			Venue:         venue,
			DecimalsKnown: true,
		})
	}
	return fills, nil
}

// ammFills decodes swap events from contracts the processor trusts: pools in the registry and
// the venues' fixed routers. Events from any other contract are ignored, whatever their shape. An
// event from a trusted contract that does not decode is logged and skipped: one unreadable fill
// must not stop the ledger, and the log line is what reveals a venue's layout has changed.
func (p *TradesProcessor) ammFills(ctx context.Context, opWrapper *TransactionOperationWrapper) ([]fill, error) {
	events, err := opWrapper.Transaction.GetContractEventsForOperation(opWrapper.Index)
	if err != nil {
		return nil, fmt.Errorf("getting contract events: %w", err)
	}
	var fills []fill
	for _, ev := range events {
		if ev.Type != xdr.ContractEventTypeContract || ev.ContractId == nil {
			continue
		}
		emitter := strkey.MustEncode(strkey.VersionByteContract, ev.ContractId[:])
		switch {
		case p.venues.AquariusRouter != "" && emitter == p.venues.AquariusRouter:
			f, ok, err := decodeAquariusRouterSwap(ev)
			if err != nil {
				log.Ctx(ctx).Warnf("prices: skipping Aquarius router event in operation %d: %v", opWrapper.ID(), err)
				continue
			}
			if ok {
				fills = append(fills, f)
			}
		default:
			pool, known := p.registry.Get(emitter)
			if !known {
				continue
			}
			f, ok, err := decodeSoroswapSwap(ev, pool)
			if err != nil {
				log.Ctx(ctx).Warnf("prices: skipping Soroswap pair %s event in operation %d: %v", emitter, opWrapper.ID(), err)
				continue
			}
			if ok {
				fills = append(fills, f)
			}
		}
	}
	return fills, nil
}

// orient turns a fill into a Trade whose base is the priced token and whose counter is the anchor.
// USDC ranks 0, XLM ranks 1, every other token ranks 2; the lower rank is the counter and ties
// break on address order so the same pair always lands the same way. Fills with a zero amount on
// either side carry no price and are dropped.
func (p *TradesProcessor) orient(f fill) (types.Trade, bool) {
	if f.AmountIn == nil || f.AmountOut == nil || f.AmountIn.Sign() <= 0 || f.AmountOut.Sign() <= 0 {
		return types.Trade{}, false
	}
	inRank, outRank := p.anchorRank(f.TokenIn), p.anchorRank(f.TokenOut)
	inIsCounter := inRank < outRank || (inRank == outRank && strings.Compare(f.TokenIn, f.TokenOut) < 0)
	t := types.Trade{Venue: f.Venue}
	if f.DecimalsKnown {
		d := classicDecimals
		t.BaseDecimals, t.CounterDecimals = &d, &d
	}
	if inIsCounter {
		t.BaseToken, t.BaseAmount = f.TokenOut, f.AmountOut
		t.CounterToken, t.CounterAmount = f.TokenIn, f.AmountIn
	} else {
		t.BaseToken, t.BaseAmount = f.TokenIn, f.AmountIn
		t.CounterToken, t.CounterAmount = f.TokenOut, f.AmountOut
	}
	return t, true
}

func (p *TradesProcessor) anchorRank(token string) int {
	switch token {
	case p.venues.USDCSAC:
		return 0
	case p.venues.XLMSAC:
		return 1
	default:
		return 2
	}
}

func (p *TradesProcessor) sacAddress(asset xdr.Asset) (string, error) {
	key := asset.StringCanonical()
	if v, ok := p.sacByAsset.Load(key); ok {
		return v.(string), nil
	}
	addr, err := SACAddress(asset, p.networkPassphrase)
	if err != nil {
		return "", err
	}
	p.sacByAsset.Store(key, addr)
	return addr, nil
}

// scValI128 converts an i128 ScVal into its raw 128-bit value.
func scValI128(val xdr.ScVal) (*big.Int, bool) {
	parts, ok := val.GetI128()
	if !ok {
		return nil, false
	}
	bi := big.NewInt(int64(parts.Hi))
	bi.Lsh(bi, 64)
	bi.Add(bi, new(big.Int).SetUint64(uint64(parts.Lo)))
	return bi, true
}

// scValU128 converts a u128 ScVal into its raw 128-bit value.
func scValU128(val xdr.ScVal) (*big.Int, bool) {
	parts, ok := val.GetU128()
	if !ok {
		return nil, false
	}
	bi := new(big.Int).SetUint64(uint64(parts.Hi))
	bi.Lsh(bi, 64)
	bi.Add(bi, new(big.Int).SetUint64(uint64(parts.Lo)))
	return bi, true
}

// scValAddress returns the strkey of an address ScVal.
func scValAddress(val xdr.ScVal) (string, bool) {
	addr, ok := val.GetAddress()
	if !ok {
		return "", false
	}
	s, err := addr.String()
	if err != nil {
		return "", false
	}
	return s, true
}

// scValSymbol returns the string of a symbol ScVal.
func scValSymbol(val xdr.ScVal) (string, bool) {
	sym, ok := val.GetSym()
	if !ok {
		return "", false
	}
	return string(sym), true
}

// scValMapFields returns a symbol-keyed ScMap as a Go map. Contract-type structs encode this way.
func scValMapFields(val xdr.ScVal) (map[string]xdr.ScVal, bool) {
	m, ok := val.GetMap()
	if !ok || m == nil {
		return nil, false
	}
	out := make(map[string]xdr.ScVal, len(*m))
	for _, entry := range *m {
		key, ok := scValSymbol(entry.Key)
		if !ok {
			return nil, false
		}
		out[key] = entry.Val
	}
	return out, true
}
