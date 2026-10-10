package processors

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
)

// AMMPoolsProcessor registers AMM pools as their venue's factory announces them, so the trades
// processor trusts a pool's swap events from the ledger it was created in.
type AMMPoolsProcessor struct {
	venues         TradeVenues
	registry       *AMMPoolRegistry
	metricsService *metrics.IngestionMetrics
}

// NewAMMPoolsProcessor creates a pools processor writing to registry.
func NewAMMPoolsProcessor(venues TradeVenues, registry *AMMPoolRegistry, metricsService *metrics.IngestionMetrics) *AMMPoolsProcessor {
	return &AMMPoolsProcessor{venues: venues, registry: registry, metricsService: metricsService}
}

// Name returns the processor name for logging and metrics.
func (p *AMMPoolsProcessor) Name() string {
	return "amm_pools"
}

// ProcessOperation returns the pools the operation's factory events announce, after adding them
// to the registry.
func (p *AMMPoolsProcessor) ProcessOperation(_ context.Context, opWrapper *TransactionOperationWrapper) ([]types.AMMPool, error) {
	startTime := time.Now()
	defer func() {
		if p.metricsService != nil {
			p.metricsService.StateChangeProcessingDuration.WithLabelValues("AMMPoolsProcessor").Observe(time.Since(startTime).Seconds())
		}
	}()

	if p.venues.SoroswapFactory == "" || opWrapper.OperationType() != xdr.OperationTypeInvokeHostFunction || !opWrapper.Transaction.Result.Successful() {
		return nil, nil
	}
	events, err := opWrapper.Transaction.GetContractEventsForOperation(opWrapper.Index)
	if err != nil {
		return nil, fmt.Errorf("getting contract events: %w", err)
	}
	var pools []types.AMMPool
	for _, ev := range events {
		if ev.Type != xdr.ContractEventTypeContract || ev.ContractId == nil {
			continue
		}
		if strkey.MustEncode(strkey.VersionByteContract, ev.ContractId[:]) != p.venues.SoroswapFactory {
			continue
		}
		pool, ok, err := decodeSoroswapNewPair(ev)
		if err != nil {
			return nil, fmt.Errorf("decoding Soroswap factory event: %w", err)
		}
		if !ok {
			continue
		}
		pool.CreatedLedger = opWrapper.LedgerSequence
		p.registry.Add(pool)
		pools = append(pools, pool)
	}
	return pools, nil
}

// decodeSoroswapNewPair decodes a factory `new_pair` event into a pool registration. ok is false
// for any other factory event.
//
// Topics: [Sym "SoroswapFactory", Sym "new_pair"].
// Data: {new_pairs_length: u32, pair: Address, token_0: Address, token_1: Address}.
func decodeSoroswapNewPair(ev xdr.ContractEvent) (types.AMMPool, bool, error) {
	topics, data, ok := contractEventV0(ev)
	if !ok || !hasTopicNames(topics, "SoroswapFactory", "new_pair") {
		return types.AMMPool{}, false, nil
	}
	fields, ok := scValMapFields(data)
	if !ok {
		return types.AMMPool{}, false, fmt.Errorf("new_pair data: want a symbol-keyed map, found %s", data.Type)
	}
	pool := types.AMMPool{Venue: types.TradeVenueSoroswap}
	var err error
	if pool.Pool, err = mapFieldAddress(fields, "pair"); err != nil {
		return types.AMMPool{}, false, fmt.Errorf("new_pair data: %w", err)
	}
	if pool.Token0, err = mapFieldAddress(fields, "token_0"); err != nil {
		return types.AMMPool{}, false, fmt.Errorf("new_pair data: %w", err)
	}
	if pool.Token1, err = mapFieldAddress(fields, "token_1"); err != nil {
		return types.AMMPool{}, false, fmt.Errorf("new_pair data: %w", err)
	}
	return pool, true, nil
}

// decodeSoroswapSwap decodes a pair `swap` event into a fill. ok is false for any other pair event,
// and for a swap that does not move exactly one token in and the other token out.
//
// Topics: [Sym "SoroswapPair", Sym "swap"].
// Data: {amount_0_in, amount_0_out, amount_1_in, amount_1_out: i128, to: Address}.
func decodeSoroswapSwap(ev xdr.ContractEvent, pool types.AMMPool) (fill, bool, error) {
	topics, data, ok := contractEventV0(ev)
	if !ok || !hasTopicNames(topics, "SoroswapPair", "swap") {
		return fill{}, false, nil
	}
	fields, ok := scValMapFields(data)
	if !ok {
		return fill{}, false, fmt.Errorf("swap data: want a symbol-keyed map, found %s", data.Type)
	}
	var in, out [2]*big.Int
	var err error
	for i, key := range [2]string{"amount_0_in", "amount_1_in"} {
		if in[i], err = mapFieldI128(fields, key); err != nil {
			return fill{}, false, fmt.Errorf("swap data: %w", err)
		}
	}
	for i, key := range [2]string{"amount_0_out", "amount_1_out"} {
		if out[i], err = mapFieldI128(fields, key); err != nil {
			return fill{}, false, fmt.Errorf("swap data: %w", err)
		}
	}
	inIdx, inOK := singlePositive(in)
	outIdx, outOK := singlePositive(out)
	if !inOK || !outOK || inIdx == outIdx {
		return fill{}, false, nil
	}
	tokens := [2]string{pool.Token0, pool.Token1}
	return fill{
		TokenIn:   tokens[inIdx],
		AmountIn:  in[inIdx],
		TokenOut:  tokens[outIdx],
		AmountOut: out[outIdx],
		Venue:     types.TradeVenueSoroswap,
	}, true, nil
}

// decodeAquariusRouterSwap decodes a router `swap` event into a fill. ok is false for any other
// router event.
//
// Topics: [Sym "swap", tokens: Vec<Address>, user: Address].
// Data: (pool: Address or BytesN<32>, token_in: Address, token_out: Address, in_amount: u128,
// out_amount: u128). The pool element is not read; its encoding differs between router versions.
func decodeAquariusRouterSwap(ev xdr.ContractEvent) (fill, bool, error) {
	topics, data, ok := contractEventV0(ev)
	if !ok || !hasTopicNames(topics, "swap") {
		return fill{}, false, nil
	}
	vec, ok := data.GetVec()
	if !ok || vec == nil {
		return fill{}, false, fmt.Errorf("swap data: want a vec, found %s", data.Type)
	}
	items := *vec
	if len(items) != 5 {
		return fill{}, false, fmt.Errorf("swap data: want 5 elements, found %d", len(items))
	}
	tokenIn, ok := scValAddress(items[1])
	if !ok {
		return fill{}, false, fmt.Errorf("swap data element 1 (token_in): want an address, found %s", items[1].Type)
	}
	tokenOut, ok := scValAddress(items[2])
	if !ok {
		return fill{}, false, fmt.Errorf("swap data element 2 (token_out): want an address, found %s", items[2].Type)
	}
	amountIn, ok := scValU128(items[3])
	if !ok {
		return fill{}, false, fmt.Errorf("swap data element 3 (in_amount): want u128, found %s", items[3].Type)
	}
	amountOut, ok := scValU128(items[4])
	if !ok {
		return fill{}, false, fmt.Errorf("swap data element 4 (out_amount): want u128, found %s", items[4].Type)
	}
	return fill{
		TokenIn:   tokenIn,
		AmountIn:  amountIn,
		TokenOut:  tokenOut,
		AmountOut: amountOut,
		Venue:     types.TradeVenueAquarius,
	}, true, nil
}

// contractEventV0 returns an event's topics and data. ok is false for a body version the decoders
// do not read.
func contractEventV0(ev xdr.ContractEvent) ([]xdr.ScVal, xdr.ScVal, bool) {
	body, ok := ev.Body.GetV0()
	if !ok {
		return nil, xdr.ScVal{}, false
	}
	return body.Topics, body.Data, true
}

// hasTopicNames reports whether topics start with the given names. A name may be encoded as a
// symbol or a string: Soroswap emits its venue name as a string and the action as a symbol.
func hasTopicNames(topics []xdr.ScVal, want ...string) bool {
	if len(topics) < len(want) {
		return false
	}
	for i, w := range want {
		if name, ok := scValName(topics[i]); !ok || name != w {
			return false
		}
	}
	return true
}

// scValName returns the text of a symbol or string ScVal.
func scValName(val xdr.ScVal) (string, bool) {
	if sym, ok := val.GetSym(); ok {
		return string(sym), true
	}
	if str, ok := val.GetStr(); ok {
		return string(str), true
	}
	return "", false
}

// mapFieldAddress returns the address stored under key in a decoded contract-type struct.
func mapFieldAddress(fields map[string]xdr.ScVal, key string) (string, error) {
	v, ok := fields[key]
	if !ok {
		return "", fmt.Errorf("field %q is missing", key)
	}
	addr, ok := scValAddress(v)
	if !ok {
		return "", fmt.Errorf("field %q: want an address, found %s", key, v.Type)
	}
	return addr, nil
}

// mapFieldI128 returns the i128 stored under key in a decoded contract-type struct.
func mapFieldI128(fields map[string]xdr.ScVal, key string) (*big.Int, error) {
	v, ok := fields[key]
	if !ok {
		return nil, fmt.Errorf("field %q is missing", key)
	}
	n, ok := scValI128(v)
	if !ok {
		return nil, fmt.Errorf("field %q: want i128, found %s", key, v.Type)
	}
	return n, nil
}

// singlePositive returns the index of the only positive amount. ok is false when neither or both
// amounts are positive.
func singlePositive(amounts [2]*big.Int) (int, bool) {
	first, second := amounts[0].Sign() > 0, amounts[1].Sign() > 0
	switch {
	case first && !second:
		return 0, true
	case second && !first:
		return 1, true
	default:
		return 0, false
	}
}
