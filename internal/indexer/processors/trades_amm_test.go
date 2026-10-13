package processors

import (
	"context"
	"math/big"
	"testing"

	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/indexer/types"
)

var (
	ammFactory  = ammTestContract(0xF1)
	ammRouter   = ammTestContract(0xF2)
	ammPair     = ammTestContract(0xF3)
	ammStranger = ammTestContract(0xF4)
	ammAquaPool = ammTestContract(0xF5)

	// ammPool is a Soroswap pair with ETH as token 0 and USDC as token 1.
	ammPool = types.AMMPool{
		Pool:   ammPair,
		Venue:  types.TradeVenueSoroswap,
		Token0: ethContractAddress,
		Token1: usdcContractAddress,
	}
)

func ammTestContract(b byte) string {
	var id [32]byte
	for i := range id {
		id[i] = b
	}
	return strkey.MustEncode(strkey.VersionByteContract, id[:])
}

func ammContractID(addr string) xdr.ContractId {
	raw := strkey.MustDecode(strkey.VersionByteContract, addr)
	var id xdr.ContractId
	copy(id[:], raw)
	return id
}

// ammStr builds a string ScVal; Soroswap emits its venue name this way.
func ammStr(s string) xdr.ScVal {
	str := xdr.ScString(s)
	return xdr.ScVal{Type: xdr.ScValTypeScvString, Str: &str}
}

func ammSym(s string) xdr.ScVal {
	sym := xdr.ScSymbol(s)
	return xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym}
}

func ammAddr(addr string) xdr.ScVal {
	id := ammContractID(addr)
	return xdr.ScVal{Type: xdr.ScValTypeScvAddress, Address: &xdr.ScAddress{
		Type:       xdr.ScAddressTypeScAddressTypeContract,
		ContractId: &id,
	}}
}

func ammI128(v int64) xdr.ScVal {
	hi := xdr.Int64(0)
	if v < 0 {
		hi = -1
	}
	parts := xdr.Int128Parts{Hi: hi, Lo: xdr.Uint64(uint64(v))}
	return xdr.ScVal{Type: xdr.ScValTypeScvI128, I128: &parts}
}

func ammU128(lo uint64) xdr.ScVal {
	parts := xdr.UInt128Parts{Lo: xdr.Uint64(lo)}
	return xdr.ScVal{Type: xdr.ScValTypeScvU128, U128: &parts}
}

func ammU32(v uint32) xdr.ScVal {
	u := xdr.Uint32(v)
	return xdr.ScVal{Type: xdr.ScValTypeScvU32, U32: &u}
}

func ammBytes32() xdr.ScVal {
	b := xdr.ScBytes(make([]byte, 32))
	return xdr.ScVal{Type: xdr.ScValTypeScvBytes, Bytes: &b}
}

func ammVec(items ...xdr.ScVal) xdr.ScVal {
	vec := xdr.ScVec(items)
	vecPtr := &vec
	return xdr.ScVal{Type: xdr.ScValTypeScvVec, Vec: &vecPtr}
}

// ammMap builds a symbol-keyed map from alternating key, value pairs.
func ammMap(kv ...any) xdr.ScVal {
	m := xdr.ScMap{}
	for i := 0; i < len(kv); i += 2 {
		m = append(m, xdr.ScMapEntry{Key: ammSym(kv[i].(string)), Val: kv[i+1].(xdr.ScVal)})
	}
	mPtr := &m
	return xdr.ScVal{Type: xdr.ScValTypeScvMap, Map: &mPtr}
}

func ammEvent(emitter string, data xdr.ScVal, topics ...xdr.ScVal) xdr.ContractEvent {
	id := ammContractID(emitter)
	return xdr.ContractEvent{
		Type:       xdr.ContractEventTypeContract,
		ContractId: &id,
		Body: xdr.ContractEventBody{V: 0, V0: &xdr.ContractEventV0{
			Topics: topics,
			Data:   data,
		}},
	}
}

func ammNewPairData() xdr.ScVal {
	return ammMap(
		"new_pairs_length", ammU32(7),
		"pair", ammAddr(ammPair),
		"token_0", ammAddr(ethContractAddress),
		"token_1", ammAddr(usdcContractAddress),
	)
}

func ammSwapData(in0, in1, out0, out1 int64) xdr.ScVal {
	return ammMap(
		"amount_0_in", ammI128(in0),
		"amount_0_out", ammI128(out0),
		"amount_1_in", ammI128(in1),
		"amount_1_out", ammI128(out1),
		"to", ammAddr(ammStranger),
	)
}

func ammSwapEvent(emitter string, data xdr.ScVal) xdr.ContractEvent {
	return ammEvent(emitter, data, ammStr("SoroswapPair"), ammSym("swap"))
}

func ammRouterSwapEvent(data xdr.ScVal) xdr.ContractEvent {
	return ammEvent(ammRouter, data,
		ammSym("swap"),
		ammVec(ammAddr(ethContractAddress), ammAddr(usdcContractAddress)),
		ammAddr(ammStranger),
	)
}

// ammAquaTradeEvent is an Aquarius pool `trade` event: ETH in, USDC out.
func ammAquaTradeEvent(emitter string, in, out int64) xdr.ContractEvent {
	return ammEvent(emitter, ammVec(ammI128(in), ammI128(out), ammI128(0)),
		ammSym("trade"), ammAddr(ethContractAddress), ammAddr(usdcContractAddress), ammAddr(ammStranger))
}

// ammTestOp wraps events in a successful single-operation invoke-host-function transaction.
func ammTestOp(events ...xdr.ContractEvent) *TransactionOperationWrapper {
	tx := createInvocationTx("", "", someTxAccount.Address(), xdr.MustNewNativeAsset(), big.NewInt(0), nil)
	tx.UnsafeMeta.V3.SorobanMeta.Events = events
	return &TransactionOperationWrapper{
		Index:          0,
		Transaction:    tx,
		Operation:      tx.Envelope.Operations()[0],
		LedgerSequence: 12345,
	}
}

func TestDecodeSoroswapNewPair(t *testing.T) {
	testCases := []struct {
		name     string
		event    xdr.ContractEvent
		wantOK   bool
		wantPool types.AMMPool
		wantErr  string
	}{
		{
			name:   "new_pair registers the pool",
			event:  ammEvent(ammFactory, ammNewPairData(), ammStr("SoroswapFactory"), ammSym("new_pair")),
			wantOK: true,
			wantPool: types.AMMPool{
				Pool:   ammPair,
				Venue:  types.TradeVenueSoroswap,
				Token0: ethContractAddress,
				Token1: usdcContractAddress,
			},
		},
		{
			name:  "other factory event is ignored",
			event: ammEvent(ammFactory, ammU32(1), ammStr("SoroswapFactory"), ammSym("fees_enabled")),
		},
		{
			name:  "wrong first topic is ignored",
			event: ammEvent(ammFactory, ammNewPairData(), ammStr("SoroswapPair"), ammSym("new_pair")),
		},
		{
			name:  "no topics is ignored",
			event: ammEvent(ammFactory, ammNewPairData()),
		},
		{
			name:    "data that is not a map is an error",
			event:   ammEvent(ammFactory, ammU32(1), ammStr("SoroswapFactory"), ammSym("new_pair")),
			wantErr: "want a symbol-keyed map, found ScValTypeScvU32",
		},
		{
			name: "missing token_1 is an error",
			event: ammEvent(ammFactory,
				ammMap("pair", ammAddr(ammPair), "token_0", ammAddr(ethContractAddress)),
				ammStr("SoroswapFactory"), ammSym("new_pair")),
			wantErr: `field "token_1" is missing`,
		},
		{
			name: "pair that is not an address is an error",
			event: ammEvent(ammFactory,
				ammMap("pair", ammSym("x"), "token_0", ammAddr(ethContractAddress), "token_1", ammAddr(usdcContractAddress)),
				ammStr("SoroswapFactory"), ammSym("new_pair")),
			wantErr: `field "pair": want an address, found ScValTypeScvSymbol`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pool, ok, err := decodeSoroswapNewPair(tc.event)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				assert.False(t, ok)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantOK, ok)
			assert.Equal(t, tc.wantPool, pool)
		})
	}
}

func TestDecodeSoroswapSwap(t *testing.T) {
	testCases := []struct {
		name     string
		event    xdr.ContractEvent
		wantOK   bool
		wantFill fill
		wantErr  string
	}{
		{
			name:   "token0 in, token1 out",
			event:  ammSwapEvent(ammPair, ammSwapData(500, 0, 0, 1200)),
			wantOK: true,
			wantFill: fill{
				TokenIn: ethContractAddress, AmountIn: big.NewInt(500),
				TokenOut: usdcContractAddress, AmountOut: big.NewInt(1200),
				Venue: types.TradeVenueSoroswap,
			},
		},
		{
			name:   "token1 in, token0 out",
			event:  ammSwapEvent(ammPair, ammSwapData(0, 1200, 499, 0)),
			wantOK: true,
			wantFill: fill{
				TokenIn: usdcContractAddress, AmountIn: big.NewInt(1200),
				TokenOut: ethContractAddress, AmountOut: big.NewInt(499),
				Venue: types.TradeVenueSoroswap,
			},
		},
		{
			name:  "sync event is ignored",
			event: ammEvent(ammPair, ammSwapData(1, 0, 0, 1), ammStr("SoroswapPair"), ammSym("sync")),
		},
		{
			name:  "wrong topic with malformed data is ignored",
			event: ammEvent(ammPair, ammU32(1), ammStr("SoroswapPair"), ammSym("deposit")),
		},
		{
			name:  "both ins positive is not a single-direction swap",
			event: ammSwapEvent(ammPair, ammSwapData(5, 5, 0, 3)),
		},
		{
			name:  "no outs positive is not a single-direction swap",
			event: ammSwapEvent(ammPair, ammSwapData(5, 0, 0, 0)),
		},
		{
			name:  "same token in and out is not a swap",
			event: ammSwapEvent(ammPair, ammSwapData(5, 0, 3, 0)),
		},
		{
			name:    "data that is not a map is an error",
			event:   ammSwapEvent(ammPair, ammVec(ammI128(1))),
			wantErr: "want a symbol-keyed map, found ScValTypeScvVec",
		},
		{
			name: "missing amount is an error",
			event: ammSwapEvent(ammPair, ammMap(
				"amount_0_in", ammI128(5), "amount_1_in", ammI128(0), "amount_0_out", ammI128(0),
			)),
			wantErr: `field "amount_1_out" is missing`,
		},
		{
			name: "amount of the wrong type is an error",
			event: ammSwapEvent(ammPair, ammMap(
				"amount_0_in", ammU128(5), "amount_0_out", ammI128(0),
				"amount_1_in", ammI128(0), "amount_1_out", ammI128(3),
			)),
			wantErr: `field "amount_0_in": want i128, found ScValTypeScvU128`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			f, ok, err := decodeSoroswapSwap(tc.event, ammPool)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				assert.False(t, ok)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantOK, ok)
			assert.Equal(t, tc.wantFill, f)
		})
	}
}

func TestDecodeAquariusRouterPool(t *testing.T) {
	routerData := func(items ...xdr.ScVal) xdr.ScVal { return ammVec(items...) }

	testCases := []struct {
		name     string
		event    xdr.ContractEvent
		wantOK   bool
		wantPool types.AMMPool
		wantErr  string
	}{
		{
			name: "swap names the pool and its tokens",
			event: ammRouterSwapEvent(routerData(
				ammAddr(ammAquaPool), ammAddr(ethContractAddress), ammAddr(usdcContractAddress), ammU128(500), ammU128(1200),
			)),
			wantOK:   true,
			wantPool: types.AMMPool{Pool: ammAquaPool, Venue: types.TradeVenueAquarius, Token0: ethContractAddress, Token1: usdcContractAddress},
		},
		{
			name:  "other router event is ignored",
			event: ammEvent(ammRouter, ammU32(1), ammSym("deposit"), ammAddr(ammStranger)),
		},
		{
			name:    "data that is not a vec is an error",
			event:   ammRouterSwapEvent(ammMap("pool", ammAddr(ammAquaPool))),
			wantErr: "want a vec",
		},
		{
			name: "pool that is not an address is an error",
			event: ammRouterSwapEvent(routerData(
				ammBytes32(), ammAddr(ethContractAddress), ammAddr(usdcContractAddress), ammU128(500), ammU128(1200),
			)),
			wantErr: "element 0 (pool)",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pool, ok, err := decodeAquariusRouterPool(tc.event)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				assert.False(t, ok)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantOK, ok)
			assert.Equal(t, tc.wantPool, pool)
		})
	}
}

func TestDecodeAquariusPoolTrade(t *testing.T) {
	// 2^64 + 7: an i128 whose high word is set.
	bigAmount := new(big.Int).Add(new(big.Int).Lsh(big.NewInt(1), 64), big.NewInt(7))
	bigParts := xdr.Int128Parts{Hi: 1, Lo: 7}
	bigI128 := xdr.ScVal{Type: xdr.ScValTypeScvI128, I128: &bigParts}

	tradeTopics := func(second xdr.ScVal) []xdr.ScVal {
		return []xdr.ScVal{ammSym("trade"), second, ammAddr(usdcContractAddress), ammAddr(ammStranger)}
	}

	testCases := []struct {
		name     string
		event    xdr.ContractEvent
		wantOK   bool
		wantFill fill
		wantErr  string
	}{
		{
			name:   "trade decodes tokens from topics and amounts from data",
			event:  ammEvent(ammAquaPool, ammVec(ammI128(500), ammI128(1200), ammI128(3)), tradeTopics(ammAddr(ethContractAddress))...),
			wantOK: true,
			wantFill: fill{
				TokenIn: ethContractAddress, AmountIn: big.NewInt(500),
				TokenOut: usdcContractAddress, AmountOut: big.NewInt(1200),
				Venue: types.TradeVenueAquarius,
			},
		},
		{
			name:   "i128 with a high word",
			event:  ammEvent(ammAquaPool, ammVec(bigI128, ammI128(3), ammI128(0)), tradeTopics(ammAddr(ethContractAddress))...),
			wantOK: true,
			wantFill: fill{
				TokenIn: ethContractAddress, AmountIn: bigAmount,
				TokenOut: usdcContractAddress, AmountOut: big.NewInt(3),
				Venue: types.TradeVenueAquarius,
			},
		},
		{
			name:  "other pool event is ignored",
			event: ammEvent(ammAquaPool, ammU32(1), ammSym("deposit"), ammAddr(ammStranger)),
		},
		{
			name:    "token_in that is not an address is an error",
			event:   ammEvent(ammAquaPool, ammVec(ammI128(500), ammI128(1200), ammI128(3)), tradeTopics(ammSym("ETH"))...),
			wantErr: "topic 1 (token_in): want an address",
		},
		{
			name:    "out amount that is not i128 is an error",
			event:   ammEvent(ammAquaPool, ammVec(ammI128(500), ammU128(1200), ammI128(3)), tradeTopics(ammAddr(ethContractAddress))...),
			wantErr: "element 1 (out_amount): want i128",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			f, ok, err := decodeAquariusPoolTrade(tc.event)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				assert.False(t, ok)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantOK, ok)
			assert.Equal(t, tc.wantFill, f)
		})
	}
}

func TestAMMPoolsProcessor_ProcessOperation(t *testing.T) {
	newPair := func(emitter string) xdr.ContractEvent {
		return ammEvent(emitter, ammNewPairData(), ammStr("SoroswapFactory"), ammSym("new_pair"))
	}
	routerSwap := func(emitter string) xdr.ContractEvent {
		ev := ammRouterSwapEvent(ammVec(
			ammAddr(ammAquaPool), ammAddr(ethContractAddress), ammAddr(usdcContractAddress), ammU128(500), ammU128(1200),
		))
		id := ammContractID(emitter)
		ev.ContractId = &id
		return ev
	}
	venues := TradeVenues{SoroswapFactory: ammFactory}
	bothVenues := TradeVenues{SoroswapFactory: ammFactory, AquariusRouter: ammRouter}

	testCases := []struct {
		name      string
		venues    TradeVenues
		events    []xdr.ContractEvent
		wantPools int
	}{
		{name: "factory new_pair registers the pool", venues: venues, events: []xdr.ContractEvent{newPair(ammFactory)}, wantPools: 1},
		{name: "new_pair from another contract is ignored", venues: venues, events: []xdr.ContractEvent{newPair(ammStranger)}},
		{name: "no factory configured registers nothing", venues: TradeVenues{}, events: []xdr.ContractEvent{newPair(ammFactory)}},
		{name: "router swap registers its pool", venues: bothVenues, events: []xdr.ContractEvent{routerSwap(ammRouter)}, wantPools: 1},
		{name: "router swap from a non-router contract is ignored", venues: bothVenues, events: []xdr.ContractEvent{routerSwap(ammStranger)}},
		{
			name:   "malformed router swap and new_pair are skipped without an error",
			venues: bothVenues,
			events: []xdr.ContractEvent{
				ammRouterSwapEvent(ammVec(ammBytes32())),
				ammEvent(ammFactory, ammU32(1), ammStr("SoroswapFactory"), ammSym("new_pair")),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			registry := NewAMMPoolRegistry(nil)
			proc := NewAMMPoolsProcessor(tc.venues, registry, nil)

			pools, err := proc.ProcessOperation(context.Background(), ammTestOp(tc.events...))
			require.NoError(t, err)
			require.Len(t, pools, tc.wantPools)
			assert.Equal(t, tc.wantPools, registry.Len())
			if tc.wantPools == 0 {
				return
			}
			want := ammPool
			if tc.events[0].ContractId != nil && strkey.MustEncode(strkey.VersionByteContract, tc.events[0].ContractId[:]) == ammRouter {
				want = types.AMMPool{Pool: ammAquaPool, Venue: types.TradeVenueAquarius, Token0: ethContractAddress, Token1: usdcContractAddress}
			}
			want.CreatedLedger = 12345
			assert.Equal(t, want, pools[0])
			got, ok := registry.Get(want.Pool)
			require.True(t, ok)
			assert.Equal(t, want, got)
		})
	}

	t.Run("an already registered pool is not returned again", func(t *testing.T) {
		registry := NewAMMPoolRegistry(nil)
		proc := NewAMMPoolsProcessor(bothVenues, registry, nil)
		op := ammTestOp(routerSwap(ammRouter))

		pools, err := proc.ProcessOperation(context.Background(), op)
		require.NoError(t, err)
		require.Len(t, pools, 1)

		pools, err = proc.ProcessOperation(context.Background(), op)
		require.NoError(t, err)
		assert.Empty(t, pools)
		assert.Equal(t, 1, registry.Len())
	})
}

func TestTradesProcessor_AMMFillsTrustOnlyKnownContracts(t *testing.T) {
	venues := TradeVenues{
		XLMSAC:          nativeContractAddress,
		USDCSAC:         usdcContractAddress,
		SoroswapFactory: ammFactory,
		AquariusRouter:  ammRouter,
	}
	routerSwap := ammRouterSwapEvent(ammVec(
		ammAddr(ammAquaPool), ammAddr(ethContractAddress), ammAddr(usdcContractAddress), ammU128(500), ammU128(1200),
	))

	testCases := []struct {
		name      string
		event     xdr.ContractEvent
		wantTrade bool
		wantVenue types.TradeVenue
	}{
		{name: "swap from an unregistered contract is ignored", event: ammSwapEvent(ammStranger, ammSwapData(500, 0, 0, 1200))},
		{name: "swap from a registered pool is a trade", event: ammSwapEvent(ammPair, ammSwapData(500, 0, 0, 1200)), wantTrade: true, wantVenue: types.TradeVenueSoroswap},
		{name: "venue name encoded as a symbol is also a trade", event: ammEvent(ammPair, ammSwapData(500, 0, 0, 1200), ammSym("SoroswapPair"), ammSym("swap")), wantTrade: true, wantVenue: types.TradeVenueSoroswap},
		{name: "trade from a registered Aquarius pool is a trade", event: ammAquaTradeEvent(ammAquaPool, 500, 1200), wantTrade: true, wantVenue: types.TradeVenueAquarius},
		{name: "trade from an unregistered contract is ignored", event: ammAquaTradeEvent(ammStranger, 500, 1200)},
		{name: "router swap alone is never a trade", event: routerSwap},
		// An unreadable event from a trusted contract is skipped, never an error: one lost fill
		// must not stop the ledger.
		{name: "unreadable pool trade is skipped", event: ammEvent(ammAquaPool, ammU32(1), ammSym("trade"), ammAddr(ethContractAddress), ammAddr(usdcContractAddress), ammAddr(ammStranger))},
		{name: "unreadable pair swap is skipped", event: ammSwapEvent(ammPair, ammU32(1))},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			registry := NewAMMPoolRegistry([]types.AMMPool{ammPool, {Pool: ammAquaPool, Venue: types.TradeVenueAquarius, Token0: ethContractAddress, Token1: usdcContractAddress}})
			proc := NewTradesProcessor(networkPassphrase, venues, registry, nil)

			op := ammTestOp(tc.event)
			trades, err := proc.ProcessOperation(context.Background(), op)
			require.NoError(t, err)
			if !tc.wantTrade {
				assert.Empty(t, trades)
				return
			}
			require.Len(t, trades, 1)
			got := trades[0]
			assert.Equal(t, op.SourceAccount().ToAccountId().Address(), got.Taker, "AMM taker is the invoking account")
			assert.NotEmpty(t, got.Taker)
			// USDC is the anchor, so ETH is the base whichever side the taker paid.
			assert.Equal(t, ethContractAddress, got.BaseToken)
			assert.Equal(t, big.NewInt(500), got.BaseAmount)
			assert.Equal(t, usdcContractAddress, got.CounterToken)
			assert.Equal(t, big.NewInt(1200), got.CounterAmount)
			assert.Equal(t, tc.wantVenue, got.Venue)
			assert.Nil(t, got.BaseDecimals)
			assert.Equal(t, op.ID(), got.OperationID)
			assert.Equal(t, uint32(12345), got.LedgerNumber)
		})
	}
}

// The indexer runs the pools processor before the trades processor on each operation, so a routed
// hop is counted once, from the pool's own trade event, even when the pool is first seen there.
func TestAMMProcessors_RoutedHopIsCountedOnce(t *testing.T) {
	venues := TradeVenues{
		XLMSAC:         nativeContractAddress,
		USDCSAC:        usdcContractAddress,
		AquariusRouter: ammRouter,
	}
	registry := NewAMMPoolRegistry(nil)
	op := ammTestOp(
		ammAquaTradeEvent(ammAquaPool, 500, 1200),
		ammRouterSwapEvent(ammVec(
			ammAddr(ammAquaPool), ammAddr(ethContractAddress), ammAddr(usdcContractAddress), ammU128(500), ammU128(1200),
		)),
	)

	pools, err := NewAMMPoolsProcessor(venues, registry, nil).ProcessOperation(context.Background(), op)
	require.NoError(t, err)
	require.Len(t, pools, 1)

	trades, err := NewTradesProcessor(networkPassphrase, venues, registry, nil).ProcessOperation(context.Background(), op)
	require.NoError(t, err)
	require.Len(t, trades, 1)
	assert.Equal(t, types.TradeVenueAquarius, trades[0].Venue)
}
