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

func ammU128(hi, lo uint64) xdr.ScVal {
	parts := xdr.UInt128Parts{Hi: xdr.Uint64(hi), Lo: xdr.Uint64(lo)}
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
	return ammEvent(emitter, data, ammSym("SoroswapPair"), ammSym("swap"))
}

func ammRouterSwapEvent(data xdr.ScVal) xdr.ContractEvent {
	return ammEvent(ammRouter, data,
		ammSym("swap"),
		ammVec(ammAddr(ethContractAddress), ammAddr(usdcContractAddress)),
		ammAddr(ammStranger),
	)
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
			event:  ammEvent(ammFactory, ammNewPairData(), ammSym("SoroswapFactory"), ammSym("new_pair")),
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
			event: ammEvent(ammFactory, ammU32(1), ammSym("SoroswapFactory"), ammSym("fees_enabled")),
		},
		{
			name:  "wrong first topic is ignored",
			event: ammEvent(ammFactory, ammNewPairData(), ammSym("SoroswapPair"), ammSym("new_pair")),
		},
		{
			name:  "no topics is ignored",
			event: ammEvent(ammFactory, ammNewPairData()),
		},
		{
			name:    "data that is not a map is an error",
			event:   ammEvent(ammFactory, ammU32(1), ammSym("SoroswapFactory"), ammSym("new_pair")),
			wantErr: "want a symbol-keyed map, found ScValTypeScvU32",
		},
		{
			name: "missing token_1 is an error",
			event: ammEvent(ammFactory,
				ammMap("pair", ammAddr(ammPair), "token_0", ammAddr(ethContractAddress)),
				ammSym("SoroswapFactory"), ammSym("new_pair")),
			wantErr: `field "token_1" is missing`,
		},
		{
			name: "pair that is not an address is an error",
			event: ammEvent(ammFactory,
				ammMap("pair", ammSym("x"), "token_0", ammAddr(ethContractAddress), "token_1", ammAddr(usdcContractAddress)),
				ammSym("SoroswapFactory"), ammSym("new_pair")),
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
			event: ammEvent(ammPair, ammSwapData(1, 0, 0, 1), ammSym("SoroswapPair"), ammSym("sync")),
		},
		{
			name:  "wrong topic with malformed data is ignored",
			event: ammEvent(ammPair, ammU32(1), ammSym("SoroswapPair"), ammSym("deposit")),
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
				"amount_0_in", ammU128(0, 5), "amount_0_out", ammI128(0),
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

func TestDecodeAquariusRouterSwap(t *testing.T) {
	// 2^64 + 7: a u128 whose high word is set.
	bigAmount := new(big.Int).Add(new(big.Int).Lsh(big.NewInt(1), 64), big.NewInt(7))

	testCases := []struct {
		name     string
		event    xdr.ContractEvent
		wantOK   bool
		wantFill fill
		wantErr  string
	}{
		{
			name: "swap decodes positionally",
			event: ammRouterSwapEvent(ammVec(
				ammBytes32(), ammAddr(ethContractAddress), ammAddr(usdcContractAddress), ammU128(0, 500), ammU128(0, 1200),
			)),
			wantOK: true,
			wantFill: fill{
				TokenIn: ethContractAddress, AmountIn: big.NewInt(500),
				TokenOut: usdcContractAddress, AmountOut: big.NewInt(1200),
				Venue: types.TradeVenueAquarius,
			},
		},
		{
			name: "u128 with a high word",
			event: ammRouterSwapEvent(ammVec(
				ammBytes32(), ammAddr(usdcContractAddress), ammAddr(ethContractAddress), ammU128(1, 7), ammU128(0, 3),
			)),
			wantOK: true,
			wantFill: fill{
				TokenIn: usdcContractAddress, AmountIn: bigAmount,
				TokenOut: ethContractAddress, AmountOut: big.NewInt(3),
				Venue: types.TradeVenueAquarius,
			},
		},
		{
			name:  "other router event is ignored",
			event: ammEvent(ammRouter, ammU32(1), ammSym("deposit"), ammAddr(ammStranger)),
		},
		{
			name:    "data that is not a vec is an error",
			event:   ammRouterSwapEvent(ammMap("in_amount", ammU128(0, 1))),
			wantErr: "want a vec, found ScValTypeScvMap",
		},
		{
			name: "vec of the wrong length is an error",
			event: ammRouterSwapEvent(ammVec(
				ammBytes32(), ammAddr(ethContractAddress), ammAddr(usdcContractAddress), ammU128(0, 500),
			)),
			wantErr: "want 5 elements, found 4",
		},
		{
			name: "pool element encoded as an address decodes like bytes",
			event: ammRouterSwapEvent(ammVec(
				ammAddr(ethContractAddress), ammAddr(ethContractAddress), ammAddr(usdcContractAddress), ammU128(0, 500), ammU128(0, 1200),
			)),
			wantOK:   true,
			wantFill: fill{TokenIn: ethContractAddress, AmountIn: big.NewInt(500), TokenOut: usdcContractAddress, AmountOut: big.NewInt(1200), Venue: types.TradeVenueAquarius},
		},
		{
			name: "token that is not an address is an error",
			event: ammRouterSwapEvent(ammVec(
				ammBytes32(), ammAddr(ethContractAddress), ammSym("USDC"), ammU128(0, 500), ammU128(0, 1200),
			)),
			wantErr: "element 2 (token_out): want an address, found ScValTypeScvSymbol",
		},
		{
			name: "i128 amount is an error",
			event: ammRouterSwapEvent(ammVec(
				ammBytes32(), ammAddr(ethContractAddress), ammAddr(usdcContractAddress), ammI128(500), ammU128(0, 1200),
			)),
			wantErr: "element 3 (in_amount): want u128, found ScValTypeScvI128",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			f, ok, err := decodeAquariusRouterSwap(tc.event)
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
		return ammEvent(emitter, ammNewPairData(), ammSym("SoroswapFactory"), ammSym("new_pair"))
	}
	venues := TradeVenues{SoroswapFactory: ammFactory}

	testCases := []struct {
		name      string
		venues    TradeVenues
		events    []xdr.ContractEvent
		wantPools int
	}{
		{name: "factory new_pair registers the pool", venues: venues, events: []xdr.ContractEvent{newPair(ammFactory)}, wantPools: 1},
		{name: "new_pair from another contract is ignored", venues: venues, events: []xdr.ContractEvent{newPair(ammStranger)}},
		{name: "no factory configured registers nothing", venues: TradeVenues{}, events: []xdr.ContractEvent{newPair(ammFactory)}},
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
			want.CreatedLedger = 12345
			assert.Equal(t, want, pools[0])
			got, ok := registry.Get(ammPair)
			require.True(t, ok)
			assert.Equal(t, want, got)
		})
	}
}

func TestTradesProcessor_AMMFillsTrustOnlyKnownContracts(t *testing.T) {
	venues := TradeVenues{
		XLMSAC:          nativeContractAddress,
		USDCSAC:         usdcContractAddress,
		SoroswapFactory: ammFactory,
		AquariusRouter:  ammRouter,
	}
	routerSwap := ammRouterSwapEvent(ammVec(
		ammBytes32(), ammAddr(ethContractAddress), ammAddr(usdcContractAddress), ammU128(0, 500), ammU128(0, 1200),
	))

	testCases := []struct {
		name      string
		event     xdr.ContractEvent
		wantTrade bool
		wantVenue types.TradeVenue
	}{
		{name: "swap from an unregistered contract is ignored", event: ammSwapEvent(ammStranger, ammSwapData(500, 0, 0, 1200))},
		{name: "swap from a registered pool is a trade", event: ammSwapEvent(ammPair, ammSwapData(500, 0, 0, 1200)), wantTrade: true, wantVenue: types.TradeVenueSoroswap},
		{name: "swap from the router is a trade", event: routerSwap, wantTrade: true, wantVenue: types.TradeVenueAquarius},
		// An unreadable event from a trusted contract is skipped, never an error: one lost fill
		// must not stop the ledger.
		{name: "unreadable router swap is skipped", event: ammRouterSwapEvent(ammVec(ammBytes32(), ammAddr(ethContractAddress)))},
		{name: "unreadable pair swap is skipped", event: ammSwapEvent(ammPair, ammU32(1))},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			registry := NewAMMPoolRegistry([]types.AMMPool{ammPool})
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
