package processors

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/indexer/types"
)

func tradesTestVenues(t *testing.T) TradeVenues {
	t.Helper()
	v, err := TradeVenuesFor(network.PublicNetworkPassphrase)
	require.NoError(t, err)
	require.NotEmpty(t, v.USDCSAC)
	return v
}

func pathPaymentResult(atoms []xdr.ClaimAtom) *xdr.OperationResult {
	return &xdr.OperationResult{
		Code: xdr.OperationResultCodeOpInner,
		Tr: &xdr.OperationResultTr{
			Type: xdr.OperationTypePathPaymentStrictSend,
			PathPaymentStrictSendResult: &xdr.PathPaymentStrictSendResult{
				Code:    xdr.PathPaymentStrictSendResultCodePathPaymentStrictSendSuccess,
				Success: &xdr.PathPaymentStrictSendResultSuccess{Offers: atoms},
			},
		},
	}
}

func manageSellOfferResult(atoms []xdr.ClaimAtom) *xdr.OperationResult {
	return &xdr.OperationResult{
		Code: xdr.OperationResultCodeOpInner,
		Tr: &xdr.OperationResultTr{
			Type: xdr.OperationTypeManageSellOffer,
			ManageSellOfferResult: &xdr.ManageSellOfferResult{
				Code: xdr.ManageSellOfferResultCodeManageSellOfferSuccess,
				Success: &xdr.ManageOfferSuccessResult{
					OffersClaimed: atoms,
					Offer:         xdr.ManageOfferSuccessResultOffer{Effect: xdr.ManageOfferEffectManageOfferDeleted},
				},
			},
		},
	}
}

func TestTradesProcessor_ClassicFills(t *testing.T) {
	ctx := context.Background()
	venues := tradesTestVenues(t)
	usdc := xdr.MustNewCreditAsset("USDC", "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN")
	xlm := xdr.MustNewNativeAsset()
	yxlm := xdr.MustNewCreditAsset("yXLM", "GARDNV3Q7YGT4AKSDF25LT32YSCCW4EV22Y2TV3I2PU2MMXJTEDL5T55")
	yxlmSAC, err := SACAddress(yxlm, network.PublicNetworkPassphrase)
	require.NoError(t, err)

	seller := keypair.MustRandom().Address()
	sellerMuxed := xdr.MustMuxedAddress(seller)
	takerKP := keypair.MustRandom()
	taker := xdr.MustMuxedAddress(takerKP.Address())
	// The same account behind a muxed id: the taker is still its G-address.
	var takerKey xdr.Uint256
	copy(takerKey[:], strkey.MustDecode(strkey.VersionByteAccountID, takerKP.Address()))
	mTaker := xdr.MuxedAccount{Type: xdr.CryptoKeyTypeKeyTypeMuxedEd25519, Med25519: &xdr.MuxedAccountMed25519{Id: 42, Ed25519: takerKey}}
	poolID := xdr.PoolId{1, 2, 3}

	closed := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)

	testCases := []struct {
		name     string
		op       xdr.Operation
		result   *xdr.OperationResult
		failed   bool
		expected []types.Trade
	}{
		{
			name: "offer fill: token sold for XLM prices the token against XLM",
			op:   pathPaymentStrictSendOp(xlm, 100, taker, yxlm, 1, nil, &taker),
			// the maker sold 500 yXLM and bought 100 XLM
			result: pathPaymentResult([]xdr.ClaimAtom{
				generateClaimAtom(xdr.ClaimAtomTypeClaimAtomTypeOrderBook, &sellerMuxed, nil, yxlm, 500, xlm, 100),
			}),
			expected: []types.Trade{{
				BaseToken: yxlmSAC, BaseAmount: big.NewInt(500),
				CounterToken: venues.XLMSAC, CounterAmount: big.NewInt(100),
				Venue: types.TradeVenueSDEXOrderbook,
			}},
		},
		{
			name: "pool fill: XLM sold for USDC prices XLM against USDC",
			op:   manageSellOfferOp(&taker),
			result: manageSellOfferResult([]xdr.ClaimAtom{
				generateClaimAtom(xdr.ClaimAtomTypeClaimAtomTypeLiquidityPool, nil, &poolID, xlm, 1_000_000_0, usdc, 190_000_0),
			}),
			expected: []types.Trade{{
				BaseToken: venues.XLMSAC, BaseAmount: big.NewInt(1_000_000_0),
				CounterToken: venues.USDCSAC, CounterAmount: big.NewInt(190_000_0),
				Venue: types.TradeVenueSDEXPool,
			}},
		},
		{
			name: "anchor as the bought side still lands as the counter",
			op:   manageSellOfferOp(&taker),
			// the maker sold 100 XLM and bought 500 yXLM: same pair, opposite direction
			result: manageSellOfferResult([]xdr.ClaimAtom{
				generateClaimAtom(xdr.ClaimAtomTypeClaimAtomTypeOrderBook, &sellerMuxed, nil, xlm, 100, yxlm, 500),
			}),
			expected: []types.Trade{{
				BaseToken: yxlmSAC, BaseAmount: big.NewInt(500),
				CounterToken: venues.XLMSAC, CounterAmount: big.NewInt(100),
				Venue: types.TradeVenueSDEXOrderbook,
			}},
		},
		{
			name: "muxed source account is reduced to its G-address taker",
			op:   pathPaymentStrictSendOp(xlm, 100, taker, yxlm, 1, nil, &mTaker),
			result: pathPaymentResult([]xdr.ClaimAtom{
				generateClaimAtom(xdr.ClaimAtomTypeClaimAtomTypeOrderBook, &sellerMuxed, nil, yxlm, 500, xlm, 100),
			}),
			expected: []types.Trade{{
				BaseToken: yxlmSAC, BaseAmount: big.NewInt(500),
				CounterToken: venues.XLMSAC, CounterAmount: big.NewInt(100),
				Venue: types.TradeVenueSDEXOrderbook,
			}},
		},
		{
			name: "multiple atoms keep meta order and index",
			op:   pathPaymentStrictSendOp(xlm, 100, taker, yxlm, 1, nil, &taker),
			result: pathPaymentResult([]xdr.ClaimAtom{
				generateClaimAtom(xdr.ClaimAtomTypeClaimAtomTypeOrderBook, &sellerMuxed, nil, yxlm, 300, xlm, 60),
				generateClaimAtom(xdr.ClaimAtomTypeClaimAtomTypeOrderBook, &sellerMuxed, nil, yxlm, 200, xlm, 40),
			}),
			expected: []types.Trade{
				{BaseToken: yxlmSAC, BaseAmount: big.NewInt(300), CounterToken: venues.XLMSAC, CounterAmount: big.NewInt(60), Venue: types.TradeVenueSDEXOrderbook},
				{BaseToken: yxlmSAC, BaseAmount: big.NewInt(200), CounterToken: venues.XLMSAC, CounterAmount: big.NewInt(40), Venue: types.TradeVenueSDEXOrderbook},
			},
		},
		{
			name: "zero-amount atoms are dropped",
			op:   manageSellOfferOp(&taker),
			result: manageSellOfferResult([]xdr.ClaimAtom{
				generateClaimAtom(xdr.ClaimAtomTypeClaimAtomTypeOrderBook, &sellerMuxed, nil, yxlm, 0, xlm, 0),
			}),
			expected: nil,
		},
		{
			name:     "failed transactions have no fills",
			op:       manageSellOfferOp(&taker),
			result:   manageSellOfferResult([]xdr.ClaimAtom{generateClaimAtom(xdr.ClaimAtomTypeClaimAtomTypeOrderBook, &sellerMuxed, nil, yxlm, 500, xlm, 100)}),
			failed:   true,
			expected: nil,
		},
		{
			name:     "operations without fills yield nothing",
			op:       setTrustlineFlagsOp(),
			expected: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			p := NewTradesProcessor(network.PublicNetworkPassphrase, venues, NewAMMPoolRegistry(nil), nil)
			tx := createTx(tc.op, nil, tc.result, tc.failed)
			opWrapper := &TransactionOperationWrapper{
				Index:          0,
				Transaction:    tx,
				Operation:      tc.op,
				LedgerSequence: 1000,
				Network:        network.PublicNetworkPassphrase,
				LedgerClosed:   closed,
			}
			got, err := p.ProcessOperation(ctx, opWrapper)
			require.NoError(t, err)
			require.Len(t, got, len(tc.expected))
			for i, want := range tc.expected {
				assert.Equal(t, want.BaseToken, got[i].BaseToken, "base token")
				assert.Equal(t, want.CounterToken, got[i].CounterToken, "counter token")
				assert.Equal(t, 0, want.BaseAmount.Cmp(got[i].BaseAmount), "base amount")
				assert.Equal(t, 0, want.CounterAmount.Cmp(got[i].CounterAmount), "counter amount")
				assert.Equal(t, want.Venue, got[i].Venue)
				assert.Equal(t, int16(i), got[i].FillIndex)
				assert.Equal(t, opWrapper.ID(), got[i].OperationID)
				assert.Equal(t, uint32(1000), got[i].LedgerNumber)
				assert.Equal(t, closed, got[i].LedgerClosed)
				assert.Equal(t, takerKP.Address(), got[i].Taker, "taker is the operation source's account")
				require.NotNil(t, got[i].BaseDecimals)
				assert.EqualValues(t, 7, *got[i].BaseDecimals)
				require.NotNil(t, got[i].CounterDecimals)
				assert.EqualValues(t, 7, *got[i].CounterDecimals)
			}
		})
	}
}

func TestTradesProcessor_Orient(t *testing.T) {
	venues := tradesTestVenues(t)
	p := NewTradesProcessor(network.PublicNetworkPassphrase, venues, NewAMMPoolRegistry(nil), nil)
	a := "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAB"
	b := "CBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"

	testCases := []struct {
		name                  string
		in, out               string
		wantBase, wantCounter string
	}{
		{"USDC beats XLM", venues.XLMSAC, venues.USDCSAC, venues.XLMSAC, venues.USDCSAC},
		{"USDC beats XLM, reversed", venues.USDCSAC, venues.XLMSAC, venues.XLMSAC, venues.USDCSAC},
		{"XLM beats other", a, venues.XLMSAC, a, venues.XLMSAC},
		{"two non-anchors: lower address is the counter", a, b, b, a},
		{"two non-anchors, reversed", b, a, b, a},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tr, ok := p.orient(fill{TokenIn: tc.in, AmountIn: big.NewInt(1), TokenOut: tc.out, AmountOut: big.NewInt(2)})
			require.True(t, ok)
			assert.Equal(t, tc.wantBase, tr.BaseToken)
			assert.Equal(t, tc.wantCounter, tr.CounterToken)
		})
	}

	_, ok := p.orient(fill{TokenIn: a, AmountIn: big.NewInt(0), TokenOut: b, AmountOut: big.NewInt(2)})
	assert.False(t, ok, "zero amount is unpriced")
}
