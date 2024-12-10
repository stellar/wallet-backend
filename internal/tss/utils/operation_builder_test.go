package utils

import (
	"encoding/base64"
	"strings"
	"testing"

	xdr3 "github.com/stellar/go-xdr/xdr3"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
)

func TestBuildOperations(t *testing.T) {
	t.Run("op_createaccount", func(t *testing.T) {
		srcAccount := keypair.MustRandom().Address()
		dstAccount := keypair.MustRandom().Address()
		c := txnbuild.CreateAccount{
			Destination:   dstAccount,
			Amount:        "10",
			SourceAccount: srcAccount,
		}
		op, _ := c.BuildXDR()
		var buf strings.Builder
		enc := xdr3.NewEncoder(&buf)
		_ = op.EncodeTo(enc)
		opXDR := buf.String()
		opXDRBase64 := base64.StdEncoding.EncodeToString([]byte(opXDR))

		ops, _ := BuildOperations([]string{opXDRBase64})

		assert.Equal(t, srcAccount, ops[0].GetSourceAccount())
		assert.Equal(t, dstAccount, ops[0].(*txnbuild.CreateAccount).Destination)
		assert.Equal(t, string("10.0000000"), ops[0].(*txnbuild.CreateAccount).Amount)
	})
	t.Run("op_payment", func(t *testing.T) {
		srcAccount := keypair.MustRandom().Address()
		dstAccount := keypair.MustRandom().Address()
		p := txnbuild.Payment{
			Destination:   dstAccount,
			Amount:        "10",
			Asset:         txnbuild.NativeAsset{},
			SourceAccount: srcAccount,
		}
		op, _ := p.BuildXDR()
		var buf strings.Builder
		enc := xdr3.NewEncoder(&buf)
		_ = op.EncodeTo(enc)
		opXDR := buf.String()
		opXDRBase64 := base64.StdEncoding.EncodeToString([]byte(opXDR))

		ops, _ := BuildOperations([]string{opXDRBase64})

		assert.Equal(t, srcAccount, ops[0].GetSourceAccount())
		assert.Equal(t, string("10.0000000"), ops[0].(*txnbuild.Payment).Amount)
		assert.Equal(t, dstAccount, ops[0].(*txnbuild.Payment).Destination)
		assert.Equal(t, txnbuild.NativeAsset{}, ops[0].(*txnbuild.Payment).Asset)
	})

	t.Run("op_manage_sell_offer", func(t *testing.T) {
		srcAccount := keypair.MustRandom().Address()
		m := txnbuild.ManageSellOffer{
			Selling:       txnbuild.NativeAsset{},
			Buying:        txnbuild.NativeAsset{},
			Amount:        "10",
			OfferID:       int64(1234),
			SourceAccount: srcAccount,
			Price:         xdr.Price{N: 10, D: 10},
		}
		op, _ := m.BuildXDR()
		var buf strings.Builder
		enc := xdr3.NewEncoder(&buf)
		_ = op.EncodeTo(enc)
		opXDR := buf.String()
		opXDRBase64 := base64.StdEncoding.EncodeToString([]byte(opXDR))

		ops, _ := BuildOperations([]string{opXDRBase64})

		assert.Equal(t, srcAccount, ops[0].GetSourceAccount())
		assert.Equal(t, string("10.0000000"), ops[0].(*txnbuild.ManageSellOffer).Amount)
		assert.Equal(t, int64(1234), ops[0].(*txnbuild.ManageSellOffer).OfferID)
		assert.Equal(t, txnbuild.NativeAsset{}, ops[0].(*txnbuild.ManageSellOffer).Selling)
		assert.Equal(t, txnbuild.NativeAsset{}, ops[0].(*txnbuild.ManageSellOffer).Buying)
		assert.Equal(t, xdr.Price{N: 10, D: 10}, ops[0].(*txnbuild.ManageSellOffer).Price)
	})

	t.Run("op_create_passive_sell_offer", func(t *testing.T) {
		srcAccount := keypair.MustRandom().Address()
		c := txnbuild.CreatePassiveSellOffer{
			Selling:       txnbuild.NativeAsset{},
			Buying:        txnbuild.NativeAsset{},
			Amount:        "10",
			Price:         xdr.Price{N: 10, D: 10},
			SourceAccount: srcAccount,
		}
		op, _ := c.BuildXDR()
		var buf strings.Builder
		enc := xdr3.NewEncoder(&buf)
		_ = op.EncodeTo(enc)
		opXDR := buf.String()
		opXDRBase64 := base64.StdEncoding.EncodeToString([]byte(opXDR))

		ops, _ := BuildOperations([]string{opXDRBase64})

		assert.Equal(t, srcAccount, ops[0].GetSourceAccount())
		assert.Equal(t, string("10.0000000"), ops[0].(*txnbuild.CreatePassiveSellOffer).Amount)
		assert.Equal(t, txnbuild.NativeAsset{}, ops[0].(*txnbuild.CreatePassiveSellOffer).Selling)
		assert.Equal(t, txnbuild.NativeAsset{}, ops[0].(*txnbuild.CreatePassiveSellOffer).Buying)
		assert.Equal(t, xdr.Price{N: 10, D: 10}, ops[0].(*txnbuild.CreatePassiveSellOffer).Price)
	})

	t.Run("op_set_options", func(t *testing.T) {
		srcAccount := keypair.MustRandom().Address()
		s := txnbuild.SetOptions{
			SourceAccount: srcAccount,
		}
		op, _ := s.BuildXDR()
		var buf strings.Builder
		enc := xdr3.NewEncoder(&buf)
		_ = op.EncodeTo(enc)
		opXDR := buf.String()
		opXDRBase64 := base64.StdEncoding.EncodeToString([]byte(opXDR))

		ops, _ := BuildOperations([]string{opXDRBase64})

		assert.Equal(t, srcAccount, ops[0].GetSourceAccount())
	})

	t.Run("op_account_merge", func(t *testing.T) {
		srcAccount := keypair.MustRandom().Address()
		dstAccount := keypair.MustRandom().Address()
		a := txnbuild.AccountMerge{
			Destination:   dstAccount,
			SourceAccount: srcAccount,
		}
		op, _ := a.BuildXDR()
		var buf strings.Builder
		enc := xdr3.NewEncoder(&buf)
		_ = op.EncodeTo(enc)
		opXDR := buf.String()
		opXDRBase64 := base64.StdEncoding.EncodeToString([]byte(opXDR))

		ops, _ := BuildOperations([]string{opXDRBase64})

		assert.Equal(t, srcAccount, ops[0].GetSourceAccount())
		assert.Equal(t, dstAccount, ops[0].(*txnbuild.AccountMerge).Destination)
	})

	t.Run("op_inflation", func(t *testing.T) {
		srcAccount := keypair.MustRandom().Address()
		i := txnbuild.Inflation{
			SourceAccount: srcAccount,
		}
		op, _ := i.BuildXDR()
		var buf strings.Builder
		enc := xdr3.NewEncoder(&buf)
		_ = op.EncodeTo(enc)
		opXDR := buf.String()
		opXDRBase64 := base64.StdEncoding.EncodeToString([]byte(opXDR))

		ops, _ := BuildOperations([]string{opXDRBase64})

		assert.Equal(t, srcAccount, ops[0].GetSourceAccount())
	})

	t.Run("op_manage_data", func(t *testing.T) {
		srcAccount := keypair.MustRandom().Address()
		m := txnbuild.ManageData{
			Name:          "foo",
			SourceAccount: srcAccount,
		}
		op, _ := m.BuildXDR()
		var buf strings.Builder
		enc := xdr3.NewEncoder(&buf)
		_ = op.EncodeTo(enc)
		opXDR := buf.String()
		opXDRBase64 := base64.StdEncoding.EncodeToString([]byte(opXDR))

		ops, _ := BuildOperations([]string{opXDRBase64})

		assert.Equal(t, srcAccount, ops[0].GetSourceAccount())
		assert.Equal(t, "foo", ops[0].(*txnbuild.ManageData).Name)
	})

	t.Run("op_bump_sequence", func(t *testing.T) {
		srcAccount := keypair.MustRandom().Address()
		b := txnbuild.BumpSequence{
			BumpTo:        int64(100),
			SourceAccount: srcAccount,
		}
		op, _ := b.BuildXDR()
		var buf strings.Builder
		enc := xdr3.NewEncoder(&buf)
		_ = op.EncodeTo(enc)
		opXDR := buf.String()
		opXDRBase64 := base64.StdEncoding.EncodeToString([]byte(opXDR))

		ops, _ := BuildOperations([]string{opXDRBase64})

		assert.Equal(t, srcAccount, ops[0].GetSourceAccount())
		assert.Equal(t, int64(100), ops[0].(*txnbuild.BumpSequence).BumpTo)
	})

	t.Run("op_manage_buy_offer", func(t *testing.T) {
		srcAccount := keypair.MustRandom().Address()
		m := txnbuild.ManageBuyOffer{
			Selling:       txnbuild.NativeAsset{},
			Buying:        txnbuild.NativeAsset{},
			Amount:        "10",
			Price:         xdr.Price{N: 10, D: 10},
			OfferID:       int64(100),
			SourceAccount: srcAccount,
		}
		op, _ := m.BuildXDR()
		var buf strings.Builder
		enc := xdr3.NewEncoder(&buf)
		_ = op.EncodeTo(enc)
		opXDR := buf.String()
		opXDRBase64 := base64.StdEncoding.EncodeToString([]byte(opXDR))

		ops, _ := BuildOperations([]string{opXDRBase64})

		assert.Equal(t, srcAccount, ops[0].GetSourceAccount())
		assert.Equal(t, txnbuild.NativeAsset{}, ops[0].(*txnbuild.ManageBuyOffer).Selling)
		assert.Equal(t, txnbuild.NativeAsset{}, ops[0].(*txnbuild.ManageBuyOffer).Buying)
		assert.Equal(t, string("10.0000000"), ops[0].(*txnbuild.ManageBuyOffer).Amount)
		assert.Equal(t, xdr.Price{N: 10, D: 10}, ops[0].(*txnbuild.ManageBuyOffer).Price)
		assert.Equal(t, int64(100), ops[0].(*txnbuild.ManageBuyOffer).OfferID)
	})

	t.Run("op_path_payment_strict_send", func(t *testing.T) {
		srcAccount := keypair.MustRandom().Address()
		dstAccount := keypair.MustRandom().Address()
		p := txnbuild.PathPaymentStrictSend{
			SendAsset:     txnbuild.NativeAsset{},
			SendAmount:    "10",
			Destination:   dstAccount,
			DestAsset:     txnbuild.NativeAsset{},
			DestMin:       "1",
			Path:          []txnbuild.Asset{},
			SourceAccount: srcAccount,
		}
		op, _ := p.BuildXDR()
		var buf strings.Builder
		enc := xdr3.NewEncoder(&buf)
		_ = op.EncodeTo(enc)
		opXDR := buf.String()
		opXDRBase64 := base64.StdEncoding.EncodeToString([]byte(opXDR))

		ops, _ := BuildOperations([]string{opXDRBase64})

		assert.Equal(t, srcAccount, ops[0].GetSourceAccount())
		assert.Equal(t, txnbuild.NativeAsset{}, ops[0].(*txnbuild.PathPaymentStrictSend).SendAsset)
		assert.Equal(t, "10.0000000", ops[0].(*txnbuild.PathPaymentStrictSend).SendAmount)
		assert.Equal(t, dstAccount, ops[0].(*txnbuild.PathPaymentStrictSend).Destination)
		assert.Equal(t, txnbuild.NativeAsset{}, ops[0].(*txnbuild.PathPaymentStrictSend).DestAsset)
		assert.Equal(t, "1.0000000", ops[0].(*txnbuild.PathPaymentStrictSend).DestMin)
		assert.Equal(t, []txnbuild.Asset{}, ops[0].(*txnbuild.PathPaymentStrictSend).Path)
	})

	t.Run("op_create_claimable_balance", func(t *testing.T) {
		srcAccount := keypair.MustRandom().Address()
		c := txnbuild.CreateClaimableBalance{
			Amount:        "10",
			Asset:         txnbuild.NativeAsset{},
			SourceAccount: srcAccount,
		}
		op, _ := c.BuildXDR()
		var buf strings.Builder
		enc := xdr3.NewEncoder(&buf)
		_ = op.EncodeTo(enc)
		opXDR := buf.String()
		opXDRBase64 := base64.StdEncoding.EncodeToString([]byte(opXDR))

		ops, _ := BuildOperations([]string{opXDRBase64})

		assert.Equal(t, srcAccount, ops[0].GetSourceAccount())
		assert.Equal(t, "10.0000000", ops[0].(*txnbuild.CreateClaimableBalance).Amount)
		assert.Equal(t, txnbuild.NativeAsset{}, ops[0].(*txnbuild.CreateClaimableBalance).Asset)
	})

	t.Run("op_end_sponsoring_future_reserves", func(t *testing.T) {
		srcAccount := keypair.MustRandom().Address()
		e := txnbuild.EndSponsoringFutureReserves{
			SourceAccount: srcAccount,
		}
		op, _ := e.BuildXDR()
		var buf strings.Builder
		enc := xdr3.NewEncoder(&buf)
		_ = op.EncodeTo(enc)
		opXDR := buf.String()
		opXDRBase64 := base64.StdEncoding.EncodeToString([]byte(opXDR))

		ops, _ := BuildOperations([]string{opXDRBase64})

		assert.Equal(t, srcAccount, ops[0].GetSourceAccount())
	})
	t.Run("op_liquidity_pool_deposit", func(t *testing.T) {
		srcAccount := keypair.MustRandom().Address()
		l := txnbuild.LiquidityPoolDeposit{
			SourceAccount: srcAccount,
			MaxAmountA:    "10",
			MaxAmountB:    "10",
			MinPrice:      xdr.Price{N: 10, D: 10},
			MaxPrice:      xdr.Price{N: 10, D: 10},
		}
		op, _ := l.BuildXDR()
		var buf strings.Builder
		enc := xdr3.NewEncoder(&buf)
		_ = op.EncodeTo(enc)
		opXDR := buf.String()
		opXDRBase64 := base64.StdEncoding.EncodeToString([]byte(opXDR))

		ops, _ := BuildOperations([]string{opXDRBase64})

		assert.Equal(t, srcAccount, ops[0].GetSourceAccount())
		assert.Equal(t, "10.0000000", ops[0].(*txnbuild.LiquidityPoolDeposit).MaxAmountA)
		assert.Equal(t, "10.0000000", ops[0].(*txnbuild.LiquidityPoolDeposit).MaxAmountB)
		assert.Equal(t, xdr.Price{N: 10, D: 10}, ops[0].(*txnbuild.LiquidityPoolDeposit).MinPrice)
		assert.Equal(t, xdr.Price{N: 10, D: 10}, ops[0].(*txnbuild.LiquidityPoolDeposit).MaxPrice)
	})

	t.Run("op_liquidity_pool_withdraw", func(t *testing.T) {
		srcAccount := keypair.MustRandom().Address()
		l := txnbuild.LiquidityPoolWithdraw{
			SourceAccount: srcAccount,
			Amount:        "10",
			MinAmountA:    "10",
			MinAmountB:    "10",
		}
		op, _ := l.BuildXDR()
		var buf strings.Builder
		enc := xdr3.NewEncoder(&buf)
		_ = op.EncodeTo(enc)
		opXDR := buf.String()
		opXDRBase64 := base64.StdEncoding.EncodeToString([]byte(opXDR))

		ops, _ := BuildOperations([]string{opXDRBase64})

		assert.Equal(t, srcAccount, ops[0].GetSourceAccount())
		assert.Equal(t, "10.0000000", ops[0].(*txnbuild.LiquidityPoolWithdraw).Amount)
		assert.Equal(t, "10.0000000", ops[0].(*txnbuild.LiquidityPoolWithdraw).MinAmountA)
		assert.Equal(t, "10.0000000", ops[0].(*txnbuild.LiquidityPoolWithdraw).MinAmountB)
	})

	t.Run("op_extend_footprint_ttl", func(t *testing.T) {
		srcAccount := keypair.MustRandom().Address()
		e := txnbuild.ExtendFootprintTtl{
			ExtendTo:      uint32(10),
			SourceAccount: srcAccount,
		}
		op, _ := e.BuildXDR()
		var buf strings.Builder
		enc := xdr3.NewEncoder(&buf)
		_ = op.EncodeTo(enc)
		opXDR := buf.String()
		opXDRBase64 := base64.StdEncoding.EncodeToString([]byte(opXDR))

		ops, _ := BuildOperations([]string{opXDRBase64})

		assert.Equal(t, srcAccount, ops[0].GetSourceAccount())
	})

	t.Run("op_restore_footprint", func(t *testing.T) {
		srcAccount := keypair.MustRandom().Address()
		r := txnbuild.RestoreFootprint{
			SourceAccount: srcAccount,
		}
		op, _ := r.BuildXDR()
		var buf strings.Builder
		enc := xdr3.NewEncoder(&buf)
		_ = op.EncodeTo(enc)
		opXDR := buf.String()
		opXDRBase64 := base64.StdEncoding.EncodeToString([]byte(opXDR))

		ops, _ := BuildOperations([]string{opXDRBase64})

		assert.Equal(t, srcAccount, ops[0].GetSourceAccount())
	})

}
