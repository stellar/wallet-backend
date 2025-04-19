package utils

import (
	"testing"

	"github.com/stellar/go/keypair"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/pkg/utils"
)

func Test_BuildOperations(t *testing.T) {
	srcAccount := keypair.MustRandom().Address()
	dstAccount := keypair.MustRandom().Address()

	operations := []txnbuild.Operation{
		&txnbuild.CreateAccount{
			Destination:   dstAccount,
			Amount:        "10.0000000",
			SourceAccount: srcAccount,
		},
		&txnbuild.Payment{
			Destination:   dstAccount,
			Amount:        "10.0000000",
			Asset:         txnbuild.NativeAsset{},
			SourceAccount: srcAccount,
		},
		&txnbuild.ManageSellOffer{
			Selling:       txnbuild.NativeAsset{},
			Buying:        txnbuild.NativeAsset{},
			Amount:        "10.0000000",
			OfferID:       int64(1234),
			SourceAccount: srcAccount,
			Price:         xdr.Price{N: 10, D: 10},
		},
		&txnbuild.CreatePassiveSellOffer{
			Selling:       txnbuild.NativeAsset{},
			Buying:        txnbuild.NativeAsset{},
			Amount:        "10.0000000",
			Price:         xdr.Price{N: 10, D: 10},
			SourceAccount: srcAccount,
		},
		&txnbuild.SetOptions{
			SourceAccount: srcAccount,
		},
		&txnbuild.AccountMerge{
			Destination:   dstAccount,
			SourceAccount: srcAccount,
		},
		&txnbuild.Inflation{
			SourceAccount: srcAccount,
		},
		&txnbuild.ManageData{
			Name:          "foo",
			SourceAccount: srcAccount,
		},
		&txnbuild.BumpSequence{
			BumpTo:        int64(100),
			SourceAccount: srcAccount,
		},
		&txnbuild.ManageBuyOffer{
			Selling:       txnbuild.NativeAsset{},
			Buying:        txnbuild.NativeAsset{},
			Amount:        "10.0000000",
			Price:         xdr.Price{N: 10, D: 10},
			OfferID:       int64(100),
			SourceAccount: srcAccount,
		},
		&txnbuild.PathPaymentStrictSend{
			SendAsset:     txnbuild.NativeAsset{},
			SendAmount:    "10.0000000",
			Destination:   dstAccount,
			DestAsset:     txnbuild.NativeAsset{},
			DestMin:       "1.0000000",
			Path:          []txnbuild.Asset{},
			SourceAccount: srcAccount,
		},
		&txnbuild.CreateClaimableBalance{
			Amount:        "10.0000000",
			Asset:         txnbuild.NativeAsset{},
			SourceAccount: srcAccount,
		},
		&txnbuild.EndSponsoringFutureReserves{
			SourceAccount: srcAccount,
		},
		&txnbuild.LiquidityPoolDeposit{
			SourceAccount: srcAccount,
			MaxAmountA:    "10.0000000",
			MaxAmountB:    "10.0000000",
			MinPrice:      xdr.Price{N: 10, D: 10},
			MaxPrice:      xdr.Price{N: 10, D: 10},
		},
		&txnbuild.LiquidityPoolWithdraw{
			SourceAccount: srcAccount,
			Amount:        "10.0000000",
			MinAmountA:    "10.0000000",
			MinAmountB:    "10.0000000",
		},
		&txnbuild.ExtendFootprintTtl{
			ExtendTo:      uint32(10),
			SourceAccount: srcAccount,
		},
		&txnbuild.RestoreFootprint{
			SourceAccount: srcAccount,
		},
	}

	opXDRs := make([]string, len(operations))
	for i, op := range operations {
		opXDR, err := op.BuildXDR()
		require.NoError(t, err)
		opXDRs[i], err = utils.OperationXDRToBase64(opXDR)
		require.NoError(t, err)
	}

	ops, err := BuildOperations(opXDRs)
	require.NoError(t, err)

	assert.Equal(t, operations, ops)
}
