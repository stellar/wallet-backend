package types

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_BlendSupplyChange_DecodesViaStateChangeNode(t *testing.T) {
	payload := []byte(`{
		"__typename": "BlendSupplyChange",
		"category": "BLEND_SUPPLY",
		"reason": "CREDIT",
		"ingestedAt": "2024-01-02T00:00:00Z",
		"ledgerCreatedAt": "2024-01-01T00:00:00Z",
		"ledgerNumber": 42,
		"blendSupplyTokenId": "CDMLFMKMMD7MWZP3FKUBZPVHTUEDLSX4BYGYKH4GCESXYHS3IHQ4EIG4",
		"amount": "1000000000",
		"poolId": "CPOOLADDRESS"
	}`)

	node, err := UnmarshalStateChangeNode(payload)
	require.NoError(t, err)

	sc, ok := node.(*BlendSupplyChange)
	require.True(t, ok, "expected *BlendSupplyChange, got %T", node)
	assert.Equal(t, "CDMLFMKMMD7MWZP3FKUBZPVHTUEDLSX4BYGYKH4GCESXYHS3IHQ4EIG4", sc.TokenID)
	assert.Equal(t, "1000000000", sc.Amount)
	assert.Equal(t, "CPOOLADDRESS", sc.PoolID)
	assert.Equal(t, uint32(42), sc.GetLedgerNumber())
	assert.Equal(t, StateChangeCategory("BLEND_SUPPLY"), sc.GetCategory())
}

func Test_BlendBackstopEmissionsClaimChange_DecodesWithoutTokenOrPool(t *testing.T) {
	payload := []byte(`{
		"__typename": "BlendBackstopEmissionsClaimChange",
		"category": "BLEND_BACKSTOP_EMISSIONS",
		"reason": "CLAIM",
		"ingestedAt": "2024-01-02T00:00:00Z",
		"ledgerCreatedAt": "2024-01-01T00:00:00Z",
		"ledgerNumber": 7,
		"amount": "250"
	}`)

	node, err := UnmarshalStateChangeNode(payload)
	require.NoError(t, err)

	sc, ok := node.(*BlendBackstopEmissionsClaimChange)
	require.True(t, ok, "expected *BlendBackstopEmissionsClaimChange, got %T", node)
	assert.Equal(t, "250", sc.Amount)
}

func Test_BlendAuctionChange_DecodesLotAndBid(t *testing.T) {
	payload := []byte(`{
		"__typename": "BlendAuctionChange",
		"category": "BLEND_AUCTION",
		"reason": "FILL",
		"ingestedAt": "2024-01-02T00:00:00Z",
		"ledgerCreatedAt": "2024-01-01T00:00:00Z",
		"ledgerNumber": 9,
		"poolId": "CPOOLADDRESS",
		"auctionType": "USER_LIQUIDATION",
		"fillPercent": 100,
		"counterparty": "GFILLER",
		"lot": [{"assetContractId": "CASSET1", "amount": "500"}],
		"bid": [{"assetContractId": "CASSET2", "amount": "300"}]
	}`)

	node, err := UnmarshalStateChangeNode(payload)
	require.NoError(t, err)

	sc, ok := node.(*BlendAuctionChange)
	require.True(t, ok, "expected *BlendAuctionChange, got %T", node)
	assert.Equal(t, "USER_LIQUIDATION", sc.AuctionType)
	assert.Equal(t, int32(100), sc.FillPercent)
	assert.Equal(t, "GFILLER", sc.Counterparty)
	require.Len(t, sc.Lot, 1)
	assert.Equal(t, BlendAuctionAmount{AssetContractID: "CASSET1", Amount: "500"}, sc.Lot[0])
	require.Len(t, sc.Bid, 1)
	assert.Equal(t, BlendAuctionAmount{AssetContractID: "CASSET2", Amount: "300"}, sc.Bid[0])
}

func Test_BlendPool_DecodesWithNestedReservesAndNullFloats(t *testing.T) {
	payload := []byte(`{
		"address": "CPOOLADDRESS",
		"name": "Fixed Pool",
		"status": "ACTIVE",
		"oracleContractId": "CORACLE",
		"backstopRate": 4750000,
		"maxPositions": 4,
		"suppliedUsd": null,
		"borrowedUsd": 123.45,
		"backstopUsd": 500.0,
		"interestApy": null,
		"netApy": 0.05,
		"reserves": [
			{
				"assetContractId": "CASSET1",
				"tokenName": "USD Coin",
				"tokenSymbol": "USDC",
				"tokenDecimals": 7,
				"enabled": true,
				"utilization": 0.5,
				"supplyApy": 0.03,
				"borrowApy": 0.08,
				"emissionsSupplyApr": null,
				"emissionsBorrowApr": null,
				"suppliedTokens": "1000000000",
				"borrowedTokens": "500000000",
				"suppliedUsd": null,
				"borrowedUsd": 250.0,
				"cFactor": 9000000,
				"lFactor": 9500000,
				"priceUsd": null
			}
		]
	}`)

	var pool BlendPool
	err := json.Unmarshal(payload, &pool)
	require.NoError(t, err)

	assert.Equal(t, "CPOOLADDRESS", pool.Address)
	require.NotNil(t, pool.Name)
	assert.Equal(t, "Fixed Pool", *pool.Name)
	require.NotNil(t, pool.Status)
	assert.Equal(t, BlendPoolStatusActive, *pool.Status)
	assert.Nil(t, pool.SuppliedUsd)
	require.NotNil(t, pool.BorrowedUsd)
	assert.InDelta(t, 123.45, *pool.BorrowedUsd, 0.0001)

	require.Len(t, pool.Reserves, 1)
	reserve := pool.Reserves[0]
	assert.Equal(t, "CASSET1", reserve.AssetContractID)
	require.NotNil(t, reserve.TokenSymbol)
	assert.Equal(t, "USDC", *reserve.TokenSymbol)
	assert.True(t, reserve.Enabled)
	assert.Nil(t, reserve.EmissionsSupplyApr)
	assert.Nil(t, reserve.SuppliedUsd)
	require.NotNil(t, reserve.CFactor)
	assert.Equal(t, int32(9000000), *reserve.CFactor)
}

func Test_BlendReservePosition_DecodesWithNullFloats(t *testing.T) {
	payload := []byte(`{
		"assetContractId": "CASSET1",
		"tokenName": null,
		"tokenSymbol": null,
		"tokenDecimals": null,
		"suppliedTokens": "100",
		"collateralTokens": "50",
		"borrowedTokens": "0",
		"suppliedUsd": null,
		"borrowedUsd": null,
		"supplyApy": null,
		"borrowApy": null,
		"emissionsSupplyApr": null,
		"emissionsBorrowApr": null,
		"interestEarned": "1",
		"interestPaid": "0",
		"emissionsEarnedBlnd": "2",
		"emissionsEarnedUsd": null,
		"priceUsd": null
	}`)

	var rp BlendReservePosition
	err := json.Unmarshal(payload, &rp)
	require.NoError(t, err)

	assert.Equal(t, "CASSET1", rp.AssetContractID)
	assert.Nil(t, rp.TokenName)
	assert.Nil(t, rp.TokenDecimals)
	assert.Equal(t, "100", rp.SuppliedTokens)
	assert.Equal(t, "50", rp.CollateralTokens)
	assert.Nil(t, rp.SuppliedUsd)
	assert.Nil(t, rp.PriceUsd)
	assert.Nil(t, rp.EmissionsSupplyApr)
	assert.Nil(t, rp.EmissionsBorrowApr)
	assert.Equal(t, "2", rp.EmissionsEarnedBlnd)
}

func Test_BlendAccountPositions_DecodesBackstopAndQ4W(t *testing.T) {
	payload := []byte(`{
		"pools": [
			{
				"poolAddress": "CPOOL1",
				"poolName": "Pool One",
				"usdValue": 10.5,
				"suppliedUsd": 10.5,
				"borrowedUsd": null,
				"netApy": 0.04,
				"claimedBlnd": "5",
				"reserves": []
			}
		],
		"backstop": [
			{
				"poolAddress": "CPOOL1",
				"poolName": "Pool One",
				"shares": "1000",
				"lpTokens": "2000",
				"usdValue": null,
				"q4w": [
					{
						"amount": "100",
						"expiration": 1735689600,
						"lpTokens": "200",
						"usdValue": null
					}
				],
				"emissionsEarnedBlnd": "3",
				"emissionsEarnedUsd": null
			}
		],
		"backstopClaimedLp": "42"
	}`)

	var positions BlendAccountPositions
	err := json.Unmarshal(payload, &positions)
	require.NoError(t, err)

	assert.Equal(t, "42", positions.BackstopClaimedLp)
	require.Len(t, positions.Pools, 1)
	assert.Equal(t, "CPOOL1", positions.Pools[0].PoolAddress)
	assert.Equal(t, "5", positions.Pools[0].ClaimedBlnd)
	assert.Nil(t, positions.Pools[0].BorrowedUsd)

	require.Len(t, positions.Backstop, 1)
	bp := positions.Backstop[0]
	assert.Equal(t, "1000", bp.Shares)
	assert.Nil(t, bp.UsdValue)
	require.Len(t, bp.Q4W, 1)
	assert.Equal(t, "100", bp.Q4W[0].Amount)
	assert.Equal(t, int64(1735689600), bp.Q4W[0].Expiration)
	assert.Nil(t, bp.Q4W[0].UsdValue)
}
