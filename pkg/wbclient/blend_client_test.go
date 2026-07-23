package wbclient

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/pkg/wbclient/types"
)

func TestGetBlendPools(t *testing.T) {
	ctx := context.Background()

	t.Run("decodes pools including enum status and null-heavy rows", func(t *testing.T) {
		srv := graphqlServer(t, `{
			"blendPools": [
				{
					"address": "CPOOLAAA",
					"name": "Fixed Pool V2",
					"status": "ACTIVE",
					"oracleContractId": "CORACLE",
					"backstopRate": 4750000,
					"maxPositions": 8,
					"suppliedUsd": 2100000.5,
					"borrowedUsd": 900000.25,
					"backstopUsd": 130000.0,
					"interestApy": 0.043,
					"netApy": 0.047,
					"admin": "GADMIN",
					"inRewardZone": true,
					"reserves": [{
						"assetContractId": "CUSDC",
						"tokenName": "USD Coin",
						"tokenSymbol": "USDC",
						"tokenDecimals": 7,
						"enabled": true,
						"utilization": 0.62,
						"supplyApy": 0.043,
						"borrowApy": 0.061,
						"emissionsSupplyApr": 0.008,
						"emissionsBorrowApr": 0,
						"suppliedTokens": "15000000000000",
						"borrowedTokens": "9300000000000",
						"suppliedUsd": 1500000.0,
						"borrowedUsd": 930000.0,
						"cFactor": 9000000,
						"lFactor": 9500000,
						"priceUsd": 1.0
					}]
				},
				{
					"address": "CPOOLBBB",
					"name": null,
					"status": null,
					"oracleContractId": null,
					"backstopRate": null,
					"maxPositions": null,
					"suppliedUsd": null,
					"borrowedUsd": null,
					"backstopUsd": null,
					"interestApy": null,
					"netApy": null,
					"admin": null,
					"inRewardZone": false,
					"reserves": []
				}
			]
		}`)
		defer srv.Close()

		c := NewClient(srv.URL, nil)
		pools, err := c.GetBlendPools(ctx)
		require.NoError(t, err)
		require.Len(t, pools, 2)

		active := pools[0]
		require.NotNil(t, active.Status)
		assert.Equal(t, types.BlendPoolStatusActive, *active.Status)
		assert.True(t, active.Status.AcceptsSupply())
		assert.True(t, active.Status.AcceptsBorrow())
		assert.True(t, active.InRewardZone)
		require.Len(t, active.Reserves, 1)
		assert.Equal(t, "15000000000000", active.Reserves[0].SuppliedTokens)
		require.NotNil(t, active.Reserves[0].CFactor)
		assert.EqualValues(t, 9000000, *active.Reserves[0].CFactor)

		// Not-yet-ingested pool: nullable fields decode to nil, never zero.
		pending := pools[1]
		assert.Nil(t, pending.Status)
		assert.Nil(t, pending.SuppliedUsd)
		assert.Nil(t, pending.Name)
		assert.NotNil(t, pending.Reserves)
	})

	t.Run("returns empty non-nil slice when there are no pools", func(t *testing.T) {
		srv := graphqlServer(t, `{"blendPools": []}`)
		defer srv.Close()

		c := NewClient(srv.URL, nil)
		pools, err := c.GetBlendPools(ctx)
		require.NoError(t, err)
		assert.NotNil(t, pools)
		assert.Empty(t, pools)
	})
}

func TestGetBlendPool(t *testing.T) {
	ctx := context.Background()

	t.Run("returns nil without error for an unknown pool", func(t *testing.T) {
		srv := graphqlServer(t, `{"blendPool": null}`)
		defer srv.Close()

		c := NewClient(srv.URL, nil)
		pool, err := c.GetBlendPool(ctx, "CUNKNOWN")
		require.NoError(t, err)
		assert.Nil(t, pool)
	})

	t.Run("sends the pool address variable", func(t *testing.T) {
		type gqlReq struct {
			Query     string         `json:"query"`
			Variables map[string]any `json:"variables"`
		}

		var received gqlReq
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			err := json.NewDecoder(r.Body).Decode(&received)
			require.NoError(t, err)
			w.Header().Set("Content-Type", "application/json")
			_, err = w.Write([]byte(`{"data":{"blendPool": null}}`))
			require.NoError(t, err)
		}))
		defer srv.Close()

		c := NewClient(srv.URL, nil)
		_, err := c.GetBlendPool(ctx, "CPOOLAAA")
		require.NoError(t, err)

		assert.Contains(t, received.Query, "blendPool(address: $address)")
		assert.Equal(t, "CPOOLAAA", received.Variables["address"])
	})
}

func TestGetAccountBlendPositions(t *testing.T) {
	ctx := context.Background()

	t.Run("returns ErrAccountNotFound when accountByAddress is null", func(t *testing.T) {
		srv := graphqlServer(t, `{"accountByAddress": null}`)
		defer srv.Close()

		c := NewClient(srv.URL, nil)
		positions, err := c.GetAccountBlendPositions(ctx, "GABC")
		assert.Nil(t, positions)
		require.Error(t, err)
		assert.True(t, errors.Is(err, ErrAccountNotFound), "expected ErrAccountNotFound, got %v", err)
	})

	t.Run("decodes a full positions tree", func(t *testing.T) {
		srv := graphqlServer(t, `{
			"accountByAddress": {
				"blendPositions": {
					"pools": [{
						"poolAddress": "CPOOLAAA",
						"poolName": "Fixed Pool V2",
						"usdValue": 77876.27,
						"suppliedUsd": 674117.02,
						"borrowedUsd": 596240.75,
						"netApy": -0.029,
						"claimedBlnd": "12345",
						"reserves": [{
							"assetContractId": "CUSDC",
							"tokenName": "USD Coin",
							"tokenSymbol": "USDC",
							"tokenDecimals": 7,
							"suppliedTokens": "1000000000",
							"collateralTokens": "5563385856000",
							"borrowedTokens": "4953691474632",
							"suppliedUsd": 556438.59,
							"borrowedUsd": 495221.33,
							"supplyApy": 0.0741,
							"borrowApy": 0.1151,
							"emissionsSupplyApr": 0.002,
							"emissionsBorrowApr": 0.001,
							"interestEarned": "6843215",
							"interestPaid": "1234567",
							"emissionsEarnedBlnd": "999",
							"emissionsEarnedUsd": 0.53,
							"priceUsd": 1.0
						}]
					}],
					"backstop": [{
						"poolAddress": "CPOOLAAA",
						"poolName": "Fixed Pool V2",
						"shares": "1000000",
						"lpTokens": "1100000",
						"usdValue": 52.5,
						"q4w": [{"amount": "5000", "expiration": 1760000000, "lpTokens": "5500", "usdValue": 0.26}],
						"emissionsEarnedBlnd": "42",
						"emissionsEarnedUsd": 0.001
					}],
					"backstopClaimedLp": "777",
					"activeAuctions": [{
						"poolAddress": "CPOOLAAA",
						"poolName": "Fixed Pool V2",
						"auctionType": "USER_LIQUIDATION",
						"bid": [{"assetContractId": "CUSDC", "amount": "100"}],
						"lot": [{"assetContractId": "CXLM", "amount": "200"}],
						"startBlock": 63400000
					}]
				}
			}
		}`)
		defer srv.Close()

		c := NewClient(srv.URL, nil)
		positions, err := c.GetAccountBlendPositions(ctx, "GABC")
		require.NoError(t, err)
		require.NotNil(t, positions)

		require.Len(t, positions.Pools, 1)
		pool := positions.Pools[0]
		require.NotNil(t, pool.NetApy)
		assert.InDelta(t, -0.029, *pool.NetApy, 1e-9)
		require.Len(t, pool.Reserves, 1)
		reserve := pool.Reserves[0]
		assert.Equal(t, "5563385856000", reserve.CollateralTokens)
		assert.Equal(t, "6843215", reserve.InterestEarned)
		require.NotNil(t, reserve.EmissionsSupplyApr)
		assert.InDelta(t, 0.002, *reserve.EmissionsSupplyApr, 1e-9)
		require.NotNil(t, reserve.EmissionsBorrowApr)
		assert.InDelta(t, 0.001, *reserve.EmissionsBorrowApr, 1e-9)

		require.Len(t, positions.Backstop, 1)
		require.Len(t, positions.Backstop[0].Q4w, 1)
		assert.EqualValues(t, 1760000000, positions.Backstop[0].Q4w[0].Expiration)
		assert.Equal(t, "777", positions.BackstopClaimedLp)

		require.Len(t, positions.ActiveAuctions, 1)
		assert.Equal(t, types.BlendAuctionTypeUserLiquidation, positions.ActiveAuctions[0].AuctionType)
	})

	t.Run("sends the account address variable", func(t *testing.T) {
		type gqlReq struct {
			Query     string         `json:"query"`
			Variables map[string]any `json:"variables"`
		}

		var received gqlReq
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			err := json.NewDecoder(r.Body).Decode(&received)
			require.NoError(t, err)
			w.Header().Set("Content-Type", "application/json")
			_, err = w.Write([]byte(`{"data":{"accountByAddress": null}}`))
			require.NoError(t, err)
		}))
		defer srv.Close()

		c := NewClient(srv.URL, nil)
		_, err := c.GetAccountBlendPositions(ctx, "GABC")
		require.ErrorIs(t, err, ErrAccountNotFound)

		assert.Contains(t, received.Query, "accountByAddress(address: $address)")
		assert.Contains(t, received.Query, "blendPositions")
		assert.Equal(t, "GABC", received.Variables["address"])
	})
}
