package wbclient

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetBlendPools(t *testing.T) {
	ctx := context.Background()

	t.Run("decodes pools with nested reserves", func(t *testing.T) {
		body := `{"blendPools":[{"address":"CPOOL1","name":"Pool One","status":1,"reserves":[
			{"assetContractId":"CASSET1","tokenSymbol":"USDC","enabled":true,"suppliedTokens":"100","borrowedTokens":"10"}
		]}]}`
		srv := graphqlServer(t, body)
		defer srv.Close()

		c := NewClient(srv.URL, nil)
		pools, err := c.GetBlendPools(ctx)
		require.NoError(t, err)
		require.Len(t, pools, 1)
		assert.Equal(t, "CPOOL1", pools[0].Address)
		require.Len(t, pools[0].Reserves, 1)
		assert.Equal(t, "CASSET1", pools[0].Reserves[0].AssetContractID)
	})

	t.Run("sends well-formed request", func(t *testing.T) {
		type gqlReq struct {
			Query string `json:"query"`
		}
		var received gqlReq
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			err := json.NewDecoder(r.Body).Decode(&received)
			require.NoError(t, err)
			w.Header().Set("Content-Type", "application/json")
			_, err = w.Write([]byte(`{"data":{"blendPools":[]}}`))
			require.NoError(t, err)
		}))
		defer srv.Close()

		c := NewClient(srv.URL, nil)
		pools, err := c.GetBlendPools(ctx)
		require.NoError(t, err)
		assert.Empty(t, pools)
		assert.Contains(t, received.Query, "blendPools")
	})
}

func TestGetBlendPool(t *testing.T) {
	ctx := context.Background()

	t.Run("returns nil when the pool does not exist", func(t *testing.T) {
		srv := graphqlServer(t, `{"blendPool": null}`)
		defer srv.Close()

		c := NewClient(srv.URL, nil)
		pool, err := c.GetBlendPool(ctx, "CPOOL1")
		require.NoError(t, err)
		assert.Nil(t, pool)
	})

	t.Run("decodes a populated pool and sends the address variable", func(t *testing.T) {
		type gqlReq struct {
			Query     string         `json:"query"`
			Variables map[string]any `json:"variables"`
		}
		var received gqlReq
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			err := json.NewDecoder(r.Body).Decode(&received)
			require.NoError(t, err)
			w.Header().Set("Content-Type", "application/json")
			_, err = w.Write([]byte(`{"data":{"blendPool":{"address":"CPOOL1","reserves":[]}}}`))
			require.NoError(t, err)
		}))
		defer srv.Close()

		c := NewClient(srv.URL, nil)
		pool, err := c.GetBlendPool(ctx, "CPOOL1")
		require.NoError(t, err)
		require.NotNil(t, pool)
		assert.Equal(t, "CPOOL1", pool.Address)
		assert.Equal(t, "CPOOL1", received.Variables["address"])
		assert.Contains(t, received.Query, "blendPool(address: $address)")
	})
}

func TestGetBlendEarnOptions(t *testing.T) {
	ctx := context.Background()

	t.Run("decodes earn options with nested pools", func(t *testing.T) {
		body := `{"blendEarnOptions":[{"assetContractId":"CASSET1","tokenSymbol":"USDC","pools":[
			{"poolAddress":"CPOOL1","supplyApy":0.05}
		]}]}`
		srv := graphqlServer(t, body)
		defer srv.Close()

		c := NewClient(srv.URL, nil)
		options, err := c.GetBlendEarnOptions(ctx)
		require.NoError(t, err)
		require.Len(t, options, 1)
		assert.Equal(t, "CASSET1", options[0].AssetContractID)
		require.Len(t, options[0].Pools, 1)
		assert.Equal(t, "CPOOL1", options[0].Pools[0].PoolAddress)
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
		require.ErrorIs(t, err, ErrAccountNotFound)
	})

	t.Run("decodes positions including backstop and q4w", func(t *testing.T) {
		body := `{"accountByAddress":{"blendPositions":{
			"pools":[{"poolAddress":"CPOOL1","claimedBlnd":"5","reserves":[]}],
			"backstop":[{"poolAddress":"CPOOL1","shares":"100","lpTokens":"200","emissionsEarnedBlnd":"1","q4w":[
				{"amount":"10","expiration":1735689600,"lpTokens":"20"}
			]}],
			"backstopClaimedLp":"42"
		}}}`
		srv := graphqlServer(t, body)
		defer srv.Close()

		c := NewClient(srv.URL, nil)
		positions, err := c.GetAccountBlendPositions(ctx, "GABC")
		require.NoError(t, err)
		require.NotNil(t, positions)
		assert.Equal(t, "42", positions.BackstopClaimedLp)
		require.Len(t, positions.Pools, 1)
		assert.Equal(t, "5", positions.Pools[0].ClaimedBlnd)
		require.Len(t, positions.Backstop, 1)
		require.Len(t, positions.Backstop[0].Q4W, 1)
		assert.Equal(t, int64(1735689600), positions.Backstop[0].Q4W[0].Expiration)
	})

	t.Run("sends the address variable", func(t *testing.T) {
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
		assert.Equal(t, "GABC", received.Variables["address"])
		assert.Contains(t, received.Query, "blendPositions")
	})
}
