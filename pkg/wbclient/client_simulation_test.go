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

func TestSimulateStateChanges(t *testing.T) {
	ctx := context.Background()

	t.Run("decodes state changes into concrete types, with tokens", func(t *testing.T) {
		body := `{"simulateStateChanges":{"latestLedger":123,"stateChanges":[
			{"__typename":"SimulatedBalanceChange","category":"BALANCE","reason":"DEBIT","accountAddress":"GSENDER","operationIndex":null,"balanceTokenId":"CXLM","amount":"100"},
			{"__typename":"SimulatedBalanceChange","category":"BALANCE","reason":"DEBIT","accountAddress":"GSENDER","operationIndex":0,"balanceTokenId":"CUSDC","amount":"5000000"},
			{"__typename":"SimulatedTrustlineAddedChange","category":"TRUSTLINE","reason":"ADD","accountAddress":"GSENDER","operationIndex":1,"trustlineAddedTokenId":"CUSDC","liquidityPoolId":null,"limit":"1000"}
		],"tokens":[{"tokenId":"CUSDC","code":"USDC","issuer":"GISSUER","name":null,"symbol":null,"decimals":7}]}}`
		srv := graphqlServer(t, body)
		defer srv.Close()

		result, err := NewClient(srv.URL, nil).SimulateStateChanges(ctx, "AAAA")
		require.NoError(t, err)
		assert.Equal(t, uint32(123), result.LatestLedger)
		require.Len(t, result.StateChanges, 3)

		fee, ok := result.StateChanges[0].(*types.SimulatedBalanceChange)
		require.True(t, ok, "expected *SimulatedBalanceChange, got %T", result.StateChanges[0])
		assert.Nil(t, fee.GetOperationIndex(), "the fee row has no operation")
		assert.Equal(t, "CXLM", fee.TokenID)

		debit, ok := result.StateChanges[1].(*types.SimulatedBalanceChange)
		require.True(t, ok)
		require.NotNil(t, debit.GetOperationIndex())
		assert.Equal(t, int32(0), *debit.GetOperationIndex())
		assert.Equal(t, types.StateChangeReasonDebit, debit.GetReason())
		assert.Equal(t, "GSENDER", debit.GetAccountAddress())
		assert.Equal(t, "5000000", debit.Amount)

		trustline, ok := result.StateChanges[2].(*types.SimulatedTrustlineAddedChange)
		require.True(t, ok, "expected *SimulatedTrustlineAddedChange, got %T", result.StateChanges[2])
		require.NotNil(t, trustline.TokenID)
		assert.Equal(t, "CUSDC", *trustline.TokenID)
		assert.Nil(t, trustline.LiquidityPoolID)

		require.Len(t, result.Tokens, 1)
		assert.Equal(t, "CUSDC", result.Tokens[0].TokenID)
		require.NotNil(t, result.Tokens[0].Code)
		assert.Equal(t, "USDC", *result.Tokens[0].Code)
		assert.Nil(t, result.Tokens[0].Name)
		assert.Equal(t, uint32(7), result.Tokens[0].Decimals)
	})

	t.Run("sends the transaction XDR as a variable", func(t *testing.T) {
		type gqlReq struct {
			Query     string         `json:"query"`
			Variables map[string]any `json:"variables"`
		}
		var received gqlReq
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			err := json.NewDecoder(r.Body).Decode(&received)
			require.NoError(t, err)
			w.Header().Set("Content-Type", "application/json")
			_, err = w.Write([]byte(`{"data":{"simulateStateChanges":{"latestLedger":1,"stateChanges":[],"tokens":[]}}}`))
			require.NoError(t, err)
		}))
		defer srv.Close()

		result, err := NewClient(srv.URL, nil).SimulateStateChanges(ctx, "AAAA")
		require.NoError(t, err)
		assert.Empty(t, result.StateChanges)
		assert.Empty(t, result.Tokens)
		assert.Contains(t, received.Query, "simulateStateChanges(transactionXdr: $transactionXdr)")
		assert.Equal(t, "AAAA", received.Variables["transactionXdr"])
	})

	t.Run("returns the server's error code for a transaction that cannot be previewed", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, err := w.Write([]byte(`{"data":null,"errors":[{"message":"operation type not supported","extensions":{"code":"UNSUPPORTED_TRANSACTION"}}]}`))
			require.NoError(t, err)
		}))
		defer srv.Close()

		result, err := NewClient(srv.URL, nil).SimulateStateChanges(ctx, "AAAA")
		assert.Nil(t, result)
		var gqlErrs GraphQLErrors
		require.True(t, errors.As(err, &gqlErrs), "expected GraphQLErrors, got %v", err)
		require.Len(t, gqlErrs, 1)
		assert.Equal(t, "UNSUPPORTED_TRANSACTION", gqlErrs[0].Extensions["code"])
	})

	t.Run("rejects a null result", func(t *testing.T) {
		srv := graphqlServer(t, `{"simulateStateChanges": null}`)
		defer srv.Close()

		result, err := NewClient(srv.URL, nil).SimulateStateChanges(ctx, "AAAA")
		require.Error(t, err, "the schema declares the result non-null")
		assert.Nil(t, result)
	})

	t.Run("rejects an unknown state change type", func(t *testing.T) {
		srv := graphqlServer(t, `{"simulateStateChanges":{"latestLedger":1,"stateChanges":[{"__typename":"SimulatedFutureChange"}],"tokens":[]}}`)
		defer srv.Close()

		_, err := NewClient(srv.URL, nil).SimulateStateChanges(ctx, "AAAA")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unknown simulated state change type: SimulatedFutureChange")
	})
}
