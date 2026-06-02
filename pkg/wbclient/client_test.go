package wbclient

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/pkg/wbclient/types"
)

// graphqlServer returns an httptest.Server that always responds with the given
// `data` JSON object wrapped in a GraphQL response envelope and no errors.
func graphqlServer(t *testing.T, dataJSON string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := io.Copy(io.Discard, r.Body)
		require.NoError(t, err)
		w.Header().Set("Content-Type", "application/json")
		_, err = w.Write([]byte(`{"data":` + dataJSON + `}`))
		require.NoError(t, err)
	}))
}

func TestGetAccountTransactions(t *testing.T) {
	ctx := context.Background()

	t.Run("returns ErrAccountNotFound when accountByAddress is null", func(t *testing.T) {
		srv := graphqlServer(t, `{"accountByAddress": null}`)
		defer srv.Close()

		c := NewClient(srv.URL, nil)
		conn, err := c.GetAccountTransactions(ctx, "GABC", nil, nil, nil, nil, nil, nil)
		assert.Nil(t, conn)
		require.Error(t, err)
		assert.True(t, errors.Is(err, ErrAccountNotFound), "expected ErrAccountNotFound, got %v", err)
	})

	t.Run("passes through null transactions connection on existing account", func(t *testing.T) {
		srv := graphqlServer(t, `{"accountByAddress": {"transactions": null}}`)
		defer srv.Close()

		c := NewClient(srv.URL, nil)
		conn, err := c.GetAccountTransactions(ctx, "GABC", nil, nil, nil, nil, nil, nil)
		require.NoError(t, err)
		assert.Nil(t, conn, "schema permits null transactions on existing account")
	})

	t.Run("returns connection with empty edges when account has no transactions", func(t *testing.T) {
		srv := graphqlServer(t, `{"accountByAddress": {"transactions": {"edges": [], "pageInfo": {"hasNextPage": false, "hasPreviousPage": false}}}}`)
		defer srv.Close()

		c := NewClient(srv.URL, nil)
		conn, err := c.GetAccountTransactions(ctx, "GABC", nil, nil, nil, nil, nil, nil)
		require.NoError(t, err)
		require.NotNil(t, conn)
		assert.Empty(t, conn.Edges)
		require.NotNil(t, conn.PageInfo)
	})

	t.Run("sends well-formed GraphQL request body", func(t *testing.T) {
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
		_, err := c.GetAccountTransactions(ctx, "GABC", nil, nil, nil, nil, nil, nil)
		require.ErrorIs(t, err, ErrAccountNotFound)

		assert.Contains(t, received.Query, "accountByAddress")
		assert.Equal(t, "GABC", received.Variables["address"])
	})
}

func TestGetAccountOperations(t *testing.T) {
	ctx := context.Background()

	t.Run("returns ErrAccountNotFound when accountByAddress is null", func(t *testing.T) {
		srv := graphqlServer(t, `{"accountByAddress": null}`)
		defer srv.Close()

		c := NewClient(srv.URL, nil)
		conn, err := c.GetAccountOperations(ctx, "GABC", nil, nil, nil, nil, nil, nil)
		assert.Nil(t, conn)
		require.Error(t, err)
		assert.True(t, errors.Is(err, ErrAccountNotFound), "expected ErrAccountNotFound, got %v", err)
	})

	t.Run("passes through null operations connection on existing account", func(t *testing.T) {
		srv := graphqlServer(t, `{"accountByAddress": {"operations": null}}`)
		defer srv.Close()

		c := NewClient(srv.URL, nil)
		conn, err := c.GetAccountOperations(ctx, "GABC", nil, nil, nil, nil, nil, nil)
		require.NoError(t, err)
		assert.Nil(t, conn, "schema permits null operations on existing account")
	})

	t.Run("returns connection with empty edges when account has no operations", func(t *testing.T) {
		srv := graphqlServer(t, `{"accountByAddress": {"operations": {"edges": [], "pageInfo": {"hasNextPage": false, "hasPreviousPage": false}}}}`)
		defer srv.Close()

		c := NewClient(srv.URL, nil)
		conn, err := c.GetAccountOperations(ctx, "GABC", nil, nil, nil, nil, nil, nil)
		require.NoError(t, err)
		require.NotNil(t, conn)
		assert.Empty(t, conn.Edges)
		require.NotNil(t, conn.PageInfo)
	})

	t.Run("sends well-formed GraphQL request body", func(t *testing.T) {
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
		_, err := c.GetAccountOperations(ctx, "GABC", nil, nil, nil, nil, nil, nil)
		require.ErrorIs(t, err, ErrAccountNotFound)

		assert.Contains(t, received.Query, "accountByAddress")
		assert.Equal(t, "GABC", received.Variables["address"])
	})
}

func TestGetAccountStateChanges(t *testing.T) {
	ctx := context.Background()

	t.Run("returns ErrAccountNotFound when accountByAddress is null", func(t *testing.T) {
		srv := graphqlServer(t, `{"accountByAddress": null}`)
		defer srv.Close()

		c := NewClient(srv.URL, nil)
		conn, err := c.GetAccountStateChanges(ctx, "GABC", nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
		assert.Nil(t, conn)
		require.Error(t, err)
		assert.True(t, errors.Is(err, ErrAccountNotFound), "expected ErrAccountNotFound, got %v", err)
	})

	t.Run("passes through null stateChanges connection on existing account", func(t *testing.T) {
		srv := graphqlServer(t, `{"accountByAddress": {"stateChanges": null}}`)
		defer srv.Close()

		c := NewClient(srv.URL, nil)
		conn, err := c.GetAccountStateChanges(ctx, "GABC", nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
		require.NoError(t, err)
		assert.Nil(t, conn, "schema permits null stateChanges on existing account")
	})

	t.Run("returns connection with empty edges when account has no state changes", func(t *testing.T) {
		srv := graphqlServer(t, `{"accountByAddress": {"stateChanges": {"edges": [], "pageInfo": {"hasNextPage": false, "hasPreviousPage": false}}}}`)
		defer srv.Close()

		c := NewClient(srv.URL, nil)
		conn, err := c.GetAccountStateChanges(ctx, "GABC", nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
		require.NoError(t, err)
		require.NotNil(t, conn)
		assert.Empty(t, conn.Edges)
		require.NotNil(t, conn.PageInfo)
	})

	t.Run("sends well-formed GraphQL request body with filter variables", func(t *testing.T) {
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
		txHash := "deadbeef"
		category := "CREDIT"
		_, err := c.GetAccountStateChanges(ctx, "GABC", &txHash, nil, &category, nil, nil, nil, nil, nil, nil, nil)
		require.ErrorIs(t, err, ErrAccountNotFound)

		assert.Contains(t, received.Query, "accountByAddress")
		assert.Equal(t, "GABC", received.Variables["address"])

		filter, ok := received.Variables["filter"].(map[string]any)
		require.True(t, ok, "expected filter to be encoded as a JSON object, got %T", received.Variables["filter"])
		assert.Equal(t, "deadbeef", filter["transactionHash"])
		assert.Equal(t, "CREDIT", filter["category"])
	})
}

func TestGetAccountTransactionsWithDetails(t *testing.T) {
	ctx := context.Background()

	t.Run("returns ErrAccountNotFound when accountByAddress is null", func(t *testing.T) {
		srv := graphqlServer(t, `{"accountByAddress": null}`)
		defer srv.Close()

		c := NewClient(srv.URL, nil)
		conn, err := c.GetAccountTransactionsWithDetails(ctx, "GABC", nil, nil, nil, nil, nil, nil)
		assert.Nil(t, conn)
		require.ErrorIs(t, err, ErrAccountNotFound)
	})

	t.Run("deserializes edges with embedded operations and state changes", func(t *testing.T) {
		body := `{"accountByAddress":{"transactions":{"edges":[{"node":{"hash":"abc"},` +
			`"operations":[{"id":1,"operationType":"PAYMENT"}],` +
			`"stateChanges":[{"__typename":"StandardBalanceChange","type":"BALANCE","amount":"10"}],` +
			`"cursor":"c1"}],"pageInfo":{"hasNextPage":false,"hasPreviousPage":false}}}}`
		srv := graphqlServer(t, body)
		defer srv.Close()

		c := NewClient(srv.URL, nil)
		conn, err := c.GetAccountTransactionsWithDetails(ctx, "GABC", nil, nil, nil, nil, nil, nil)
		require.NoError(t, err)
		require.NotNil(t, conn)
		require.Len(t, conn.Edges, 1)
		edge := conn.Edges[0]
		assert.Equal(t, "abc", edge.Node.Hash)
		require.Len(t, edge.Operations, 1)
		assert.Equal(t, int64(1), edge.Operations[0].ID)
		require.Len(t, edge.StateChanges, 1)
		assert.Equal(t, types.StateChangeCategoryBalance, edge.StateChanges[0].GetType())
	})

	t.Run("passes through null transactions connection on existing account", func(t *testing.T) {
		srv := graphqlServer(t, `{"accountByAddress":{"transactions":null}}`)
		defer srv.Close()

		c := NewClient(srv.URL, nil)
		conn, err := c.GetAccountTransactionsWithDetails(ctx, "GABC", nil, nil, nil, nil, nil, nil)
		require.NoError(t, err)
		assert.Nil(t, conn, "schema permits null transactions on existing account")
	})

	t.Run("returns an error when edges list is null", func(t *testing.T) {
		srv := graphqlServer(t, `{"accountByAddress":{"transactions":{"edges":null,"pageInfo":{"hasNextPage":false,"hasPreviousPage":false}}}}`)
		defer srv.Close()

		c := NewClient(srv.URL, nil)
		conn, err := c.GetAccountTransactionsWithDetails(ctx, "GABC", nil, nil, nil, nil, nil, nil)
		require.Error(t, err)
		assert.Nil(t, conn)
	})

	t.Run("returns an error when an edge has a null node", func(t *testing.T) {
		body := `{"accountByAddress":{"transactions":{"edges":[{"node":null,"operations":[],"stateChanges":[],"cursor":"c1"}],` +
			`"pageInfo":{"hasNextPage":false,"hasPreviousPage":false}}}}`
		srv := graphqlServer(t, body)
		defer srv.Close()

		c := NewClient(srv.URL, nil)
		conn, err := c.GetAccountTransactionsWithDetails(ctx, "GABC", nil, nil, nil, nil, nil, nil)
		require.Error(t, err)
		assert.Nil(t, conn)
	})

	t.Run("returns an error when an edge has a null operations list", func(t *testing.T) {
		body := `{"accountByAddress":{"transactions":{"edges":[{"node":{"hash":"abc"},"operations":null,"stateChanges":[],"cursor":"c1"}],` +
			`"pageInfo":{"hasNextPage":false,"hasPreviousPage":false}}}}`
		srv := graphqlServer(t, body)
		defer srv.Close()

		c := NewClient(srv.URL, nil)
		conn, err := c.GetAccountTransactionsWithDetails(ctx, "GABC", nil, nil, nil, nil, nil, nil)
		require.Error(t, err)
		assert.Nil(t, conn)
	})

	t.Run("returns an error when an edge has a null stateChanges list", func(t *testing.T) {
		body := `{"accountByAddress":{"transactions":{"edges":[{"node":{"hash":"abc"},"operations":[],"stateChanges":null,"cursor":"c1"}],` +
			`"pageInfo":{"hasNextPage":false,"hasPreviousPage":false}}}}`
		srv := graphqlServer(t, body)
		defer srv.Close()

		c := NewClient(srv.URL, nil)
		conn, err := c.GetAccountTransactionsWithDetails(ctx, "GABC", nil, nil, nil, nil, nil, nil)
		require.Error(t, err)
		assert.Nil(t, conn)
	})
}
