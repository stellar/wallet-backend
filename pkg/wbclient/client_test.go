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

func TestGetAccountTransactions_AccountNotFound(t *testing.T) {
	srv := graphqlServer(t, `{"accountByAddress": null}`)
	defer srv.Close()

	c := NewClient(srv.URL, nil)
	conn, err := c.GetAccountTransactions(context.Background(), "GABC", nil, nil, nil, nil, nil, nil)
	assert.Nil(t, conn)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrAccountNotFound), "expected ErrAccountNotFound, got %v", err)
}

func TestGetAccountTransactions_NullConnectionPassesThrough(t *testing.T) {
	srv := graphqlServer(t, `{"accountByAddress": {"transactions": null}}`)
	defer srv.Close()

	c := NewClient(srv.URL, nil)
	conn, err := c.GetAccountTransactions(context.Background(), "GABC", nil, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	assert.Nil(t, conn, "schema permits null transactions on existing account")
}

func TestGetAccountTransactions_EmptyEdgesReturnsConnection(t *testing.T) {
	srv := graphqlServer(t, `{"accountByAddress": {"transactions": {"edges": [], "pageInfo": {"hasNextPage": false, "hasPreviousPage": false}}}}`)
	defer srv.Close()

	c := NewClient(srv.URL, nil)
	conn, err := c.GetAccountTransactions(context.Background(), "GABC", nil, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, conn)
	assert.Empty(t, conn.Edges)
	require.NotNil(t, conn.PageInfo)
}

func TestGetAccountOperations_AccountNotFound(t *testing.T) {
	srv := graphqlServer(t, `{"accountByAddress": null}`)
	defer srv.Close()

	c := NewClient(srv.URL, nil)
	conn, err := c.GetAccountOperations(context.Background(), "GABC", nil, nil, nil, nil, nil, nil)
	assert.Nil(t, conn)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrAccountNotFound), "expected ErrAccountNotFound, got %v", err)
}

func TestGetAccountOperations_NullConnectionPassesThrough(t *testing.T) {
	srv := graphqlServer(t, `{"accountByAddress": {"operations": null}}`)
	defer srv.Close()

	c := NewClient(srv.URL, nil)
	conn, err := c.GetAccountOperations(context.Background(), "GABC", nil, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	assert.Nil(t, conn, "schema permits null operations on existing account")
}

func TestGetAccountOperations_EmptyEdgesReturnsConnection(t *testing.T) {
	srv := graphqlServer(t, `{"accountByAddress": {"operations": {"edges": [], "pageInfo": {"hasNextPage": false, "hasPreviousPage": false}}}}`)
	defer srv.Close()

	c := NewClient(srv.URL, nil)
	conn, err := c.GetAccountOperations(context.Background(), "GABC", nil, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, conn)
	assert.Empty(t, conn.Edges)
	require.NotNil(t, conn.PageInfo)
}

func TestGetAccountStateChanges_AccountNotFound(t *testing.T) {
	srv := graphqlServer(t, `{"accountByAddress": null}`)
	defer srv.Close()

	c := NewClient(srv.URL, nil)
	conn, err := c.GetAccountStateChanges(context.Background(), "GABC", nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	assert.Nil(t, conn)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrAccountNotFound), "expected ErrAccountNotFound, got %v", err)
}

func TestGetAccountStateChanges_NullConnectionPassesThrough(t *testing.T) {
	srv := graphqlServer(t, `{"accountByAddress": {"stateChanges": null}}`)
	defer srv.Close()

	c := NewClient(srv.URL, nil)
	conn, err := c.GetAccountStateChanges(context.Background(), "GABC", nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	assert.Nil(t, conn, "schema permits null stateChanges on existing account")
}

func TestGetAccountStateChanges_EmptyEdgesReturnsConnection(t *testing.T) {
	srv := graphqlServer(t, `{"accountByAddress": {"stateChanges": {"edges": [], "pageInfo": {"hasNextPage": false, "hasPreviousPage": false}}}}`)
	defer srv.Close()

	c := NewClient(srv.URL, nil)
	conn, err := c.GetAccountStateChanges(context.Background(), "GABC", nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, conn)
	assert.Empty(t, conn.Edges)
	require.NotNil(t, conn.PageInfo)
}

// Sanity check that the request body is well-formed GraphQL JSON. The
// account-not-found path is the same regardless of arguments, so we only
// verify the body shape on one method.
func TestGetAccountTransactions_SendsGraphQLRequest(t *testing.T) {
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
	_, err := c.GetAccountTransactions(context.Background(), "GABC", nil, nil, nil, nil, nil, nil)
	require.ErrorIs(t, err, ErrAccountNotFound)

	assert.Contains(t, received.Query, "accountByAddress")
	assert.Equal(t, "GABC", received.Variables["address"])
}
