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
		conn, err := c.GetAccountTransactions(ctx, "GABC", nil, nil)
		assert.Nil(t, conn)
		require.Error(t, err)
		assert.True(t, errors.Is(err, ErrAccountNotFound), "expected ErrAccountNotFound, got %v", err)
	})

	t.Run("rejects null transactions connection on existing account", func(t *testing.T) {
		srv := graphqlServer(t, `{"accountByAddress": {"transactions": null}}`)
		defer srv.Close()

		c := NewClient(srv.URL, nil)
		conn, err := c.GetAccountTransactions(ctx, "GABC", nil, nil)
		require.Error(t, err, "the schema declares the transactions connection non-null")
		assert.Nil(t, conn)
	})

	t.Run("returns connection with empty edges when account has no transactions", func(t *testing.T) {
		srv := graphqlServer(t, `{"accountByAddress": {"transactions": {"edges": [], "pageInfo": {"hasNextPage": false, "hasPreviousPage": false}}}}`)
		defer srv.Close()

		c := NewClient(srv.URL, nil)
		conn, err := c.GetAccountTransactions(ctx, "GABC", nil, nil)
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
		_, err := c.GetAccountTransactions(ctx, "GABC", nil, nil)
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
		conn, err := c.GetAccountOperations(ctx, "GABC", nil, nil)
		assert.Nil(t, conn)
		require.Error(t, err)
		assert.True(t, errors.Is(err, ErrAccountNotFound), "expected ErrAccountNotFound, got %v", err)
	})

	t.Run("rejects null operations connection on existing account", func(t *testing.T) {
		srv := graphqlServer(t, `{"accountByAddress": {"operations": null}}`)
		defer srv.Close()

		c := NewClient(srv.URL, nil)
		conn, err := c.GetAccountOperations(ctx, "GABC", nil, nil)
		require.Error(t, err, "the schema declares the operations connection non-null")
		assert.Nil(t, conn)
	})

	t.Run("returns connection with empty edges when account has no operations", func(t *testing.T) {
		srv := graphqlServer(t, `{"accountByAddress": {"operations": {"edges": [], "pageInfo": {"hasNextPage": false, "hasPreviousPage": false}}}}`)
		defer srv.Close()

		c := NewClient(srv.URL, nil)
		conn, err := c.GetAccountOperations(ctx, "GABC", nil, nil)
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
		_, err := c.GetAccountOperations(ctx, "GABC", nil, nil)
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
		conn, err := c.GetAccountStateChanges(ctx, "GABC", nil, nil, nil)
		assert.Nil(t, conn)
		require.Error(t, err)
		assert.True(t, errors.Is(err, ErrAccountNotFound), "expected ErrAccountNotFound, got %v", err)
	})

	t.Run("rejects null stateChanges connection on existing account", func(t *testing.T) {
		srv := graphqlServer(t, `{"accountByAddress": {"stateChanges": null}}`)
		defer srv.Close()

		c := NewClient(srv.URL, nil)
		conn, err := c.GetAccountStateChanges(ctx, "GABC", nil, nil, nil)
		require.Error(t, err, "the schema declares the stateChanges connection non-null")
		assert.Nil(t, conn)
	})

	t.Run("returns connection with empty edges when account has no state changes", func(t *testing.T) {
		srv := graphqlServer(t, `{"accountByAddress": {"stateChanges": {"edges": [], "pageInfo": {"hasNextPage": false, "hasPreviousPage": false}}}}`)
		defer srv.Close()

		c := NewClient(srv.URL, nil)
		conn, err := c.GetAccountStateChanges(ctx, "GABC", nil, nil, nil)
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
		category := types.StateChangeCategoryBalance
		_, err := c.GetAccountStateChanges(ctx, "GABC", &StateChangeFilter{TransactionHash: &txHash, Category: &category}, nil, nil)
		require.ErrorIs(t, err, ErrAccountNotFound)

		assert.Contains(t, received.Query, "accountByAddress")
		assert.Equal(t, "GABC", received.Variables["address"])

		filter, ok := received.Variables["filter"].(map[string]any)
		require.True(t, ok, "expected filter to be encoded as a JSON object, got %T", received.Variables["filter"])
		assert.Equal(t, "deadbeef", filter["transactionHash"])
		assert.Equal(t, "BALANCE", filter["category"])
	})
}

func TestGetAccountTransactionsWithOpsAndStateChanges(t *testing.T) {
	ctx := context.Background()

	t.Run("returns ErrAccountNotFound when accountByAddress is null", func(t *testing.T) {
		srv := graphqlServer(t, `{"accountByAddress": null}`)
		defer srv.Close()

		c := NewClient(srv.URL, nil)
		conn, err := c.GetAccountTransactionsWithOpsAndStateChanges(ctx, "GABC", nil, nil)
		assert.Nil(t, conn)
		require.ErrorIs(t, err, ErrAccountNotFound)
	})

	t.Run("deserializes edges with embedded operations and state changes", func(t *testing.T) {
		body := `{"accountByAddress":{"transactions":{"edges":[{"node":{"hash":"abc"},` +
			`"operations":[{"id":1,"type":"PAYMENT"}],` +
			`"stateChanges":[{"__typename":"BalanceChange","category":"BALANCE","balanceTokenId":"native","amount":"10"}],` +
			`"cursor":"c1"}],"pageInfo":{"hasNextPage":false,"hasPreviousPage":false}}}}`
		srv := graphqlServer(t, body)
		defer srv.Close()

		c := NewClient(srv.URL, nil)
		conn, err := c.GetAccountTransactionsWithOpsAndStateChanges(ctx, "GABC", nil, nil)
		require.NoError(t, err)
		require.NotNil(t, conn)
		require.Len(t, conn.Edges, 1)
		edge := conn.Edges[0]
		assert.Equal(t, "abc", edge.Node.Hash)
		require.Len(t, edge.Operations, 1)
		assert.Equal(t, int64(1), edge.Operations[0].ID)
		require.Len(t, edge.StateChanges, 1)
		assert.Equal(t, types.StateChangeCategoryBalance, edge.StateChanges[0].GetCategory())
	})

	t.Run("rejects null transactions connection on existing account", func(t *testing.T) {
		srv := graphqlServer(t, `{"accountByAddress":{"transactions":null}}`)
		defer srv.Close()

		c := NewClient(srv.URL, nil)
		conn, err := c.GetAccountTransactionsWithOpsAndStateChanges(ctx, "GABC", nil, nil)
		require.Error(t, err, "the schema declares the transactions connection non-null")
		assert.Nil(t, conn)
	})

	t.Run("returns an error when edges list is null", func(t *testing.T) {
		srv := graphqlServer(t, `{"accountByAddress":{"transactions":{"edges":null,"pageInfo":{"hasNextPage":false,"hasPreviousPage":false}}}}`)
		defer srv.Close()

		c := NewClient(srv.URL, nil)
		conn, err := c.GetAccountTransactionsWithOpsAndStateChanges(ctx, "GABC", nil, nil)
		require.Error(t, err)
		assert.Nil(t, conn)
	})

	t.Run("returns an error when an edge has a null node", func(t *testing.T) {
		body := `{"accountByAddress":{"transactions":{"edges":[{"node":null,"operations":[],"stateChanges":[],"cursor":"c1"}],` +
			`"pageInfo":{"hasNextPage":false,"hasPreviousPage":false}}}}`
		srv := graphqlServer(t, body)
		defer srv.Close()

		c := NewClient(srv.URL, nil)
		conn, err := c.GetAccountTransactionsWithOpsAndStateChanges(ctx, "GABC", nil, nil)
		require.Error(t, err)
		assert.Nil(t, conn)
	})

	t.Run("returns an error when an edge has a null operations list", func(t *testing.T) {
		body := `{"accountByAddress":{"transactions":{"edges":[{"node":{"hash":"abc"},"operations":null,"stateChanges":[],"cursor":"c1"}],` +
			`"pageInfo":{"hasNextPage":false,"hasPreviousPage":false}}}}`
		srv := graphqlServer(t, body)
		defer srv.Close()

		c := NewClient(srv.URL, nil)
		conn, err := c.GetAccountTransactionsWithOpsAndStateChanges(ctx, "GABC", nil, nil)
		require.Error(t, err)
		assert.Nil(t, conn)
	})

	t.Run("returns an error when an edge has a null stateChanges list", func(t *testing.T) {
		body := `{"accountByAddress":{"transactions":{"edges":[{"node":{"hash":"abc"},"operations":[],"stateChanges":null,"cursor":"c1"}],` +
			`"pageInfo":{"hasNextPage":false,"hasPreviousPage":false}}}}`
		srv := graphqlServer(t, body)
		defer srv.Close()

		c := NewClient(srv.URL, nil)
		conn, err := c.GetAccountTransactionsWithOpsAndStateChanges(ctx, "GABC", nil, nil)
		require.Error(t, err)
		assert.Nil(t, conn)
	})
}

func TestGetTransactionByHash(t *testing.T) {
	ctx := context.Background()

	t.Run("returns ErrTransactionNotFound when transactionByHash is null", func(t *testing.T) {
		srv := graphqlServer(t, `{"transactionByHash": null}`)
		defer srv.Close()

		c := NewClient(srv.URL, nil)
		tx, err := c.GetTransactionByHash(ctx, "deadbeef")
		assert.Nil(t, tx)
		require.Error(t, err)
		assert.True(t, errors.Is(err, ErrTransactionNotFound), "expected ErrTransactionNotFound, got %v", err)
	})

	t.Run("returns the transaction when it exists", func(t *testing.T) {
		srv := graphqlServer(t, `{"transactionByHash": {"hash": "deadbeef"}}`)
		defer srv.Close()

		c := NewClient(srv.URL, nil)
		tx, err := c.GetTransactionByHash(ctx, "deadbeef")
		require.NoError(t, err)
		require.NotNil(t, tx)
		assert.Equal(t, "deadbeef", tx.Hash)
	})
}

func TestGetAccountByAddress(t *testing.T) {
	ctx := context.Background()

	t.Run("returns ErrAccountNotFound when accountByAddress is null", func(t *testing.T) {
		srv := graphqlServer(t, `{"accountByAddress": null}`)
		defer srv.Close()

		c := NewClient(srv.URL, nil)
		account, err := c.GetAccountByAddress(ctx, "GABC")
		assert.Nil(t, account)
		require.Error(t, err)
		assert.True(t, errors.Is(err, ErrAccountNotFound), "expected ErrAccountNotFound, got %v", err)
	})

	t.Run("returns the account when it exists", func(t *testing.T) {
		srv := graphqlServer(t, `{"accountByAddress": {"address": "GABC"}}`)
		defer srv.Close()

		c := NewClient(srv.URL, nil)
		account, err := c.GetAccountByAddress(ctx, "GABC")
		require.NoError(t, err)
		require.NotNil(t, account)
		assert.Equal(t, "GABC", account.Address)
	})
}

func TestGetOperationByID(t *testing.T) {
	ctx := context.Background()

	t.Run("returns ErrOperationNotFound when operationById is null", func(t *testing.T) {
		srv := graphqlServer(t, `{"operationById": null}`)
		defer srv.Close()

		c := NewClient(srv.URL, nil)
		op, err := c.GetOperationByID(ctx, 42)
		assert.Nil(t, op)
		require.Error(t, err)
		assert.True(t, errors.Is(err, ErrOperationNotFound), "expected ErrOperationNotFound, got %v", err)
	})

	t.Run("returns the operation when it exists", func(t *testing.T) {
		srv := graphqlServer(t, `{"operationById": {"id": 42, "type": "PAYMENT"}}`)
		defer srv.Close()

		c := NewClient(srv.URL, nil)
		op, err := c.GetOperationByID(ctx, 42)
		require.NoError(t, err)
		require.NotNil(t, op)
		assert.Equal(t, int64(42), op.ID)
		assert.Equal(t, types.OperationTypePayment, op.Type)
	})
}

func TestGetTransactionOperations(t *testing.T) {
	ctx := context.Background()

	t.Run("returns ErrTransactionNotFound when transactionByHash is null", func(t *testing.T) {
		srv := graphqlServer(t, `{"transactionByHash": null}`)
		defer srv.Close()

		c := NewClient(srv.URL, nil)
		conn, err := c.GetTransactionOperations(ctx, "deadbeef", nil)
		assert.Nil(t, conn)
		require.Error(t, err)
		assert.True(t, errors.Is(err, ErrTransactionNotFound), "expected ErrTransactionNotFound, got %v", err)
	})

	t.Run("rejects null operations connection on existing transaction", func(t *testing.T) {
		srv := graphqlServer(t, `{"transactionByHash": {"operations": null}}`)
		defer srv.Close()

		c := NewClient(srv.URL, nil)
		conn, err := c.GetTransactionOperations(ctx, "deadbeef", nil)
		require.Error(t, err, "the schema declares the operations connection non-null")
		assert.NotErrorIs(t, err, ErrTransactionNotFound, "an existing transaction must not be reported as not found")
		assert.Nil(t, conn)
	})

	t.Run("returns connection with empty edges when transaction has no operations", func(t *testing.T) {
		srv := graphqlServer(t, `{"transactionByHash": {"operations": {"edges": [], "pageInfo": {"hasNextPage": false, "hasPreviousPage": false}}}}`)
		defer srv.Close()

		c := NewClient(srv.URL, nil)
		conn, err := c.GetTransactionOperations(ctx, "deadbeef", nil)
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
			_, err = w.Write([]byte(`{"data":{"transactionByHash": null}}`))
			require.NoError(t, err)
		}))
		defer srv.Close()

		c := NewClient(srv.URL, nil)
		_, err := c.GetTransactionOperations(ctx, "deadbeef", nil)
		require.ErrorIs(t, err, ErrTransactionNotFound)

		assert.Contains(t, received.Query, "transactionByHash")
		assert.Equal(t, "deadbeef", received.Variables["hash"])
	})
}

func TestGetTransactionStateChanges(t *testing.T) {
	ctx := context.Background()

	t.Run("returns ErrTransactionNotFound when transactionByHash is null", func(t *testing.T) {
		srv := graphqlServer(t, `{"transactionByHash": null}`)
		defer srv.Close()

		c := NewClient(srv.URL, nil)
		conn, err := c.GetTransactionStateChanges(ctx, "deadbeef", nil)
		assert.Nil(t, conn)
		require.Error(t, err)
		assert.True(t, errors.Is(err, ErrTransactionNotFound), "expected ErrTransactionNotFound, got %v", err)
	})

	t.Run("rejects null stateChanges connection on existing transaction", func(t *testing.T) {
		srv := graphqlServer(t, `{"transactionByHash": {"stateChanges": null}}`)
		defer srv.Close()

		c := NewClient(srv.URL, nil)
		conn, err := c.GetTransactionStateChanges(ctx, "deadbeef", nil)
		require.Error(t, err, "the schema declares the stateChanges connection non-null")
		assert.NotErrorIs(t, err, ErrTransactionNotFound, "an existing transaction must not be reported as not found")
		assert.Nil(t, conn)
	})

	t.Run("returns connection with empty edges when transaction has no state changes", func(t *testing.T) {
		srv := graphqlServer(t, `{"transactionByHash": {"stateChanges": {"edges": [], "pageInfo": {"hasNextPage": false, "hasPreviousPage": false}}}}`)
		defer srv.Close()

		c := NewClient(srv.URL, nil)
		conn, err := c.GetTransactionStateChanges(ctx, "deadbeef", nil)
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
			_, err = w.Write([]byte(`{"data":{"transactionByHash": null}}`))
			require.NoError(t, err)
		}))
		defer srv.Close()

		c := NewClient(srv.URL, nil)
		_, err := c.GetTransactionStateChanges(ctx, "deadbeef", nil)
		require.ErrorIs(t, err, ErrTransactionNotFound)

		assert.Contains(t, received.Query, "transactionByHash")
		assert.Equal(t, "deadbeef", received.Variables["hash"])
	})
}

func TestGetOperationStateChanges(t *testing.T) {
	ctx := context.Background()

	t.Run("returns ErrOperationNotFound when operationById is null", func(t *testing.T) {
		srv := graphqlServer(t, `{"operationById": null}`)
		defer srv.Close()

		c := NewClient(srv.URL, nil)
		conn, err := c.GetOperationStateChanges(ctx, 42, nil)
		assert.Nil(t, conn)
		require.Error(t, err)
		assert.True(t, errors.Is(err, ErrOperationNotFound), "expected ErrOperationNotFound, got %v", err)
	})

	t.Run("rejects null stateChanges connection on existing operation", func(t *testing.T) {
		srv := graphqlServer(t, `{"operationById": {"stateChanges": null}}`)
		defer srv.Close()

		c := NewClient(srv.URL, nil)
		conn, err := c.GetOperationStateChanges(ctx, 42, nil)
		require.Error(t, err, "the schema declares the stateChanges connection non-null")
		assert.NotErrorIs(t, err, ErrOperationNotFound, "an existing operation must not be reported as not found")
		assert.Nil(t, conn)
	})

	t.Run("returns connection with empty edges when operation has no state changes", func(t *testing.T) {
		srv := graphqlServer(t, `{"operationById": {"stateChanges": {"edges": [], "pageInfo": {"hasNextPage": false, "hasPreviousPage": false}}}}`)
		defer srv.Close()

		c := NewClient(srv.URL, nil)
		conn, err := c.GetOperationStateChanges(ctx, 42, nil)
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
			_, err = w.Write([]byte(`{"data":{"operationById": null}}`))
			require.NoError(t, err)
		}))
		defer srv.Close()

		c := NewClient(srv.URL, nil)
		_, err := c.GetOperationStateChanges(ctx, 42, nil)
		require.ErrorIs(t, err, ErrOperationNotFound)

		assert.Contains(t, received.Query, "operationById")
		assert.EqualValues(t, 42, received.Variables["id"])
	})
}

func TestGraphQLErrorsError(t *testing.T) {
	t.Run("joins all messages, prefixing each with its code when present", func(t *testing.T) {
		errs := GraphQLErrors{
			{Message: "first must be greater than 0", Extensions: map[string]any{"code": "BAD_USER_INPUT"}},
			{Message: "something else failed"},
		}
		assert.Equal(t, "BAD_USER_INPUT: first must be greater than 0; something else failed", errs.Error())
	})
}

func TestExecuteGraphQLSurfacesTypedErrors(t *testing.T) {
	ctx := context.Background()

	t.Run("returns a GraphQLErrors that carries every error and its extensions", func(t *testing.T) {
		body := `{"errors":[` +
			`{"message":"first must be greater than 0","extensions":{"code":"BAD_USER_INPUT"}},` +
			`{"message":"downstream unavailable","extensions":{"code":"INTERNAL_SERVER_ERROR"}}` +
			`]}`
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, err := io.Copy(io.Discard, r.Body)
			require.NoError(t, err)
			w.Header().Set("Content-Type", "application/json")
			_, err = w.Write([]byte(body))
			require.NoError(t, err)
		}))
		defer srv.Close()

		c := NewClient(srv.URL, nil)
		_, err := c.GetTransactionByHash(ctx, "deadbeef")
		require.Error(t, err)

		// Both messages (with codes) are present in the joined error string.
		assert.Contains(t, err.Error(), "first must be greater than 0")
		assert.Contains(t, err.Error(), "downstream unavailable")

		// errors.As extracts the typed slice.
		var gqlErrs GraphQLErrors
		require.ErrorAs(t, err, &gqlErrs)
		require.Len(t, gqlErrs, 2)

		// The extensions code is accessible per error for classification.
		assert.Equal(t, "BAD_USER_INPUT", gqlErrs[0].Extensions["code"])
		assert.Equal(t, "INTERNAL_SERVER_ERROR", gqlErrs[1].Extensions["code"])
	})
}
