// Package wbclient provides client tests for the wallet backend GraphQL API
package wbclient

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/pkg/wbclient/types"
)

// mockGraphQLHandler creates a test HTTP handler that returns a GraphQL response
func mockGraphQLHandler(t *testing.T, expectedQuery string, responseData interface{}) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req GraphQLRequest
		err := json.NewDecoder(r.Body).Decode(&req)
		require.NoError(t, err)

		// Verify it's a GraphQL request
		assert.Contains(t, req.Query, expectedQuery)

		// Marshal response data
		dataBytes, err := json.Marshal(responseData)
		require.NoError(t, err)

		// Create GraphQL response
		response := GraphQLResponse{
			Data: dataBytes,
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(response) //nolint:errcheck // test code
	}
}

// mockGraphQLErrorHandler creates a test HTTP handler that returns a GraphQL error
func mockGraphQLErrorHandler(errorMessage string, errorCode string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		response := GraphQLResponse{
			Errors: []GraphQLError{
				{
					Message: errorMessage,
					Extensions: map[string]interface{}{
						"code": errorCode,
					},
				},
			},
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(response) //nolint:errcheck // test code
	}
}

// mockHTTPErrorHandler creates a test HTTP handler that returns an HTTP error
func mockHTTPErrorHandler(statusCode int) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(statusCode)
		_, _ = w.Write([]byte("HTTP error")) //nolint:errcheck // test code
	}
}

// createTestClient creates a client for testing with a mock server
func createTestClient(handler http.HandlerFunc) (*Client, *httptest.Server) {
	server := httptest.NewServer(handler)
	client := NewClient(server.URL, nil)
	return client, server
}

func TestClient_RegisterAccount(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		address := "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"
		responseData := map[string]interface{}{
			"registerAccount": types.RegisterAccountPayload{
				Success: true,
				Account: &types.Account{
					Address: address,
				},
			},
		}

		client, server := createTestClient(mockGraphQLHandler(t, "registerAccount", responseData))
		defer server.Close()

		result, err := client.RegisterAccount(context.Background(), address)
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.True(t, result.Success)
		assert.NotNil(t, result.Account)
		assert.Equal(t, address, result.Account.Address)
	})

	t.Run("graphql_error", func(t *testing.T) {
		client, server := createTestClient(mockGraphQLErrorHandler("Account already exists", "ACCOUNT_ALREADY_EXISTS"))
		defer server.Close()

		result, err := client.RegisterAccount(context.Background(), "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N")
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "Account already exists")
	})

	t.Run("http_error", func(t *testing.T) {
		client, server := createTestClient(mockHTTPErrorHandler(http.StatusInternalServerError))
		defer server.Close()

		result, err := client.RegisterAccount(context.Background(), "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N")
		assert.Error(t, err)
		assert.Nil(t, result)
	})
}

func TestClient_DeregisterAccount(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		address := "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"
		message := "Account successfully deregistered"
		responseData := map[string]interface{}{
			"deregisterAccount": types.DeregisterAccountPayload{
				Success: true,
				Message: &message,
			},
		}

		client, server := createTestClient(mockGraphQLHandler(t, "deregisterAccount", responseData))
		defer server.Close()

		result, err := client.DeregisterAccount(context.Background(), address)
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.True(t, result.Success)
		assert.NotNil(t, result.Message)
		assert.Equal(t, message, *result.Message)
	})

	t.Run("graphql_error", func(t *testing.T) {
		client, server := createTestClient(mockGraphQLErrorHandler("Account not found", "ACCOUNT_NOT_FOUND"))
		defer server.Close()

		result, err := client.DeregisterAccount(context.Background(), "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N")
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "Account not found")
	})

	t.Run("http_error", func(t *testing.T) {
		client, server := createTestClient(mockHTTPErrorHandler(http.StatusInternalServerError))
		defer server.Close()

		result, err := client.DeregisterAccount(context.Background(), "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N")
		assert.Error(t, err)
		assert.Nil(t, result)
	})
}

func TestClient_GetTransactionByHash(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		hash := "abc123"
		responseData := map[string]interface{}{
			"transactionByHash": &types.GraphQLTransaction{
				Hash:         hash,
				EnvelopeXdr:  "envelopeXdr",
				ResultXdr:    "resultXdr",
				MetaXdr:      "metaXdr",
				LedgerNumber: 12345,
			},
		}

		client, server := createTestClient(mockGraphQLHandler(t, "transactionByHash", responseData))
		defer server.Close()

		result, err := client.GetTransactionByHash(context.Background(), hash)
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, hash, result.Hash)
		assert.Equal(t, uint32(12345), result.LedgerNumber)
	})

	t.Run("not_found", func(t *testing.T) {
		responseData := map[string]interface{}{
			"transactionByHash": nil,
		}

		client, server := createTestClient(mockGraphQLHandler(t, "transactionByHash", responseData))
		defer server.Close()

		result, err := client.GetTransactionByHash(context.Background(), "nonexistent")
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("graphql_error", func(t *testing.T) {
		client, server := createTestClient(mockGraphQLErrorHandler("Transaction not found", "TRANSACTION_NOT_FOUND"))
		defer server.Close()

		result, err := client.GetTransactionByHash(context.Background(), "abc123")
		assert.Error(t, err)
		assert.Nil(t, result)
	})
}

func TestClient_GetTransactions(t *testing.T) {
	t.Run("success_with_pagination", func(t *testing.T) {
		first := int32(10)
		after := "cursor1"
		startCursor := "cursor1"
		endCursor := "cursor10"

		responseData := map[string]interface{}{
			"transactions": &types.TransactionConnection{
				Edges: []*types.TransactionEdge{
					{
						Node: &types.GraphQLTransaction{
							Hash:         "tx1",
							LedgerNumber: 100,
						},
						Cursor: "cursor2",
					},
				},
				PageInfo: &types.PageInfo{
					StartCursor:     &startCursor,
					EndCursor:       &endCursor,
					HasNextPage:     true,
					HasPreviousPage: false,
				},
			},
		}

		client, server := createTestClient(mockGraphQLHandler(t, "transactions", responseData))
		defer server.Close()

		result, err := client.GetTransactions(context.Background(), &first, nil, &after, nil)
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Len(t, result.Edges, 1)
		assert.True(t, result.PageInfo.HasNextPage)
		assert.False(t, result.PageInfo.HasPreviousPage)
	})

	t.Run("graphql_error", func(t *testing.T) {
		client, server := createTestClient(mockGraphQLErrorHandler("Invalid pagination parameters", "INVALID_PAGINATION"))
		defer server.Close()

		first := int32(10)
		result, err := client.GetTransactions(context.Background(), &first, nil, nil, nil)
		assert.Error(t, err)
		assert.Nil(t, result)
	})
}

func TestClient_GetAccountByAddress(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		address := "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"
		responseData := map[string]interface{}{
			"accountByAddress": &types.Account{
				Address: address,
			},
		}

		client, server := createTestClient(mockGraphQLHandler(t, "accountByAddress", responseData))
		defer server.Close()

		result, err := client.GetAccountByAddress(context.Background(), address)
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, address, result.Address)
	})

	t.Run("not_found", func(t *testing.T) {
		responseData := map[string]interface{}{
			"accountByAddress": nil,
		}

		client, server := createTestClient(mockGraphQLHandler(t, "accountByAddress", responseData))
		defer server.Close()

		result, err := client.GetAccountByAddress(context.Background(), "nonexistent")
		require.NoError(t, err)
		assert.Nil(t, result)
	})
}

func TestClient_GetOperations(t *testing.T) {
	t.Run("success_with_pagination", func(t *testing.T) {
		first := int32(5)
		startCursor := "cursor1"
		endCursor := "cursor5"

		responseData := map[string]interface{}{
			"operations": &types.OperationConnection{
				Edges: []*types.OperationEdge{
					{
						Node: &types.Operation{
							ID:            123,
							OperationType: types.OperationTypePayment,
							LedgerNumber:  100,
						},
						Cursor: "cursor2",
					},
				},
				PageInfo: &types.PageInfo{
					StartCursor:     &startCursor,
					EndCursor:       &endCursor,
					HasNextPage:     false,
					HasPreviousPage: false,
				},
			},
		}

		client, server := createTestClient(mockGraphQLHandler(t, "operations", responseData))
		defer server.Close()

		result, err := client.GetOperations(context.Background(), &first, nil, nil, nil)
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Len(t, result.Edges, 1)
		assert.Equal(t, int64(123), result.Edges[0].Node.ID)
		assert.Equal(t, types.OperationTypePayment, result.Edges[0].Node.OperationType)
	})
}

func TestClient_GetOperationByID(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		opID := int64(456)
		responseData := map[string]interface{}{
			"operationById": &types.Operation{
				ID:            opID,
				OperationType: types.OperationTypeCreateAccount,
				OperationXdr:  "operationXdr",
				LedgerNumber:  200,
			},
		}

		client, server := createTestClient(mockGraphQLHandler(t, "operationById", responseData))
		defer server.Close()

		result, err := client.GetOperationByID(context.Background(), opID)
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, opID, result.ID)
		assert.Equal(t, types.OperationTypeCreateAccount, result.OperationType)
	})

	t.Run("not_found", func(t *testing.T) {
		responseData := map[string]interface{}{
			"operationById": nil,
		}

		client, server := createTestClient(mockGraphQLHandler(t, "operationById", responseData))
		defer server.Close()

		result, err := client.GetOperationByID(context.Background(), 999)
		require.NoError(t, err)
		assert.Nil(t, result)
	})
}

func TestClient_GetStateChanges(t *testing.T) {
	t.Run("success_with_balance_change", func(t *testing.T) {
		first := int32(10)

		// Use raw JSON to include __typename field
		responseJSON := `{
			"data": {
				"stateChanges": {
					"edges": [
						{
							"node": {
								"__typename": "StandardBalanceChange",
								"type": "BALANCE",
								"reason": "CREDIT",
								"ingestedAt": "2024-01-01T00:00:00Z",
								"ledgerCreatedAt": "2024-01-01T00:00:00Z",
								"ledgerNumber": 100,
								"tokenId": "native",
								"amount": "100.0000000"
							},
							"cursor": "cursor2"
						}
					],
					"pageInfo": {
						"startCursor": "cursor1",
						"endCursor": "cursor10",
						"hasNextPage": true,
						"hasPreviousPage": false
					}
				}
			}
		}`

		handler := func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			if _, err := w.Write([]byte(responseJSON)); err != nil {
				t.Errorf("failed to write response: %v", err)
			}
		}

		client, server := createTestClient(handler)
		defer server.Close()

		result, err := client.GetStateChanges(context.Background(), &first, nil, nil, nil)
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Len(t, result.Edges, 1)

		// Use type assertion to access the specific state change type
		balanceChange, ok := result.Edges[0].Node.(*types.StandardBalanceChange)
		require.True(t, ok, "expected StandardBalanceChange type")
		assert.Equal(t, types.StateChangeCategoryBalance, balanceChange.Type)
		assert.Equal(t, types.StateChangeReasonCredit, balanceChange.Reason)
		assert.Equal(t, "native", balanceChange.TokenID)
		assert.Equal(t, "100.0000000", balanceChange.Amount)
	})

	t.Run("success_with_signer_change", func(t *testing.T) {
		first := int32(10)
		signerAddr := "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"

		// Use raw JSON to include __typename field
		responseJSON := `{
			"data": {
				"stateChanges": {
					"edges": [
						{
							"node": {
								"__typename": "SignerChange",
								"type": "SIGNER",
								"reason": "ADD",
								"ingestedAt": "2024-01-01T00:00:00Z",
								"ledgerCreatedAt": "2024-01-01T00:00:00Z",
								"ledgerNumber": 100,
								"signerAddress": "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N",
								"signerWeights": "1"
							},
							"cursor": "cursor3"
						}
					],
					"pageInfo": {
						"hasNextPage": false,
						"hasPreviousPage": false
					}
				}
			}
		}`

		handler := func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			if _, err := w.Write([]byte(responseJSON)); err != nil {
				t.Errorf("failed to write response: %v", err)
			}
		}

		client, server := createTestClient(handler)
		defer server.Close()

		result, err := client.GetStateChanges(context.Background(), &first, nil, nil, nil)
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Len(t, result.Edges, 1)

		// Use type assertion to access the specific state change type
		signerChange, ok := result.Edges[0].Node.(*types.SignerChange)
		require.True(t, ok, "expected SignerChange type")
		assert.Equal(t, types.StateChangeCategorySigner, signerChange.Type)
		assert.NotNil(t, signerChange.SignerAddress)
		assert.Equal(t, signerAddr, *signerChange.SignerAddress)
	})
}

func TestClient_GetTransactionByHash_WithFieldSelection(t *testing.T) {
	t.Run("success_with_all_fields", func(t *testing.T) {
		hash := "abc123"
		responseData := map[string]interface{}{
			"transactionByHash": &types.GraphQLTransaction{
				Hash:         hash,
				EnvelopeXdr:  "envelopeXdr",
				ResultXdr:    "resultXdr",
				MetaXdr:      "metaXdr",
				LedgerNumber: 12345,
			},
		}

		client, server := createTestClient(mockGraphQLHandler(t, "transactionByHash", responseData))
		defer server.Close()

		opts := &QueryOptions{
			TransactionFields: TransactionFields.AllFields(),
		}

		result, err := client.GetTransactionByHash(context.Background(), hash, opts)
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, hash, result.Hash)
	})

	t.Run("success_with_custom_fields", func(t *testing.T) {
		hash := "abc123"
		responseData := map[string]interface{}{
			"transactionByHash": &types.GraphQLTransaction{
				Hash:         hash,
				EnvelopeXdr:  "envelopeXdr",
				LedgerNumber: 12345,
			},
		}

		client, server := createTestClient(mockGraphQLHandler(t, "transactionByHash", responseData))
		defer server.Close()

		opts := &QueryOptions{
			TransactionFields: []string{"hash", "envelopeXdr", "ledgerNumber"},
		}

		result, err := client.GetTransactionByHash(context.Background(), hash, opts)
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, hash, result.Hash)
		assert.Equal(t, "envelopeXdr", result.EnvelopeXdr)
	})
}

func TestClient_GetOperations_WithFieldSelection(t *testing.T) {
	t.Run("success_with_all_fields", func(t *testing.T) {
		first := int32(5)

		responseData := map[string]interface{}{
			"operations": &types.OperationConnection{
				Edges: []*types.OperationEdge{
					{
						Node: &types.Operation{
							ID:            123,
							OperationType: types.OperationTypePayment,
							OperationXdr:  "xdr",
							LedgerNumber:  100,
						},
						Cursor: "cursor2",
					},
				},
				PageInfo: &types.PageInfo{
					HasNextPage:     false,
					HasPreviousPage: false,
				},
			},
		}

		client, server := createTestClient(mockGraphQLHandler(t, "operations", responseData))
		defer server.Close()

		opts := &QueryOptions{
			OperationFields: OperationFields.AllFields(),
		}

		result, err := client.GetOperations(context.Background(), &first, nil, nil, nil, opts)
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Len(t, result.Edges, 1)
	})
}

func TestClient_GetAccountByAddress_WithFieldSelection(t *testing.T) {
	t.Run("success_with_all_fields", func(t *testing.T) {
		address := "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"
		responseData := map[string]interface{}{
			"accountByAddress": &types.Account{
				Address: address,
			},
		}

		client, server := createTestClient(mockGraphQLHandler(t, "accountByAddress", responseData))
		defer server.Close()

		opts := &QueryOptions{
			AccountFields: AccountFields.AllFields(),
		}

		result, err := client.GetAccountByAddress(context.Background(), address, opts)
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, address, result.Address)
	})

	t.Run("success_with_minimal_fields", func(t *testing.T) {
		address := "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"
		responseData := map[string]interface{}{
			"accountByAddress": &types.Account{
				Address: address,
			},
		}

		client, server := createTestClient(mockGraphQLHandler(t, "accountByAddress", responseData))
		defer server.Close()

		opts := &QueryOptions{
			AccountFields: AccountFields.MinimalFields(),
		}

		result, err := client.GetAccountByAddress(context.Background(), address, opts)
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, address, result.Address)
	})
}

func TestClient_GetStateChanges_WithPolymorphicTypes(t *testing.T) {
	t.Run("success_with_multiple_types", func(t *testing.T) {
		first := int32(10)

		// Mock response with different state change types
		responseJSON := `{
			"data": {
				"stateChanges": {
					"edges": [
						{
							"node": {
								"__typename": "StandardBalanceChange",
								"type": "BALANCE",
								"reason": "CREDIT",
								"ingestedAt": "2024-01-01T00:00:00Z",
								"ledgerCreatedAt": "2024-01-01T00:00:00Z",
								"ledgerNumber": 100,
								"tokenId": "native",
								"amount": "100.0000000"
							},
							"cursor": "cursor1"
						},
						{
							"node": {
								"__typename": "SignerChange",
								"type": "SIGNER",
								"reason": "ADD",
								"ingestedAt": "2024-01-01T00:00:00Z",
								"ledgerCreatedAt": "2024-01-01T00:00:00Z",
								"ledgerNumber": 101,
								"signerAddress": "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N",
								"signerWeights": "1"
							},
							"cursor": "cursor2"
						},
						{
							"node": {
								"__typename": "MetadataChange",
								"type": "METADATA",
								"reason": "SET",
								"ingestedAt": "2024-01-01T00:00:00Z",
								"ledgerCreatedAt": "2024-01-01T00:00:00Z",
								"ledgerNumber": 102,
								"keyValue": "{\"key\":\"value\"}"
							},
							"cursor": "cursor3"
						}
					],
					"pageInfo": {
						"hasNextPage": false,
						"hasPreviousPage": false
					}
				}
			}
		}`

		handler := func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			if _, err := w.Write([]byte(responseJSON)); err != nil {
				t.Errorf("failed to write response: %v", err)
			}
		}

		client, server := createTestClient(handler)
		defer server.Close()

		result, err := client.GetStateChanges(context.Background(), &first, nil, nil, nil)
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Len(t, result.Edges, 3)

		// Check first edge is StandardBalanceChange
		balanceChange, ok := result.Edges[0].Node.(*types.StandardBalanceChange)
		require.True(t, ok, "expected StandardBalanceChange type")
		assert.Equal(t, types.StateChangeCategoryBalance, balanceChange.Type)
		assert.Equal(t, types.StateChangeReasonCredit, balanceChange.Reason)
		assert.Equal(t, "native", balanceChange.TokenID)
		assert.Equal(t, "100.0000000", balanceChange.Amount)

		// Check second edge is SignerChange
		signerChange, ok := result.Edges[1].Node.(*types.SignerChange)
		require.True(t, ok, "expected SignerChange type")
		assert.Equal(t, types.StateChangeCategorySigner, signerChange.Type)
		assert.Equal(t, types.StateChangeReasonAdd, signerChange.Reason)
		assert.NotNil(t, signerChange.SignerAddress)
		assert.Equal(t, "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N", *signerChange.SignerAddress)

		// Check third edge is MetadataChange
		metadataChange, ok := result.Edges[2].Node.(*types.MetadataChange)
		require.True(t, ok, "expected MetadataChange type")
		assert.Equal(t, types.StateChangeCategoryMetadata, metadataChange.Type)
		assert.Equal(t, types.StateChangeReasonSet, metadataChange.Reason)
		assert.Equal(t, "{\"key\":\"value\"}", metadataChange.KeyValue)
	})
}
