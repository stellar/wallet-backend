package types

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_BalanceEdge_UnmarshalJSON_RejectsNullNode(t *testing.T) {
	var edge BalanceEdge
	err := json.Unmarshal([]byte(`{"node": null, "cursor": "abc"}`), &edge)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing required node")
	assert.Contains(t, err.Error(), `cursor="abc"`)
}

func Test_BalanceEdge_UnmarshalJSON_RejectsMissingNode(t *testing.T) {
	var edge BalanceEdge
	err := json.Unmarshal([]byte(`{"cursor": "abc"}`), &edge)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing required node")
}

func Test_BalanceEdge_UnmarshalJSON_DecodesNativeBalance(t *testing.T) {
	payload := []byte(`{
		"cursor": "cur-1",
		"node": {
			"__typename": "NativeBalance",
			"balance": "10000.0000000",
			"tokenId": "native",
			"tokenType": "NATIVE",
			"minimumBalance": "10",
			"buyingLiabilities": "0",
			"sellingLiabilities": "0",
			"lastModifiedLedger": 12345
		}
	}`)

	var edge BalanceEdge
	err := json.Unmarshal(payload, &edge)
	require.NoError(t, err)
	require.Equal(t, "cur-1", edge.Cursor)
	require.NotNil(t, edge.Node)

	native, ok := edge.Node.(*NativeBalance)
	require.True(t, ok, "expected node to decode into *NativeBalance, got %T", edge.Node)
	assert.Equal(t, "10000.0000000", native.BalanceValue)
	assert.Equal(t, "native", native.TokenID)
	assert.Equal(t, TokenTypeNative, native.TokenType)
	assert.Equal(t, "10", native.MinimumBalance)
	assert.Equal(t, uint32(12345), native.LastModifiedLedger)
}

func Test_BalanceConnection_Balances(t *testing.T) {
	cursor1, cursor2 := "c1", "c2"
	native := &NativeBalance{BalanceValue: "100", TokenID: "native", TokenType: TokenTypeNative}
	trustline := &TrustlineBalance{BalanceValue: "50", TokenID: "USDC", TokenType: TokenTypeClassic}

	testCases := []struct {
		name string
		conn *BalanceConnection
		want []Balance
	}{
		{
			name: "nil receiver returns nil",
			conn: nil,
			want: nil,
		},
		{
			name: "empty edges returns nil",
			conn: &BalanceConnection{Edges: nil},
			want: nil,
		},
		{
			name: "populated edges flatten in order",
			conn: &BalanceConnection{Edges: []*BalanceEdge{
				{Node: native, Cursor: cursor1},
				{Node: trustline, Cursor: cursor2},
			}},
			want: []Balance{native, trustline},
		},
		{
			name: "nil edge entries are skipped",
			conn: &BalanceConnection{Edges: []*BalanceEdge{
				{Node: native, Cursor: cursor1},
				nil,
				{Node: trustline, Cursor: cursor2},
			}},
			want: []Balance{native, trustline},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.conn.Balances()
			assert.Equal(t, tc.want, got)
		})
	}
}

func Test_BalanceConnection_UnmarshalJSON_RejectsMissingEdgesField(t *testing.T) {
	payload := []byte(`{
		"pageInfo": {"hasNextPage": false, "hasPreviousPage": false}
	}`)

	var conn BalanceConnection
	err := json.Unmarshal(payload, &conn)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing required edges field")
}

func Test_BalanceConnection_UnmarshalJSON_RejectsNullEdgesField(t *testing.T) {
	payload := []byte(`{
		"edges": null,
		"pageInfo": {"hasNextPage": false, "hasPreviousPage": false}
	}`)

	var conn BalanceConnection
	err := json.Unmarshal(payload, &conn)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing required edges field")
}

func Test_BalanceConnection_UnmarshalJSON_AcceptsEmptyEdgesArray(t *testing.T) {
	payload := []byte(`{
		"edges": [],
		"pageInfo": {"hasNextPage": false, "hasPreviousPage": false}
	}`)

	var conn BalanceConnection
	err := json.Unmarshal(payload, &conn)
	require.NoError(t, err)
	require.NotNil(t, conn.Edges)
	assert.Empty(t, conn.Edges)
}

func Test_BalanceConnection_UnmarshalJSON_RejectsNullEdge(t *testing.T) {
	payload := []byte(`{
		"edges": [
			{
				"cursor": "c1",
				"node": {
					"__typename": "NativeBalance",
					"balance": "1",
					"tokenId": "native",
					"tokenType": "NATIVE"
				}
			},
			null
		],
		"pageInfo": {"hasNextPage": false, "hasPreviousPage": false}
	}`)

	var conn BalanceConnection
	err := json.Unmarshal(payload, &conn)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "edge at index 1 is null")
}

func Test_BalanceConnection_UnmarshalJSON_RejectsMissingPageInfo(t *testing.T) {
	payload := []byte(`{
		"edges": []
	}`)

	var conn BalanceConnection
	err := json.Unmarshal(payload, &conn)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing required pageInfo field")
}

func Test_BalanceConnection_UnmarshalJSON_RejectsNullPageInfo(t *testing.T) {
	payload := []byte(`{
		"edges": [],
		"pageInfo": null
	}`)

	var conn BalanceConnection
	err := json.Unmarshal(payload, &conn)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing required pageInfo field")
}

func Test_BalanceConnection_UnmarshalJSON_DecodesValidConnection(t *testing.T) {
	payload := []byte(`{
		"edges": [
			{
				"cursor": "c1",
				"node": {
					"__typename": "NativeBalance",
					"balance": "100",
					"tokenId": "native",
					"tokenType": "NATIVE"
				}
			}
		],
		"pageInfo": {"hasNextPage": true, "hasPreviousPage": false, "endCursor": "c1"}
	}`)

	var conn BalanceConnection
	err := json.Unmarshal(payload, &conn)
	require.NoError(t, err)
	require.Len(t, conn.Edges, 1)
	require.NotNil(t, conn.Edges[0])
	require.NotNil(t, conn.Edges[0].Node)
	assert.Equal(t, "c1", conn.Edges[0].Cursor)

	require.NotNil(t, conn.PageInfo)
	assert.True(t, conn.PageInfo.HasNextPage)
	require.NotNil(t, conn.PageInfo.EndCursor)
	assert.Equal(t, "c1", *conn.PageInfo.EndCursor)
}

// ---- TransactionEdge ----

func Test_TransactionEdge_UnmarshalJSON_AcceptsNullNode(t *testing.T) {
	var edge TransactionEdge
	err := json.Unmarshal([]byte(`{"node": null, "cursor": "abc"}`), &edge)
	require.NoError(t, err)
	assert.Nil(t, edge.Node)
	assert.Equal(t, "abc", edge.Cursor)
}

func Test_TransactionEdge_UnmarshalJSON_AcceptsMissingNode(t *testing.T) {
	var edge TransactionEdge
	err := json.Unmarshal([]byte(`{"cursor": "abc"}`), &edge)
	require.NoError(t, err)
	assert.Nil(t, edge.Node)
	assert.Equal(t, "abc", edge.Cursor)
}

func Test_TransactionEdge_UnmarshalJSON_DecodesPopulatedNode(t *testing.T) {
	payload := []byte(`{
		"cursor": "tx-1",
		"node": {
			"hash": "abc123",
			"feeCharged": 100,
			"resultCode": "tx_success",
			"ledgerNumber": 42,
			"ledgerCreatedAt": "2024-01-01T00:00:00Z",
			"isFeeBump": false,
			"ingestedAt": "2024-01-02T00:00:00Z"
		}
	}`)

	var edge TransactionEdge
	err := json.Unmarshal(payload, &edge)
	require.NoError(t, err)
	require.NotNil(t, edge.Node)
	assert.Equal(t, "tx-1", edge.Cursor)
	assert.Equal(t, "abc123", edge.Node.Hash)
	assert.Equal(t, int64(100), edge.Node.FeeCharged)
	assert.Equal(t, uint32(42), edge.Node.LedgerNumber)
}

// ---- OperationEdge ----

func Test_OperationEdge_UnmarshalJSON_AcceptsNullNode(t *testing.T) {
	var edge OperationEdge
	err := json.Unmarshal([]byte(`{"node": null, "cursor": "abc"}`), &edge)
	require.NoError(t, err)
	assert.Nil(t, edge.Node)
	assert.Equal(t, "abc", edge.Cursor)
}

func Test_OperationEdge_UnmarshalJSON_AcceptsMissingNode(t *testing.T) {
	var edge OperationEdge
	err := json.Unmarshal([]byte(`{"cursor": "abc"}`), &edge)
	require.NoError(t, err)
	assert.Nil(t, edge.Node)
	assert.Equal(t, "abc", edge.Cursor)
}

func Test_OperationEdge_UnmarshalJSON_DecodesPopulatedNode(t *testing.T) {
	payload := []byte(`{
		"cursor": "op-1",
		"node": {
			"id": 1234567,
			"operationType": "PAYMENT",
			"operationXdr": "AAAA",
			"resultCode": "op_success",
			"successful": true,
			"ledgerNumber": 99,
			"ledgerCreatedAt": "2024-01-01T00:00:00Z",
			"ingestedAt": "2024-01-02T00:00:00Z"
		}
	}`)

	var edge OperationEdge
	err := json.Unmarshal(payload, &edge)
	require.NoError(t, err)
	require.NotNil(t, edge.Node)
	assert.Equal(t, "op-1", edge.Cursor)
	assert.Equal(t, int64(1234567), edge.Node.ID)
	assert.Equal(t, OperationTypePayment, edge.Node.OperationType)
	assert.True(t, edge.Node.Successful)
}

// ---- TransactionConnection ----

func Test_TransactionConnection_UnmarshalJSON_AcceptsMissingEdgesField(t *testing.T) {
	payload := []byte(`{
		"pageInfo": {"hasNextPage": false, "hasPreviousPage": false}
	}`)

	var conn TransactionConnection
	err := json.Unmarshal(payload, &conn)
	require.NoError(t, err)
	assert.Nil(t, conn.Edges)
	require.NotNil(t, conn.PageInfo)
}

func Test_TransactionConnection_UnmarshalJSON_AcceptsNullEdgesField(t *testing.T) {
	payload := []byte(`{
		"edges": null,
		"pageInfo": {"hasNextPage": false, "hasPreviousPage": false}
	}`)

	var conn TransactionConnection
	err := json.Unmarshal(payload, &conn)
	require.NoError(t, err)
	assert.Nil(t, conn.Edges)
	require.NotNil(t, conn.PageInfo)
}

func Test_TransactionConnection_UnmarshalJSON_AcceptsEmptyEdgesArray(t *testing.T) {
	payload := []byte(`{
		"edges": [],
		"pageInfo": {"hasNextPage": false, "hasPreviousPage": false}
	}`)

	var conn TransactionConnection
	err := json.Unmarshal(payload, &conn)
	require.NoError(t, err)
	require.NotNil(t, conn.Edges)
	assert.Empty(t, conn.Edges)
}

func Test_TransactionConnection_UnmarshalJSON_RejectsNullEdge(t *testing.T) {
	payload := []byte(`{
		"edges": [
			{"cursor": "c1", "node": null},
			null
		],
		"pageInfo": {"hasNextPage": false, "hasPreviousPage": false}
	}`)

	var conn TransactionConnection
	err := json.Unmarshal(payload, &conn)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "edge at index 1 is null")
	assert.Contains(t, err.Error(), "TransactionEdge as non-null")
}

func Test_TransactionConnection_UnmarshalJSON_RejectsMissingPageInfo(t *testing.T) {
	payload := []byte(`{"edges": []}`)

	var conn TransactionConnection
	err := json.Unmarshal(payload, &conn)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing required pageInfo field")
}

func Test_TransactionConnection_UnmarshalJSON_RejectsNullPageInfo(t *testing.T) {
	payload := []byte(`{"edges": [], "pageInfo": null}`)

	var conn TransactionConnection
	err := json.Unmarshal(payload, &conn)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing required pageInfo field")
}

func Test_TransactionConnection_UnmarshalJSON_DecodesValidConnection(t *testing.T) {
	payload := []byte(`{
		"edges": [
			{
				"cursor": "tx-1",
				"node": {
					"hash": "abc",
					"feeCharged": 100,
					"resultCode": "tx_success",
					"ledgerNumber": 1,
					"ledgerCreatedAt": "2024-01-01T00:00:00Z",
					"isFeeBump": false,
					"ingestedAt": "2024-01-02T00:00:00Z"
				}
			}
		],
		"pageInfo": {"hasNextPage": true, "hasPreviousPage": false, "endCursor": "tx-1"}
	}`)

	var conn TransactionConnection
	err := json.Unmarshal(payload, &conn)
	require.NoError(t, err)
	require.Len(t, conn.Edges, 1)
	require.NotNil(t, conn.Edges[0].Node)
	assert.Equal(t, "abc", conn.Edges[0].Node.Hash)
	require.NotNil(t, conn.PageInfo)
	assert.True(t, conn.PageInfo.HasNextPage)
}

// ---- OperationConnection ----

func Test_OperationConnection_UnmarshalJSON_AcceptsMissingEdgesField(t *testing.T) {
	payload := []byte(`{
		"pageInfo": {"hasNextPage": false, "hasPreviousPage": false}
	}`)

	var conn OperationConnection
	err := json.Unmarshal(payload, &conn)
	require.NoError(t, err)
	assert.Nil(t, conn.Edges)
	require.NotNil(t, conn.PageInfo)
}

func Test_OperationConnection_UnmarshalJSON_AcceptsNullEdgesField(t *testing.T) {
	payload := []byte(`{
		"edges": null,
		"pageInfo": {"hasNextPage": false, "hasPreviousPage": false}
	}`)

	var conn OperationConnection
	err := json.Unmarshal(payload, &conn)
	require.NoError(t, err)
	assert.Nil(t, conn.Edges)
}

func Test_OperationConnection_UnmarshalJSON_AcceptsEmptyEdgesArray(t *testing.T) {
	payload := []byte(`{
		"edges": [],
		"pageInfo": {"hasNextPage": false, "hasPreviousPage": false}
	}`)

	var conn OperationConnection
	err := json.Unmarshal(payload, &conn)
	require.NoError(t, err)
	require.NotNil(t, conn.Edges)
	assert.Empty(t, conn.Edges)
}

func Test_OperationConnection_UnmarshalJSON_RejectsNullEdge(t *testing.T) {
	payload := []byte(`{
		"edges": [null],
		"pageInfo": {"hasNextPage": false, "hasPreviousPage": false}
	}`)

	var conn OperationConnection
	err := json.Unmarshal(payload, &conn)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "edge at index 0 is null")
	assert.Contains(t, err.Error(), "OperationEdge as non-null")
}

func Test_OperationConnection_UnmarshalJSON_RejectsMissingPageInfo(t *testing.T) {
	payload := []byte(`{"edges": []}`)

	var conn OperationConnection
	err := json.Unmarshal(payload, &conn)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing required pageInfo field")
}

func Test_OperationConnection_UnmarshalJSON_RejectsNullPageInfo(t *testing.T) {
	payload := []byte(`{"edges": [], "pageInfo": null}`)

	var conn OperationConnection
	err := json.Unmarshal(payload, &conn)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing required pageInfo field")
}

func Test_OperationConnection_UnmarshalJSON_DecodesValidConnection(t *testing.T) {
	payload := []byte(`{
		"edges": [
			{
				"cursor": "op-1",
				"node": {
					"id": 42,
					"operationType": "PAYMENT",
					"operationXdr": "AAAA",
					"resultCode": "op_success",
					"successful": true,
					"ledgerNumber": 1,
					"ledgerCreatedAt": "2024-01-01T00:00:00Z",
					"ingestedAt": "2024-01-02T00:00:00Z"
				}
			}
		],
		"pageInfo": {"hasNextPage": false, "hasPreviousPage": false, "endCursor": "op-1"}
	}`)

	var conn OperationConnection
	err := json.Unmarshal(payload, &conn)
	require.NoError(t, err)
	require.Len(t, conn.Edges, 1)
	require.NotNil(t, conn.Edges[0].Node)
	assert.Equal(t, int64(42), conn.Edges[0].Node.ID)
}

// ---- StateChangeConnection ----

func Test_StateChangeConnection_UnmarshalJSON_AcceptsMissingEdgesField(t *testing.T) {
	payload := []byte(`{
		"pageInfo": {"hasNextPage": false, "hasPreviousPage": false}
	}`)

	var conn StateChangeConnection
	err := json.Unmarshal(payload, &conn)
	require.NoError(t, err)
	assert.Nil(t, conn.Edges)
}

func Test_StateChangeConnection_UnmarshalJSON_AcceptsNullEdgesField(t *testing.T) {
	payload := []byte(`{
		"edges": null,
		"pageInfo": {"hasNextPage": false, "hasPreviousPage": false}
	}`)

	var conn StateChangeConnection
	err := json.Unmarshal(payload, &conn)
	require.NoError(t, err)
	assert.Nil(t, conn.Edges)
}

func Test_StateChangeConnection_UnmarshalJSON_AcceptsEmptyEdgesArray(t *testing.T) {
	payload := []byte(`{
		"edges": [],
		"pageInfo": {"hasNextPage": false, "hasPreviousPage": false}
	}`)

	var conn StateChangeConnection
	err := json.Unmarshal(payload, &conn)
	require.NoError(t, err)
	require.NotNil(t, conn.Edges)
	assert.Empty(t, conn.Edges)
}

func Test_StateChangeConnection_UnmarshalJSON_RejectsNullEdge(t *testing.T) {
	payload := []byte(`{
		"edges": [null],
		"pageInfo": {"hasNextPage": false, "hasPreviousPage": false}
	}`)

	var conn StateChangeConnection
	err := json.Unmarshal(payload, &conn)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "edge at index 0 is null")
	assert.Contains(t, err.Error(), "StateChangeEdge as non-null")
}

func Test_StateChangeConnection_UnmarshalJSON_RejectsMissingPageInfo(t *testing.T) {
	payload := []byte(`{"edges": []}`)

	var conn StateChangeConnection
	err := json.Unmarshal(payload, &conn)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing required pageInfo field")
}

func Test_StateChangeConnection_UnmarshalJSON_RejectsNullPageInfo(t *testing.T) {
	payload := []byte(`{"edges": [], "pageInfo": null}`)

	var conn StateChangeConnection
	err := json.Unmarshal(payload, &conn)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing required pageInfo field")
}

func Test_StateChangeConnection_UnmarshalJSON_DecodesValidConnection(t *testing.T) {
	payload := []byte(`{
		"edges": [
			{
				"cursor": "sc-1",
				"node": {
					"__typename": "StandardBalanceChange",
					"type": "CREDIT",
					"reason": "PAYMENT",
					"ingestedAt": "2024-01-02T00:00:00Z",
					"ledgerCreatedAt": "2024-01-01T00:00:00Z",
					"ledgerNumber": 1,
					"standardBalanceTokenId": "native",
					"amount": "100"
				}
			},
			{
				"cursor": "sc-2",
				"node": null
			}
		],
		"pageInfo": {"hasNextPage": false, "hasPreviousPage": false, "endCursor": "sc-2"}
	}`)

	var conn StateChangeConnection
	err := json.Unmarshal(payload, &conn)
	require.NoError(t, err)
	require.Len(t, conn.Edges, 2)

	require.NotNil(t, conn.Edges[0].Node)
	standard, ok := conn.Edges[0].Node.(*StandardBalanceChange)
	require.True(t, ok, "expected first node to decode into *StandardBalanceChange, got %T", conn.Edges[0].Node)
	assert.Equal(t, "100", standard.Amount)
	assert.Equal(t, "native", standard.TokenID)

	assert.Nil(t, conn.Edges[1].Node, "schema permits null node and StateChangeEdge must preserve that")
	assert.Equal(t, "sc-2", conn.Edges[1].Cursor)
}
