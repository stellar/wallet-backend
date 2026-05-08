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
