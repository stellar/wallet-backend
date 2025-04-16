package tss

import (
	"errors"
	"testing"

	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/entities"
)

func TestParseToRPCSendTxResponse(t *testing.T) {
	t.Run("rpc_request_fails", func(t *testing.T) {
		resp, err := ParseToRPCGetIngestTxResponse(entities.RPCGetTransactionResult{}, errors.New("sending sendTransaction request: sending POST request to RPC: connection failed"))
		require.Error(t, err)

		assert.Equal(t, entities.ErrorStatus, resp.Status)
		assert.ErrorContains(t, err, "sending sendTransaction request: sending POST request to RPC: connection failed")
	})

	t.Run("response_has_empty_errorResultXdr", func(t *testing.T) {
		resp, err := ParseToRPCSendTxResponse("", entities.RPCSendTransactionResult{
			Status:         "PENDING",
			ErrorResultXDR: "",
		}, nil)

		assert.Equal(t, entities.PendingStatus, resp.Status.RPCStatus)
		assert.Equal(t, "", resp.ErrorResultXDR)
		assert.Equal(t, EmptyCode, resp.Code.OtherCodes)
		assert.Empty(t, err)
	})

	t.Run("response_has_unparsable_errorResultXdr", func(t *testing.T) {
		resp, err := ParseToRPCSendTxResponse("", entities.RPCSendTransactionResult{
			ErrorResultXDR: "ABC123",
		}, nil)

		assert.Equal(t, UnmarshalBinaryCode, resp.Code.OtherCodes)
		assert.Equal(t, "parse error result xdr string: unable to parse: unable to unmarshal errorResultXDR: ABC123", err.Error())
	})

	t.Run("response_has_errorResultXdr", func(t *testing.T) {
		resp, err := ParseToRPCSendTxResponse("", entities.RPCSendTransactionResult{
			ErrorResultXDR: "AAAAAAAAAMj////9AAAAAA==",
		}, nil)

		assert.Equal(t, "AAAAAAAAAMj////9AAAAAA==", resp.ErrorResultXDR)
		assert.Equal(t, xdr.TransactionResultCodeTxTooLate, resp.Code.TxResultCode)
		assert.Empty(t, err)
	})
}

func TestParseToRPCGetIngestTxResponse(t *testing.T) {
	t.Run("rpc_request_fails", func(t *testing.T) {
		resp, err := ParseToRPCGetIngestTxResponse(entities.RPCGetTransactionResult{}, errors.New("sending getTransaction request: sending POST request to RPC: connection failed"))
		require.Error(t, err)

		assert.Equal(t, entities.ErrorStatus, resp.Status)
		assert.ErrorContains(t, err, "sending getTransaction request: sending POST request to RPC: connection failed")
	})

	t.Run("unable_to_parse_createdAt", func(t *testing.T) {
		resp, err := ParseToRPCGetIngestTxResponse(entities.RPCGetTransactionResult{
			Status:    "SUCCESS",
			CreatedAt: "ABCD",
		}, nil)
		require.Error(t, err)

		assert.Equal(t, entities.ErrorStatus, resp.Status)
		assert.ErrorContains(t, err, "unable to parse createdAt: strconv.ParseInt: parsing \"ABCD\": invalid syntax")
	})

	t.Run("response_has_createdAt_field", func(t *testing.T) {
		resp, err := ParseToRPCGetIngestTxResponse(entities.RPCGetTransactionResult{
			CreatedAt: "1234567",
		}, nil)
		require.NoError(t, err)

		assert.Equal(t, int64(1234567), resp.CreatedAt)
		assert.Empty(t, err)
	})

	t.Run("response_has_errorResultXdr", func(t *testing.T) {
		resp, err := ParseToRPCGetIngestTxResponse(entities.RPCGetTransactionResult{
			Status:    entities.ErrorStatus,
			CreatedAt: "1234567",
			ResultXDR: "AAAAAAAAAMj////9AAAAAA==",
		}, nil)

		assert.Equal(t, xdr.TransactionResultCodeTxTooLate, resp.Code.TxResultCode)
		assert.Empty(t, err)
	})
}
