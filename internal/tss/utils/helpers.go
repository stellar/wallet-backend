package utils

import (
	"github.com/stellar/wallet-backend/internal/tss"
)

func PayloadTOTSSResponse(payload tss.Payload) tss.TSSResponse {
	response := tss.TSSResponse{}
	response.TransactionHash = payload.TransactionHash
	if payload.RPCSubmitTxResponse.Status.Status() != "" {
		response.Status = string(payload.RPCSubmitTxResponse.Status.Status())
		response.TransactionResultCode = payload.RPCSubmitTxResponse.Code.TxResultCode.String()
		response.EnvelopeXDR = payload.RPCSubmitTxResponse.TransactionXDR
		response.ResultXDR = payload.RPCSubmitTxResponse.ErrorResultXDR
	} else if payload.RPCGetIngestTxResponse.Status != "" {
		response.Status = string(payload.RPCGetIngestTxResponse.Status)
		response.TransactionResultCode = payload.RPCGetIngestTxResponse.Code.TxResultCode.String()
		response.EnvelopeXDR = payload.RPCGetIngestTxResponse.EnvelopeXDR
		response.ResultXDR = payload.RPCGetIngestTxResponse.ResultXDR
		response.CreatedAt = payload.RPCGetIngestTxResponse.CreatedAt
	}
	return response
}
