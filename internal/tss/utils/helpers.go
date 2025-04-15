package utils

import (
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/txnbuild"

	"github.com/stellar/wallet-backend/internal/tss"
)

func PayloadTOTSSResponse(payload tss.Payload) tss.TSSResponse {
	response := tss.TSSResponse{}
	response.TransactionHash = payload.TransactionHash
	if payload.RpcSubmitTxResponse.Status.Status() != "" {
		response.Status = string(payload.RpcSubmitTxResponse.Status.Status())
		response.TransactionResultCode = payload.RpcSubmitTxResponse.Code.TxResultCode.String()
		response.EnvelopeXDR = payload.RpcSubmitTxResponse.TransactionXDR
		response.ResultXDR = payload.RpcSubmitTxResponse.ErrorResultXDR
	} else if payload.RpcGetIngestTxResponse.Status != "" {
		response.Status = string(payload.RpcGetIngestTxResponse.Status)
		response.TransactionResultCode = payload.RpcGetIngestTxResponse.Code.TxResultCode.String()
		response.EnvelopeXDR = payload.RpcGetIngestTxResponse.EnvelopeXDR
		response.ResultXDR = payload.RpcGetIngestTxResponse.ResultXDR
		response.CreatedAt = payload.RpcGetIngestTxResponse.CreatedAt
	}
	return response
}

func BuildTestTransaction() *txnbuild.Transaction {
	accountToSponsor := keypair.MustRandom()

	tx, _ := txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount: &txnbuild.SimpleAccount{
			AccountID: accountToSponsor.Address(),
			Sequence:  124,
		},
		IncrementSequenceNum: true,
		Operations: []txnbuild.Operation{
			&txnbuild.Payment{
				Destination: keypair.MustRandom().Address(),
				Amount:      "14.0000000",
				Asset:       txnbuild.NativeAsset{},
			},
		},
		BaseFee:       104,
		Preconditions: txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(10)},
	})
	return tx
}

func BuildTestFeeBumpTransaction() *txnbuild.FeeBumpTransaction {
	feeBumpTx, _ := txnbuild.NewFeeBumpTransaction(
		txnbuild.FeeBumpTransactionParams{
			Inner:      BuildTestTransaction(),
			FeeAccount: keypair.MustRandom().Address(),
			BaseFee:    110,
		})
	return feeBumpTx
}
