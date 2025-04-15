package utils

import (
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/txnbuild"

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
