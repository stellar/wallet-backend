package tss

type RPCIngestTxResponse struct {
	// the raw TransactionEnvelope XDR of the transaction corresponding to the sendTransaction call that returned a PENDING status
	envelopeXdr string
	// the raw TransactionResult XDR of the envelopeXdr
	resultXdr string
}

type RPCSendTxResponse struct {
	// the status of an RPC sendTransaction call. Can be one of [PENDING, DUPLICATE, TRY_AGAIN_LATER, ERROR]
	status string
	/*
		the (optional) error code that is derived by deserialzing the errorResultXdr string in the sendTransaction response
		list of possible errror codes: https://developers.stellar.org/docs/data/horizon/api-reference/errors/result-codes/transactions
	*/
	errorCode string
}

type RPCPayload struct {
	// the transaction xdr submitted by the client
	transactionId       string
	rpcSubmitTxResponse RPCSendTxResponse
	rpcIngestTxResponse RPCIngestTxResponse
}

type Channel interface {
	send(payload RPCPayload)
	receive(payload RPCPayload)
}
