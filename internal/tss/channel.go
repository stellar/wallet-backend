package tss

type RPCIngestTxResponse struct {
	// a status that indicated whether this transaction failed or successly made it to the ledger
	Status string
	// the raw TransactionEnvelope XDR for this transaction
	EnvelopeXdr string
	// the raw TransactionResult XDR of the envelopeXdr
	ResultXdr string
	// The unix timestamp of when the transaction was included in the ledger
	CreatedAt int64
}

type RPCSendTxResponse struct {
	// the status of an RPC sendTransaction call. Can be one of [PENDING, DUPLICATE, TRY_AGAIN_LATER, ERROR]
	Status string
	// the (optional) error code that is derived by deserialzing the errorResultXdr string in the sendTransaction response
	// list of possible errror codes: https://developers.stellar.org/docs/data/horizon/api-reference/errors/result-codes/transactions
	ErrorCode string
}

type Payload struct {
	// the hash of the transaction xdr submitted by the client - the id of the transaction submitted by a client
	TransactionHash string
	// relevant fields in an RPC sendTransaction response
	RpcSubmitTxResponse RPCSendTxResponse
	// relevant fields in the transaction list inside the RPC getTransactions response
	RpcIngestTxResponse RPCIngestTxResponse
}

type Channel interface {
	send(payload Payload)
	receive(payload Payload)
}
