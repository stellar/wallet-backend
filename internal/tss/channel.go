package tss

type RPCTXStatus string

const (
	// RPC sendTransaction statuses
	PendingStatus       RPCTXStatus = "PENDING"
	DuplicateStatus     RPCTXStatus = "DUPLICATE"
	TryAgainLaterStatus RPCTXStatus = "TRY_AGAIN_LATER"
	ErrorStatus         RPCTXStatus = "ERROR"
	// RPC getTransaction(s) statuses
	NotFoundStatus RPCTXStatus = "NOT_FOUND"
	FailedStatus   RPCTXStatus = "FAILED"
	SuccessStatus  RPCTXStatus = "SUCCESS"
)

type RPCGetIngestTxResponse struct {
	// A status that indicated whether this transaction failed or successly made it to the ledger
	Status RPCTXStatus
	// The raw TransactionEnvelope XDR for this transaction
	EnvelopeXDR string
	// The raw TransactionResult XDR of the envelopeXdr
	ResultXDR string
	// The unix timestamp of when the transaction was included in the ledger
	CreatedAt int64
}

type RPCSendTxResponse struct {
	// The status of an RPC sendTransaction call. Can be one of [PENDING, DUPLICATE, TRY_AGAIN_LATER, ERROR]
	Status RPCTXStatus
	// The (optional) error code that is derived by deserialzing the errorResultXdr string in the sendTransaction response
	// list of possible errror codes: https://developers.stellar.org/docs/data/horizon/api-reference/errors/result-codes/transactions
	ErrorCode string
}

type Payload struct {
	// The hash of the transaction xdr submitted by the client - the id of the transaction submitted by a client
	TransactionHash string
	// Relevant fields in an RPC sendTransaction response
	RpcSubmitTxResponse RPCSendTxResponse
	// Relevant fields in the transaction list inside the RPC getTransactions response
	RpcGetIngestTxResponse RPCGetIngestTxResponse
}

type Channel interface {
	send(payload Payload)
	receive(payload Payload)
}
