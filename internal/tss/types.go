package tss

import "github.com/stellar/go/xdr"

type RPCTXStatus string
type OtherCodes int32

type TransactionResultCode int32

const (
	// Do not use NoCode
	NoCode OtherCodes = 0
	// These values need to not overlap the values in xdr.TransactionResultCode
	NewCode             OtherCodes = 100
	RPCFailCode         OtherCodes = 101
	UnMarshalBinaryCode OtherCodes = 102
)

type RPCTXCode struct {
	TxResultCode xdr.TransactionResultCode
	OtherCodes   OtherCodes
}

const (
	// Brand new transaction, not sent to RPC yet
	NewStatus RPCTXStatus = "NEW"
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

var NonJitterErrorCodes = []xdr.TransactionResultCode{
	xdr.TransactionResultCodeTxTooEarly,
	xdr.TransactionResultCodeTxTooLate,
	xdr.TransactionResultCodeTxBadSeq,
}

var JitterErrorCodes = []xdr.TransactionResultCode{
	xdr.TransactionResultCodeTxInsufficientFee,
	xdr.TransactionResultCodeTxInternalError,
}

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
	// The hash of the transaction submitted to RPC
	TransactionHash string
	TransactionXDR  string
	// The status of an RPC sendTransaction call. Can be one of [PENDING, DUPLICATE, TRY_AGAIN_LATER, ERROR]
	Status RPCTXStatus
	// The (optional) error code that is derived by deserialzing the errorResultXdr string in the sendTransaction response
	// list of possible errror codes: https://developers.stellar.org/docs/data/horizon/api-reference/errors/result-codes/transactions
	Code RPCTXCode
}

type Payload struct {
	WebhookURL string
	// The hash of the transaction xdr submitted by the client - the id of the transaction submitted by a client
	TransactionHash string
	// The xdr of the transaction
	TransactionXDR string
	// Relevant fields in an RPC sendTransaction response
	RpcSubmitTxResponse RPCSendTxResponse
	// Relevant fields in the transaction list inside the RPC getTransactions response
	RpcGetIngestTxResponse RPCGetIngestTxResponse
}

type Channel interface {
	Send(payload Payload)
	Receive(payload Payload)
	Stop()
}
