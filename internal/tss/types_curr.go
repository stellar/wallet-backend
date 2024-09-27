package tss

/*
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

func (c RPCTXCode) Code() int {
	if c.OtherCodes != NoCode {
		return int(c.OtherCodes)
	}
	return int(c.TxResultCode)
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
	// The error code that is derived by deserialzing the ResultXdr string in the sendTransaction response
	// list of possible errror codes: https://developers.stellar.org/docs/data/horizon/api-reference/errors/result-codes/transactions
	Code RPCTXCode
	// The raw TransactionEnvelope XDR for this transaction
	EnvelopeXDR string
	// The raw TransactionResult XDR of the envelopeXdr
	ResultXDR        string
	Ledger           int
	ApplicationOrder int
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

type Pagination struct {
	Cursor string `json:"cursor,omitempty"`
	Limit  int    `json:"limit"`
}

type RPCParams struct {
	Transaction string     `json:"transaction,omitempty"`
	Hash        string     `json:"hash,omitempty"`
	StartLedger int        `json:"startLedger,omitempty"`
	Pagination  Pagination `json:"pagination,omitempty"`
}

type Transaction struct {
	Status           string `json:"status"`
	ApplicationOrder int    `json:"applicationOrder"`
	FeeBump          bool   `json:"feeBump"`
	EnvelopeXDR      string `json:"envelopeXdr"`
	ResultXDR        string `json:"resultXdr"`
	ResultMetaXDR    string `json:"resultMetaXdr"`
	Ledger           int    `json:"ledger"`
	CreatedAt        int    `json:"createdAt"`
}

type RPCResult struct {
	Status         string        `json:"status"`
	EnvelopeXDR    string        `json:"envelopeXdr"`
	ResultXDR      string        `json:"resultXdr"`
	ErrorResultXDR string        `json:"errorResultXdr"`
	Hash           string        `json:"hash"`
	Transactions   []Transaction `json:"transactions,omitempty"`
	Cursor         string        `json:"cursor"`
	CreatedAt      string        `json:"createdAt"`
}

type RPCResponse struct {
	RPCResult `json:"result"`
}

type TSSResponse struct {
	TransactionHash       string `json:"tx_hash"`
	TransactionResultCode string `json:"tx_result_code"`
	Status                string `json:"status"`
	CreatedAt             int64  `json:"created_at"`
	EnvelopeXDR           string `json:"envelopeXdr"`
	ResultXDR             string `json:"resultXdr"`
}

type Channel interface {
	Send(payload Payload)
	Receive(payload Payload)
	Stop()
}
*/
