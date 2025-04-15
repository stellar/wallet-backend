package tss

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"strconv"

	xdr3 "github.com/stellar/go-xdr/xdr3"
	"github.com/stellar/go/xdr"

	"github.com/stellar/wallet-backend/internal/entities"
)

type RPCGetIngestTxResponse struct {
	// A status that indicated whether this transaction failed or successly made it to the ledger
	Status entities.RPCStatus
	// The error code that is derived by deserialzing the ResultXdr string in the sendTransaction response
	// list of possible errror codes: https://developers.stellar.org/docs/data/horizon/api-reference/errors/result-codes/transactions
	Code RPCTXCode
	// The raw TransactionEnvelope XDR for this transaction
	EnvelopeXDR string
	// The raw TransactionResult XDR of the envelopeXdr
	ResultXDR string
	// The unix timestamp of when the transaction was included in the ledger
	CreatedAt int64
}

func ParseToRPCGetIngestTxResponse(result entities.RPCGetTransactionResult, err error) (RPCGetIngestTxResponse, error) {
	if err != nil {
		return RPCGetIngestTxResponse{Status: entities.ErrorStatus}, err
	}

	getIngestTxResponse := RPCGetIngestTxResponse{
		Status:      result.Status,
		EnvelopeXDR: result.EnvelopeXDR,
		ResultXDR:   result.ResultXDR,
	}
	if getIngestTxResponse.Status != entities.NotFoundStatus {
		getIngestTxResponse.CreatedAt, err = strconv.ParseInt(result.CreatedAt, 10, 64)
		if err != nil {
			return RPCGetIngestTxResponse{Status: entities.ErrorStatus}, fmt.Errorf("unable to parse createdAt: %w", err)
		}
	}
	getIngestTxResponse.Code, err = TransactionResultXDRToCode(result.ResultXDR)
	if err != nil {
		return getIngestTxResponse, fmt.Errorf("parse error result xdr string: %w", err)
	}
	return getIngestTxResponse, nil
}

type OtherStatus string

type OtherCodes int32

type TransactionResultCode int32

const (
	NewStatus     OtherStatus = "NEW"
	NoStatus      OtherStatus = ""
	SentStatus    OtherStatus = "SENT"
	NotSentStatus OtherStatus = "NOT_SENT"
)

type RPCTXStatus struct {
	RPCStatus   entities.RPCStatus
	OtherStatus OtherStatus
}

func (s RPCTXStatus) Status() string {
	if s.OtherStatus != NoStatus {
		return string(s.OtherStatus)
	}
	return string(s.RPCStatus)
}

const (
	// Do not use NoCode
	NoCode OtherCodes = 0
	// These values need to not overlap the values in xdr.TransactionResultCode
	NewCode             OtherCodes = 100
	RPCFailCode         OtherCodes = 101
	UnmarshalBinaryCode OtherCodes = 102
	EmptyCode           OtherCodes = 103
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

var FinalCodes = []xdr.TransactionResultCode{
	xdr.TransactionResultCodeTxSuccess,
	xdr.TransactionResultCodeTxFailed,
	xdr.TransactionResultCodeTxMissingOperation,
	xdr.TransactionResultCodeTxInsufficientBalance,
	xdr.TransactionResultCodeTxBadAuth,
	xdr.TransactionResultCodeTxBadAuthExtra,
	xdr.TransactionResultCodeTxMalformed,
	xdr.TransactionResultCodeTxNotSupported,
	xdr.TransactionResultCodeTxFeeBumpInnerFailed,
	xdr.TransactionResultCodeTxFeeBumpInnerSuccess,
	xdr.TransactionResultCodeTxNoAccount,
	xdr.TransactionResultCodeTxBadSponsorship,
	xdr.TransactionResultCodeTxSorobanInvalid,
	xdr.TransactionResultCodeTxBadMinSeqAgeOrGap,
}

var NonJitterErrorCodes = []xdr.TransactionResultCode{
	xdr.TransactionResultCodeTxTooEarly,
	xdr.TransactionResultCodeTxTooLate,
	xdr.TransactionResultCodeTxBadSeq,
}

var JitterErrorCodes = []xdr.TransactionResultCode{
	xdr.TransactionResultCodeTxInsufficientFee,
	xdr.TransactionResultCodeTxInternalError,
}

type RPCSendTxResponse struct {
	// The hash of the transaction submitted to RPC
	TransactionHash string
	TransactionXDR  string
	// The status of an RPC sendTransaction call. Can be one of [PENDING, DUPLICATE, TRY_AGAIN_LATER, ERROR]
	Status RPCTXStatus
	// The (optional) error code that is derived by deserialzing the errorResultXdr string in the sendTransaction response
	// list of possible errror codes: https://developers.stellar.org/docs/data/horizon/api-reference/errors/result-codes/transactions
	Code           RPCTXCode
	ErrorResultXDR string
}

func ParseToRPCSendTxResponse(transactionXDR string, result entities.RPCSendTransactionResult, err error) (RPCSendTxResponse, error) {
	sendTxResponse := RPCSendTxResponse{}
	sendTxResponse.TransactionXDR = transactionXDR
	if err != nil {
		sendTxResponse.Status.RPCStatus = entities.ErrorStatus
		sendTxResponse.Code.OtherCodes = RPCFailCode
		return sendTxResponse, fmt.Errorf("RPC fail: %w", err)
	}
	sendTxResponse.Status.RPCStatus = result.Status
	sendTxResponse.TransactionHash = result.Hash
	sendTxResponse.ErrorResultXDR = result.ErrorResultXDR
	sendTxResponse.Code, err = TransactionResultXDRToCode(result.ErrorResultXDR)
	if err != nil {
		return sendTxResponse, fmt.Errorf("parse error result xdr string: %w", err)
	}
	return sendTxResponse, nil
}

func UnmarshallTransactionResultXDR(resultXDR string) (xdr.TransactionResult, error) {
	unmarshalErr := "unable to unmarshal errorResultXDR: %s"
	decodedBytes, err := base64.StdEncoding.DecodeString(resultXDR)
	if err != nil {
		return xdr.TransactionResult{}, fmt.Errorf(unmarshalErr, resultXDR)
	}
	var txResultXDR xdr.TransactionResult
	_, err = xdr3.Unmarshal(bytes.NewReader(decodedBytes), &txResultXDR)
	if err != nil {
		return xdr.TransactionResult{}, fmt.Errorf(unmarshalErr, resultXDR)
	}
	return txResultXDR, nil
}

func TransactionResultXDRToCode(errorResultXDR string) (RPCTXCode, error) {
	if errorResultXDR == "" {
		return RPCTXCode{
			OtherCodes: EmptyCode,
		}, nil
	}
	errorResult, err := UnmarshallTransactionResultXDR(errorResultXDR)
	if err != nil {
		return RPCTXCode{OtherCodes: UnmarshalBinaryCode}, fmt.Errorf("unable to parse: %w", err)
	}
	return RPCTXCode{
		TxResultCode: errorResult.Result.Code,
	}, nil
}

type TSSResponse struct {
	TransactionHash       string `json:"tx_hash"`
	TransactionResultCode string `json:"tx_result_code"`
	Status                string `json:"status"`
	CreatedAt             int64  `json:"created_at"`
	EnvelopeXDR           string `json:"envelopeXdr"`
	ResultXDR             string `json:"resultXdr"`
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
	// indicates if the transaction to be built from this payload should be wrapped in a fee bump transaction
	FeeBump bool
}

type Channel interface {
	Send(payload Payload)
	Receive(payload Payload)
	Stop()
}
