package entities

import (
	"encoding/json"
)

type RPCStatus string

const (
	// sendTransaction statuses
	PendingStatus       RPCStatus = "PENDING"
	DuplicateStatus     RPCStatus = "DUPLICATE"
	TryAgainLaterStatus RPCStatus = "TRY_AGAIN_LATER"
	ErrorStatus         RPCStatus = "ERROR"
	// getTransaction statuses
	NotFoundStatus RPCStatus = "NOT_FOUND"
	FailedStatus   RPCStatus = "FAILED"
	SuccessStatus  RPCStatus = "SUCCESS"
)

type RPCResponse struct {
	Result  json.RawMessage `json:"result"`
	JSONRPC string          `json:"jsonrpc"`
	ID      int64           `json:"id"`
}

type RPCGetTransactionResult struct {
	Status                RPCStatus `json:"status"`
	LatestLedger          int64     `json:"latestLedger"`
	LatestLedgerCloseTime string    `json:"latestLedgerCloseTime"`
	OldestLedger          int64     `json:"oldestLedger"`
	OldestLedgerCloseTime string    `json:"oldestLedgerCloseTime"`
	ApplicationOrder      int64     `json:"applicationOrder"`
	EnvelopeXDR           string    `json:"envelopeXdr"`
	ResultXDR             string    `json:"resultXdr"`
	ResultMetaXDR         string    `json:"resultMetaXdr"`
	Ledger                int64     `json:"ledger"`
	CreatedAt             string    `json:"createdAt"`
	ErrorResultXDR        string    `json:"errorResultXdr"`
}

type RPCSendTransactionResult struct {
	Status                RPCStatus `json:"status"`
	LatestLedger          int64     `json:"latestLedger"`
	LatestLedgerCloseTime string    `json:"latestLedgerCloseTime"`
	Hash                  string    `json:"hash"`
	ErrorResultXDR        string    `json:"errorResultXdr"`
}
