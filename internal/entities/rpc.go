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

type RPCGetHealthResult struct {
	Status                string `json:"status"`
	LatestLedger          uint32 `json:"latestLedger"`
	OldestLedger          uint32 `json:"oldestLedger"`
	LedgerRetentionWindow uint32 `json:"ledgerRetentionWindow"`
}

type RPCGetTransactionResult struct {
	Status                RPCStatus `json:"status"`
	LatestLedger          int64     `json:"latestLedger"`
	LatestLedgerCloseTime string    `json:"latestLedgerCloseTime"`
	OldestLedger          int64     `json:"oldestLedger"`
	OldestLedgerCloseTime string    `json:"oldestLedgerCloseTime"`
	ApplicationOrder      int64     `json:"applicationOrder"`
	Hash                  string    `json:"txHash"`
	EnvelopeXDR           string    `json:"envelopeXdr"`
	ResultXDR             string    `json:"resultXdr"`
	ResultMetaXDR         string    `json:"resultMetaXdr"`
	Ledger                int64     `json:"ledger"`
	CreatedAt             string    `json:"createdAt"`
	ErrorResultXDR        string    `json:"errorResultXdr"`
}

type Transaction struct {
	Status              RPCStatus `json:"status"`
	Hash                string    `json:"txHash"`
	ApplicationOrder    int64     `json:"applicationOrder"`
	FeeBump             bool      `json:"feeBump"`
	EnvelopeXDR         string    `json:"envelopeXdr"`
	ResultXDR           string    `json:"resultXdr"`
	ResultMetaXDR       string    `json:"resultMetaXdr"`
	Ledger              int64     `json:"ledger"`
	DiagnosticEventsXDR []string  `json:"diagnosticEventsXdr"`
	CreatedAt           uint32    `json:"createdAt"`
}

type RPCGetTransactionsResult struct {
	Transactions          []Transaction `json:"transactions"`
	LatestLedger          int64         `json:"latestLedger"`
	LatestLedgerCloseTime int64         `json:"latestLedgerCloseTimestamp"`
	OldestLedger          int64         `json:"oldestLedger"`
	OldestLedgerCloseTime int64         `json:"oldestLedgerCloseTimestamp"`
	Cursor                string        `json:"cursor"`
}

type RPCSendTransactionResult struct {
	Status                RPCStatus `json:"status"`
	LatestLedger          int64     `json:"latestLedger"`
	LatestLedgerCloseTime string    `json:"latestLedgerCloseTime"`
	Hash                  string    `json:"hash"`
	ErrorResultXDR        string    `json:"errorResultXdr"`
}

type LedgerEntryResult struct {
	KeyXDR             string `json:"key,omitempty"`
	DataXDR            string `json:"xdr,omitempty"`
	LastModifiedLedger uint32 `json:"lastModifiedLedgerSeq"`
	// The ledger sequence until the entry is live, available for entries that have associated ttl ledger entries.
	LiveUntilLedgerSeq *uint32 `json:"liveUntilLedgerSeq,omitempty"`
}

type RPCGetLedgerEntriesResult struct {
	LatestLedger uint32              `json:"latestLedger"`
	Entries      []LedgerEntryResult `json:"entries"`
}

type RPCPagination struct {
	Cursor string `json:"cursor,omitempty"`
	Limit  int    `json:"limit"`
}

type RPCParams struct {
	Transaction string        `json:"transaction,omitempty"`
	Hash        string        `json:"hash,omitempty"`
	StartLedger int64         `json:"startLedger,omitempty"`
	Pagination  RPCPagination `json:"pagination,omitempty"`
	LedgerKeys  []string      `json:"keys,omitempty"`
}
