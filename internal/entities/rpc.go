package entities

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

type RPCEntry struct {
	Key                   string `json:"key"`
	XDR                   string `json:"xdr"`
	LastModifiedLedgerSeq int64  `json:"lastModifiedLedgerSeq"`
}

type RPCResult struct {
	Status                string `json:"status"`
	LatestLedger          int64  `json:"latestLedger"`
	LatestLedgerCloseTime string `json:"latestLedgerCloseTime"`
	// sendTransaction fields
	Hash string `json:"hash"`
	// getTransaction fields
	OldestLedger          string `json:"oldestLedger"`
	OldestLedgerCloseTime string `json:"oldestLedgerCloseTime"`
	ApplicationOrder      string `json:"applicationOrder"`
	EnvelopeXDR           string `json:"envelopeXdr"`
	ResultXDR             string `json:"resultXdr"`
	ResultMetaXDR         string `json:"resultMetaXdr"`
	Ledger                string `json:"ledger"`
	CreatedAt             string `json:"createdAt"`
	ErrorResultXDR        string `json:"errorResultXdr"`
	// getLedgerEntries fields
	Entries []RPCEntry `json:"entries"`
}

type RPCResponse struct {
	Result  RPCResult `json:"result"`
	JSONRPC string    `json:"jsonrpc"`
	ID      int64     `json:"id"`
}
