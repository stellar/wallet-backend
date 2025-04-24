package entities

import (
	"encoding/json"
	"fmt"

	"github.com/stellar/go/xdr"
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
	Transaction    string            `json:"transaction,omitempty"`
	Hash           string            `json:"hash,omitempty"`
	StartLedger    int64             `json:"startLedger,omitempty"`
	Pagination     RPCPagination     `json:"pagination,omitempty"`
	LedgerKeys     []string          `json:"keys,omitempty"`
	ResourceConfig RPCResourceConfig `json:"resourceConfig,omitempty"`
}

type RPCResourceConfig struct {
	InstructionLeeway int `json:"instructionLeeway,omitempty"`
}

type RPCSimulateHostFunctionResult struct {
	Auth []xdr.SorobanAuthorizationEntry `json:"auth"`
	XDR  xdr.ScVal                       `json:"xdr"`
}

func (r *RPCSimulateHostFunctionResult) UnmarshalJSON(data []byte) error {
	type rawRPCSimulateHostFunctionResult struct {
		Auth []string `json:"auth"`
		XDR  string   `json:"xdr"`
	}

	var raw rawRPCSimulateHostFunctionResult
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("unmarshalling simulate host function result raw: %w", err)
	}

	r.Auth = make([]xdr.SorobanAuthorizationEntry, len(raw.Auth))
	for i, auth := range raw.Auth {
		if err := xdr.SafeUnmarshalBase64(auth, &r.Auth[i]); err != nil {
			return fmt.Errorf("unmarshalling simulate host function result auth: %w", err)
		}
	}

	var xdrScVal xdr.ScVal
	if err := xdr.SafeUnmarshalBase64(raw.XDR, &xdrScVal); err != nil {
		return fmt.Errorf("unmarshalling simulate host function result xdr: %w", err)
	}
	r.XDR = xdrScVal

	return nil
}

type RPCRestorePreamble struct {
	MinResourceFee  string                     `json:"minResourceFee"`
	TransactionData xdr.SorobanTransactionData `json:"transactionData"`
}

// UnmarshalJSON implements custom JSON unmarshalling for RPCRestorePreamble, parsing base64-encoded XDR
// fields into `xdr.*` objects.
func (r *RPCRestorePreamble) UnmarshalJSON(data []byte) error {
	type rawRPCRestorePreamble struct {
		MinResourceFee  string `json:"minResourceFee"`
		TransactionData string `json:"transactionData"`
	}

	var raw rawRPCRestorePreamble
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("unmarshalling restore preamble raw: %w", err)
	}

	if raw.TransactionData != "" {
		var txData xdr.SorobanTransactionData
		if err := xdr.SafeUnmarshalBase64(raw.TransactionData, &txData); err != nil {
			return fmt.Errorf("unmarshalling transaction data: %w", err)
		}
		r.TransactionData = txData
	}

	// Assign fields that don't need special handling
	r.MinResourceFee = raw.MinResourceFee

	return nil
}

type RPCSimulateTransactionResult struct {
	TransactionData xdr.SorobanTransactionData      `json:"transactionData"`
	Events          []string                        `json:"events"`
	MinResourceFee  string                          `json:"minResourceFee"`
	Results         []RPCSimulateHostFunctionResult `json:"results"`
	LatestLedger    int64                           `json:"latestLedger"`
	// Error is only present if the transaction failed.
	Error string `json:"error,omitempty"`
	// RestorePreamble is only present if the transaction result indicates the account needs to be restored.
	RestorePreamble RPCRestorePreamble `json:"restorePreamble,omitempty"`
}

// UnmarshalJSON implements custom JSON unmarshalling for RPCSimulateTransactionResult, parsing base64-encoded XDR
// fields into `xdr.*` objects.
func (r *RPCSimulateTransactionResult) UnmarshalJSON(data []byte) error {
	// Define raw structure for initial JSON unmarshalling
	type rawRPCSimulateTransactionResult struct {
		TransactionData string                          `json:"transactionData"`
		Events          []string                        `json:"events"`
		MinResourceFee  string                          `json:"minResourceFee"`
		Results         []RPCSimulateHostFunctionResult `json:"results"`
		LatestLedger    int64                           `json:"latestLedger"`
		Error           string                          `json:"error,omitempty"`
		RestorePreamble RPCRestorePreamble              `json:"restorePreamble,omitempty"`
	}

	var raw rawRPCSimulateTransactionResult
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("unmarshalling simulate transaction result raw: %w", err)
	}

	// Parse the main TransactionData field
	if raw.TransactionData != "" {
		var txData xdr.SorobanTransactionData
		if err := xdr.SafeUnmarshalBase64(raw.TransactionData, &txData); err != nil {
			return fmt.Errorf("unmarshalling transaction data: %w", err)
		}
		r.TransactionData = txData
	}

	// Assign fields that don't need special handling
	r.Events = raw.Events
	r.MinResourceFee = raw.MinResourceFee
	r.Results = raw.Results
	r.LatestLedger = raw.LatestLedger
	r.Error = raw.Error
	r.RestorePreamble = raw.RestorePreamble

	return nil
}
