package types

type Transaction struct {
	Operations []string `json:"operations" validate:"required"`
	TimeBounds int64    `json:"timebounds" validate:"required"`
}

type BuildTransactionsRequest struct {
	Transactions []Transaction `json:"transactions" validate:"required,gt=0"`
}

type BuildTransactionsResponse struct {
	TransactionXDRs []string `json:"transaction_xdrs"`
}

type CreateFeeBumpTransactionRequest struct {
	Transaction string `json:"transaction" validate:"required"`
}

type TransactionEnvelopeResponse struct {
	Transaction       string `json:"transaction"`
	NetworkPassphrase string `json:"networkPassphrase"`
}
