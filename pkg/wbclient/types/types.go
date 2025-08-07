package types

import "github.com/stellar/wallet-backend/internal/entities"

type Transaction struct {
	Operations       []string                              `json:"operations" validate:"required"`
	Timeout          int64                                 `json:"timeout" validate:"required"`
	SimulationResult entities.RPCSimulateTransactionResult `json:"simulationResult,omitempty"`
}

type BuildTransactionsRequest struct {
	Transactions []Transaction `json:"transactions" validate:"required,gt=0"`
}

type BuildTransactionsResponse struct {
	TransactionXDRs []string `json:"transactionXdrs"`
}

type BuildTransactionResponse struct {
	TransactionXDR string `json:"transactionXdr"`
}

type CreateFeeBumpTransactionRequest struct {
	Transaction string `json:"transaction" validate:"required"`
}

type TransactionEnvelopeResponse struct {
	Transaction       string `json:"transaction"`
	NetworkPassphrase string `json:"networkPassphrase"`
}
