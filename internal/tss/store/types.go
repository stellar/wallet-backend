package store

import (
	"github.com/stellar/wallet-backend/internal/tss"
)

type Store interface {
	UpsertTransaction(WebhookURL string, txHash string, txXDR string, status tss.RPCTXStatus) error
	UpsertTry(transactionHash string, feeBumpTxHash string, feeBumpTxXDR string, status tss.RPCTXCode) error
}
