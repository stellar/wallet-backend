package tss_store

import (
	"github.com/stellar/wallet-backend/internal/tss"
)

type Store interface {
	UpsertTransaction(clientID string, txHash string, txXDR string, status tss.RPCTXStatus) error
	UpsertTry(transactionHash string, feeBumpTxHash string, feeBumpTxXDR string, status tss.RPCTXCode) error
}
