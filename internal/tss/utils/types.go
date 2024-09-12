package utils

import (
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/wallet-backend/internal/tss"
)

type TransactionService interface {
	SignAndBuildNewTransaction(origTxXdr string) (*txnbuild.FeeBumpTransaction, error)
	SendTransaction(transactionXdr string) (tss.RPCSendTxResponse, error)
	GetTransaction(transactionHash string) (tss.RPCGetIngestTxResponse, error)
}
