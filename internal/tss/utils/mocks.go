package utils

import (
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/wallet-backend/internal/tss"
	"github.com/stretchr/testify/mock"
)

type TransactionServiceMock struct {
	mock.Mock
}

var _ TransactionService = (*TransactionServiceMock)(nil)

func (t *TransactionServiceMock) NetworkPassPhrase() string {
	args := t.Called()
	return args.String(0)
}

func (t *TransactionServiceMock) SignAndBuildNewTransaction(origTxXdr string) (*txnbuild.FeeBumpTransaction, error) {
	args := t.Called(origTxXdr)
	if result := args.Get(0); result != nil {
		return result.(*txnbuild.FeeBumpTransaction), args.Error(1)
	}
	return nil, args.Error(1)

}

func (t *TransactionServiceMock) SendTransaction(transactionXdr string) (tss.RPCSendTxResponse, error) {
	args := t.Called(transactionXdr)
	return args.Get(0).(tss.RPCSendTxResponse), args.Error(1)
}

func (t *TransactionServiceMock) GetTransaction(transactionHash string) (tss.RPCGetIngestTxResponse, error) {
	args := t.Called(transactionHash)
	return args.Get(0).(tss.RPCGetIngestTxResponse), args.Error(1)
}
