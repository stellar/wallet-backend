package services

import (
	"context"

	"github.com/stellar/go/txnbuild"
	"github.com/stellar/wallet-backend/internal/tss"

	"github.com/stretchr/testify/mock"
)

type TransactionServiceMock struct {
	mock.Mock
}

var _ TransactionService = (*TransactionServiceMock)(nil)

func (t *TransactionServiceMock) NetworkPassphrase() string {
	args := t.Called()
	return args.String(0)
}

func (t *TransactionServiceMock) SignAndBuildNewFeeBumpTransaction(ctx context.Context, origTxXdr string) (*txnbuild.FeeBumpTransaction, error) {
	args := t.Called(ctx, origTxXdr)
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

type TransactionManagerMock struct {
	mock.Mock
}

var _ TransactionManager = (*TransactionManagerMock)(nil)

func (t *TransactionManagerMock) BuildAndSubmitTransaction(ctx context.Context, channelName string, payload tss.Payload) (tss.RPCSendTxResponse, error) {
	args := t.Called(ctx, channelName, payload)
	return args.Get(0).(tss.RPCSendTxResponse), args.Error(1)
}
