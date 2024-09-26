package services

import (
	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stretchr/testify/mock"
)

type RPCServiceMock struct {
	mock.Mock
}

var _ RPCService = (*RPCServiceMock)(nil)

func (r *RPCServiceMock) SendTransaction(transactionXdr string) (entities.RPCSendTransactionResult, error) {
	args := r.Called(transactionXdr)
	return args.Get(0).(entities.RPCSendTransactionResult), args.Error(1)
}

func (r *RPCServiceMock) GetTransaction(transactionHash string) (entities.RPCGetTransactionResult, error) {
	args := r.Called(transactionHash)
	return args.Get(0).(entities.RPCGetTransactionResult), args.Error(1)
}

type TransactionManagerMock struct {
	mock.Mock
}
