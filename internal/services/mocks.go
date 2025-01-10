package services

import (
	"github.com/stretchr/testify/mock"

	"github.com/stellar/wallet-backend/internal/entities"
)

type RPCServiceMock struct {
	mock.Mock
}

var _ RPCService = (*RPCServiceMock)(nil)

func (r *RPCServiceMock) GetHeartbeatChannel() chan entities.RPCGetHealthResult {
	args := r.Called()
	return args.Get(0).(chan entities.RPCGetHealthResult)
}

func (r *RPCServiceMock) SendTransaction(transactionXdr string) (entities.RPCSendTransactionResult, error) {
	args := r.Called(transactionXdr)
	return args.Get(0).(entities.RPCSendTransactionResult), args.Error(1)
}

func (r *RPCServiceMock) GetTransaction(transactionHash string) (entities.RPCGetTransactionResult, error) {
	args := r.Called(transactionHash)
	return args.Get(0).(entities.RPCGetTransactionResult), args.Error(1)
}

func (r *RPCServiceMock) GetTransactions(startLedger int64, startCursor string, limit int) (entities.RPCGetTransactionsResult, error) {
	args := r.Called(startLedger, startCursor, limit)
	return args.Get(0).(entities.RPCGetTransactionsResult), args.Error(1)
}

func (r *RPCServiceMock) GetHealth() (entities.RPCGetHealthResult, error) {
	args := r.Called()
	return args.Get(0).(entities.RPCGetHealthResult), args.Error(1)
}

func (r *RPCServiceMock) GetLedgerEntries(keys []string) (entities.RPCGetLedgerEntriesResult, error) {
	args := r.Called(keys)
	return args.Get(0).(entities.RPCGetLedgerEntriesResult), args.Error(1)
}

func (r *RPCServiceMock) GetAccountLedgerSequence(address string) (int64, error) {
	args := r.Called(address)
	return args.Get(0).(int64), args.Error(1)
}
