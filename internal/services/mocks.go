package services

import (
	"context"

	"github.com/stretchr/testify/mock"

	"github.com/stellar/go/txnbuild"

	"github.com/stellar/wallet-backend/internal/entities"
)

type RPCServiceMock struct {
	mock.Mock
}

var _ RPCService = (*RPCServiceMock)(nil)

func (r *RPCServiceMock) TrackRPCServiceHealth(ctx context.Context) {
	r.Called(ctx)
}

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

func (r *RPCServiceMock) SimulateTransaction(transactionXDR string, resourceConfig entities.RPCResourceConfig) (entities.RPCSimulateTransactionResult, error) {
	args := r.Called(transactionXDR, resourceConfig)
	return args.Get(0).(entities.RPCSimulateTransactionResult), args.Error(1)
}

type AccountSponsorshipServiceMock struct {
	mock.Mock
}

var _ AccountSponsorshipService = (*AccountSponsorshipServiceMock)(nil)

func (s *AccountSponsorshipServiceMock) SponsorAccountCreationTransaction(ctx context.Context, accountToSponsor string, signers []entities.Signer, assets []entities.Asset) (string, string, error) {
	args := s.Called(ctx, accountToSponsor, signers, assets)
	return args.String(0), args.String(1), args.Error(2)
}

func (s *AccountSponsorshipServiceMock) WrapTransaction(ctx context.Context, tx *txnbuild.Transaction) (string, string, error) {
	args := s.Called(ctx, tx)
	return args.String(0), args.String(1), args.Error(2)
}
