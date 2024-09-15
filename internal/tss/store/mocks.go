package store

import (
	"github.com/stellar/wallet-backend/internal/tss"
	"github.com/stretchr/testify/mock"
)

type MockStore struct {
	mock.Mock
}

var _ Store = (*MockStore)(nil)

func (s *MockStore) UpsertTransaction(webhookURL string, txHash string, txXDR string, status tss.RPCTXStatus) error {
	args := s.Called(webhookURL, txHash, txXDR, status)
	return args.Error(0)
}

func (s *MockStore) UpsertTry(txHash string, feeBumpTxHash string, feeBumpTxXDR string, status tss.RPCTXCode) error {
	args := s.Called(txHash, feeBumpTxHash, feeBumpTxXDR, status)
	return args.Error(0)
}
