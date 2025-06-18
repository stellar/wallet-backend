package store

import (
	"context"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/stellar/wallet-backend/internal/db"
)

type ChannelAccountStoreMock struct {
	mock.Mock
}

var _ ChannelAccountStore = (*ChannelAccountStoreMock)(nil)

func (s *ChannelAccountStoreMock) GetAndLockIdleChannelAccount(ctx context.Context, lockedUntil time.Duration) (*ChannelAccount, error) {
	args := s.Called(ctx, lockedUntil)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*ChannelAccount), args.Error(1)
}

func (s *ChannelAccountStoreMock) Get(ctx context.Context, sqlExec db.SQLExecuter, publicKey string) (*ChannelAccount, error) {
	args := s.Called(ctx, sqlExec, publicKey)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*ChannelAccount), args.Error(1)
}

func (s *ChannelAccountStoreMock) GetAllByPublicKey(ctx context.Context, sqlExec db.SQLExecuter, publicKeys ...string) ([]*ChannelAccount, error) {
	args := s.Called(ctx, sqlExec, publicKeys)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*ChannelAccount), args.Error(1)
}

func (s *ChannelAccountStoreMock) AssignTxToChannelAccount(ctx context.Context, publicKey string, txHash string) error {
	args := s.Called(ctx, publicKey, txHash)
	return args.Error(0)
}

func (s *ChannelAccountStoreMock) UnassignTxAndUnlockChannelAccounts(ctx context.Context, sqlExec db.SQLExecuter, txHashes ...string) (int64, error) {
	_ca := []any{ctx, sqlExec}
	for _, txHash := range txHashes {
		_ca = append(_ca, txHash)
	}
	args := s.Called(_ca...)
	return args.Get(0).(int64), args.Error(1)
}

func (s *ChannelAccountStoreMock) BatchInsert(ctx context.Context, sqlExec db.SQLExecuter, channelAccounts []*ChannelAccount) error {
	args := s.Called(ctx, sqlExec, channelAccounts)
	return args.Error(0)
}

func (s *ChannelAccountStoreMock) Count(ctx context.Context) (int64, error) {
	args := s.Called(ctx)
	return int64(args.Int(0)), args.Error(1)
}

type KeypairStoreMock struct {
	mock.Mock
}

var _ KeypairStore = (*KeypairStoreMock)(nil)

func (s *KeypairStoreMock) GetByPublicKey(ctx context.Context, publicKey string) (*Keypair, error) {
	args := s.Called(ctx, publicKey)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*Keypair), args.Error(1)
}

func (s *KeypairStoreMock) Insert(ctx context.Context, publicKey string, encryptedPrivateKey []byte) error {
	args := s.Called(ctx, publicKey, encryptedPrivateKey)
	return args.Error(0)
}

// NewChannelAccountStoreMock creates a new instance of ChannelAccountStoreMock. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewChannelAccountStoreMock(t interface {
	mock.TestingT
	Cleanup(func())
}) *ChannelAccountStoreMock {
	mock := &ChannelAccountStoreMock{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
