package store

import (
	"context"
	"time"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stretchr/testify/mock"
)

type ChannelAccountStoreMock struct {
	mock.Mock
}

var _ ChannelAccountStore = (*ChannelAccountStoreMock)(nil)

func (s *ChannelAccountStoreMock) GetIdleChannelAccount(ctx context.Context, lockedUntil time.Duration) (*ChannelAccount, error) {
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

func (s *ChannelAccountStoreMock) BatchInsert(ctx context.Context, sqlExec db.SQLExecuter, channelAccounts []*ChannelAccount) error {
	args := s.Called(ctx, sqlExec, channelAccounts)
	return args.Error(0)
}

func (s *ChannelAccountStoreMock) Count(ctx context.Context) (int64, error) {
	args := s.Called(ctx)
	return int64(args.Int(0)), args.Error(1)
}
