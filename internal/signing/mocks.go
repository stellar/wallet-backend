package signing

import (
	"context"

	"github.com/stellar/go/txnbuild"
	"github.com/stretchr/testify/mock"
)

type SignatureClientMock struct {
	mock.Mock
}

var _ SignatureClient = (*SignatureClientMock)(nil)

func (s *SignatureClientMock) NetworkPassphrase() string {
	args := s.Called()
	return args.String(0)
}

func (s *SignatureClientMock) GetDistributionAccountPublicKey() string {
	args := s.Called()
	return args.String(0)
}

func (s *SignatureClientMock) SignStellarTransaction(ctx context.Context, tx *txnbuild.Transaction) (*txnbuild.Transaction, error) {
	args := s.Called(ctx, tx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*txnbuild.Transaction), args.Error(1)
}

func (s *SignatureClientMock) SignStellarFeeBumpTransaction(ctx context.Context, feeBumpTx *txnbuild.FeeBumpTransaction) (*txnbuild.FeeBumpTransaction, error) {
	args := s.Called(ctx, feeBumpTx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*txnbuild.FeeBumpTransaction), args.Error(1)
}
