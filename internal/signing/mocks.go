package signing

import (
	"context"

	"github.com/stellar/go-stellar-sdk/txnbuild"
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

func (s *SignatureClientMock) GetAccountPublicKey(ctx context.Context, opts ...int) (string, error) {
	_ca := []any{ctx}
	for _, opt := range opts {
		_ca = append(_ca, opt)
	}
	args := s.Called(_ca...)
	return args.String(0), args.Error(1)
}

func (s *SignatureClientMock) SignStellarTransaction(ctx context.Context, tx *txnbuild.Transaction, accounts ...string) (*txnbuild.Transaction, error) {
	args := s.Called(ctx, tx, accounts)
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

// NewSignatureClientMock creates a new instance of SignatureClientMock. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewSignatureClientMock(t interface {
	mock.TestingT
	Cleanup(func())
},
) *SignatureClientMock {
	mock := &SignatureClientMock{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
