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

func (s *SignatureClientMock) GetAccountPublicKey(ctx context.Context, opts ...int) (string, error) {
	_ca := []any{ctx}
	for _, opt := range opts {
		_ca = append(_ca, opt)
	}
	args := s.Called(_ca...)
	return args.String(0), args.Error(1)
}

// SignStellarTransaction provides a mock function for the type MockSignatureClient
func (_mock *SignatureClientMock) SignStellarTransaction(ctx context.Context, tx *txnbuild.Transaction, stellarAccounts ...string) (*txnbuild.Transaction, error) {
	var tmpRet mock.Arguments
	if len(stellarAccounts) > 0 {
		tmpRet = _mock.Called(ctx, tx, stellarAccounts)
	} else {
		tmpRet = _mock.Called(ctx, tx)
	}
	ret := tmpRet

	if len(ret) == 0 {
		panic("no return value specified for SignStellarTransaction")
	}

	var r0 *txnbuild.Transaction
	var r1 error
	if returnFunc, ok := ret.Get(0).(func(context.Context, *txnbuild.Transaction, ...string) (*txnbuild.Transaction, error)); ok {
		return returnFunc(ctx, tx, stellarAccounts...)
	}
	if returnFunc, ok := ret.Get(0).(func(context.Context, *txnbuild.Transaction, ...string) *txnbuild.Transaction); ok {
		r0 = returnFunc(ctx, tx, stellarAccounts...)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*txnbuild.Transaction)
		}
	}
	if returnFunc, ok := ret.Get(1).(func(context.Context, *txnbuild.Transaction, ...string) error); ok {
		r1 = returnFunc(ctx, tx, stellarAccounts...)
	} else {
		r1 = ret.Error(1)
	}
	return r0, r1
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
