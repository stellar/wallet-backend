package services

import (
	"context"

	"github.com/stretchr/testify/mock"

	"github.com/stellar/wallet-backend/pkg/wbclient/types"
)

type TransactionServiceMock struct {
	mock.Mock
}

var _ TransactionService = (*TransactionServiceMock)(nil)

func (t *TransactionServiceMock) NetworkPassphrase() string {
	args := t.Called()
	return args.String(0)
}

func (t *TransactionServiceMock) BuildAndSignTransactionsWithChannelAccounts(ctx context.Context, transactions ...types.Transaction) (txXDRs []string, err error) {
	_args := []any{ctx}
	for _, transaction := range transactions {
		_args = append(_args, transaction)
	}
	args := t.Called(_args...)
	if result := args.Get(0); result != nil {
		return result.([]string), args.Error(1)
	}
	return nil, args.Error(1)
}
