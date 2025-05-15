package services

import (
	"context"

	"github.com/stellar/go/txnbuild"

	"github.com/stellar/wallet-backend/internal/entities"

	"github.com/stretchr/testify/mock"
)

type TransactionServiceMock struct {
	mock.Mock
}

var _ TransactionService = (*TransactionServiceMock)(nil)

func (t *TransactionServiceMock) NetworkPassphrase() string {
	args := t.Called()
	return args.String(0)
}

func (t *TransactionServiceMock) BuildAndSignTransactionWithChannelAccount(ctx context.Context, operations []txnbuild.Operation, timeoutInSecs int64, simulationResult entities.RPCSimulateTransactionResult) (*txnbuild.Transaction, error) {
	args := t.Called(ctx, operations, timeoutInSecs, simulationResult)
	if result := args.Get(0); result != nil {
		return result.(*txnbuild.Transaction), args.Error(1)
	}
	return nil, args.Error(1)
}

func (t *TransactionServiceMock) BuildFeeBumpTransaction(ctx context.Context, tx *txnbuild.Transaction) (*txnbuild.FeeBumpTransaction, error) {
	args := t.Called(ctx, tx)
	if result := args.Get(0); result != nil {
		return result.(*txnbuild.FeeBumpTransaction), args.Error(1)
	}
	return nil, args.Error(1)
}
