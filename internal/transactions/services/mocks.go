package services

import (
	"context"

	"github.com/stellar/go/txnbuild"
	"github.com/stretchr/testify/mock"

	"github.com/stellar/wallet-backend/internal/entities"
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

func (t *TransactionServiceMock) BuildAndSignTransactionsWithChannelAccounts(ctx context.Context, transactions []types.Transaction) (txXDRs []string, err error) {
	args := t.Called(ctx, transactions)
	if result := args.Get(0); result != nil {
		return result.([]string), args.Error(1)
	}
	return nil, args.Error(1)
}

func (t *TransactionServiceMock) BuildAndSignTransactionWithChannelAccount(ctx context.Context, operations []txnbuild.Operation, timeoutInSecs int64, simulationResult entities.RPCSimulateTransactionResult) (*txnbuild.Transaction, error) {
	args := t.Called(ctx, operations, timeoutInSecs, simulationResult)
	if result := args.Get(0); result != nil {
		return result.(*txnbuild.Transaction), args.Error(1)
	}
	return nil, args.Error(1)
}
