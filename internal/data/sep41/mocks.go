// Mock implementations for SEP-41 data models used in testing.
package sep41

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/mock"
)

// BalanceModelMock mocks BalanceModelInterface.
type BalanceModelMock struct {
	mock.Mock
}

var _ BalanceModelInterface = (*BalanceModelMock)(nil)

func NewBalanceModelMock(t interface {
	mock.TestingT
	Cleanup(func())
},
) *BalanceModelMock {
	m := &BalanceModelMock{}
	m.Mock.Test(t)
	t.Cleanup(func() { m.AssertExpectations(t) })
	return m
}

func (m *BalanceModelMock) GetByAccount(ctx context.Context, accountAddress string) ([]Balance, error) {
	args := m.Called(ctx, accountAddress)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]Balance), args.Error(1)
}

func (m *BalanceModelMock) BatchUpsert(ctx context.Context, dbTx pgx.Tx, upserts []Balance, deletes []Balance) error {
	args := m.Called(ctx, dbTx, upserts, deletes)
	return args.Error(0)
}

func (m *BalanceModelMock) BatchCopy(ctx context.Context, dbTx pgx.Tx, balances []Balance) error {
	args := m.Called(ctx, dbTx, balances)
	return args.Error(0)
}

// AllowanceModelMock mocks AllowanceModelInterface.
type AllowanceModelMock struct {
	mock.Mock
}

var _ AllowanceModelInterface = (*AllowanceModelMock)(nil)

func NewAllowanceModelMock(t interface {
	mock.TestingT
	Cleanup(func())
},
) *AllowanceModelMock {
	m := &AllowanceModelMock{}
	m.Mock.Test(t)
	t.Cleanup(func() { m.AssertExpectations(t) })
	return m
}

func (m *AllowanceModelMock) GetByOwner(ctx context.Context, ownerAddress string, currentLedger uint32) ([]Allowance, error) {
	args := m.Called(ctx, ownerAddress, currentLedger)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]Allowance), args.Error(1)
}

func (m *AllowanceModelMock) BatchUpsert(ctx context.Context, dbTx pgx.Tx, upserts []Allowance, deletes []Allowance) error {
	args := m.Called(ctx, dbTx, upserts, deletes)
	return args.Error(0)
}
