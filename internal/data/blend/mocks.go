// Mock implementations for Blend v2 data models used in testing.
package blend

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/mock"

	"github.com/stellar/wallet-backend/internal/indexer/types"
)

// PositionModelMock mocks PositionModelInterface.
type PositionModelMock struct {
	mock.Mock
}

var _ PositionModelInterface = (*PositionModelMock)(nil)

func NewPositionModelMock(t interface {
	mock.TestingT
	Cleanup(func())
},
) *PositionModelMock {
	m := &PositionModelMock{}
	m.Mock.Test(t)
	t.Cleanup(func() { m.AssertExpectations(t) })
	return m
}

func (m *PositionModelMock) DeleteByPoolUser(ctx context.Context, dbTx pgx.Tx, keys []PoolUserKey) error {
	args := m.Called(ctx, dbTx, keys)
	return args.Error(0)
}

func (m *PositionModelMock) ZeroAbsentReserves(ctx context.Context, dbTx pgx.Tx, presences []PositionPresence) error {
	args := m.Called(ctx, dbTx, presences)
	return args.Error(0)
}

func (m *PositionModelMock) BatchUpsertSnapshots(ctx context.Context, dbTx pgx.Tx, rows []PositionSnapshot) error {
	args := m.Called(ctx, dbTx, rows)
	return args.Error(0)
}

func (m *PositionModelMock) BatchApplyNetDeltas(ctx context.Context, dbTx pgx.Tx, deltas []PositionNetDelta) error {
	args := m.Called(ctx, dbTx, deltas)
	return args.Error(0)
}

func (m *PositionModelMock) ApplyAuctionAdjustments(ctx context.Context, dbTx pgx.Tx, adjs []PositionAuctionAdjustment) error {
	args := m.Called(ctx, dbTx, adjs)
	return args.Error(0)
}

// PoolModelMock mocks PoolModelInterface.
type PoolModelMock struct {
	mock.Mock
}

var _ PoolModelInterface = (*PoolModelMock)(nil)

func NewPoolModelMock(t interface {
	mock.TestingT
	Cleanup(func())
},
) *PoolModelMock {
	m := &PoolModelMock{}
	m.Mock.Test(t)
	t.Cleanup(func() { m.AssertExpectations(t) })
	return m
}

func (m *PoolModelMock) BatchUpsert(ctx context.Context, dbTx pgx.Tx, rows []Pool) error {
	args := m.Called(ctx, dbTx, rows)
	return args.Error(0)
}

func (m *PoolModelMock) SetRewardZone(ctx context.Context, dbTx pgx.Tx, poolIDs []types.AddressBytea, ledger int32) error {
	args := m.Called(ctx, dbTx, poolIDs, ledger)
	return args.Error(0)
}

// ReserveModelMock mocks ReserveModelInterface.
type ReserveModelMock struct {
	mock.Mock
}

var _ ReserveModelInterface = (*ReserveModelMock)(nil)

func NewReserveModelMock(t interface {
	mock.TestingT
	Cleanup(func())
},
) *ReserveModelMock {
	m := &ReserveModelMock{}
	m.Mock.Test(t)
	t.Cleanup(func() { m.AssertExpectations(t) })
	return m
}

func (m *ReserveModelMock) BatchUpsert(ctx context.Context, dbTx pgx.Tx, rows []Reserve) error {
	args := m.Called(ctx, dbTx, rows)
	return args.Error(0)
}

func (m *ReserveModelMock) BatchUpdateData(ctx context.Context, dbTx pgx.Tx, rows []ReserveDataUpdate) error {
	args := m.Called(ctx, dbTx, rows)
	return args.Error(0)
}

// BackstopPositionModelMock mocks BackstopPositionModelInterface.
type BackstopPositionModelMock struct {
	mock.Mock
}

var _ BackstopPositionModelInterface = (*BackstopPositionModelMock)(nil)

func NewBackstopPositionModelMock(t interface {
	mock.TestingT
	Cleanup(func())
},
) *BackstopPositionModelMock {
	m := &BackstopPositionModelMock{}
	m.Mock.Test(t)
	t.Cleanup(func() { m.AssertExpectations(t) })
	return m
}

func (m *BackstopPositionModelMock) BatchUpsert(ctx context.Context, dbTx pgx.Tx, rows []BackstopPosition) error {
	args := m.Called(ctx, dbTx, rows)
	return args.Error(0)
}

func (m *BackstopPositionModelMock) DeleteByPoolUser(ctx context.Context, dbTx pgx.Tx, keys []PoolUserKey) error {
	args := m.Called(ctx, dbTx, keys)
	return args.Error(0)
}

// BackstopPoolModelMock mocks BackstopPoolModelInterface.
type BackstopPoolModelMock struct {
	mock.Mock
}

var _ BackstopPoolModelInterface = (*BackstopPoolModelMock)(nil)

func NewBackstopPoolModelMock(t interface {
	mock.TestingT
	Cleanup(func())
},
) *BackstopPoolModelMock {
	m := &BackstopPoolModelMock{}
	m.Mock.Test(t)
	t.Cleanup(func() { m.AssertExpectations(t) })
	return m
}

func (m *BackstopPoolModelMock) BatchUpsertBalances(ctx context.Context, dbTx pgx.Tx, rows []BackstopPool) error {
	args := m.Called(ctx, dbTx, rows)
	return args.Error(0)
}

func (m *BackstopPoolModelMock) BatchUpsertEmissions(ctx context.Context, dbTx pgx.Tx, rows []BackstopPoolEmission) error {
	args := m.Called(ctx, dbTx, rows)
	return args.Error(0)
}

// ReserveEmissionModelMock mocks ReserveEmissionModelInterface.
type ReserveEmissionModelMock struct {
	mock.Mock
}

var _ ReserveEmissionModelInterface = (*ReserveEmissionModelMock)(nil)

func NewReserveEmissionModelMock(t interface {
	mock.TestingT
	Cleanup(func())
},
) *ReserveEmissionModelMock {
	m := &ReserveEmissionModelMock{}
	m.Mock.Test(t)
	t.Cleanup(func() { m.AssertExpectations(t) })
	return m
}

func (m *ReserveEmissionModelMock) BatchUpsert(ctx context.Context, dbTx pgx.Tx, rows []ReserveEmission) error {
	args := m.Called(ctx, dbTx, rows)
	return args.Error(0)
}

// EmissionModelMock mocks EmissionModelInterface.
type EmissionModelMock struct {
	mock.Mock
}

var _ EmissionModelInterface = (*EmissionModelMock)(nil)

func NewEmissionModelMock(t interface {
	mock.TestingT
	Cleanup(func())
},
) *EmissionModelMock {
	m := &EmissionModelMock{}
	m.Mock.Test(t)
	t.Cleanup(func() { m.AssertExpectations(t) })
	return m
}

func (m *EmissionModelMock) BatchUpsert(ctx context.Context, dbTx pgx.Tx, rows []Emission) error {
	args := m.Called(ctx, dbTx, rows)
	return args.Error(0)
}
