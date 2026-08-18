package sep41

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
	sep41data "github.com/stellar/wallet-backend/internal/data/sep41"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/services"
)

func TestRepairerListUnits(t *testing.T) {
	ctx := context.Background()

	t.Run("maps pairs to units and derives the contract filter from the scope", func(t *testing.T) {
		balances := sep41data.NewBalanceModelMock(t)
		wantUUID := data.DeterministicContractID(testContractA)
		balances.On("ListPairs", ctx, &wantUUID, testAccountA, (*sep41data.Balance)(nil), int32(10)).
			Return([]sep41data.Balance{
				{AccountID: types.AddressBytea(testAccountA), ContractID: wantUUID, TokenID: testContractA},
			}, nil).Once()

		r := &repairer{balances: balances}
		units, next, err := r.ListUnits(ctx, services.RepairScope{ContractAddress: testContractA, AccountAddress: testAccountA}, "", 10)
		require.NoError(t, err)
		assert.Empty(t, next, "short page ends iteration")
		require.Len(t, units, 1)
		u, ok := units[0].(repairUnit)
		require.True(t, ok)
		assert.Equal(t, testAccountA, u.holder)
		assert.Equal(t, testContractA, u.tokenAddress)
		assert.Equal(t, wantUUID, u.tokenUUID)
	})

	t.Run("returns a cursor for a full page and round-trips it", func(t *testing.T) {
		cid := data.DeterministicContractID(testContractA)
		balances := sep41data.NewBalanceModelMock(t)
		balances.On("ListPairs", ctx, (*uuid.UUID)(nil), "", (*sep41data.Balance)(nil), int32(1)).
			Return([]sep41data.Balance{
				{AccountID: types.AddressBytea(testAccountA), ContractID: cid, TokenID: testContractA},
			}, nil).Once()
		balances.On("ListPairs", ctx, (*uuid.UUID)(nil), "",
			&sep41data.Balance{AccountID: types.AddressBytea(testAccountA), ContractID: cid}, int32(1)).
			Return(nil, nil).Once()

		r := &repairer{balances: balances}
		_, next, err := r.ListUnits(ctx, services.RepairScope{}, "", 1)
		require.NoError(t, err)
		require.NotEmpty(t, next)

		units, next2, err := r.ListUnits(ctx, services.RepairScope{}, next, 1)
		require.NoError(t, err)
		assert.Empty(t, units)
		assert.Empty(t, next2)
	})

	t.Run("rejects a malformed cursor", func(t *testing.T) {
		r := &repairer{balances: sep41data.NewBalanceModelMock(t)}
		_, _, err := r.ListUnits(ctx, services.RepairScope{}, "garbage", 1)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "malformed repair cursor")
	})
}

func TestRepairerApplyAndFinalize(t *testing.T) {
	ctx := context.Background()
	cid := data.DeterministicContractID(testContractA)
	unit := repairUnit{holder: testAccountA, tokenAddress: testContractA, tokenUUID: cid}

	t.Run("applies truth and remembers only applied zero rows for finalize", func(t *testing.T) {
		balances := sep41data.NewBalanceModelMock(t)
		r := &repairer{balances: balances}

		nonZero := sep41data.Balance{AccountID: types.AddressBytea(testAccountA), ContractID: cid, Balance: "77", LedgerNumber: 100}
		balances.On("ApplyAbsolute", ctx, nil, nonZero).Return(true, nil).Once()
		applied, err := r.Apply(ctx, nil, unit, "77", 100)
		require.NoError(t, err)
		assert.True(t, applied)

		zero := sep41data.Balance{AccountID: types.AddressBytea(testAccountA), ContractID: cid, Balance: "0", LedgerNumber: 101}
		balances.On("ApplyAbsolute", ctx, nil, zero).Return(true, nil).Once()
		applied, err = r.Apply(ctx, nil, unit, "0", 101)
		require.NoError(t, err)
		assert.True(t, applied)

		// A zero that loses the conditional write must NOT be remembered.
		lostZero := sep41data.Balance{AccountID: types.AddressBytea(testAccountA), ContractID: cid, Balance: "0", LedgerNumber: 99}
		balances.On("ApplyAbsolute", ctx, nil, lostZero).Return(false, nil).Once()
		applied, err = r.Apply(ctx, nil, unit, "0", 99)
		require.NoError(t, err)
		assert.False(t, applied)

		balances.On("DeleteZeroRows", ctx, nil, []sep41data.Balance{zero}, uint32(200)).Return(int64(1), nil).Once()
		require.NoError(t, r.Finalize(ctx, nil, 200))
	})

	t.Run("finalize without zero rows makes no delete call", func(t *testing.T) {
		balances := sep41data.NewBalanceModelMock(t)
		r := &repairer{balances: balances}
		require.NoError(t, r.Finalize(ctx, nil, 200))
		balances.AssertNotCalled(t, "DeleteZeroRows", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("rejects foreign unit and truth types", func(t *testing.T) {
		r := &repairer{balances: sep41data.NewBalanceModelMock(t)}
		_, err := r.Apply(ctx, nil, "not-a-unit", "0", 1)
		require.Error(t, err)
		_, err = r.Apply(ctx, nil, unit, 123, 1)
		require.Error(t, err)
		_, _, err = r.FetchTruth(ctx, "not-a-unit")
		require.Error(t, err)
	})
}
