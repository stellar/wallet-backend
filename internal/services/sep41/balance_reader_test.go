package sep41

import (
	"context"
	"errors"
	"testing"

	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/services"
	"github.com/stellar/wallet-backend/internal/utils"
)

func TestReadBalance(t *testing.T) {
	ctx := context.Background()

	t.Run("decodes the i128 result and reports the simulation ledger", func(t *testing.T) {
		rpc := services.NewContractMetadataServiceMock(t)
		rpc.On("FetchSingleFieldWithLedger", mock.Anything, testContractA, "balance",
			[]xdr.ScVal{mustAddressScVal(t, testAccountA)}).
			Return(i128ScVal(4321), uint32(987654), nil).Once()

		balance, ledger, err := NewBalanceReader(rpc).ReadBalance(ctx, testContractA, testAccountA)
		require.NoError(t, err)
		assert.Equal(t, "4321", balance)
		assert.Equal(t, uint32(987654), ledger)
	})

	t.Run("returns the raw i128 without a decimals divisor", func(t *testing.T) {
		// One unit of a 7-decimal token. amount.String128 would render this as
		// "1.0000000"; the processor persists the raw integer, so the reference
		// value has to stay raw for the two to be comparable.
		rpc := services.NewContractMetadataServiceMock(t)
		rpc.On("FetchSingleFieldWithLedger", mock.Anything, testContractA, "balance", mock.Anything).
			Return(i128ScVal(10_000_000), uint32(1), nil).Once()

		balance, _, err := NewBalanceReader(rpc).ReadBalance(ctx, testContractA, testAccountA)
		require.NoError(t, err)
		assert.Equal(t, "10000000", balance)
	})

	t.Run("decodes a negative balance", func(t *testing.T) {
		rpc := services.NewContractMetadataServiceMock(t)
		rpc.On("FetchSingleFieldWithLedger", mock.Anything, testContractA, "balance", mock.Anything).
			Return(i128ScVal(-25), uint32(1), nil).Once()

		balance, _, err := NewBalanceReader(rpc).ReadBalance(ctx, testContractA, testAccountA)
		require.NoError(t, err)
		assert.Equal(t, "-25", balance)
	})

	t.Run("decodes a balance wider than int64", func(t *testing.T) {
		parts := xdr.Int128Parts{Hi: 1, Lo: 0} // 2^64
		rpc := services.NewContractMetadataServiceMock(t)
		rpc.On("FetchSingleFieldWithLedger", mock.Anything, testContractA, "balance", mock.Anything).
			Return(xdr.ScVal{Type: xdr.ScValTypeScvI128, I128: &parts}, uint32(1), nil).Once()

		balance, _, err := NewBalanceReader(rpc).ReadBalance(ctx, testContractA, testAccountA)
		require.NoError(t, err)
		assert.Equal(t, "18446744073709551616", balance)
	})

	t.Run("encodes a contract holder as a contract Address", func(t *testing.T) {
		wantArg, err := utils.AddressScVal(testContractB)
		require.NoError(t, err)
		require.Equal(t, xdr.ScAddressTypeScAddressTypeContract, wantArg.Address.Type)

		rpc := services.NewContractMetadataServiceMock(t)
		rpc.On("FetchSingleFieldWithLedger", mock.Anything, testContractA, "balance", []xdr.ScVal{wantArg}).
			Return(i128ScVal(7), uint32(42), nil).Once()

		balance, ledger, err := NewBalanceReader(rpc).ReadBalance(ctx, testContractA, testContractB)
		require.NoError(t, err)
		assert.Equal(t, "7", balance)
		assert.Equal(t, uint32(42), ledger)
	})

	t.Run("propagates a simulation error", func(t *testing.T) {
		rpc := services.NewContractMetadataServiceMock(t)
		simErr := errors.New("simulation failed: HostError")
		rpc.On("FetchSingleFieldWithLedger", mock.Anything, testContractA, "balance", mock.Anything).
			Return(xdr.ScVal{}, uint32(0), simErr).Once()

		_, _, err := NewBalanceReader(rpc).ReadBalance(ctx, testContractA, testAccountA)
		require.ErrorIs(t, err, simErr)
		assert.Contains(t, err.Error(), "simulating balance")
	})

	t.Run("rejects a non-i128 result", func(t *testing.T) {
		rpc := services.NewContractMetadataServiceMock(t)
		rpc.On("FetchSingleFieldWithLedger", mock.Anything, testContractA, "balance", mock.Anything).
			Return(u32ScVal(5), uint32(1), nil).Once()

		_, _, err := NewBalanceReader(rpc).ReadBalance(ctx, testContractA, testAccountA)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "expected i128")
	})

	t.Run("rejects a holder that is not an account or contract address", func(t *testing.T) {
		rpc := services.NewContractMetadataServiceMock(t)

		_, _, err := NewBalanceReader(rpc).ReadBalance(ctx, testContractA, "not-an-address")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "neither an account nor a contract address")
		rpc.AssertNotCalled(t, "FetchSingleFieldWithLedger")
	})
}
