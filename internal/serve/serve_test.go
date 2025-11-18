// Tests for serve package initialization and validation functions.
package serve

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/services"
)

func TestValidateDistributionAccount(t *testing.T) {
	testAccountAddress := "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5"

	t.Run("successful_with_sufficient_balance", func(t *testing.T) {
		mockRPCService := services.NewRPCServiceMock(t)
		mockRPCService.On("GetAccountInfo", testAccountAddress).
			Return(services.AccountInfo{
				Balance: 100_000_000, // 10 XLM
				SeqNum:  12345,
			}, nil).Once()

		err := validateDistributionAccount(mockRPCService, testAccountAddress, 100_000_000)
		require.NoError(t, err)
	})

	t.Run("successful_with_balance_above_threshold", func(t *testing.T) {
		mockRPCService := services.NewRPCServiceMock(t)
		mockRPCService.On("GetAccountInfo", testAccountAddress).
			Return(services.AccountInfo{
				Balance: 500_000_000, // 50 XLM
				SeqNum:  12345,
			}, nil).Once()

		err := validateDistributionAccount(mockRPCService, testAccountAddress, 100_000_000) // 10 XLM threshold
		require.NoError(t, err)
	})

	t.Run("successful_with_zero_threshold_existence_only", func(t *testing.T) {
		mockRPCService := services.NewRPCServiceMock(t)
		mockRPCService.On("GetAccountInfo", testAccountAddress).
			Return(services.AccountInfo{
				Balance: 1_000_000, // 0.1 XLM (below typical threshold but should pass)
				SeqNum:  12345,
			}, nil).Once()

		err := validateDistributionAccount(mockRPCService, testAccountAddress, 0) // 0 means only check existence
		require.NoError(t, err)
	})

	t.Run("account_not_found", func(t *testing.T) {
		mockRPCService := services.NewRPCServiceMock(t)
		mockRPCService.On("GetAccountInfo", testAccountAddress).
			Return(services.AccountInfo{}, services.ErrAccountNotFound).Once()

		err := validateDistributionAccount(mockRPCService, testAccountAddress, 100_000_000)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "does not exist on the network")
		assert.Contains(t, err.Error(), testAccountAddress)
	})

	t.Run("insufficient_balance", func(t *testing.T) {
		mockRPCService := services.NewRPCServiceMock(t)
		mockRPCService.On("GetAccountInfo", testAccountAddress).
			Return(services.AccountInfo{
				Balance: 50_000_000, // 5 XLM
				SeqNum:  12345,
			}, nil).Once()

		err := validateDistributionAccount(mockRPCService, testAccountAddress, 100_000_000) // 10 XLM threshold
		require.Error(t, err)
		assert.Contains(t, err.Error(), "insufficient balance")
		assert.Contains(t, err.Error(), testAccountAddress)
		assert.Contains(t, err.Error(), "50000000 stroops")
		assert.Contains(t, err.Error(), "100000000 stroops")
	})

	t.Run("rpc_error", func(t *testing.T) {
		mockRPCService := services.NewRPCServiceMock(t)
		mockRPCService.On("GetAccountInfo", testAccountAddress).
			Return(services.AccountInfo{}, errors.New("connection failed")).Once()

		err := validateDistributionAccount(mockRPCService, testAccountAddress, 100_000_000)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "validating distribution account")
		assert.Contains(t, err.Error(), "connection failed")
	})
}
