// Package services provides business logic for the wallet-backend.
// This file contains tests for ContractMetadataService.
package services

import (
	"context"
	"errors"
	"testing"

	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/entities"
)

func init() {
	// Disable the FetchSingleField retry loop in tests so existing mock-call
	// expectations (.Once(), .Twice()) stay readable. Tests that exercise the
	// retry path opt in by overriding these vars locally with t.Cleanup.
	simulateMaxAttempts = 1
	simulateInitialBackoff = 0
}

// Helper functions for creating test XDR values
func ptrToScString(s string) *xdr.ScString {
	str := xdr.ScString(s)
	return &str
}

func TestNewContractMetadataService(t *testing.T) {
	t.Run("returns error when rpcService is nil", func(t *testing.T) {
		_, err := NewContractMetadataService(nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "rpcService cannot be nil")
	})

	t.Run("creates service successfully", func(t *testing.T) {
		mockRPCService := NewRPCServiceMock(t)

		service, err := NewContractMetadataService(mockRPCService)
		assert.NoError(t, err)
		assert.NotNil(t, service)
	})
}

func TestFetchSingleField(t *testing.T) {
	ctx := context.Background()

	t.Run("returns error for invalid contract address", func(t *testing.T) {
		mockRPCService := NewRPCServiceMock(t)
		service, err := NewContractMetadataService(mockRPCService)
		require.NoError(t, err)

		cms := service.(*contractMetadataService)
		_, err = cms.FetchSingleField(ctx, "INVALID_CONTRACT_ID", "name")

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "decoding contract address")
	})

	t.Run("returns error when RPC simulation fails", func(t *testing.T) {
		mockRPCService := NewRPCServiceMock(t)
		contractID := "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"

		mockRPCService.On("SimulateTransaction", mock.Anything, mock.Anything).Return(
			entities.RPCSimulateTransactionResult{}, errors.New("network error"),
		)

		service, err := NewContractMetadataService(mockRPCService)
		require.NoError(t, err)

		cms := service.(*contractMetadataService)
		_, err = cms.FetchSingleField(ctx, contractID, "name")

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "simulating transaction")
		assert.Contains(t, err.Error(), "network error")
	})

	t.Run("returns error when simulation result has error", func(t *testing.T) {
		mockRPCService := NewRPCServiceMock(t)
		contractID := "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"

		mockRPCService.On("SimulateTransaction", mock.Anything, mock.Anything).Return(
			entities.RPCSimulateTransactionResult{Error: "contract not found"},
			nil,
		)

		service, err := NewContractMetadataService(mockRPCService)
		require.NoError(t, err)

		cms := service.(*contractMetadataService)
		_, err = cms.FetchSingleField(ctx, contractID, "name")

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "simulation failed")
		assert.Contains(t, err.Error(), "contract not found")
	})

	t.Run("returns error when no results returned", func(t *testing.T) {
		mockRPCService := NewRPCServiceMock(t)
		contractID := "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"

		mockRPCService.On("SimulateTransaction", mock.Anything, mock.Anything).Return(
			entities.RPCSimulateTransactionResult{Results: []entities.RPCSimulateHostFunctionResult{}},
			nil,
		)

		service, err := NewContractMetadataService(mockRPCService)
		require.NoError(t, err)

		cms := service.(*contractMetadataService)
		_, err = cms.FetchSingleField(ctx, contractID, "name")

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no simulation results returned")
	})

	t.Run("returns correct value for successful simulation", func(t *testing.T) {
		mockRPCService := NewRPCServiceMock(t)
		contractID := "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"

		expectedScVal := xdr.ScVal{Type: xdr.ScValTypeScvString, Str: ptrToScString("TestToken")}

		mockRPCService.On("SimulateTransaction", mock.Anything, mock.Anything).Return(
			entities.RPCSimulateTransactionResult{
				Results: []entities.RPCSimulateHostFunctionResult{{XDR: expectedScVal}},
			},
			nil,
		)

		service, err := NewContractMetadataService(mockRPCService)
		require.NoError(t, err)

		cms := service.(*contractMetadataService)
		result, err := cms.FetchSingleField(ctx, contractID, "name")

		require.NoError(t, err)
		str, ok := result.GetStr()
		assert.True(t, ok)
		assert.Equal(t, "TestToken", string(str))
	})

	t.Run("returns error when context is cancelled", func(t *testing.T) {
		mockRPCService := NewRPCServiceMock(t)
		contractID := "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"

		// Create cancelled context
		cancelledCtx, cancel := context.WithCancel(ctx)
		cancel()

		service, err := NewContractMetadataService(mockRPCService)
		require.NoError(t, err)

		cms := service.(*contractMetadataService)
		_, err = cms.FetchSingleField(cancelledCtx, contractID, "name")

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "context error")
	})

	t.Run("retries transient RPC errors then succeeds", func(t *testing.T) {
		// Override retries for this test only.
		origAttempts, origBackoff := simulateMaxAttempts, simulateInitialBackoff
		simulateMaxAttempts, simulateInitialBackoff = 3, 0
		t.Cleanup(func() { simulateMaxAttempts, simulateInitialBackoff = origAttempts, origBackoff })

		mockRPCService := NewRPCServiceMock(t)
		contractID := "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"

		// Two transient failures (a "latency" message we whitelisted) then success.
		mockRPCService.On("SimulateTransaction", mock.Anything, mock.Anything).Return(
			entities.RPCSimulateTransactionResult{}, errors.New("[-32603] latency since last known ledger closed is too high"),
		).Twice()
		mockRPCService.On("SimulateTransaction", mock.Anything, mock.Anything).Return(
			entities.RPCSimulateTransactionResult{
				Results: []entities.RPCSimulateHostFunctionResult{
					{XDR: xdr.ScVal{Type: xdr.ScValTypeScvString, Str: ptrToScString("OK")}},
				},
			}, nil,
		).Once()

		service, err := NewContractMetadataService(mockRPCService)
		require.NoError(t, err)

		cms := service.(*contractMetadataService)
		val, err := cms.FetchSingleField(ctx, contractID, "name")
		require.NoError(t, err)
		s, ok := val.GetStr()
		require.True(t, ok)
		assert.Equal(t, "OK", string(s))
	})

	t.Run("does not retry permanent errors", func(t *testing.T) {
		origAttempts, origBackoff := simulateMaxAttempts, simulateInitialBackoff
		simulateMaxAttempts, simulateInitialBackoff = 3, 0
		t.Cleanup(func() { simulateMaxAttempts, simulateInitialBackoff = origAttempts, origBackoff })

		mockRPCService := NewRPCServiceMock(t)
		contractID := "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"

		// Permanent-shaped error (no transient marker) — must bail on first attempt.
		mockRPCService.On("SimulateTransaction", mock.Anything, mock.Anything).Return(
			entities.RPCSimulateTransactionResult{}, errors.New("invalid contract: function not found"),
		).Once()

		service, err := NewContractMetadataService(mockRPCService)
		require.NoError(t, err)

		cms := service.(*contractMetadataService)
		_, err = cms.FetchSingleField(ctx, contractID, "name")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "function not found")
	})
}
