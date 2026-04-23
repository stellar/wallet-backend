// Package services provides business logic for the wallet-backend.
// This file contains tests for ContractMetadataService.
package services

import (
	"context"
	"errors"
	"testing"

	"github.com/alitto/pond/v2"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/entities"
)

// Helper functions for creating test XDR values
func ptrToScString(s string) *xdr.ScString {
	str := xdr.ScString(s)
	return &str
}

func TestNewContractMetadataService(t *testing.T) {
	t.Run("returns error when rpcService is nil", func(t *testing.T) {
		_, err := NewContractMetadataService(nil, data.NewContractModelMock(t), pond.NewPool(0))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "rpcService cannot be nil")
	})

	t.Run("returns error when contractModel is nil", func(t *testing.T) {
		mockRPCService := NewRPCServiceMock(t)
		_, err := NewContractMetadataService(mockRPCService, nil, pond.NewPool(0))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "contractModel cannot be nil")
	})

	t.Run("returns error when pool is nil", func(t *testing.T) {
		mockRPCService := NewRPCServiceMock(t)
		mockContractModel := data.NewContractModelMock(t)
		_, err := NewContractMetadataService(mockRPCService, mockContractModel, nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "pool cannot be nil")
	})

	t.Run("creates service successfully", func(t *testing.T) {
		mockRPCService := NewRPCServiceMock(t)
		mockContractModel := data.NewContractModelMock(t)
		pool := pond.NewPool(0)
		defer pool.Stop()

		service, err := NewContractMetadataService(mockRPCService, mockContractModel, pool)
		assert.NoError(t, err)
		assert.NotNil(t, service)
	})
}

func TestFetchSingleField(t *testing.T) {
	ctx := context.Background()

	t.Run("returns error for invalid contract address", func(t *testing.T) {
		mockRPCService := NewRPCServiceMock(t)
		mockContractModel := data.NewContractModelMock(t)
		pool := pond.NewPool(2)
		defer pool.Stop()

		service, err := NewContractMetadataService(mockRPCService, mockContractModel, pool)
		require.NoError(t, err)

		cms := service.(*contractMetadataService)
		_, err = cms.FetchSingleField(ctx, "INVALID_CONTRACT_ID", "name")

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "decoding contract address")
	})

	t.Run("returns error when RPC simulation fails", func(t *testing.T) {
		mockRPCService := NewRPCServiceMock(t)
		mockContractModel := data.NewContractModelMock(t)
		pool := pond.NewPool(2)
		defer pool.Stop()

		contractID := "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"

		mockRPCService.On("SimulateTransaction", mock.Anything, mock.Anything).Return(
			entities.RPCSimulateTransactionResult{}, errors.New("network error"),
		)

		service, err := NewContractMetadataService(mockRPCService, mockContractModel, pool)
		require.NoError(t, err)

		cms := service.(*contractMetadataService)
		_, err = cms.FetchSingleField(ctx, contractID, "name")

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "simulating transaction")
		assert.Contains(t, err.Error(), "network error")
	})

	t.Run("returns error when simulation result has error", func(t *testing.T) {
		mockRPCService := NewRPCServiceMock(t)
		mockContractModel := data.NewContractModelMock(t)
		pool := pond.NewPool(2)
		defer pool.Stop()

		contractID := "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"

		mockRPCService.On("SimulateTransaction", mock.Anything, mock.Anything).Return(
			entities.RPCSimulateTransactionResult{Error: "contract not found"},
			nil,
		)

		service, err := NewContractMetadataService(mockRPCService, mockContractModel, pool)
		require.NoError(t, err)

		cms := service.(*contractMetadataService)
		_, err = cms.FetchSingleField(ctx, contractID, "name")

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "simulation failed")
		assert.Contains(t, err.Error(), "contract not found")
	})

	t.Run("returns error when no results returned", func(t *testing.T) {
		mockRPCService := NewRPCServiceMock(t)
		mockContractModel := data.NewContractModelMock(t)
		pool := pond.NewPool(2)
		defer pool.Stop()

		contractID := "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"

		mockRPCService.On("SimulateTransaction", mock.Anything, mock.Anything).Return(
			entities.RPCSimulateTransactionResult{Results: []entities.RPCSimulateHostFunctionResult{}},
			nil,
		)

		service, err := NewContractMetadataService(mockRPCService, mockContractModel, pool)
		require.NoError(t, err)

		cms := service.(*contractMetadataService)
		_, err = cms.FetchSingleField(ctx, contractID, "name")

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no simulation results returned")
	})

	t.Run("returns correct value for successful simulation", func(t *testing.T) {
		mockRPCService := NewRPCServiceMock(t)
		mockContractModel := data.NewContractModelMock(t)
		pool := pond.NewPool(2)
		defer pool.Stop()

		contractID := "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"

		expectedScVal := xdr.ScVal{Type: xdr.ScValTypeScvString, Str: ptrToScString("TestToken")}

		mockRPCService.On("SimulateTransaction", mock.Anything, mock.Anything).Return(
			entities.RPCSimulateTransactionResult{
				Results: []entities.RPCSimulateHostFunctionResult{{XDR: expectedScVal}},
			},
			nil,
		)

		service, err := NewContractMetadataService(mockRPCService, mockContractModel, pool)
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
		mockContractModel := data.NewContractModelMock(t)
		pool := pond.NewPool(2)
		defer pool.Stop()

		contractID := "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"

		// Create cancelled context
		cancelledCtx, cancel := context.WithCancel(ctx)
		cancel()

		service, err := NewContractMetadataService(mockRPCService, mockContractModel, pool)
		require.NoError(t, err)

		cms := service.(*contractMetadataService)
		_, err = cms.FetchSingleField(cancelledCtx, contractID, "name")

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "context error")
	})
}

func TestFetchSACMetadata(t *testing.T) {
	ctx := context.Background()

	t.Run("returns empty slice for empty input", func(t *testing.T) {
		mockRPCService := NewRPCServiceMock(t)
		mockContractModel := data.NewContractModelMock(t)
		pool := pond.NewPool(5)
		defer pool.Stop()

		service, err := NewContractMetadataService(mockRPCService, mockContractModel, pool)
		require.NoError(t, err)

		result, err := service.FetchSACMetadata(ctx, []string{})

		require.NoError(t, err)
		assert.Empty(t, result)
		// Verify no RPC calls were made
		mockRPCService.AssertNotCalled(t, "SimulateTransaction", mock.Anything, mock.Anything)
	})

	t.Run("parses code:issuer format successfully", func(t *testing.T) {
		mockRPCService := NewRPCServiceMock(t)
		mockContractModel := data.NewContractModelMock(t)
		pool := pond.NewPool(5)
		defer pool.Stop()

		contractID := "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"

		// Mock name() returning "USDC:GCNY..."
		nameScVal := xdr.ScVal{Type: xdr.ScValTypeScvString, Str: ptrToScString("USDC:GCNY5OXYSY4FKHOPT2SPOQZAOEIGXB5LBYW3HVU3OWSTQITS65M5RCNY")}

		mockRPCService.On("SimulateTransaction", mock.Anything, mock.Anything).Return(
			entities.RPCSimulateTransactionResult{
				Results: []entities.RPCSimulateHostFunctionResult{{XDR: nameScVal}},
			}, nil,
		)

		service, err := NewContractMetadataService(mockRPCService, mockContractModel, pool)
		require.NoError(t, err)

		result, err := service.FetchSACMetadata(ctx, []string{contractID})

		require.NoError(t, err)
		require.Len(t, result, 1)

		contract := result[0]
		assert.Equal(t, contractID, contract.ContractID)
		assert.Equal(t, "SAC", contract.Type)
		assert.Equal(t, "USDC", *contract.Code)
		assert.Equal(t, "GCNY5OXYSY4FKHOPT2SPOQZAOEIGXB5LBYW3HVU3OWSTQITS65M5RCNY", *contract.Issuer)
		assert.Equal(t, "USDC:GCNY5OXYSY4FKHOPT2SPOQZAOEIGXB5LBYW3HVU3OWSTQITS65M5RCNY", *contract.Name)
		assert.Equal(t, "USDC", *contract.Symbol)
		assert.Equal(t, uint32(7), contract.Decimals)
	})

	t.Run("handles native XLM asset", func(t *testing.T) {
		mockRPCService := NewRPCServiceMock(t)
		mockContractModel := data.NewContractModelMock(t)
		pool := pond.NewPool(5)
		defer pool.Stop()

		contractID := "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"

		// Mock name() returning "native"
		nameScVal := xdr.ScVal{Type: xdr.ScValTypeScvString, Str: ptrToScString("native")}

		mockRPCService.On("SimulateTransaction", mock.Anything, mock.Anything).Return(
			entities.RPCSimulateTransactionResult{
				Results: []entities.RPCSimulateHostFunctionResult{{XDR: nameScVal}},
			}, nil,
		)

		service, err := NewContractMetadataService(mockRPCService, mockContractModel, pool)
		require.NoError(t, err)

		result, err := service.FetchSACMetadata(ctx, []string{contractID})

		require.NoError(t, err)
		require.Len(t, result, 1)

		contract := result[0]
		assert.Equal(t, contractID, contract.ContractID)
		assert.Equal(t, "SAC", contract.Type)
		assert.Equal(t, "XLM", *contract.Code)
		assert.Equal(t, "", *contract.Issuer)
		assert.Equal(t, "native", *contract.Name)
		assert.Equal(t, "XLM", *contract.Symbol)
		assert.Equal(t, uint32(7), contract.Decimals)
	})

	t.Run("returns error for contract with malformed name", func(t *testing.T) {
		mockRPCService := NewRPCServiceMock(t)
		mockContractModel := data.NewContractModelMock(t)
		pool := pond.NewPool(5)
		defer pool.Stop()

		contractID := "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"

		// Mock name() returning malformed value (no colon)
		nameScVal := xdr.ScVal{Type: xdr.ScValTypeScvString, Str: ptrToScString("MALFORMED_NO_COLON")}

		mockRPCService.On("SimulateTransaction", mock.Anything, mock.Anything).Return(
			entities.RPCSimulateTransactionResult{
				Results: []entities.RPCSimulateHostFunctionResult{{XDR: nameScVal}},
			}, nil,
		)

		service, err := NewContractMetadataService(mockRPCService, mockContractModel, pool)
		require.NoError(t, err)

		result, err := service.FetchSACMetadata(ctx, []string{contractID})

		// Should return error for malformed contract name
		require.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "failed to fetch metadata")
		assert.Contains(t, err.Error(), "malformed SAC name")
	})

	t.Run("returns error when RPC fails", func(t *testing.T) {
		mockRPCService := NewRPCServiceMock(t)
		mockContractModel := data.NewContractModelMock(t)
		pool := pond.NewPool(5)
		defer pool.Stop()

		contractID := "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"

		mockRPCService.On("SimulateTransaction", mock.Anything, mock.Anything).Return(
			entities.RPCSimulateTransactionResult{}, errors.New("RPC timeout"),
		)

		service, err := NewContractMetadataService(mockRPCService, mockContractModel, pool)
		require.NoError(t, err)

		result, err := service.FetchSACMetadata(ctx, []string{contractID})

		// Should return error when RPC fails
		require.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "failed to fetch metadata")
		assert.Contains(t, err.Error(), "RPC timeout")
	})

	t.Run("processes multiple contracts successfully", func(t *testing.T) {
		mockRPCService := NewRPCServiceMock(t)
		mockContractModel := data.NewContractModelMock(t)
		pool := pond.NewPool(5)
		defer pool.Stop()

		contractID1 := "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"
		contractID2 := "CA7QYNF7SOWQ3GLR2BGMZEHXAVIRZA4KVWLTJJFC7MGXUA74P7UJUWDA"

		// Mock responses for two contracts - use mock.Anything for both calls
		nameScVal1 := xdr.ScVal{Type: xdr.ScValTypeScvString, Str: ptrToScString("USDC:GCNY5OXYSY4FKHOPT2SPOQZAOEIGXB5LBYW3HVU3OWSTQITS65M5RCNY")}
		nameScVal2 := xdr.ScVal{Type: xdr.ScValTypeScvString, Str: ptrToScString("native")}

		// Return different values for the two calls
		mockRPCService.On("SimulateTransaction", mock.Anything, mock.Anything).Return(
			entities.RPCSimulateTransactionResult{
				Results: []entities.RPCSimulateHostFunctionResult{{XDR: nameScVal1}},
			}, nil,
		).Once()
		mockRPCService.On("SimulateTransaction", mock.Anything, mock.Anything).Return(
			entities.RPCSimulateTransactionResult{
				Results: []entities.RPCSimulateHostFunctionResult{{XDR: nameScVal2}},
			}, nil,
		).Once()

		service, err := NewContractMetadataService(mockRPCService, mockContractModel, pool)
		require.NoError(t, err)

		result, err := service.FetchSACMetadata(ctx, []string{contractID1, contractID2})

		require.NoError(t, err)
		assert.Len(t, result, 2)
	})

	t.Run("returns error and no partial results when one contract fails", func(t *testing.T) {
		mockRPCService := NewRPCServiceMock(t)
		mockContractModel := data.NewContractModelMock(t)
		pool := pond.NewPool(5)
		defer pool.Stop()

		contractID1 := "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"
		contractID2 := "CA7QYNF7SOWQ3GLR2BGMZEHXAVIRZA4KVWLTJJFC7MGXUA74P7UJUWDA"

		// First contract succeeds, second fails
		nameScVal1 := xdr.ScVal{Type: xdr.ScValTypeScvString, Str: ptrToScString("USDC:GCNY5OXYSY4FKHOPT2SPOQZAOEIGXB5LBYW3HVU3OWSTQITS65M5RCNY")}

		mockRPCService.On("SimulateTransaction", mock.Anything, mock.Anything).Return(
			entities.RPCSimulateTransactionResult{
				Results: []entities.RPCSimulateHostFunctionResult{{XDR: nameScVal1}},
			}, nil,
		).Once()
		mockRPCService.On("SimulateTransaction", mock.Anything, mock.Anything).Return(
			entities.RPCSimulateTransactionResult{}, errors.New("RPC timeout"),
		).Once()

		service, err := NewContractMetadataService(mockRPCService, mockContractModel, pool)
		require.NoError(t, err)

		result, err := service.FetchSACMetadata(ctx, []string{contractID1, contractID2})

		// Should return error and no partial results
		require.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "failed to fetch metadata for 1 SAC contracts")
	})
}
