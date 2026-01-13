// Package services provides business logic for the wallet-backend.
// This file contains tests for ContractMetadataService.
package services

import (
	"context"
	"encoding/base64"
	"errors"
	"strings"
	"testing"

	"github.com/alitto/pond/v2"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
)

// Helper functions for creating test XDR values
func ptrToScString(s string) *xdr.ScString {
	str := xdr.ScString(s)
	return &str
}

func ptrToXdrUint32(n uint32) *xdr.Uint32 {
	u := xdr.Uint32(n)
	return &u
}

// containsFunction checks if a base64-encoded XDR transaction contains a specific function name.
// This is used for dynamic mock responses based on the function being called.
func containsFunction(txXDR string, functionName string) bool {
	decoded, err := base64.StdEncoding.DecodeString(txXDR)
	if err != nil {
		return false
	}
	return strings.Contains(string(decoded), functionName)
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

func TestParseSACMetadata(t *testing.T) {
	mockRPCService := NewRPCServiceMock(t)
	mockContractModel := data.NewContractModelMock(t)
	pool := pond.NewPool(0)
	defer pool.Stop()

	service, err := NewContractMetadataService(mockRPCService, mockContractModel, pool)
	assert.NoError(t, err)

	cms := service.(*contractMetadataService)

	tests := []struct {
		name     string
		input    map[string]ContractMetadata
		expected map[string]ContractMetadata
	}{
		{
			name: "parses SAC code:issuer format",
			input: map[string]ContractMetadata{
				"CAAAA": {Type: types.ContractTypeSAC, Name: "USDC:GAISSUERTESTADDRESS"},
			},
			expected: map[string]ContractMetadata{
				"CAAAA": {Type: types.ContractTypeSAC, Name: "USDC:GAISSUERTESTADDRESS", Code: "USDC", Issuer: "GAISSUERTESTADDRESS"},
			},
		},
		{
			name: "skips non-SAC contracts",
			input: map[string]ContractMetadata{
				"CAAAA": {Type: types.ContractTypeSEP41, Name: "MyToken:SomeIssuer"},
			},
			expected: map[string]ContractMetadata{
				"CAAAA": {Type: types.ContractTypeSEP41, Name: "MyToken:SomeIssuer"},
			},
		},
		{
			name: "skips SAC with empty name",
			input: map[string]ContractMetadata{
				"CAAAA": {Type: types.ContractTypeSAC, Name: ""},
			},
			expected: map[string]ContractMetadata{
				"CAAAA": {Type: types.ContractTypeSAC, Name: ""},
			},
		},
		{
			name: "skips SAC with invalid format",
			input: map[string]ContractMetadata{
				"CAAAA": {Type: types.ContractTypeSAC, Name: "InvalidFormat"},
			},
			expected: map[string]ContractMetadata{
				"CAAAA": {Type: types.ContractTypeSAC, Name: "InvalidFormat"},
			},
		},
		{
			name: "handles multiple colons",
			input: map[string]ContractMetadata{
				"CAAAA": {Type: types.ContractTypeSAC, Name: "USDC:ISSUER:EXTRA"},
			},
			expected: map[string]ContractMetadata{
				"CAAAA": {Type: types.ContractTypeSAC, Name: "USDC:ISSUER:EXTRA"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cms.parseSACMetadata(tt.input)
			assert.Equal(t, tt.expected, tt.input)
		})
	}
}

func TestFetchAndStoreMetadata(t *testing.T) {
	ctx := context.Background()

	t.Run("returns nil for empty contracts", func(t *testing.T) {
		mockRPCService := NewRPCServiceMock(t)
		mockContractModel := data.NewContractModelMock(t)
		pool := pond.NewPool(0)
		defer pool.Stop()

		service, err := NewContractMetadataService(mockRPCService, mockContractModel, pool)
		require.NoError(t, err)

		err = service.FetchAndStoreMetadata(ctx, nil, map[string]types.ContractType{})
		assert.NoError(t, err)

		// Verify no RPC or DB calls were made
		mockRPCService.AssertNotCalled(t, "SimulateTransaction")
		mockContractModel.AssertNotCalled(t, "BatchInsert")
	})

	t.Run("fetches and stores metadata successfully for SAC contract", func(t *testing.T) {
		dbt := dbtest.Open(t)
		defer dbt.Close()
		dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
		require.NoError(t, err)
		defer dbConnectionPool.Close()

		cleanUpDB := func() {
			_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM contract_tokens`)
			require.NoError(t, err)
		}
		cleanUpDB()

		mockRPCService := NewRPCServiceMock(t)
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", mock.Anything, mock.Anything, mock.Anything).Return()
		mockMetricsService.On("ObserveDBBatchSize", mock.Anything, mock.Anything, mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", mock.Anything, mock.Anything).Return()
		mockMetricsService.On("IncDBQueryError", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
		defer mockMetricsService.AssertExpectations(t)

		contractModel := &data.ContractModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}
		pool := pond.NewPool(5)
		defer pool.Stop()

		// Valid contract ID (C prefix, 56 characters)
		contractID := "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"

		contractTypes := map[string]types.ContractType{
			contractID: types.ContractTypeSAC,
		}

		// Mock RPC responses - use MatchedBy to return different values based on the function name in the transaction
		// The calls happen in parallel, so we match on the encoded function name
		nameScVal := xdr.ScVal{
			Type: xdr.ScValTypeScvString,
			Str:  ptrToScString("USDC:GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5"),
		}
		symbolScVal := xdr.ScVal{
			Type: xdr.ScValTypeScvString,
			Str:  ptrToScString("USDC"),
		}
		decimalsScVal := xdr.ScVal{
			Type: xdr.ScValTypeScvU32,
			U32:  ptrToXdrUint32(7),
		}

		mockRPCService.On("SimulateTransaction", mock.MatchedBy(func(txXDR string) bool {
			return containsFunction(txXDR, "name")
		}), mock.Anything).Return(
			entities.RPCSimulateTransactionResult{
				Results: []entities.RPCSimulateHostFunctionResult{{XDR: nameScVal}},
			}, nil,
		)
		mockRPCService.On("SimulateTransaction", mock.MatchedBy(func(txXDR string) bool {
			return containsFunction(txXDR, "symbol")
		}), mock.Anything).Return(
			entities.RPCSimulateTransactionResult{
				Results: []entities.RPCSimulateHostFunctionResult{{XDR: symbolScVal}},
			}, nil,
		)
		mockRPCService.On("SimulateTransaction", mock.MatchedBy(func(txXDR string) bool {
			return containsFunction(txXDR, "decimals")
		}), mock.Anything).Return(
			entities.RPCSimulateTransactionResult{
				Results: []entities.RPCSimulateHostFunctionResult{{XDR: decimalsScVal}},
			}, nil,
		)

		service, err := NewContractMetadataService(mockRPCService, contractModel, pool)
		require.NoError(t, err)

		pgxTx, err := dbConnectionPool.PgxPool().Begin(ctx)
		require.NoError(t, err)
		err = service.FetchAndStoreMetadata(ctx, pgxTx, contractTypes)
		require.NoError(t, err)
		require.NoError(t, pgxTx.Commit(ctx))

		// Verify contract was stored correctly in database
		contract, err := contractModel.GetByContractID(ctx, contractID)
		require.NoError(t, err)
		assert.Equal(t, contractID, contract.ContractID)
		assert.Equal(t, string(types.ContractTypeSAC), contract.Type)
		assert.Equal(t, "USDC", *contract.Code)
		assert.Equal(t, "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5", *contract.Issuer)
		assert.Equal(t, "USDC:GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5", *contract.Name)
		assert.Equal(t, "USDC", *contract.Symbol)
		assert.Equal(t, uint32(7), contract.Decimals)

		cleanUpDB()
	})

	t.Run("fetches and stores metadata successfully for SEP41 contract", func(t *testing.T) {
		dbt := dbtest.Open(t)
		defer dbt.Close()
		dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
		require.NoError(t, err)
		defer dbConnectionPool.Close()

		cleanUpDB := func() {
			_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM contract_tokens`)
			require.NoError(t, err)
		}
		cleanUpDB()

		mockRPCService := NewRPCServiceMock(t)
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", mock.Anything, mock.Anything, mock.Anything).Return()
		mockMetricsService.On("ObserveDBBatchSize", mock.Anything, mock.Anything, mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", mock.Anything, mock.Anything).Return()
		mockMetricsService.On("IncDBQueryError", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
		defer mockMetricsService.AssertExpectations(t)

		contractModel := &data.ContractModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}
		pool := pond.NewPool(5)
		defer pool.Stop()

		contractID := "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"

		contractTypes := map[string]types.ContractType{
			contractID: types.ContractTypeSEP41,
		}

		// Mock RPC responses - use MatchedBy to return different values based on the function name in the transaction
		// The calls happen in parallel, so we match on the encoded function name
		nameScVal := xdr.ScVal{
			Type: xdr.ScValTypeScvString,
			Str:  ptrToScString("MyToken"),
		}
		symbolScVal := xdr.ScVal{
			Type: xdr.ScValTypeScvString,
			Str:  ptrToScString("MTK"),
		}
		decimalsScVal := xdr.ScVal{
			Type: xdr.ScValTypeScvU32,
			U32:  ptrToXdrUint32(18),
		}

		mockRPCService.On("SimulateTransaction", mock.MatchedBy(func(txXDR string) bool {
			return containsFunction(txXDR, "name")
		}), mock.Anything).Return(
			entities.RPCSimulateTransactionResult{
				Results: []entities.RPCSimulateHostFunctionResult{{XDR: nameScVal}},
			}, nil,
		)
		mockRPCService.On("SimulateTransaction", mock.MatchedBy(func(txXDR string) bool {
			return containsFunction(txXDR, "symbol")
		}), mock.Anything).Return(
			entities.RPCSimulateTransactionResult{
				Results: []entities.RPCSimulateHostFunctionResult{{XDR: symbolScVal}},
			}, nil,
		)
		mockRPCService.On("SimulateTransaction", mock.MatchedBy(func(txXDR string) bool {
			return containsFunction(txXDR, "decimals")
		}), mock.Anything).Return(
			entities.RPCSimulateTransactionResult{
				Results: []entities.RPCSimulateHostFunctionResult{{XDR: decimalsScVal}},
			}, nil,
		)

		service, err := NewContractMetadataService(mockRPCService, contractModel, pool)
		require.NoError(t, err)

		pgxTx, err := dbConnectionPool.PgxPool().Begin(ctx)
		require.NoError(t, err)
		err = service.FetchAndStoreMetadata(ctx, pgxTx, contractTypes)
		require.NoError(t, err)
		require.NoError(t, pgxTx.Commit(ctx))

		// Verify contract was stored correctly in database
		// SEP41 should NOT have Code/Issuer parsed (name is not in code:issuer format)
		contract, err := contractModel.GetByContractID(ctx, contractID)
		require.NoError(t, err)
		assert.Equal(t, contractID, contract.ContractID)
		assert.Equal(t, string(types.ContractTypeSEP41), contract.Type)
		assert.Equal(t, "", *contract.Code)
		assert.Equal(t, "", *contract.Issuer)
		assert.Equal(t, "MyToken", *contract.Name)
		assert.Equal(t, "MTK", *contract.Symbol)
		assert.Equal(t, uint32(18), contract.Decimals)

		cleanUpDB()
	})

	t.Run("returns error when database insert fails", func(t *testing.T) {
		mockRPCService := NewRPCServiceMock(t)
		mockContractModel := data.NewContractModelMock(t)
		pool := pond.NewPool(2)
		defer pool.Stop()

		contractID := "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"

		contractTypes := map[string]types.ContractType{
			contractID: types.ContractTypeSEP41,
		}

		// Mock RPC to allow any calls (metadata fetching happens before DB insert)
		mockRPCService.On("SimulateTransaction", mock.Anything, mock.Anything).Return(
			entities.RPCSimulateTransactionResult{}, nil,
		).Maybe()

		// Mock DB error
		mockContractModel.On("BatchInsert", ctx, mock.Anything, mock.Anything).Return(
			map[string]int64{}, errors.New("database connection failed"),
		)

		service, err := NewContractMetadataService(mockRPCService, mockContractModel, pool)
		require.NoError(t, err)

		err = service.FetchAndStoreMetadata(ctx, nil, contractTypes)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "storing contract metadata in database")
		assert.Contains(t, err.Error(), "database connection failed")
	})

	t.Run("handles RPC simulation error gracefully and stores contract with empty metadata", func(t *testing.T) {
		dbt := dbtest.Open(t)
		defer dbt.Close()
		dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
		require.NoError(t, err)
		defer dbConnectionPool.Close()

		cleanUpDB := func() {
			_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM contract_tokens`)
			require.NoError(t, err)
		}
		cleanUpDB()

		mockRPCService := NewRPCServiceMock(t)
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", mock.Anything, mock.Anything, mock.Anything).Return()
		mockMetricsService.On("ObserveDBBatchSize", mock.Anything, mock.Anything, mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", mock.Anything, mock.Anything).Return()
		mockMetricsService.On("IncDBQueryError", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
		defer mockMetricsService.AssertExpectations(t)

		contractModel := &data.ContractModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}
		pool := pond.NewPool(5)
		defer pool.Stop()

		contractID := "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"

		contractTypes := map[string]types.ContractType{
			contractID: types.ContractTypeSEP41,
		}

		// Mock RPC to return error
		mockRPCService.On("SimulateTransaction", mock.Anything, mock.Anything).Return(
			entities.RPCSimulateTransactionResult{}, errors.New("RPC unavailable"),
		)

		service, err := NewContractMetadataService(mockRPCService, contractModel, pool)
		require.NoError(t, err)

		pgxTx, err := dbConnectionPool.PgxPool().Begin(ctx)
		require.NoError(t, err)
		err = service.FetchAndStoreMetadata(ctx, pgxTx, contractTypes)
		require.NoError(t, err)
		require.NoError(t, pgxTx.Commit(ctx))

		// Verify contract was stored with empty metadata
		contract, err := contractModel.GetByContractID(ctx, contractID)
		require.NoError(t, err)
		assert.Equal(t, contractID, contract.ContractID)
		assert.Equal(t, string(types.ContractTypeSEP41), contract.Type)
		assert.Equal(t, "", *contract.Name)
		assert.Equal(t, "", *contract.Symbol)
		assert.Equal(t, uint32(0), contract.Decimals)

		cleanUpDB()
	})

	t.Run("handles RPC simulation failure response and stores contract with empty metadata", func(t *testing.T) {
		dbt := dbtest.Open(t)
		defer dbt.Close()
		dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
		require.NoError(t, err)
		defer dbConnectionPool.Close()

		cleanUpDB := func() {
			_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM contract_tokens`)
			require.NoError(t, err)
		}
		cleanUpDB()

		mockRPCService := NewRPCServiceMock(t)
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", mock.Anything, mock.Anything, mock.Anything).Return()
		mockMetricsService.On("ObserveDBBatchSize", mock.Anything, mock.Anything, mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", mock.Anything, mock.Anything).Return()
		mockMetricsService.On("IncDBQueryError", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
		defer mockMetricsService.AssertExpectations(t)

		contractModel := &data.ContractModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}
		pool := pond.NewPool(5)
		defer pool.Stop()

		contractID := "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"

		contractTypes := map[string]types.ContractType{
			contractID: types.ContractTypeSEP41,
		}

		// Mock RPC to return error in result
		mockRPCService.On("SimulateTransaction", mock.Anything, mock.Anything).Return(
			entities.RPCSimulateTransactionResult{Error: "simulation failed: contract not found"},
			nil,
		)

		service, err := NewContractMetadataService(mockRPCService, contractModel, pool)
		require.NoError(t, err)

		pgxTx, err := dbConnectionPool.PgxPool().Begin(ctx)
		require.NoError(t, err)
		err = service.FetchAndStoreMetadata(ctx, pgxTx, contractTypes)
		require.NoError(t, err)
		require.NoError(t, pgxTx.Commit(ctx))

		// Verify contract was stored with empty metadata
		contract, err := contractModel.GetByContractID(ctx, contractID)
		require.NoError(t, err)
		assert.Equal(t, contractID, contract.ContractID)
		assert.Equal(t, string(types.ContractTypeSEP41), contract.Type)
		assert.Equal(t, "", *contract.Name)
		assert.Equal(t, "", *contract.Symbol)
		assert.Equal(t, uint32(0), contract.Decimals)

		cleanUpDB()
	})

	t.Run("handles empty simulation results and stores contract with empty metadata", func(t *testing.T) {
		dbt := dbtest.Open(t)
		defer dbt.Close()
		dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
		require.NoError(t, err)
		defer dbConnectionPool.Close()

		cleanUpDB := func() {
			_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM contract_tokens`)
			require.NoError(t, err)
		}
		cleanUpDB()

		mockRPCService := NewRPCServiceMock(t)
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", mock.Anything, mock.Anything, mock.Anything).Return()
		mockMetricsService.On("ObserveDBBatchSize", mock.Anything, mock.Anything, mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", mock.Anything, mock.Anything).Return()
		mockMetricsService.On("IncDBQueryError", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
		defer mockMetricsService.AssertExpectations(t)

		contractModel := &data.ContractModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}
		pool := pond.NewPool(5)
		defer pool.Stop()

		contractID := "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"

		contractTypes := map[string]types.ContractType{
			contractID: types.ContractTypeSEP41,
		}

		// Mock RPC to return empty results
		mockRPCService.On("SimulateTransaction", mock.Anything, mock.Anything).Return(
			entities.RPCSimulateTransactionResult{Results: []entities.RPCSimulateHostFunctionResult{}},
			nil,
		)

		service, err := NewContractMetadataService(mockRPCService, contractModel, pool)
		require.NoError(t, err)

		pgxTx, err := dbConnectionPool.PgxPool().Begin(ctx)
		require.NoError(t, err)
		err = service.FetchAndStoreMetadata(ctx, pgxTx, contractTypes)
		require.NoError(t, err)
		require.NoError(t, pgxTx.Commit(ctx))

		// Verify contract was stored with empty metadata
		contract, err := contractModel.GetByContractID(ctx, contractID)
		require.NoError(t, err)
		assert.Equal(t, contractID, contract.ContractID)
		assert.Equal(t, string(types.ContractTypeSEP41), contract.Type)
		assert.Equal(t, "", *contract.Name)
		assert.Equal(t, "", *contract.Symbol)
		assert.Equal(t, uint32(0), contract.Decimals)

		cleanUpDB()
	})

	t.Run("handles type conversion errors gracefully and stores contract with empty metadata", func(t *testing.T) {
		dbt := dbtest.Open(t)
		defer dbt.Close()
		dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
		require.NoError(t, err)
		defer dbConnectionPool.Close()

		cleanUpDB := func() {
			_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM contract_tokens`)
			require.NoError(t, err)
		}
		cleanUpDB()

		mockRPCService := NewRPCServiceMock(t)
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", mock.Anything, mock.Anything, mock.Anything).Return()
		mockMetricsService.On("ObserveDBBatchSize", mock.Anything, mock.Anything, mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", mock.Anything, mock.Anything).Return()
		mockMetricsService.On("IncDBQueryError", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
		defer mockMetricsService.AssertExpectations(t)

		contractModel := &data.ContractModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}
		pool := pond.NewPool(5)
		defer pool.Stop()

		contractID := "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"

		contractTypes := map[string]types.ContractType{
			contractID: types.ContractTypeSEP41,
		}

		// Mock RPC to return wrong types - name as U32 instead of string
		wrongTypeScVal := xdr.ScVal{
			Type: xdr.ScValTypeScvU32,
			U32:  ptrToXdrUint32(123),
		}

		mockRPCService.On("SimulateTransaction", mock.Anything, mock.Anything).Return(
			entities.RPCSimulateTransactionResult{
				Results: []entities.RPCSimulateHostFunctionResult{{XDR: wrongTypeScVal}},
			}, nil,
		)

		service, err := NewContractMetadataService(mockRPCService, contractModel, pool)
		require.NoError(t, err)

		pgxTx, err := dbConnectionPool.PgxPool().Begin(ctx)
		require.NoError(t, err)
		err = service.FetchAndStoreMetadata(ctx, pgxTx, contractTypes)
		require.NoError(t, err)
		require.NoError(t, pgxTx.Commit(ctx))

		// Verify contract was stored with empty metadata
		contract, err := contractModel.GetByContractID(ctx, contractID)
		require.NoError(t, err)
		assert.Equal(t, contractID, contract.ContractID)
		assert.Equal(t, string(types.ContractTypeSEP41), contract.Type)
		assert.Equal(t, "", *contract.Name)
		assert.Equal(t, "", *contract.Symbol)
		assert.Equal(t, uint32(0), contract.Decimals)

		cleanUpDB()
	})
}

func TestStoreInDB(t *testing.T) {
	ctx := context.Background()

	t.Run("returns nil for empty metadata map", func(t *testing.T) {
		mockRPCService := NewRPCServiceMock(t)
		mockContractModel := data.NewContractModelMock(t)
		pool := pond.NewPool(2)
		defer pool.Stop()

		service, err := NewContractMetadataService(mockRPCService, mockContractModel, pool)
		require.NoError(t, err)

		cms := service.(*contractMetadataService)
		err = cms.storeInDB(ctx, nil, map[string]ContractMetadata{})
		assert.NoError(t, err)

		mockContractModel.AssertNotCalled(t, "BatchInsert")
	})

	t.Run("stores contracts correctly in database", func(t *testing.T) {
		dbt := dbtest.Open(t)
		defer dbt.Close()
		dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
		require.NoError(t, err)
		defer dbConnectionPool.Close()

		cleanUpDB := func() {
			_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM contract_tokens`)
			require.NoError(t, err)
		}
		cleanUpDB()

		mockRPCService := NewRPCServiceMock(t)
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", mock.Anything, mock.Anything, mock.Anything).Return()
		mockMetricsService.On("ObserveDBBatchSize", mock.Anything, mock.Anything, mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", mock.Anything, mock.Anything).Return()
		mockMetricsService.On("IncDBQueryError", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
		defer mockMetricsService.AssertExpectations(t)

		contractModel := &data.ContractModel{
			DB:             dbConnectionPool,
			MetricsService: mockMetricsService,
		}
		pool := pond.NewPool(2)
		defer pool.Stop()

		metadataMap := map[string]ContractMetadata{
			"CONTRACT1": {
				ContractID: "CONTRACT1",
				Type:       types.ContractTypeSAC,
				Code:       "USDC",
				Issuer:     "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5",
				Name:       "USDC:GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5",
				Symbol:     "USDC",
				Decimals:   7,
			},
			"CONTRACT2": {
				ContractID: "CONTRACT2",
				Type:       types.ContractTypeSEP41,
				Name:       "MyToken",
				Symbol:     "MTK",
				Decimals:   18,
			},
		}

		service, err := NewContractMetadataService(mockRPCService, contractModel, pool)
		require.NoError(t, err)

		pgxTx, err := dbConnectionPool.PgxPool().Begin(ctx)
		require.NoError(t, err)
		cms := service.(*contractMetadataService)
		err = cms.storeInDB(ctx, pgxTx, metadataMap)
		require.NoError(t, err)
		require.NoError(t, pgxTx.Commit(ctx))

		// Verify CONTRACT1 was stored correctly
		contract1, err := contractModel.GetByContractID(ctx, "CONTRACT1")
		require.NoError(t, err)
		assert.Equal(t, "CONTRACT1", contract1.ContractID)
		assert.Equal(t, string(types.ContractTypeSAC), contract1.Type)
		assert.Equal(t, "USDC", *contract1.Code)
		assert.Equal(t, "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5", *contract1.Issuer)
		assert.Equal(t, "USDC:GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5", *contract1.Name)
		assert.Equal(t, "USDC", *contract1.Symbol)
		assert.Equal(t, uint32(7), contract1.Decimals)

		// Verify CONTRACT2 was stored correctly
		contract2, err := contractModel.GetByContractID(ctx, "CONTRACT2")
		require.NoError(t, err)
		assert.Equal(t, "CONTRACT2", contract2.ContractID)
		assert.Equal(t, string(types.ContractTypeSEP41), contract2.Type)
		assert.Equal(t, "MyToken", *contract2.Name)
		assert.Equal(t, "MTK", *contract2.Symbol)
		assert.Equal(t, uint32(18), contract2.Decimals)

		cleanUpDB()
	})

	t.Run("returns error when batch insert fails", func(t *testing.T) {
		mockRPCService := NewRPCServiceMock(t)
		mockContractModel := data.NewContractModelMock(t)
		pool := pond.NewPool(2)
		defer pool.Stop()

		metadataMap := map[string]ContractMetadata{
			"CONTRACT1": {
				ContractID: "CONTRACT1",
				Type:       types.ContractTypeSAC,
			},
		}

		mockContractModel.On("BatchInsert", ctx, mock.Anything, mock.Anything).Return(
			errors.New("unique constraint violation"),
		)

		service, err := NewContractMetadataService(mockRPCService, mockContractModel, pool)
		require.NoError(t, err)

		cms := service.(*contractMetadataService)
		err = cms.storeInDB(ctx, nil, metadataMap)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "storing contract metadata in database")
		assert.Contains(t, err.Error(), "unique constraint violation")
	})
}

func TestFetchMetadata(t *testing.T) {
	ctx := context.Background()

	t.Run("fetches all three fields successfully", func(t *testing.T) {
		mockRPCService := NewRPCServiceMock(t)
		mockContractModel := data.NewContractModelMock(t)
		pool := pond.NewPool(5)
		defer pool.Stop()

		contractID := "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"

		// Mock RPC responses - use MatchedBy to return different values based on the function name
		nameScVal := xdr.ScVal{Type: xdr.ScValTypeScvString, Str: ptrToScString("TestName")}
		symbolScVal := xdr.ScVal{Type: xdr.ScValTypeScvString, Str: ptrToScString("TST")}
		decimalsScVal := xdr.ScVal{Type: xdr.ScValTypeScvU32, U32: ptrToXdrUint32(8)}

		mockRPCService.On("SimulateTransaction", mock.MatchedBy(func(txXDR string) bool {
			return containsFunction(txXDR, "name")
		}), mock.Anything).Return(
			entities.RPCSimulateTransactionResult{
				Results: []entities.RPCSimulateHostFunctionResult{{XDR: nameScVal}},
			}, nil,
		)
		mockRPCService.On("SimulateTransaction", mock.MatchedBy(func(txXDR string) bool {
			return containsFunction(txXDR, "symbol")
		}), mock.Anything).Return(
			entities.RPCSimulateTransactionResult{
				Results: []entities.RPCSimulateHostFunctionResult{{XDR: symbolScVal}},
			}, nil,
		)
		mockRPCService.On("SimulateTransaction", mock.MatchedBy(func(txXDR string) bool {
			return containsFunction(txXDR, "decimals")
		}), mock.Anything).Return(
			entities.RPCSimulateTransactionResult{
				Results: []entities.RPCSimulateHostFunctionResult{{XDR: decimalsScVal}},
			}, nil,
		)

		service, err := NewContractMetadataService(mockRPCService, mockContractModel, pool)
		require.NoError(t, err)

		cms := service.(*contractMetadataService)
		metadata, err := cms.fetchMetadata(ctx, contractID, types.ContractTypeSEP41)

		require.NoError(t, err)
		assert.Equal(t, contractID, metadata.ContractID)
		assert.Equal(t, types.ContractTypeSEP41, metadata.Type)
		assert.Equal(t, "TestName", metadata.Name)
		assert.Equal(t, "TST", metadata.Symbol)
		assert.Equal(t, uint32(8), metadata.Decimals)
	})

	t.Run("returns error when any field fetch fails", func(t *testing.T) {
		mockRPCService := NewRPCServiceMock(t)
		mockContractModel := data.NewContractModelMock(t)
		pool := pond.NewPool(5)
		defer pool.Stop()

		contractID := "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"

		// Two succeed, one fails
		nameScVal := xdr.ScVal{Type: xdr.ScValTypeScvString, Str: ptrToScString("TestName")}
		symbolScVal := xdr.ScVal{Type: xdr.ScValTypeScvString, Str: ptrToScString("TST")}

		mockRPCService.On("SimulateTransaction", mock.Anything, mock.Anything).Return(
			entities.RPCSimulateTransactionResult{
				Results: []entities.RPCSimulateHostFunctionResult{{XDR: nameScVal}},
			}, nil,
		).Once()
		mockRPCService.On("SimulateTransaction", mock.Anything, mock.Anything).Return(
			entities.RPCSimulateTransactionResult{
				Results: []entities.RPCSimulateHostFunctionResult{{XDR: symbolScVal}},
			}, nil,
		).Once()
		mockRPCService.On("SimulateTransaction", mock.Anything, mock.Anything).Return(
			entities.RPCSimulateTransactionResult{}, errors.New("RPC timeout"),
		).Once()

		service, err := NewContractMetadataService(mockRPCService, mockContractModel, pool)
		require.NoError(t, err)

		cms := service.(*contractMetadataService)
		_, err = cms.fetchMetadata(ctx, contractID, types.ContractTypeSEP41)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "fetching contract metadata")
	})

	t.Run("returns error when name is not a string", func(t *testing.T) {
		mockRPCService := NewRPCServiceMock(t)
		mockContractModel := data.NewContractModelMock(t)
		pool := pond.NewPool(5)
		defer pool.Stop()

		contractID := "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"

		// Name returns wrong type
		wrongTypeScVal := xdr.ScVal{Type: xdr.ScValTypeScvU32, U32: ptrToXdrUint32(123)}
		symbolScVal := xdr.ScVal{Type: xdr.ScValTypeScvString, Str: ptrToScString("TST")}
		decimalsScVal := xdr.ScVal{Type: xdr.ScValTypeScvU32, U32: ptrToXdrUint32(8)}

		mockRPCService.On("SimulateTransaction", mock.Anything, mock.Anything).Return(
			entities.RPCSimulateTransactionResult{
				Results: []entities.RPCSimulateHostFunctionResult{{XDR: wrongTypeScVal}},
			}, nil,
		).Once()
		mockRPCService.On("SimulateTransaction", mock.Anything, mock.Anything).Return(
			entities.RPCSimulateTransactionResult{
				Results: []entities.RPCSimulateHostFunctionResult{{XDR: symbolScVal}},
			}, nil,
		).Once()
		mockRPCService.On("SimulateTransaction", mock.Anything, mock.Anything).Return(
			entities.RPCSimulateTransactionResult{
				Results: []entities.RPCSimulateHostFunctionResult{{XDR: decimalsScVal}},
			}, nil,
		).Once()

		service, err := NewContractMetadataService(mockRPCService, mockContractModel, pool)
		require.NoError(t, err)

		cms := service.(*contractMetadataService)
		_, err = cms.fetchMetadata(ctx, contractID, types.ContractTypeSEP41)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not a string")
	})

	t.Run("returns error when decimals is not a uint32", func(t *testing.T) {
		mockRPCService := NewRPCServiceMock(t)
		mockContractModel := data.NewContractModelMock(t)
		pool := pond.NewPool(5)
		defer pool.Stop()

		contractID := "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"

		// Decimals returns wrong type
		nameScVal := xdr.ScVal{Type: xdr.ScValTypeScvString, Str: ptrToScString("TestName")}
		symbolScVal := xdr.ScVal{Type: xdr.ScValTypeScvString, Str: ptrToScString("TST")}
		wrongTypeScVal := xdr.ScVal{Type: xdr.ScValTypeScvString, Str: ptrToScString("notANumber")}

		mockRPCService.On("SimulateTransaction", mock.Anything, mock.Anything).Return(
			entities.RPCSimulateTransactionResult{
				Results: []entities.RPCSimulateHostFunctionResult{{XDR: nameScVal}},
			}, nil,
		).Once()
		mockRPCService.On("SimulateTransaction", mock.Anything, mock.Anything).Return(
			entities.RPCSimulateTransactionResult{
				Results: []entities.RPCSimulateHostFunctionResult{{XDR: symbolScVal}},
			}, nil,
		).Once()
		mockRPCService.On("SimulateTransaction", mock.Anything, mock.Anything).Return(
			entities.RPCSimulateTransactionResult{
				Results: []entities.RPCSimulateHostFunctionResult{{XDR: wrongTypeScVal}},
			}, nil,
		).Once()

		service, err := NewContractMetadataService(mockRPCService, mockContractModel, pool)
		require.NoError(t, err)

		cms := service.(*contractMetadataService)
		_, err = cms.fetchMetadata(ctx, contractID, types.ContractTypeSEP41)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not a uint32")
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

func TestFetchBatch(t *testing.T) {
	ctx := context.Background()

	t.Run("processes contracts in batches", func(t *testing.T) {
		mockRPCService := NewRPCServiceMock(t)
		mockContractModel := data.NewContractModelMock(t)
		pool := pond.NewPool(10)
		defer pool.Stop()

		contractID1 := "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"
		contractID2 := "CA7QYNF7SOWQ3GLR2BGMZEHXAVIRZA4KVWLTJJFC7MGXUA74P7UJUWDA"

		metadataMap := map[string]ContractMetadata{
			contractID1: {ContractID: contractID1, Type: types.ContractTypeSAC},
			contractID2: {ContractID: contractID2, Type: types.ContractTypeSEP41},
		}
		contractIDs := []string{contractID1, contractID2}

		// Mock successful responses - use MatchedBy to return different values based on the function name
		nameScVal := xdr.ScVal{Type: xdr.ScValTypeScvString, Str: ptrToScString("Token1")}
		symbolScVal := xdr.ScVal{Type: xdr.ScValTypeScvString, Str: ptrToScString("TK1")}
		decimalsScVal := xdr.ScVal{Type: xdr.ScValTypeScvU32, U32: ptrToXdrUint32(6)}

		mockRPCService.On("SimulateTransaction", mock.MatchedBy(func(txXDR string) bool {
			return containsFunction(txXDR, "name")
		}), mock.Anything).Return(
			entities.RPCSimulateTransactionResult{
				Results: []entities.RPCSimulateHostFunctionResult{{XDR: nameScVal}},
			}, nil,
		)
		mockRPCService.On("SimulateTransaction", mock.MatchedBy(func(txXDR string) bool {
			return containsFunction(txXDR, "symbol")
		}), mock.Anything).Return(
			entities.RPCSimulateTransactionResult{
				Results: []entities.RPCSimulateHostFunctionResult{{XDR: symbolScVal}},
			}, nil,
		)
		mockRPCService.On("SimulateTransaction", mock.MatchedBy(func(txXDR string) bool {
			return containsFunction(txXDR, "decimals")
		}), mock.Anything).Return(
			entities.RPCSimulateTransactionResult{
				Results: []entities.RPCSimulateHostFunctionResult{{XDR: decimalsScVal}},
			}, nil,
		)

		service, err := NewContractMetadataService(mockRPCService, mockContractModel, pool)
		require.NoError(t, err)

		cms := service.(*contractMetadataService)
		result := cms.fetchBatch(ctx, metadataMap, contractIDs)

		// Verify both contracts have metadata
		assert.Equal(t, 2, len(result))
		assert.Equal(t, "Token1", result[contractID1].Name)
		assert.Equal(t, "TK1", result[contractID1].Symbol)
		assert.Equal(t, uint32(6), result[contractID1].Decimals)
	})

	t.Run("handles partial failures gracefully", func(t *testing.T) {
		mockRPCService := NewRPCServiceMock(t)
		mockContractModel := data.NewContractModelMock(t)
		pool := pond.NewPool(10)
		defer pool.Stop()

		contractID1 := "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"

		metadataMap := map[string]ContractMetadata{
			contractID1: {ContractID: contractID1, Type: types.ContractTypeSAC},
		}
		contractIDs := []string{contractID1}

		// All RPC calls fail
		mockRPCService.On("SimulateTransaction", mock.Anything, mock.Anything).Return(
			entities.RPCSimulateTransactionResult{}, errors.New("RPC failed"),
		)

		service, err := NewContractMetadataService(mockRPCService, mockContractModel, pool)
		require.NoError(t, err)

		cms := service.(*contractMetadataService)
		result := cms.fetchBatch(ctx, metadataMap, contractIDs)

		// Contract should still be in map with original values
		assert.Equal(t, 1, len(result))
		assert.Equal(t, "", result[contractID1].Name)
		assert.Equal(t, "", result[contractID1].Symbol)
		assert.Equal(t, uint32(0), result[contractID1].Decimals)
	})
}
