// Package services provides business logic for the wallet-backend.
// This file contains tests for ContractMetadataService.
package services

import (
	"context"
	"testing"

	"github.com/alitto/pond/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer/types"
)

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
		assert.NoError(t, err)

		err = service.FetchAndStoreMetadata(ctx, map[string]types.ContractType{})
		assert.NoError(t, err)
	})

	t.Run("stores contracts in database", func(t *testing.T) {
		mockRPCService := NewRPCServiceMock(t)
		mockContractModel := data.NewContractModelMock(t)
		pool := pond.NewPool(2)
		defer pool.Stop()

		contractTypes := map[string]types.ContractType{
			"CAAAA": types.ContractTypeSAC,
		}

		// Expect batch insert to be called - metadata will be empty since RPC is not mocked
		mockContractModel.On("BatchInsert", ctx, mock.Anything, mock.MatchedBy(func(contracts []*data.Contract) bool {
			return len(contracts) == 1 && contracts[0].ID == "CAAAA" && contracts[0].Type == string(types.ContractTypeSAC)
		})).Return([]string{"CAAAA"}, nil)

		service, err := NewContractMetadataService(mockRPCService, mockContractModel, pool)
		assert.NoError(t, err)

		err = service.FetchAndStoreMetadata(ctx, contractTypes)
		assert.NoError(t, err)
	})
}
