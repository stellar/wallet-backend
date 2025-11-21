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

func TestExtractUniqueContractIDs(t *testing.T) {
	mockRPCService := NewRPCServiceMock(t)
	mockContractModel := data.NewContractModelMock(t)
	pool := pond.NewPool(0)
	defer pool.Stop()

	service, err := NewContractMetadataService(mockRPCService, mockContractModel, pool)
	assert.NoError(t, err)

	// Access internal method via type assertion
	cms := service.(*contractMetadataService)

	tests := []struct {
		name     string
		changes  []types.ContractChange
		expected []string
	}{
		{
			name:     "empty changes",
			changes:  []types.ContractChange{},
			expected: nil,
		},
		{
			name: "filters out non-SAC/SEP41 contracts",
			changes: []types.ContractChange{
				{ContractID: "CAAAA", ContractType: types.ContractTypeSAC},
				{ContractID: "CBBBB", ContractType: types.ContractTypeSEP41},
				{ContractID: "CCCCC", ContractType: types.ContractTypeNative},
				{ContractID: "CDDDD", ContractType: types.ContractTypeUnknown},
			},
			expected: []string{"CAAAA", "CBBBB"},
		},
		{
			name: "deduplicates contract IDs",
			changes: []types.ContractChange{
				{ContractID: "CAAAA", ContractType: types.ContractTypeSAC},
				{ContractID: "CAAAA", ContractType: types.ContractTypeSAC},
				{ContractID: "CBBBB", ContractType: types.ContractTypeSEP41},
			},
			expected: []string{"CAAAA", "CBBBB"},
		},
		{
			name: "skips empty contract IDs",
			changes: []types.ContractChange{
				{ContractID: "", ContractType: types.ContractTypeSAC},
				{ContractID: "CAAAA", ContractType: types.ContractTypeSAC},
			},
			expected: []string{"CAAAA"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := cms.extractUniqueContractIDs(tt.changes)
			assert.ElementsMatch(t, tt.expected, result)
		})
	}
}

func TestFilterNewContracts(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name                string
		contractIDs         []string
		contractChanges     []types.ContractChange
		existingContracts   []*data.Contract
		dbError             error
		expectedNewIDs      []string
		expectedTypeMapKeys []string
		wantErr             bool
	}{
		{
			name:        "all contracts are new",
			contractIDs: []string{"CAAAA", "CBBBB"},
			contractChanges: []types.ContractChange{
				{ContractID: "CAAAA", ContractType: types.ContractTypeSAC},
				{ContractID: "CBBBB", ContractType: types.ContractTypeSEP41},
			},
			existingContracts:   []*data.Contract{},
			expectedNewIDs:      []string{"CAAAA", "CBBBB"},
			expectedTypeMapKeys: []string{"CAAAA", "CBBBB"},
			wantErr:             false,
		},
		{
			name:        "some contracts exist",
			contractIDs: []string{"CAAAA", "CBBBB"},
			contractChanges: []types.ContractChange{
				{ContractID: "CAAAA", ContractType: types.ContractTypeSAC},
				{ContractID: "CBBBB", ContractType: types.ContractTypeSEP41},
			},
			existingContracts:   []*data.Contract{{ID: "CAAAA"}},
			expectedNewIDs:      []string{"CBBBB"},
			expectedTypeMapKeys: []string{"CAAAA", "CBBBB"},
			wantErr:             false,
		},
		{
			name:        "all contracts exist",
			contractIDs: []string{"CAAAA", "CBBBB"},
			contractChanges: []types.ContractChange{
				{ContractID: "CAAAA", ContractType: types.ContractTypeSAC},
				{ContractID: "CBBBB", ContractType: types.ContractTypeSEP41},
			},
			existingContracts:   []*data.Contract{{ID: "CAAAA"}, {ID: "CBBBB"}},
			expectedNewIDs:      nil,
			expectedTypeMapKeys: []string{"CAAAA", "CBBBB"},
			wantErr:             false,
		},
		{
			name:        "database error",
			contractIDs: []string{"CAAAA"},
			contractChanges: []types.ContractChange{
				{ContractID: "CAAAA", ContractType: types.ContractTypeSAC},
			},
			dbError: assert.AnError,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockRPCService := NewRPCServiceMock(t)
			mockContractModel := data.NewContractModelMock(t)
			pool := pond.NewPool(0)
			defer pool.Stop()

			if tt.dbError != nil {
				mockContractModel.On("BatchGetByIDs", ctx, tt.contractIDs).Return(nil, tt.dbError)
			} else {
				mockContractModel.On("BatchGetByIDs", ctx, tt.contractIDs).Return(tt.existingContracts, nil)
			}

			service, err := NewContractMetadataService(mockRPCService, mockContractModel, pool)
			assert.NoError(t, err)

			cms := service.(*contractMetadataService)
			newIDs, typeMap, err := cms.filterNewContracts(ctx, tt.contractIDs, tt.contractChanges)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.ElementsMatch(t, tt.expectedNewIDs, newIDs)
			for _, key := range tt.expectedTypeMapKeys {
				assert.Contains(t, typeMap, key)
			}
		})
	}
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

func TestFetchAndStoreForChanges(t *testing.T) {
	ctx := context.Background()

	t.Run("returns nil for empty changes", func(t *testing.T) {
		mockRPCService := NewRPCServiceMock(t)
		mockContractModel := data.NewContractModelMock(t)
		pool := pond.NewPool(0)
		defer pool.Stop()

		service, err := NewContractMetadataService(mockRPCService, mockContractModel, pool)
		assert.NoError(t, err)

		err = service.FetchAndStoreForChanges(ctx, []types.ContractChange{})
		assert.NoError(t, err)
	})

	t.Run("returns nil when no SAC/SEP41 contracts", func(t *testing.T) {
		mockRPCService := NewRPCServiceMock(t)
		mockContractModel := data.NewContractModelMock(t)
		pool := pond.NewPool(0)
		defer pool.Stop()

		service, err := NewContractMetadataService(mockRPCService, mockContractModel, pool)
		assert.NoError(t, err)

		changes := []types.ContractChange{
			{ContractID: "CAAAA", ContractType: types.ContractTypeNative},
			{ContractID: "CBBBB", ContractType: types.ContractTypeUnknown},
		}
		err = service.FetchAndStoreForChanges(ctx, changes)
		assert.NoError(t, err)
		// No mock calls expected since no SAC/SEP41 contracts
	})

	t.Run("returns nil when all contracts exist", func(t *testing.T) {
		mockRPCService := NewRPCServiceMock(t)
		mockContractModel := data.NewContractModelMock(t)
		pool := pond.NewPool(0)
		defer pool.Stop()

		changes := []types.ContractChange{
			{ContractID: "CAAAA", ContractType: types.ContractTypeSAC},
		}

		// All contracts already exist
		mockContractModel.On("BatchGetByIDs", ctx, []string{"CAAAA"}).Return([]*data.Contract{{ID: "CAAAA"}}, nil)

		service, err := NewContractMetadataService(mockRPCService, mockContractModel, pool)
		assert.NoError(t, err)

		err = service.FetchAndStoreForChanges(ctx, changes)
		assert.NoError(t, err)
	})

	t.Run("handles database error gracefully", func(t *testing.T) {
		mockRPCService := NewRPCServiceMock(t)
		mockContractModel := data.NewContractModelMock(t)
		pool := pond.NewPool(0)
		defer pool.Stop()

		changes := []types.ContractChange{
			{ContractID: "CAAAA", ContractType: types.ContractTypeSAC},
		}

		mockContractModel.On("BatchGetByIDs", ctx, []string{"CAAAA"}).Return(nil, assert.AnError)

		service, err := NewContractMetadataService(mockRPCService, mockContractModel, pool)
		assert.NoError(t, err)

		// Should not return error - just logs warning
		err = service.FetchAndStoreForChanges(ctx, changes)
		assert.NoError(t, err)
	})

	t.Run("stores metadata for new contracts with pool", func(t *testing.T) {
		mockRPCService := NewRPCServiceMock(t)
		mockContractModel := data.NewContractModelMock(t)
		pool := pond.NewPool(2)
		defer pool.Stop()

		changes := []types.ContractChange{
			{ContractID: "CAAAA", ContractType: types.ContractTypeSEP41},
		}

		// Contract doesn't exist
		mockContractModel.On("BatchGetByIDs", ctx, []string{"CAAAA"}).Return([]*data.Contract{}, nil)

		// Expect batch insert to be called - metadata will be empty since RPC is not mocked
		mockContractModel.On("BatchInsert", ctx, mock.Anything, mock.MatchedBy(func(contracts []*data.Contract) bool {
			return len(contracts) == 1 && contracts[0].ID == "CAAAA" && contracts[0].Type == string(types.ContractTypeSEP41)
		})).Return([]string{"CAAAA"}, nil)

		service, err := NewContractMetadataService(mockRPCService, mockContractModel, pool)
		assert.NoError(t, err)

		err = service.FetchAndStoreForChanges(ctx, changes)
		assert.NoError(t, err)
	})
}

func TestFetchAndStoreForContracts(t *testing.T) {
	ctx := context.Background()

	t.Run("returns nil for empty contracts", func(t *testing.T) {
		mockRPCService := NewRPCServiceMock(t)
		mockContractModel := data.NewContractModelMock(t)
		pool := pond.NewPool(0)
		defer pool.Stop()

		service, err := NewContractMetadataService(mockRPCService, mockContractModel, pool)
		assert.NoError(t, err)

		err = service.FetchAndStoreForContracts(ctx, map[string]types.ContractType{})
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

		err = service.FetchAndStoreForContracts(ctx, contractTypes)
		assert.NoError(t, err)
	})
}
