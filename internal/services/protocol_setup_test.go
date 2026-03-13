package services

import (
	"context"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/indexer/types"
)

func TestProtocolSetupService_Run(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()

	// Set up test data
	testProtocolID := "TEST_PROTO"
	testWasmHash := xdr.Hash{1, 2, 3, 4, 5}
	testWasmHex := hex.EncodeToString(testWasmHash[:])
	testWasmCode := []byte("fake-wasm-bytecode")
	testSpecEntries := []xdr.ScSpecEntry{
		{Kind: xdr.ScSpecEntryKindScSpecEntryFunctionV0},
	}

	// Helper to build a mock RPC response for a given wasm hash and code
	buildRPCResponse := func(wasmHash xdr.Hash, wasmCode []byte) entities.RPCGetLedgerEntriesResult {
		codeEntry := xdr.ContractCodeEntry{Hash: wasmHash, Code: wasmCode}
		entryData := xdr.LedgerEntryData{Type: xdr.LedgerEntryTypeContractCode, ContractCode: &codeEntry}
		dataXDR, _ := xdr.MarshalBase64(entryData)
		return entities.RPCGetLedgerEntriesResult{
			Entries: []entities.LedgerEntryResult{{DataXDR: dataXDR}},
		}
	}

	t.Run("successfully classifies WASMs", func(t *testing.T) {
		rpcServiceMock := NewRPCServiceMock(t)
		protocolModelMock := data.NewProtocolsModelMock(t)
		protocolWasmModelMock := data.NewProtocolWasmsModelMock(t)
		specExtractorMock := NewWasmSpecExtractorMock(t)
		validatorMock := NewProtocolValidatorMock(t)

		validatorMock.On("ProtocolID").Return(testProtocolID)
		validatorMock.On("Validate", testSpecEntries).Return(true)

		// Validate protocol exists in DB
		protocolModelMock.On("GetByIDs", ctx, []string{testProtocolID}).Return([]data.Protocols{{ID: testProtocolID}}, nil)

		// Set status to in_progress
		protocolModelMock.On("UpdateClassificationStatus", ctx, mock.Anything, []string{testProtocolID}, data.StatusInProgress).Return(nil)

		// GetUnclassified returns 1 hash
		protocolWasmModelMock.On("GetUnclassified", ctx).Return([]data.ProtocolWasms{
			{WasmHash: types.HashBytea(testWasmHex)},
		}, nil)

		// RPC returns bytecode
		rpcServiceMock.On("GetLedgerEntries", mock.Anything).Return(buildRPCResponse(testWasmHash, testWasmCode), nil)

		// Extract spec
		specExtractorMock.On("ExtractSpec", ctx, testWasmCode).Return(testSpecEntries, nil)

		// Persist results
		protocolWasmModelMock.On(
			"BatchUpdateProtocolID",
			ctx,
			mock.Anything,
			[]types.HashBytea{types.HashBytea(testWasmHex)},
			testProtocolID,
		).Return(nil)

		// Set status to success
		protocolModelMock.On("UpdateClassificationStatus", ctx, mock.Anything, []string{testProtocolID}, data.StatusSuccess).Return(nil)

		specExtractorMock.On("Close", ctx).Return(nil)

		svc := &protocolSetupService{
			db:                dbConnectionPool,
			rpcService:        rpcServiceMock,
			protocolModel:     protocolModelMock,
			protocolWasmModel: protocolWasmModelMock,
			specExtractor:     specExtractorMock,
			validators:        []ProtocolValidator{validatorMock},
		}

		err := svc.Run(ctx, []string{testProtocolID})
		require.NoError(t, err)
	})

	t.Run("sets status to failed on RPC error", func(t *testing.T) {
		rpcServiceMock := NewRPCServiceMock(t)
		protocolModelMock := data.NewProtocolsModelMock(t)
		protocolWasmModelMock := data.NewProtocolWasmsModelMock(t)
		specExtractorMock := NewWasmSpecExtractorMock(t)
		validatorMock := NewProtocolValidatorMock(t)

		validatorMock.On("ProtocolID").Return(testProtocolID)

		// Validate protocol exists in DB
		protocolModelMock.On("GetByIDs", ctx, []string{testProtocolID}).Return([]data.Protocols{{ID: testProtocolID}}, nil)

		// Set status to in_progress
		protocolModelMock.On("UpdateClassificationStatus", ctx, mock.Anything, []string{testProtocolID}, data.StatusInProgress).Return(nil)

		// GetUnclassified returns 1 hash
		protocolWasmModelMock.On("GetUnclassified", ctx).Return([]data.ProtocolWasms{
			{WasmHash: types.HashBytea(testWasmHex)},
		}, nil)

		// RPC fails
		rpcServiceMock.On("GetLedgerEntries", mock.Anything).Return(entities.RPCGetLedgerEntriesResult{}, errors.New("rpc unavailable"))

		// Set status to failed (uses a fresh context.WithTimeout, so match any context)
		protocolModelMock.On("UpdateClassificationStatus", mock.Anything, mock.Anything, []string{testProtocolID}, data.StatusFailed).Return(nil)

		specExtractorMock.On("Close", ctx).Return(nil)

		svc := &protocolSetupService{
			db:                dbConnectionPool,
			rpcService:        rpcServiceMock,
			protocolModel:     protocolModelMock,
			protocolWasmModel: protocolWasmModelMock,
			specExtractor:     specExtractorMock,
			validators:        []ProtocolValidator{validatorMock},
		}

		err := svc.Run(ctx, []string{testProtocolID})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "rpc unavailable")
	})

	t.Run("returns error when no validators provided", func(t *testing.T) {
		svc := &protocolSetupService{
			db:         dbConnectionPool,
			validators: nil,
		}

		err := svc.Run(ctx, []string{"UNKNOWN"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no protocol validators provided")
	})

	t.Run("returns error for unmatched protocol ID", func(t *testing.T) {
		validatorMock := NewProtocolValidatorMock(t)
		validatorMock.On("ProtocolID").Return(testProtocolID)

		svc := &protocolSetupService{
			db:         dbConnectionPool,
			validators: []ProtocolValidator{validatorMock},
		}

		err := svc.Run(ctx, []string{"NONEXISTENT"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no validator found for protocol")
	})

	t.Run("no unclassified WASMs skips RPC", func(t *testing.T) {
		rpcServiceMock := NewRPCServiceMock(t)
		protocolModelMock := data.NewProtocolsModelMock(t)
		protocolWasmModelMock := data.NewProtocolWasmsModelMock(t)
		specExtractorMock := NewWasmSpecExtractorMock(t)
		validatorMock := NewProtocolValidatorMock(t)

		validatorMock.On("ProtocolID").Return(testProtocolID)

		protocolModelMock.On("GetByIDs", ctx, []string{testProtocolID}).Return([]data.Protocols{{ID: testProtocolID}}, nil)
		protocolModelMock.On("UpdateClassificationStatus", ctx, mock.Anything, []string{testProtocolID}, data.StatusInProgress).Return(nil)

		// No unclassified WASMs
		protocolWasmModelMock.On("GetUnclassified", ctx).Return([]data.ProtocolWasms{}, nil)

		// No RPC calls expected (rpcServiceMock has no expectations set for GetLedgerEntries)

		protocolModelMock.On("UpdateClassificationStatus", ctx, mock.Anything, []string{testProtocolID}, data.StatusSuccess).Return(nil)
		specExtractorMock.On("Close", ctx).Return(nil)

		svc := &protocolSetupService{
			db:                dbConnectionPool,
			rpcService:        rpcServiceMock,
			protocolModel:     protocolModelMock,
			protocolWasmModel: protocolWasmModelMock,
			specExtractor:     specExtractorMock,
			validators:        []ProtocolValidator{validatorMock},
		}

		err := svc.Run(ctx, []string{testProtocolID})
		require.NoError(t, err)
	})

	t.Run("WASM expired/evicted from ledger is skipped", func(t *testing.T) {
		rpcServiceMock := NewRPCServiceMock(t)
		protocolModelMock := data.NewProtocolsModelMock(t)
		protocolWasmModelMock := data.NewProtocolWasmsModelMock(t)
		specExtractorMock := NewWasmSpecExtractorMock(t)
		validatorMock := NewProtocolValidatorMock(t)

		validatorMock.On("ProtocolID").Return(testProtocolID)

		protocolModelMock.On("GetByIDs", ctx, []string{testProtocolID}).Return([]data.Protocols{{ID: testProtocolID}}, nil)
		protocolModelMock.On("UpdateClassificationStatus", ctx, mock.Anything, []string{testProtocolID}, data.StatusInProgress).Return(nil)

		// GetUnclassified returns 1 hash
		protocolWasmModelMock.On("GetUnclassified", ctx).Return([]data.ProtocolWasms{
			{WasmHash: types.HashBytea(testWasmHex)},
		}, nil)

		// RPC returns empty entries (WASM expired/evicted)
		rpcServiceMock.On("GetLedgerEntries", mock.Anything).Return(entities.RPCGetLedgerEntriesResult{
			Entries: []entities.LedgerEntryResult{},
		}, nil)

		// No classification, no BatchUpdateProtocolID expected

		protocolModelMock.On("UpdateClassificationStatus", ctx, mock.Anything, []string{testProtocolID}, data.StatusSuccess).Return(nil)
		specExtractorMock.On("Close", ctx).Return(nil)

		svc := &protocolSetupService{
			db:                dbConnectionPool,
			rpcService:        rpcServiceMock,
			protocolModel:     protocolModelMock,
			protocolWasmModel: protocolWasmModelMock,
			specExtractor:     specExtractorMock,
			validators:        []ProtocolValidator{validatorMock},
		}

		err := svc.Run(ctx, []string{testProtocolID})
		require.NoError(t, err)
	})

	t.Run("WASM that doesn't match any validator is not classified", func(t *testing.T) {
		rpcServiceMock := NewRPCServiceMock(t)
		protocolModelMock := data.NewProtocolsModelMock(t)
		protocolWasmModelMock := data.NewProtocolWasmsModelMock(t)
		specExtractorMock := NewWasmSpecExtractorMock(t)
		validatorMock := NewProtocolValidatorMock(t)

		validatorMock.On("ProtocolID").Return(testProtocolID)
		validatorMock.On("Validate", testSpecEntries).Return(false)

		protocolModelMock.On("GetByIDs", ctx, []string{testProtocolID}).Return([]data.Protocols{{ID: testProtocolID}}, nil)
		protocolModelMock.On("UpdateClassificationStatus", ctx, mock.Anything, []string{testProtocolID}, data.StatusInProgress).Return(nil)

		// GetUnclassified returns 1 hash
		protocolWasmModelMock.On("GetUnclassified", ctx).Return([]data.ProtocolWasms{
			{WasmHash: types.HashBytea(testWasmHex)},
		}, nil)

		// RPC returns bytecode
		rpcServiceMock.On("GetLedgerEntries", mock.Anything).Return(buildRPCResponse(testWasmHash, testWasmCode), nil)

		// Extract spec
		specExtractorMock.On("ExtractSpec", ctx, testWasmCode).Return(testSpecEntries, nil)

		// Validator returns false → no BatchUpdateProtocolID expected

		protocolModelMock.On("UpdateClassificationStatus", ctx, mock.Anything, []string{testProtocolID}, data.StatusSuccess).Return(nil)
		specExtractorMock.On("Close", ctx).Return(nil)

		svc := &protocolSetupService{
			db:                dbConnectionPool,
			rpcService:        rpcServiceMock,
			protocolModel:     protocolModelMock,
			protocolWasmModel: protocolWasmModelMock,
			specExtractor:     specExtractorMock,
			validators:        []ProtocolValidator{validatorMock},
		}

		err := svc.Run(ctx, []string{testProtocolID})
		require.NoError(t, err)
	})

	t.Run("batches RPC calls", func(t *testing.T) {
		rpcServiceMock := NewRPCServiceMock(t)
		protocolModelMock := data.NewProtocolsModelMock(t)
		protocolWasmModelMock := data.NewProtocolWasmsModelMock(t)
		specExtractorMock := NewWasmSpecExtractorMock(t)
		validatorMock := NewProtocolValidatorMock(t)

		validatorMock.On("ProtocolID").Return(testProtocolID)

		protocolModelMock.On("GetByIDs", ctx, []string{testProtocolID}).Return([]data.Protocols{{ID: testProtocolID}}, nil)
		protocolModelMock.On("UpdateClassificationStatus", ctx, mock.Anything, []string{testProtocolID}, data.StatusInProgress).Return(nil)

		// Build 201 unclassified hashes
		numHashes := rpcLedgerEntryBatchSize + 1
		unclassifiedWasms := make([]data.ProtocolWasms, numHashes)
		for i := 0; i < numHashes; i++ {
			hash := xdr.Hash{}
			hash[0] = byte(i >> 8)
			hash[1] = byte(i)
			unclassifiedWasms[i] = data.ProtocolWasms{WasmHash: types.HashBytea(hex.EncodeToString(hash[:]))}
		}
		protocolWasmModelMock.On("GetUnclassified", ctx).Return(unclassifiedWasms, nil)

		// RPC should be called twice: once with 200 keys, once with 1 key
		rpcServiceMock.On("GetLedgerEntries", mock.MatchedBy(func(keys []string) bool {
			return len(keys) == rpcLedgerEntryBatchSize
		})).Return(entities.RPCGetLedgerEntriesResult{Entries: []entities.LedgerEntryResult{}}, nil).Once()

		rpcServiceMock.On("GetLedgerEntries", mock.MatchedBy(func(keys []string) bool {
			return len(keys) == 1
		})).Return(entities.RPCGetLedgerEntriesResult{Entries: []entities.LedgerEntryResult{}}, nil).Once()

		// No bytecodes returned → no classification

		protocolModelMock.On("UpdateClassificationStatus", ctx, mock.Anything, []string{testProtocolID}, data.StatusSuccess).Return(nil)
		specExtractorMock.On("Close", ctx).Return(nil)

		svc := &protocolSetupService{
			db:                dbConnectionPool,
			rpcService:        rpcServiceMock,
			protocolModel:     protocolModelMock,
			protocolWasmModel: protocolWasmModelMock,
			specExtractor:     specExtractorMock,
			validators:        []ProtocolValidator{validatorMock},
		}

		err := svc.Run(ctx, []string{testProtocolID})
		require.NoError(t, err)
	})
}
