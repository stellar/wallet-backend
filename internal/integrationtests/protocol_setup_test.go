package integrationtests

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/jackc/pgx/v5"
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
	"github.com/stellar/wallet-backend/internal/services"
)

// testdataDir returns the absolute path to the testdata directory.
func testdataDir() string {
	_, filename, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(filename), "infrastructure", "testdata")
}

// loadTestWasm reads a WASM file from testdata and returns (bytes, xdr.Hash, hexString).
func loadTestWasm(t *testing.T, filename string) ([]byte, xdr.Hash, string) {
	t.Helper()
	wasmBytes, err := os.ReadFile(filepath.Join(testdataDir(), filename))
	require.NoError(t, err, "reading test WASM file %s", filename)

	hashBytes := sha256.Sum256(wasmBytes)
	var xdrHash xdr.Hash
	copy(xdrHash[:], hashBytes[:])
	hexStr := hex.EncodeToString(hashBytes[:])
	return wasmBytes, xdrHash, hexStr
}

// buildRPCResponse builds an RPCGetLedgerEntriesResult wrapping real WASM bytecodes in XDR.
func buildRPCResponse(wasmHash xdr.Hash, wasmCode []byte) entities.RPCGetLedgerEntriesResult {
	codeEntry := xdr.ContractCodeEntry{Hash: wasmHash, Code: wasmCode}
	entryData := xdr.LedgerEntryData{Type: xdr.LedgerEntryTypeContractCode, ContractCode: &codeEntry}
	dataXDR, err := xdr.MarshalBase64(entryData)
	if err != nil {
		panic(fmt.Sprintf("buildRPCResponse: marshal XDR: %v", err))
	}
	return entities.RPCGetLedgerEntriesResult{
		Entries: []entities.LedgerEntryResult{{DataXDR: dataXDR}},
	}
}

// buildMultiRPCResponse builds an RPCGetLedgerEntriesResult containing multiple WASM entries.
func buildMultiRPCResponse(entries []struct {
	hash xdr.Hash
	code []byte
},
) entities.RPCGetLedgerEntriesResult {
	var results []entities.LedgerEntryResult
	for _, e := range entries {
		codeEntry := xdr.ContractCodeEntry{Hash: e.hash, Code: e.code}
		entryData := xdr.LedgerEntryData{Type: xdr.LedgerEntryTypeContractCode, ContractCode: &codeEntry}
		dataXDR, err := xdr.MarshalBase64(entryData)
		if err != nil {
			panic(fmt.Sprintf("buildMultiRPCResponse: marshal XDR: %v", err))
		}
		results = append(results, entities.LedgerEntryResult{DataXDR: dataXDR})
	}
	return entities.RPCGetLedgerEntriesResult{Entries: results}
}

type testDB struct {
	pool              db.ConnectionPool
	protocolModel     *data.ProtocolsModel
	protocolWasmModel *data.ProtocolWasmsModel
}

func setupTestDB(t *testing.T) testDB {
	t.Helper()
	dbt := dbtest.Open(t)
	t.Cleanup(func() { dbt.Close() })

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	t.Cleanup(func() { dbConnectionPool.Close() })

	mockMetrics := metrics.NewMockMetricsService()
	mockMetrics.On("ObserveDBQueryDuration", mock.Anything, mock.Anything, mock.Anything).Return()
	mockMetrics.On("ObserveDBBatchSize", mock.Anything, mock.Anything, mock.Anything).Return()
	mockMetrics.On("IncDBQuery", mock.Anything, mock.Anything).Return()
	mockMetrics.On("IncDBQueryError", mock.Anything, mock.Anything, mock.Anything).Return()

	return testDB{
		pool:              dbConnectionPool,
		protocolModel:     &data.ProtocolsModel{DB: dbConnectionPool, MetricsService: mockMetrics},
		protocolWasmModel: &data.ProtocolWasmsModel{DB: dbConnectionPool, MetricsService: mockMetrics},
	}
}

func TestWasmSpecExtractor_RealWasm(t *testing.T) {
	ctx := context.Background()
	extractor := services.NewWasmSpecExtractor()
	defer func() { require.NoError(t, extractor.Close(ctx)) }()

	t.Run("token contract", func(t *testing.T) {
		wasmBytes, _, _ := loadTestWasm(t, "soroban_token_contract.wasm")
		specs, err := extractor.ExtractSpec(ctx, wasmBytes)
		require.NoError(t, err)
		assert.NotEmpty(t, specs)

		hasFunctionEntry := false
		for _, spec := range specs {
			if spec.Kind == xdr.ScSpecEntryKindScSpecEntryFunctionV0 && spec.FunctionV0 != nil {
				hasFunctionEntry = true
				break
			}
		}
		assert.True(t, hasFunctionEntry, "expected at least one ScSpecEntryFunctionV0 entry")
	})

	t.Run("increment contract", func(t *testing.T) {
		wasmBytes, _, _ := loadTestWasm(t, "soroban_increment_contract.wasm")
		specs, err := extractor.ExtractSpec(ctx, wasmBytes)
		require.NoError(t, err)
		assert.NotEmpty(t, specs)

		hasFunctionEntry := false
		for _, spec := range specs {
			if spec.Kind == xdr.ScSpecEntryKindScSpecEntryFunctionV0 && spec.FunctionV0 != nil {
				hasFunctionEntry = true
				break
			}
		}
		assert.True(t, hasFunctionEntry, "expected at least one ScSpecEntryFunctionV0 entry")
	})
}

func TestSEP41ProtocolValidator_RealWasm(t *testing.T) {
	ctx := context.Background()
	extractor := services.NewWasmSpecExtractor()
	defer func() { require.NoError(t, extractor.Close(ctx)) }()

	validator := services.NewSEP41ProtocolValidator()

	t.Run("token contract validates as SEP-41", func(t *testing.T) {
		wasmBytes, _, _ := loadTestWasm(t, "soroban_token_contract.wasm")
		specs, err := extractor.ExtractSpec(ctx, wasmBytes)
		require.NoError(t, err)

		assert.True(t, validator.Validate(specs), "token contract should validate as SEP-41")
		assert.Equal(t, "SEP41", validator.ProtocolID())
	})

	t.Run("increment contract does not validate as SEP-41", func(t *testing.T) {
		wasmBytes, _, _ := loadTestWasm(t, "soroban_increment_contract.wasm")
		specs, err := extractor.ExtractSpec(ctx, wasmBytes)
		require.NoError(t, err)

		assert.False(t, validator.Validate(specs), "increment contract should not validate as SEP-41")
	})
}

func TestProtocolSetupService_RealPipeline(t *testing.T) {
	ctx := context.Background()

	t.Run("classifies token WASM as SEP-41", func(t *testing.T) {
		tdb := setupTestDB(t)
		tokenBytes, tokenHash, tokenHex := loadTestWasm(t, "soroban_token_contract.wasm")

		// Insert protocol and unclassified WASM row
		err := db.RunInPgxTransaction(ctx, tdb.pool, func(dbTx pgx.Tx) error {
			if err := tdb.protocolModel.InsertIfNotExists(ctx, dbTx, "SEP41"); err != nil {
				return err
			}
			return tdb.protocolWasmModel.BatchInsert(ctx, dbTx, []data.ProtocolWasms{
				{WasmHash: types.HashBytea(tokenHex)},
			})
		})
		require.NoError(t, err)

		// Mock RPC returning real token bytecodes
		rpcMock := services.NewRPCServiceMock(t)
		rpcMock.On("GetLedgerEntries", mock.Anything).Return(buildRPCResponse(tokenHash, tokenBytes), nil)

		specExtractor := services.NewWasmSpecExtractor()
		validator := services.NewSEP41ProtocolValidator()

		svc := services.NewProtocolSetupService(
			tdb.pool, rpcMock, tdb.protocolModel, tdb.protocolWasmModel,
			specExtractor, []services.ProtocolValidator{validator},
		)
		require.NoError(t, svc.Run(ctx, []string{"SEP41"}))

		// Assert WASM is classified as SEP-41
		wasms, err := tdb.protocolWasmModel.GetUnclassified(ctx)
		require.NoError(t, err)
		assert.Empty(t, wasms, "token WASM should no longer be unclassified")

		// Assert protocol classification status is success
		protocols, err := tdb.protocolModel.GetByIDs(ctx, []string{"SEP41"})
		require.NoError(t, err)
		require.Len(t, protocols, 1)
		assert.Equal(t, data.StatusSuccess, protocols[0].ClassificationStatus)
	})

	t.Run("non-SEP-41 stays unclassified", func(t *testing.T) {
		tdb := setupTestDB(t)
		incrBytes, incrHash, incrHex := loadTestWasm(t, "soroban_increment_contract.wasm")

		err := db.RunInPgxTransaction(ctx, tdb.pool, func(dbTx pgx.Tx) error {
			if err := tdb.protocolModel.InsertIfNotExists(ctx, dbTx, "SEP41"); err != nil {
				return err
			}
			return tdb.protocolWasmModel.BatchInsert(ctx, dbTx, []data.ProtocolWasms{
				{WasmHash: types.HashBytea(incrHex)},
			})
		})
		require.NoError(t, err)

		rpcMock := services.NewRPCServiceMock(t)
		rpcMock.On("GetLedgerEntries", mock.Anything).Return(buildRPCResponse(incrHash, incrBytes), nil)

		specExtractor := services.NewWasmSpecExtractor()
		validator := services.NewSEP41ProtocolValidator()

		svc := services.NewProtocolSetupService(
			tdb.pool, rpcMock, tdb.protocolModel, tdb.protocolWasmModel,
			specExtractor, []services.ProtocolValidator{validator},
		)
		require.NoError(t, svc.Run(ctx, []string{"SEP41"}))

		// Assert WASM is still unclassified (protocol_id IS NULL)
		wasms, err := tdb.protocolWasmModel.GetUnclassified(ctx)
		require.NoError(t, err)
		require.Len(t, wasms, 1)
		assert.Equal(t, types.HashBytea(incrHex), wasms[0].WasmHash)
		assert.Nil(t, wasms[0].ProtocolID)

		// Assert protocol classification status is success
		protocols, err := tdb.protocolModel.GetByIDs(ctx, []string{"SEP41"})
		require.NoError(t, err)
		require.Len(t, protocols, 1)
		assert.Equal(t, data.StatusSuccess, protocols[0].ClassificationStatus)
	})

	t.Run("mixed WASMs — token classified, increment not", func(t *testing.T) {
		tdb := setupTestDB(t)
		tokenBytes, tokenHash, tokenHex := loadTestWasm(t, "soroban_token_contract.wasm")
		incrBytes, incrHash, incrHex := loadTestWasm(t, "soroban_increment_contract.wasm")

		err := db.RunInPgxTransaction(ctx, tdb.pool, func(dbTx pgx.Tx) error {
			if err := tdb.protocolModel.InsertIfNotExists(ctx, dbTx, "SEP41"); err != nil {
				return err
			}
			return tdb.protocolWasmModel.BatchInsert(ctx, dbTx, []data.ProtocolWasms{
				{WasmHash: types.HashBytea(tokenHex)},
				{WasmHash: types.HashBytea(incrHex)},
			})
		})
		require.NoError(t, err)

		// Mock RPC returns both WASMs
		rpcMock := services.NewRPCServiceMock(t)
		rpcMock.On("GetLedgerEntries", mock.Anything).Return(buildMultiRPCResponse([]struct {
			hash xdr.Hash
			code []byte
		}{
			{hash: tokenHash, code: tokenBytes},
			{hash: incrHash, code: incrBytes},
		}), nil)

		specExtractor := services.NewWasmSpecExtractor()
		validator := services.NewSEP41ProtocolValidator()

		svc := services.NewProtocolSetupService(
			tdb.pool, rpcMock, tdb.protocolModel, tdb.protocolWasmModel,
			specExtractor, []services.ProtocolValidator{validator},
		)
		require.NoError(t, svc.Run(ctx, []string{"SEP41"}))

		// Only the increment WASM should remain unclassified
		wasms, err := tdb.protocolWasmModel.GetUnclassified(ctx)
		require.NoError(t, err)
		require.Len(t, wasms, 1)
		assert.Equal(t, types.HashBytea(incrHex), wasms[0].WasmHash)
		assert.Nil(t, wasms[0].ProtocolID)

		// Protocol status should be success
		protocols, err := tdb.protocolModel.GetByIDs(ctx, []string{"SEP41"})
		require.NoError(t, err)
		require.Len(t, protocols, 1)
		assert.Equal(t, data.StatusSuccess, protocols[0].ClassificationStatus)
	})
}
