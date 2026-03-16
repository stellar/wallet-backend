package integrationtests

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/jackc/pgx/v5"
	_ "github.com/lib/pq"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/integrationtests/infrastructure"
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

// loadTestWasmSuite reads a WASM file from testdata for use in suite tests.
func loadTestWasmSuite(s *suite.Suite, filename string) ([]byte, xdr.Hash, string) {
	wasmBytes, err := os.ReadFile(filepath.Join(testdataDir(), filename))
	s.Require().NoError(err, "reading test WASM file %s", filename)

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

// --- Standalone tests (no Docker DB required) ---

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

// --- ProtocolSetupTestSuite (requires Docker DB via integration test infra) ---

type ProtocolSetupTestSuite struct {
	suite.Suite
	testEnv *infrastructure.TestEnvironment
}

func (s *ProtocolSetupTestSuite) setupDB() (db.ConnectionPool, func()) {
	ctx := context.Background()
	dbURL, err := s.testEnv.Containers.GetWalletDBConnectionString(ctx)
	s.Require().NoError(err)
	pool, err := db.OpenDBConnectionPool(dbURL)
	s.Require().NoError(err)
	return pool, func() { pool.Close() }
}

func (s *ProtocolSetupTestSuite) setupModels(pool db.ConnectionPool) (*data.ProtocolsModel, *data.ProtocolWasmsModel) {
	mockMetrics := metrics.NewMockMetricsService()
	mockMetrics.On("ObserveDBQueryDuration", mock.Anything, mock.Anything, mock.Anything).Return()
	mockMetrics.On("ObserveDBBatchSize", mock.Anything, mock.Anything, mock.Anything).Return()
	mockMetrics.On("IncDBQuery", mock.Anything, mock.Anything).Return()
	mockMetrics.On("IncDBQueryError", mock.Anything, mock.Anything, mock.Anything).Return()
	return &data.ProtocolsModel{DB: pool, MetricsService: mockMetrics},
		&data.ProtocolWasmsModel{DB: pool, MetricsService: mockMetrics}
}

// SetupTest cleans protocol tables before each test to avoid conflicts with the shared DB.
func (s *ProtocolSetupTestSuite) SetupTest() {
	ctx := context.Background()
	dbURL, err := s.testEnv.Containers.GetWalletDBConnectionString(ctx)
	s.Require().NoError(err)

	sqlDB, err := sql.Open("postgres", dbURL)
	s.Require().NoError(err)
	defer sqlDB.Close()

	_, err = sqlDB.ExecContext(ctx, "TRUNCATE TABLE protocol_wasms CASCADE")
	s.Require().NoError(err)
	_, err = sqlDB.ExecContext(ctx, "DELETE FROM protocols WHERE id IN ('SEP41')")
	s.Require().NoError(err)
}

func (s *ProtocolSetupTestSuite) TestClassifiesTokenWasmAsSEP41() {
	ctx := context.Background()
	pool, cleanup := s.setupDB()
	defer cleanup()
	protocolModel, protocolWasmModel := s.setupModels(pool)

	tokenBytes, tokenHash, tokenHex := loadTestWasmSuite(&s.Suite, "soroban_token_contract.wasm")

	err := db.RunInPgxTransaction(ctx, pool, func(dbTx pgx.Tx) error {
		if err := protocolModel.InsertIfNotExists(ctx, dbTx, "SEP41"); err != nil {
			return err
		}
		return protocolWasmModel.BatchInsert(ctx, dbTx, []data.ProtocolWasms{
			{WasmHash: types.HashBytea(tokenHex)},
		})
	})
	s.Require().NoError(err)

	rpcMock := services.NewRPCServiceMock(s.T())
	rpcMock.On("GetLedgerEntries", mock.Anything).Return(buildRPCResponse(tokenHash, tokenBytes), nil)

	specExtractor := services.NewWasmSpecExtractor()
	validator := services.NewSEP41ProtocolValidator()

	svc := services.NewProtocolSetupService(
		pool, rpcMock, protocolModel, protocolWasmModel,
		specExtractor, []services.ProtocolValidator{validator},
	)
	s.Require().NoError(svc.Run(ctx, []string{"SEP41"}))

	wasms, err := protocolWasmModel.GetUnclassified(ctx)
	s.Require().NoError(err)
	s.Assert().Empty(wasms, "token WASM should no longer be unclassified")

	protocols, err := protocolModel.GetByIDs(ctx, []string{"SEP41"})
	s.Require().NoError(err)
	s.Require().Len(protocols, 1)
	s.Assert().Equal(data.StatusSuccess, protocols[0].ClassificationStatus)
}

func (s *ProtocolSetupTestSuite) TestNonSEP41StaysUnclassified() {
	ctx := context.Background()
	pool, cleanup := s.setupDB()
	defer cleanup()
	protocolModel, protocolWasmModel := s.setupModels(pool)

	incrBytes, incrHash, incrHex := loadTestWasmSuite(&s.Suite, "soroban_increment_contract.wasm")

	err := db.RunInPgxTransaction(ctx, pool, func(dbTx pgx.Tx) error {
		if err := protocolModel.InsertIfNotExists(ctx, dbTx, "SEP41"); err != nil {
			return err
		}
		return protocolWasmModel.BatchInsert(ctx, dbTx, []data.ProtocolWasms{
			{WasmHash: types.HashBytea(incrHex)},
		})
	})
	s.Require().NoError(err)

	rpcMock := services.NewRPCServiceMock(s.T())
	rpcMock.On("GetLedgerEntries", mock.Anything).Return(buildRPCResponse(incrHash, incrBytes), nil)

	specExtractor := services.NewWasmSpecExtractor()
	validator := services.NewSEP41ProtocolValidator()

	svc := services.NewProtocolSetupService(
		pool, rpcMock, protocolModel, protocolWasmModel,
		specExtractor, []services.ProtocolValidator{validator},
	)
	s.Require().NoError(svc.Run(ctx, []string{"SEP41"}))

	wasms, err := protocolWasmModel.GetUnclassified(ctx)
	s.Require().NoError(err)
	s.Require().Len(wasms, 1)
	s.Assert().Equal(types.HashBytea(incrHex), wasms[0].WasmHash)
	s.Assert().Nil(wasms[0].ProtocolID)

	protocols, err := protocolModel.GetByIDs(ctx, []string{"SEP41"})
	s.Require().NoError(err)
	s.Require().Len(protocols, 1)
	s.Assert().Equal(data.StatusSuccess, protocols[0].ClassificationStatus)
}

func (s *ProtocolSetupTestSuite) TestMixedWasms() {
	ctx := context.Background()
	pool, cleanup := s.setupDB()
	defer cleanup()
	protocolModel, protocolWasmModel := s.setupModels(pool)

	tokenBytes, tokenHash, tokenHex := loadTestWasmSuite(&s.Suite, "soroban_token_contract.wasm")
	incrBytes, incrHash, incrHex := loadTestWasmSuite(&s.Suite, "soroban_increment_contract.wasm")

	err := db.RunInPgxTransaction(ctx, pool, func(dbTx pgx.Tx) error {
		if err := protocolModel.InsertIfNotExists(ctx, dbTx, "SEP41"); err != nil {
			return err
		}
		return protocolWasmModel.BatchInsert(ctx, dbTx, []data.ProtocolWasms{
			{WasmHash: types.HashBytea(tokenHex)},
			{WasmHash: types.HashBytea(incrHex)},
		})
	})
	s.Require().NoError(err)

	rpcMock := services.NewRPCServiceMock(s.T())
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
		pool, rpcMock, protocolModel, protocolWasmModel,
		specExtractor, []services.ProtocolValidator{validator},
	)
	s.Require().NoError(svc.Run(ctx, []string{"SEP41"}))

	wasms, err := protocolWasmModel.GetUnclassified(ctx)
	s.Require().NoError(err)
	s.Require().Len(wasms, 1)
	s.Assert().Equal(types.HashBytea(incrHex), wasms[0].WasmHash)
	s.Assert().Nil(wasms[0].ProtocolID)

	protocols, err := protocolModel.GetByIDs(ctx, []string{"SEP41"})
	s.Require().NoError(err)
	s.Require().Len(protocols, 1)
	s.Assert().Equal(data.StatusSuccess, protocols[0].ClassificationStatus)
}
