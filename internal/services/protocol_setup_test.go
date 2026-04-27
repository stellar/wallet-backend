package services

import (
	"context"
	"encoding/hex"
	"errors"
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
)

const testProtocolID = "TEST_PROTO"

// stubValidator is a minimal ProtocolValidator for tests. It claims any
// candidate WASM whose hash is in claim, and records the inputs it received.
type stubValidator struct {
	claim       map[types.HashBytea]struct{}
	gotBatch    int
	classifyErr error
}

func newStubValidator(claim ...types.HashBytea) *stubValidator {
	c := &stubValidator{claim: map[types.HashBytea]struct{}{}}
	for _, h := range claim {
		c.claim[h] = struct{}{}
	}
	return c
}

func (s *stubValidator) ProtocolID() string { return testProtocolID }

func (s *stubValidator) Validate(_ context.Context, _ pgx.Tx, input ValidationInput) (ValidationResult, error) {
	s.gotBatch++
	if s.classifyErr != nil {
		return ValidationResult{}, s.classifyErr
	}
	out := ValidationResult{}
	for _, c := range input.Candidates {
		if _, ok := s.claim[c.Hash]; ok {
			out.MatchedWasms = append(out.MatchedWasms, c.Hash)
		}
	}
	return out, nil
}

func TestProtocolSetupService_Run(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(context.Background(), dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()

	testWasmHash := xdr.Hash{1, 2, 3, 4, 5}
	testWasmHex := hex.EncodeToString(testWasmHash[:])
	testWasmCode := []byte("fake-wasm-bytecode")

	buildRPCResponse := func(hash xdr.Hash, code []byte) entities.RPCGetLedgerEntriesResult {
		entryData := xdr.LedgerEntryData{
			Type:         xdr.LedgerEntryTypeContractCode,
			ContractCode: &xdr.ContractCodeEntry{Hash: hash, Code: code},
		}
		dataXDR, marshalErr := xdr.MarshalBase64(entryData)
		require.NoError(t, marshalErr)
		return entities.RPCGetLedgerEntriesResult{
			Entries: []entities.LedgerEntryResult{{DataXDR: dataXDR}},
		}
	}

	models := &data.Models{
		DB: dbConnectionPool,
	}

	t.Run("happy path: classify and stamp protocol_id", func(t *testing.T) {
		rpcMock := NewRPCServiceMock(t)
		protocolModelMock := data.NewProtocolsModelMock(t)
		protocolWasmModelMock := data.NewProtocolWasmsModelMock(t)
		specExtractor := NewWasmSpecExtractorMock(t)

		protocolModelMock.On("GetByIDs", ctx, []string{testProtocolID}).Return([]data.Protocols{{ID: testProtocolID}}, nil)
		protocolModelMock.On("UpdateClassificationStatus", ctx, mock.Anything, []string{testProtocolID}, data.StatusInProgress).Return(nil)
		protocolModelMock.On("UpdateClassificationStatus", ctx, mock.Anything, []string{testProtocolID}, data.StatusSuccess).Return(nil)
		protocolWasmModelMock.On("GetUnclassified", ctx).Return([]data.ProtocolWasms{{WasmHash: types.HashBytea(testWasmHex)}}, nil)
		rpcMock.On("GetLedgerEntries", mock.Anything).Return(buildRPCResponse(testWasmHash, testWasmCode), nil)
		specExtractor.On("ExtractSpec", ctx, testWasmCode).Return([]xdr.ScSpecEntry{{Kind: xdr.ScSpecEntryKindScSpecEntryFunctionV0}}, nil)
		specExtractor.On("Close", ctx).Return(nil)
		protocolWasmModelMock.On(
			"BatchUpdateProtocolID",
			ctx,
			mock.Anything,
			[]types.HashBytea{types.HashBytea(testWasmHex)},
			testProtocolID,
		).Return(nil)

		// The service holds *data.Models (used to feed validators); mock-injected
		// table-specific models override the DB-bound defaults.
		svcModels := *models
		svcModels.Protocols = protocolModelMock
		svcModels.ProtocolWasms = protocolWasmModelMock
		svcModels.ProtocolContracts = data.NewProtocolContractsModelMock(t)
		svc := &protocolSetupService{
			db:                     dbConnectionPool,
			rpcService:             rpcMock,
			models:                 &svcModels,
			protocolModel:          protocolModelMock,
			protocolWasmModel:      protocolWasmModelMock,
			protocolContractsModel: svcModels.ProtocolContracts,
			specExtractor:          specExtractor,
			validators:             []ProtocolValidator{newStubValidator(types.HashBytea(testWasmHex))},
		}

		require.NoError(t, svc.Run(ctx, []string{testProtocolID}))
	})

	t.Run("returns error when no validators provided", func(t *testing.T) {
		svc := &protocolSetupService{db: dbConnectionPool}
		err := svc.Run(ctx, []string{testProtocolID})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no protocol validators provided")
	})

	t.Run("returns error for unmatched protocol ID", func(t *testing.T) {
		protocolModelMock := data.NewProtocolsModelMock(t)
		protocolModelMock.On("GetByIDs", ctx, []string{"NONEXISTENT"}).Return([]data.Protocols{{ID: "NONEXISTENT"}}, nil)
		specExtractor := NewWasmSpecExtractorMock(t)
		specExtractor.On("Close", ctx).Return(nil)
		svc := &protocolSetupService{
			db:            dbConnectionPool,
			protocolModel: protocolModelMock,
			specExtractor: specExtractor,
			validators:    []ProtocolValidator{newStubValidator()},
		}
		err := svc.Run(ctx, []string{"NONEXISTENT"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no validator provided for protocol")
	})

	t.Run("RPC error transitions status to failed", func(t *testing.T) {
		rpcMock := NewRPCServiceMock(t)
		protocolModelMock := data.NewProtocolsModelMock(t)
		protocolWasmModelMock := data.NewProtocolWasmsModelMock(t)
		specExtractor := NewWasmSpecExtractorMock(t)

		protocolModelMock.On("GetByIDs", ctx, []string{testProtocolID}).Return([]data.Protocols{{ID: testProtocolID}}, nil)
		protocolModelMock.On("UpdateClassificationStatus", ctx, mock.Anything, []string{testProtocolID}, data.StatusInProgress).Return(nil)
		protocolWasmModelMock.On("GetUnclassified", ctx).Return([]data.ProtocolWasms{{WasmHash: types.HashBytea(testWasmHex)}}, nil)
		rpcMock.On("GetLedgerEntries", mock.Anything).Return(entities.RPCGetLedgerEntriesResult{}, errors.New("rpc unavailable"))
		protocolModelMock.On("UpdateClassificationStatus", mock.Anything, mock.Anything, []string{testProtocolID}, data.StatusFailed).Return(nil)
		specExtractor.On("Close", ctx).Return(nil)

		svcModels := *models
		svcModels.Protocols = protocolModelMock
		svcModels.ProtocolWasms = protocolWasmModelMock
		svcModels.ProtocolContracts = data.NewProtocolContractsModelMock(t)
		svc := &protocolSetupService{
			db:                     dbConnectionPool,
			rpcService:             rpcMock,
			models:                 &svcModels,
			protocolModel:          protocolModelMock,
			protocolWasmModel:      protocolWasmModelMock,
			protocolContractsModel: svcModels.ProtocolContracts,
			specExtractor:          specExtractor,
			validators:             []ProtocolValidator{newStubValidator()},
		}

		err := svc.Run(ctx, []string{testProtocolID})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "rpc unavailable")
	})

	t.Run("no unclassified WASMs is a clean no-op", func(t *testing.T) {
		protocolModelMock := data.NewProtocolsModelMock(t)
		protocolWasmModelMock := data.NewProtocolWasmsModelMock(t)
		specExtractor := NewWasmSpecExtractorMock(t)

		protocolModelMock.On("GetByIDs", ctx, []string{testProtocolID}).Return([]data.Protocols{{ID: testProtocolID}}, nil)
		protocolModelMock.On("UpdateClassificationStatus", ctx, mock.Anything, []string{testProtocolID}, data.StatusInProgress).Return(nil)
		protocolWasmModelMock.On("GetUnclassified", ctx).Return([]data.ProtocolWasms{}, nil)
		protocolModelMock.On("UpdateClassificationStatus", ctx, mock.Anything, []string{testProtocolID}, data.StatusSuccess).Return(nil)
		specExtractor.On("Close", ctx).Return(nil)

		svcModels := *models
		svcModels.Protocols = protocolModelMock
		svcModels.ProtocolWasms = protocolWasmModelMock
		svc := &protocolSetupService{
			db:                dbConnectionPool,
			models:            &svcModels,
			protocolModel:     protocolModelMock,
			protocolWasmModel: protocolWasmModelMock,
			specExtractor:     specExtractor,
			validators:        []ProtocolValidator{newStubValidator()},
		}

		require.NoError(t, svc.Run(ctx, []string{testProtocolID}))
	})
}
