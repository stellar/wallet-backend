package services

import (
	"context"
	"encoding/hex"
	"errors"
	"io"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stellar/go-stellar-sdk/historyarchive"
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	wbdata "github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/indexer/types"
)

// Test helpers

func makeContractCodeChange(hash xdr.Hash, code []byte) ingest.Change {
	return ingest.Change{
		Type: xdr.LedgerEntryTypeContractCode,
		Post: &xdr.LedgerEntry{
			Data: xdr.LedgerEntryData{
				Type: xdr.LedgerEntryTypeContractCode,
				ContractCode: &xdr.ContractCodeEntry{
					Hash: hash,
					Code: code,
				},
			},
		},
	}
}

func makeAccountChange() ingest.Change {
	return ingest.Change{
		Type: xdr.LedgerEntryTypeAccount,
		Post: &xdr.LedgerEntry{
			Data: xdr.LedgerEntryData{
				Type: xdr.LedgerEntryTypeAccount,
				Account: &xdr.AccountEntry{
					AccountId: xdr.MustAddress("GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"),
					Balance:   100,
				},
			},
		},
	}
}

// makeContractInstanceChange builds an ingest.Change for a ContractData entry with
// ScvLedgerKeyContractInstance key and a WASM executable (non-SAC).
func makeContractInstanceChange(contractHash [32]byte, wasmHash xdr.Hash) ingest.Change {
	return ingest.Change{
		Type: xdr.LedgerEntryTypeContractData,
		Post: &xdr.LedgerEntry{
			Data: xdr.LedgerEntryData{
				Type: xdr.LedgerEntryTypeContractData,
				ContractData: &xdr.ContractDataEntry{
					Contract: xdr.ScAddress{
						Type:       xdr.ScAddressTypeScAddressTypeContract,
						ContractId: (*xdr.ContractId)(&contractHash),
					},
					Key:        xdr.ScVal{Type: xdr.ScValTypeScvLedgerKeyContractInstance},
					Durability: xdr.ContractDataDurabilityPersistent,
					Val: xdr.ScVal{
						Type: xdr.ScValTypeScvContractInstance,
						Instance: &xdr.ScContractInstance{
							Executable: xdr.ContractExecutable{
								Type:     xdr.ContractExecutableTypeContractExecutableWasm,
								WasmHash: &wasmHash,
							},
						},
					},
				},
			},
		},
	}
}

// checkpointTestFixture holds a checkpointService and all mocked dependencies.
type checkpointTestFixture struct {
	svc                        *checkpointService
	reader                     *ChangeReaderMock
	contractValidator          *ContractValidatorMock
	contractMetadataService    *ContractMetadataServiceMock
	trustlineAssetModel        *wbdata.TrustlineAssetModelMock
	trustlineBalanceModel      *wbdata.TrustlineBalanceModelMock
	nativeBalanceModel         *wbdata.NativeBalanceModelMock
	sacBalanceModel            *wbdata.SACBalanceModelMock
	accountContractTokensModel *wbdata.AccountContractTokensModelMock
	contractModel              *wbdata.ContractModelMock
	protocolWasmModel          *wbdata.ProtocolWasmModelMock
	protocolContractsModel     *wbdata.ProtocolContractsModelMock
}

// setupCheckpointTest creates a checkpointService with mocked dependencies and a real DB pool.
func setupCheckpointTest(t *testing.T) checkpointTestFixture {
	t.Helper()

	dbt := dbtest.Open(t)
	t.Cleanup(func() { dbt.Close() })
	dbPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	t.Cleanup(func() { dbPool.Close() })

	readerMock := NewChangeReaderMock(t)
	contractValidatorMock := NewContractValidatorMock(t)
	contractMetadataServiceMock := NewContractMetadataServiceMock(t)
	trustlineAssetModelMock := wbdata.NewTrustlineAssetModelMock(t)
	trustlineBalanceModelMock := wbdata.NewTrustlineBalanceModelMock(t)
	nativeBalanceModelMock := wbdata.NewNativeBalanceModelMock(t)
	sacBalanceModelMock := wbdata.NewSACBalanceModelMock(t)
	accountContractTokensModelMock := wbdata.NewAccountContractTokensModelMock(t)
	contractModelMock := wbdata.NewContractModelMock(t)
	protocolWasmModelMock := wbdata.NewProtocolWasmModelMock(t)
	protocolContractsModelMock := wbdata.NewProtocolContractsModelMock(t)

	svc := &checkpointService{
		db:                         dbPool,
		archive:                    &HistoryArchiveMock{},
		contractValidator:          contractValidatorMock,
		contractMetadataService:    contractMetadataServiceMock,
		trustlineAssetModel:        trustlineAssetModelMock,
		trustlineBalanceModel:      trustlineBalanceModelMock,
		nativeBalanceModel:         nativeBalanceModelMock,
		sacBalanceModel:            sacBalanceModelMock,
		accountContractTokensModel: accountContractTokensModelMock,
		contractModel:              contractModelMock,
		protocolWasmModel:          protocolWasmModelMock,
		protocolContractsModel:     protocolContractsModelMock,
		networkPassphrase:          network.TestNetworkPassphrase,
		readerFactory: func(_ context.Context, _ historyarchive.ArchiveInterface, _ uint32) (ingest.ChangeReader, error) {
			return readerMock, nil
		},
	}

	return checkpointTestFixture{
		svc:                        svc,
		reader:                     readerMock,
		contractValidator:          contractValidatorMock,
		contractMetadataService:    contractMetadataServiceMock,
		trustlineAssetModel:        trustlineAssetModelMock,
		trustlineBalanceModel:      trustlineBalanceModelMock,
		nativeBalanceModel:         nativeBalanceModelMock,
		sacBalanceModel:            sacBalanceModelMock,
		accountContractTokensModel: accountContractTokensModelMock,
		contractModel:              contractModelMock,
		protocolWasmModel:          protocolWasmModelMock,
		protocolContractsModel:     protocolContractsModelMock,
	}
}

func TestCheckpointService_PopulateFromCheckpoint_NilArchive(t *testing.T) {
	svc := &checkpointService{
		archive: nil,
	}

	err := svc.PopulateFromCheckpoint(context.Background(), 100, func(_ pgx.Tx) error { return nil })
	require.Error(t, err)
	assert.ErrorContains(t, err, "history archive not configured")
}

func TestCheckpointService_PopulateFromCheckpoint_ReaderCreationFails(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbPool.Close()

	contractValidatorMock := NewContractValidatorMock(t)
	contractValidatorMock.On("Close", mock.Anything).Return(nil).Once()

	svc := &checkpointService{
		db:                dbPool,
		archive:           &HistoryArchiveMock{},
		contractValidator: contractValidatorMock,
		readerFactory: func(_ context.Context, _ historyarchive.ArchiveInterface, _ uint32) (ingest.ChangeReader, error) {
			return nil, errors.New("archive unavailable")
		},
	}

	err = svc.PopulateFromCheckpoint(context.Background(), 100, func(_ pgx.Tx) error { return nil })
	require.Error(t, err)
	assert.ErrorContains(t, err, "creating checkpoint change reader")
}

func TestCheckpointService_PopulateFromCheckpoint_EmptyCheckpoint(t *testing.T) {
	f := setupCheckpointTest(t)

	f.reader.On("Read").Return(ingest.Change{}, io.EOF).Once()
	f.reader.On("Close").Return(nil).Once()
	f.contractValidator.On("Close", mock.Anything).Return(nil).Once()

	cursorsCalled := false
	err := f.svc.PopulateFromCheckpoint(context.Background(), 100, func(_ pgx.Tx) error {
		cursorsCalled = true
		return nil
	})
	require.NoError(t, err)
	assert.True(t, cursorsCalled, "initializeCursors should be called")
}

func TestCheckpointService_PopulateFromCheckpoint_ContractCodeEntry(t *testing.T) {
	f := setupCheckpointTest(t)

	hash := xdr.Hash{1, 2, 3}
	code := []byte{0xDE, 0xAD}
	change := makeContractCodeChange(hash, code)

	// Reader returns one ContractCode then EOF
	f.reader.On("Read").Return(change, nil).Once()
	f.reader.On("Read").Return(ingest.Change{}, io.EOF).Once()
	f.reader.On("Close").Return(nil).Once()
	f.contractValidator.On("Close", mock.Anything).Return(nil).Once()

	// Validator accepts this as SEP-41
	f.contractValidator.On("ValidateFromContractCode", mock.Anything, code).
		Return(types.ContractTypeSEP41, nil).Once()

	// finalize -> persistProtocolWasms inserts the tracked WASM hash
	f.protocolWasmModel.On("BatchInsert", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

	err := f.svc.PopulateFromCheckpoint(context.Background(), 100, func(_ pgx.Tx) error { return nil })
	require.NoError(t, err)
}

func TestCheckpointService_PopulateFromCheckpoint_AccountEntry(t *testing.T) {
	f := setupCheckpointTest(t)

	accountChange := makeAccountChange()

	f.reader.On("Read").Return(accountChange, nil).Once()
	f.reader.On("Read").Return(ingest.Change{}, io.EOF).Once()
	f.reader.On("Close").Return(nil).Once()
	f.contractValidator.On("Close", mock.Anything).Return(nil).Once()

	// The batch will flush with 1 native balance
	f.nativeBalanceModel.On("BatchCopy", mock.Anything, mock.Anything,
		mock.MatchedBy(func(b []wbdata.NativeBalance) bool { return len(b) == 1 }),
	).Return(nil).Once()
	f.trustlineBalanceModel.On("BatchCopy", mock.Anything, mock.Anything,
		mock.MatchedBy(func(b []wbdata.TrustlineBalance) bool { return len(b) == 0 }),
	).Return(nil).Once()
	f.sacBalanceModel.On("BatchCopy", mock.Anything, mock.Anything,
		mock.MatchedBy(func(b []wbdata.SACBalance) bool { return len(b) == 0 }),
	).Return(nil).Once()

	err := f.svc.PopulateFromCheckpoint(context.Background(), 100, func(_ pgx.Tx) error { return nil })
	require.NoError(t, err)
}

func TestCheckpointService_PopulateFromCheckpoint_ContractDataEntry(t *testing.T) {
	f := setupCheckpointTest(t)

	contractHash := [32]byte{10, 20, 30}
	wasmHash := xdr.Hash{40, 50, 60}
	contractDataChange := makeContractInstanceChange(contractHash, wasmHash)

	f.reader.On("Read").Return(contractDataChange, nil).Once()
	f.reader.On("Read").Return(ingest.Change{}, io.EOF).Once()
	f.reader.On("Close").Return(nil).Once()
	f.contractValidator.On("Close", mock.Anything).Return(nil).Once()

	// Flush remaining batch (empty balances but 0 entries)
	f.trustlineBalanceModel.On("BatchCopy", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	f.nativeBalanceModel.On("BatchCopy", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	f.sacBalanceModel.On("BatchCopy", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	// Finalize: contract model + account contract tokens
	f.contractModel.On("BatchInsert", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	f.accountContractTokensModel.On("BatchInsert", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	// Protocol WASM + contracts: we tracked contract data but no contract code hash matched
	f.protocolWasmModel.On("BatchInsert", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	f.protocolContractsModel.On("BatchInsert", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	err := f.svc.PopulateFromCheckpoint(context.Background(), 100, func(_ pgx.Tx) error { return nil })
	require.NoError(t, err)
}

func TestCheckpointService_PopulateFromCheckpoint_ErrorPropagation(t *testing.T) {
	tests := []struct {
		name           string
		setupMocks     func(f *checkpointTestFixture) func(pgx.Tx) error
		expectedErrMsg string
	}{
		{
			name: "reader_read_error",
			setupMocks: func(f *checkpointTestFixture) func(pgx.Tx) error {
				f.reader.On("Read").Return(ingest.Change{}, errors.New("network timeout")).Once()
				f.reader.On("Close").Return(nil).Once()
				f.contractValidator.On("Close", mock.Anything).Return(nil).Once()
				return func(_ pgx.Tx) error { return nil }
			},
			expectedErrMsg: "reading checkpoint changes",
		},
		{
			name: "initialize_cursors_error",
			setupMocks: func(f *checkpointTestFixture) func(pgx.Tx) error {
				f.reader.On("Read").Return(ingest.Change{}, io.EOF).Once()
				f.reader.On("Close").Return(nil).Once()
				f.contractValidator.On("Close", mock.Anything).Return(nil).Once()
				return func(_ pgx.Tx) error { return errors.New("cursor init failed") }
			},
			expectedErrMsg: "initializing cursors",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := setupCheckpointTest(t)
			initializeCursors := tt.setupMocks(&f)

			err := f.svc.PopulateFromCheckpoint(context.Background(), 100, initializeCursors)
			require.Error(t, err)
			assert.ErrorContains(t, err, tt.expectedErrMsg)
		})
	}
}

func TestCheckpointService_PopulateFromCheckpoint_ContextCancellation(t *testing.T) {
	f := setupCheckpointTest(t)

	ctx, cancel := context.WithCancel(context.Background())

	f.reader.On("Close").Return(nil).Once()
	f.contractValidator.On("Close", mock.Anything).Return(nil).Once()

	// First Read succeeds but cancels the context; the next loop iteration detects cancellation
	f.reader.On("Read").Run(func(_ mock.Arguments) {
		cancel()
	}).Return(makeAccountChange(), nil).Once()

	err := f.svc.PopulateFromCheckpoint(ctx, 100, func(_ pgx.Tx) error { return nil })
	require.Error(t, err)
	assert.ErrorContains(t, err, "checkpoint processing cancelled")
}

// Tests ported from wasm_ingestion_test.go

func TestCheckpointProcessor_ProcessContractCode(t *testing.T) {
	ctx := context.Background()

	t.Run("tracks_hash_and_validates_sep41", func(t *testing.T) {
		contractValidatorMock := NewContractValidatorMock(t)
		proc := &checkpointProcessor{
			contractValidator:             contractValidatorMock,
			data:                          newCheckpointData(),
			wasmHashes:                    make(map[xdr.Hash]struct{}),
			protocolContractIDsByWasmHash: make(map[xdr.Hash][]types.HashBytea),
		}

		hash := xdr.Hash{1, 2, 3}
		code := []byte{0xDE, 0xAD}
		contractValidatorMock.On("ValidateFromContractCode", mock.Anything, code).
			Return(types.ContractTypeSEP41, nil).Once()

		proc.processContractCode(ctx, hash, code)

		// WASM hash tracked
		_, tracked := proc.wasmHashes[hash]
		assert.True(t, tracked, "hash should be tracked in wasmHashes")

		// SEP-41 type stored
		assert.Equal(t, types.ContractTypeSEP41, proc.data.contractTypesByWasmHash[hash])
		assert.Equal(t, 1, proc.entries)
	})

	t.Run("duplicate_hash_deduplicated", func(t *testing.T) {
		contractValidatorMock := NewContractValidatorMock(t)
		proc := &checkpointProcessor{
			contractValidator:             contractValidatorMock,
			data:                          newCheckpointData(),
			wasmHashes:                    make(map[xdr.Hash]struct{}),
			protocolContractIDsByWasmHash: make(map[xdr.Hash][]types.HashBytea),
		}

		hash := xdr.Hash{1, 2, 3}
		code := []byte{0xDE, 0xAD}
		contractValidatorMock.On("ValidateFromContractCode", mock.Anything, code).
			Return(types.ContractTypeSEP41, nil).Twice()

		proc.processContractCode(ctx, hash, code)
		proc.processContractCode(ctx, hash, code)

		assert.Len(t, proc.wasmHashes, 1, "duplicate hash should be deduplicated in map")
	})

	t.Run("validator_error_still_tracks_hash", func(t *testing.T) {
		contractValidatorMock := NewContractValidatorMock(t)
		proc := &checkpointProcessor{
			contractValidator:             contractValidatorMock,
			data:                          newCheckpointData(),
			wasmHashes:                    make(map[xdr.Hash]struct{}),
			protocolContractIDsByWasmHash: make(map[xdr.Hash][]types.HashBytea),
		}

		hash := xdr.Hash{4, 5, 6}
		code := []byte{0xBA, 0xD0}
		contractValidatorMock.On("ValidateFromContractCode", mock.Anything, code).
			Return(types.ContractTypeUnknown, errors.New("invalid WASM")).Once()

		proc.processContractCode(ctx, hash, code)

		// WASM hash should still be tracked for protocol_wasms
		_, tracked := proc.wasmHashes[hash]
		assert.True(t, tracked, "hash should be tracked even when validation fails")

		// But no SEP-41 type stored
		assert.Empty(t, proc.data.contractTypesByWasmHash)
		assert.Equal(t, 0, proc.entries)
	})
}

func TestCheckpointProcessor_ProcessWasmContractData(t *testing.T) {
	t.Run("wasm_contract_tracked", func(t *testing.T) {
		proc := &checkpointProcessor{
			protocolContractIDsByWasmHash: make(map[xdr.Hash][]types.HashBytea),
		}

		contractHash := [32]byte{10, 20, 30}
		wasmHash := xdr.Hash{40, 50, 60}
		change := makeContractInstanceChange(contractHash, wasmHash)
		contractDataEntry := change.Post.Data.MustContractData()

		proc.processWasmContractData(contractDataEntry, xdr.Hash(contractHash))

		require.Contains(t, proc.protocolContractIDsByWasmHash, wasmHash)
		expectedContractID := types.HashBytea(hex.EncodeToString(contractHash[:]))
		assert.Equal(t, []types.HashBytea{expectedContractID}, proc.protocolContractIDsByWasmHash[wasmHash])
	})

	t.Run("multiple_contracts_same_wasm_hash", func(t *testing.T) {
		proc := &checkpointProcessor{
			protocolContractIDsByWasmHash: make(map[xdr.Hash][]types.HashBytea),
		}

		wasmHash := xdr.Hash{1, 2, 3}
		contractHash1 := [32]byte{10}
		contractHash2 := [32]byte{20}

		change1 := makeContractInstanceChange(contractHash1, wasmHash)
		change2 := makeContractInstanceChange(contractHash2, wasmHash)

		proc.processWasmContractData(change1.Post.Data.MustContractData(), xdr.Hash(contractHash1))
		proc.processWasmContractData(change2.Post.Data.MustContractData(), xdr.Hash(contractHash2))

		require.Contains(t, proc.protocolContractIDsByWasmHash, wasmHash)
		assert.Len(t, proc.protocolContractIDsByWasmHash[wasmHash], 2)
	})

	t.Run("sac_contract_skipped", func(t *testing.T) {
		proc := &checkpointProcessor{
			protocolContractIDsByWasmHash: make(map[xdr.Hash][]types.HashBytea),
		}

		contractHash := [32]byte{5, 6, 7}
		contractDataEntry := xdr.ContractDataEntry{
			Contract: xdr.ScAddress{
				Type:       xdr.ScAddressTypeScAddressTypeContract,
				ContractId: (*xdr.ContractId)(&contractHash),
			},
			Key:        xdr.ScVal{Type: xdr.ScValTypeScvLedgerKeyContractInstance},
			Durability: xdr.ContractDataDurabilityPersistent,
			Val: xdr.ScVal{
				Type: xdr.ScValTypeScvContractInstance,
				Instance: &xdr.ScContractInstance{
					Executable: xdr.ContractExecutable{
						Type: xdr.ContractExecutableTypeContractExecutableStellarAsset,
					},
				},
			},
		}

		proc.processWasmContractData(contractDataEntry, xdr.Hash(contractHash))
		assert.Empty(t, proc.protocolContractIDsByWasmHash, "SAC contract should be skipped")
	})
}

func TestCheckpointService_PersistProtocolWasms(t *testing.T) {
	ctx := context.Background()

	t.Run("no_hashes_skips_insert", func(t *testing.T) {
		protocolWasmModelMock := wbdata.NewProtocolWasmModelMock(t)
		svc := &checkpointService{protocolWasmModel: protocolWasmModelMock}

		err := svc.persistProtocolWasms(ctx, nil, map[xdr.Hash]struct{}{})
		require.NoError(t, err)
		protocolWasmModelMock.AssertNotCalled(t, "BatchInsert", mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("single_hash_persisted", func(t *testing.T) {
		protocolWasmModelMock := wbdata.NewProtocolWasmModelMock(t)
		svc := &checkpointService{protocolWasmModel: protocolWasmModelMock}

		hash := xdr.Hash{10, 20, 30}
		wasmHashes := map[xdr.Hash]struct{}{hash: {}}

		protocolWasmModelMock.On("BatchInsert", mock.Anything, mock.Anything,
			mock.MatchedBy(func(wasms []wbdata.ProtocolWasm) bool {
				if len(wasms) != 1 {
					return false
				}
				return wasms[0].WasmHash == types.HashBytea(hex.EncodeToString(hash[:])) && wasms[0].ProtocolID == nil
			}),
		).Return(nil).Once()

		err := svc.persistProtocolWasms(ctx, nil, wasmHashes)
		require.NoError(t, err)
	})

	t.Run("batch_insert_error_propagated", func(t *testing.T) {
		protocolWasmModelMock := wbdata.NewProtocolWasmModelMock(t)
		svc := &checkpointService{protocolWasmModel: protocolWasmModelMock}

		hash := xdr.Hash{99}
		wasmHashes := map[xdr.Hash]struct{}{hash: {}}
		insertErr := errors.New("db connection lost")

		protocolWasmModelMock.On("BatchInsert", mock.Anything, mock.Anything, mock.Anything).
			Return(insertErr).Once()

		err := svc.persistProtocolWasms(ctx, nil, wasmHashes)
		require.Error(t, err)
		assert.ErrorContains(t, err, "persisting protocol wasms")
		assert.ErrorIs(t, err, insertErr)
	})
}

func TestCheckpointService_PersistProtocolContracts(t *testing.T) {
	ctx := context.Background()

	t.Run("empty_no_op", func(t *testing.T) {
		protocolContractsModelMock := wbdata.NewProtocolContractsModelMock(t)
		svc := &checkpointService{protocolContractsModel: protocolContractsModelMock}

		err := svc.persistProtocolContracts(ctx, nil, map[xdr.Hash]struct{}{}, map[xdr.Hash][]types.HashBytea{})
		require.NoError(t, err)
		protocolContractsModelMock.AssertNotCalled(t, "BatchInsert", mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("single_contract", func(t *testing.T) {
		protocolContractsModelMock := wbdata.NewProtocolContractsModelMock(t)
		svc := &checkpointService{protocolContractsModel: protocolContractsModelMock}

		contractHash := [32]byte{10, 20, 30}
		wasmHash := xdr.Hash{40, 50, 60}
		wasmHashes := map[xdr.Hash]struct{}{wasmHash: {}}
		contractIDsByWasmHash := map[xdr.Hash][]types.HashBytea{
			wasmHash: {types.HashBytea(hex.EncodeToString(contractHash[:]))},
		}

		protocolContractsModelMock.On("BatchInsert", mock.Anything, mock.Anything,
			mock.MatchedBy(func(contracts []wbdata.ProtocolContracts) bool {
				if len(contracts) != 1 {
					return false
				}
				return contracts[0].ContractID == types.HashBytea(hex.EncodeToString(contractHash[:])) &&
					contracts[0].WasmHash == types.HashBytea(hex.EncodeToString(wasmHash[:])) &&
					contracts[0].Name == nil
			}),
		).Return(nil).Once()

		err := svc.persistProtocolContracts(ctx, nil, wasmHashes, contractIDsByWasmHash)
		require.NoError(t, err)
	})

	t.Run("contracts_with_missing_wasm_skipped", func(t *testing.T) {
		protocolContractsModelMock := wbdata.NewProtocolContractsModelMock(t)
		svc := &checkpointService{protocolContractsModel: protocolContractsModelMock}

		knownWasm := xdr.Hash{1}
		unknownWasm := xdr.Hash{2}
		contractHash1 := [32]byte{10}
		contractHash2 := [32]byte{20}

		wasmHashes := map[xdr.Hash]struct{}{knownWasm: {}}
		contractIDsByWasmHash := map[xdr.Hash][]types.HashBytea{
			knownWasm:   {types.HashBytea(hex.EncodeToString(contractHash1[:]))},
			unknownWasm: {types.HashBytea(hex.EncodeToString(contractHash2[:]))},
		}

		protocolContractsModelMock.On("BatchInsert", mock.Anything, mock.Anything,
			mock.MatchedBy(func(contracts []wbdata.ProtocolContracts) bool {
				return len(contracts) == 1 && contracts[0].WasmHash == types.HashBytea(hex.EncodeToString(knownWasm[:]))
			}),
		).Return(nil).Once()

		err := svc.persistProtocolContracts(ctx, nil, wasmHashes, contractIDsByWasmHash)
		require.NoError(t, err)
	})
}

// Tests ported from token_ingestion_test.go for checkpoint-specific logic

func TestCheckpointProcessor_ProcessEntry(t *testing.T) {
	// newTestCheckpointProcessor creates a checkpointProcessor with minimal deps for unit testing.
	newTestCheckpointProcessor := func() *checkpointProcessor {
		svc := &checkpointService{networkPassphrase: network.TestNetworkPassphrase}
		return &checkpointProcessor{
			service:                       svc,
			checkpointLedger:              100,
			data:                          newCheckpointData(),
			wasmHashes:                    make(map[xdr.Hash]struct{}),
			protocolContractIDsByWasmHash: make(map[xdr.Hash][]types.HashBytea),
			batch: &batch{
				nativeBalances:    make([]wbdata.NativeBalance, 0),
				trustlineBalances: make([]wbdata.TrustlineBalance, 0),
				sacBalances:       make([]wbdata.SACBalance, 0),
			},
		}
	}

	t.Run("account_entry", func(t *testing.T) {
		proc := newTestCheckpointProcessor()
		address := "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"

		change := makeAccountChangeWithBalance(address, 100_000_000, 3, 5_000_000, 2_000_000)
		proc.processEntry(change)

		require.Len(t, proc.batch.nativeBalances, 1)
		nb := proc.batch.nativeBalances[0]
		assert.Equal(t, address, nb.AccountAddress)
		assert.Equal(t, int64(100_000_000), nb.Balance)
		assert.Equal(t, uint32(100), nb.LedgerNumber)
		assert.Equal(t, 1, proc.entries)
		assert.Equal(t, 1, proc.accountCount)
	})

	t.Run("trustline_entry", func(t *testing.T) {
		proc := newTestCheckpointProcessor()
		address := "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"
		issuer := "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN"
		assetCode := "USDC"

		change := makeTrustlineChange(address, assetCode, issuer, 5_000_000, 100_000_000)
		proc.processEntry(change)

		require.Len(t, proc.batch.trustlineBalances, 1)
		tb := proc.batch.trustlineBalances[0]
		assert.Equal(t, address, tb.AccountAddress)
		assert.Equal(t, wbdata.DeterministicAssetID(assetCode, issuer), tb.AssetID)
		assert.Equal(t, 1, proc.entries)
		assert.Equal(t, 1, proc.trustlineCount)
	})

	t.Run("trustline_pool_share_skipped", func(t *testing.T) {
		proc := newTestCheckpointProcessor()
		change := makePoolShareTrustlineChange("GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N")
		proc.processEntry(change)

		assert.Empty(t, proc.batch.trustlineBalances)
		assert.Equal(t, 0, proc.entries)
	})

	t.Run("contract_instance_non_sac", func(t *testing.T) {
		proc := newTestCheckpointProcessor()
		contractHash := [32]byte{0xAA, 0xBB, 0xCC}
		wasmHash := xdr.Hash{0x11, 0x22, 0x33}

		change := makeContractInstanceChange(contractHash, wasmHash)
		proc.processEntry(change)

		contractAddr := strkey.MustEncode(strkey.VersionByteContract, contractHash[:])
		contractUUID := wbdata.DeterministicContractID(contractAddr)

		require.Contains(t, proc.data.uniqueContractTokens, contractUUID)
		contract := proc.data.uniqueContractTokens[contractUUID]
		assert.Equal(t, contractAddr, contract.ContractID)
		assert.Equal(t, string(types.ContractTypeUnknown), contract.Type)

		require.Contains(t, proc.data.contractIDsByWasmHash, wasmHash)
		assert.Equal(t, []string{contractAddr}, proc.data.contractIDsByWasmHash[wasmHash])

		// Also tracked for protocol contracts
		require.Contains(t, proc.protocolContractIDsByWasmHash, wasmHash)

		assert.Equal(t, 2, proc.entries)
	})

	t.Run("contract_balance_non_sac", func(t *testing.T) {
		proc := newTestCheckpointProcessor()
		contractHash := [32]byte{0xDD, 0xEE, 0xFF}
		holderAddress := "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"

		change := makeContractBalanceChange(contractHash, holderAddress)
		proc.processEntry(change)

		contractAddr := strkey.MustEncode(strkey.VersionByteContract, contractHash[:])
		contractUUID := wbdata.DeterministicContractID(contractAddr)

		require.Contains(t, proc.data.contractTokensByHolderAddress, holderAddress)
		assert.Equal(t, []uuid.UUID{contractUUID}, proc.data.contractTokensByHolderAddress[holderAddress])
		assert.Equal(t, 1, proc.entries)
		assert.Empty(t, proc.batch.sacBalances)
	})

	t.Run("unhandled_entry_type_ignored", func(t *testing.T) {
		proc := newTestCheckpointProcessor()

		change := ingest.Change{
			Type: xdr.LedgerEntryTypeOffer,
			Post: &xdr.LedgerEntry{
				Data: xdr.LedgerEntryData{
					Type: xdr.LedgerEntryTypeOffer,
					Offer: &xdr.OfferEntry{
						SellerId: xdr.MustAddress("GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"),
						OfferId:  1,
					},
				},
			},
		}
		proc.processEntry(change)

		assert.Equal(t, 0, proc.entries)
	})
}

func TestCheckpointService_ExtractHolderAddress(t *testing.T) {
	service := &checkpointService{}

	tests := []struct {
		name    string
		key     xdr.ScVal
		want    string
		wantErr bool
	}{
		{
			name: "valid balance entry",
			key: xdr.ScVal{
				Type: xdr.ScValTypeScvVec,
				Vec: ptrToScVec([]xdr.ScVal{
					{
						Type: xdr.ScValTypeScvSymbol,
						Sym:  ptrToScSymbol("Balance"),
					},
					{
						Type: xdr.ScValTypeScvAddress,
						Address: &xdr.ScAddress{
							Type:      xdr.ScAddressTypeScAddressTypeAccount,
							AccountId: ptrToAccountID("GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"),
						},
					},
				}),
			},
			want:    "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N",
			wantErr: false,
		},
		{
			name: "not a vector",
			key: xdr.ScVal{
				Type: xdr.ScValTypeScvU32,
				U32:  ptrToUint32(123),
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "wrong vector length",
			key: xdr.ScVal{
				Type: xdr.ScValTypeScvVec,
				Vec: ptrToScVec([]xdr.ScVal{
					{Type: xdr.ScValTypeScvSymbol, Sym: ptrToScSymbol("Balance")},
				}),
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "wrong symbol",
			key: xdr.ScVal{
				Type: xdr.ScValTypeScvVec,
				Vec: ptrToScVec([]xdr.ScVal{
					{Type: xdr.ScValTypeScvSymbol, Sym: ptrToScSymbol("NotBalance")},
					{
						Type: xdr.ScValTypeScvAddress,
						Address: &xdr.ScAddress{
							Type:      xdr.ScAddressTypeScAddressTypeAccount,
							AccountId: ptrToAccountID("GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"),
						},
					},
				}),
			},
			want:    "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := service.extractHolderAddress(tt.key)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Empty(t, got)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}
