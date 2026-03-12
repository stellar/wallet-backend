package services

import (
	"context"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer/types"
)

func TestWasmIngestionService_ProcessContractCode(t *testing.T) {
	ctx := context.Background()
	hash := xdr.Hash{1, 2, 3}

	t.Run("tracks_hash", func(t *testing.T) {
		protocolWasmModelMock := data.NewProtocolWasmModelMock(t)
		protocolContractsModelMock := data.NewProtocolContractsModelMock(t)
		svc := NewWasmIngestionService(protocolWasmModelMock, protocolContractsModelMock)

		err := svc.ProcessContractCode(ctx, hash)
		require.NoError(t, err)

		_, tracked := svc.wasmHashes[hash]
		assert.True(t, tracked, "hash should be tracked")
	})

	t.Run("duplicate_hash_deduplicated", func(t *testing.T) {
		protocolWasmModelMock := data.NewProtocolWasmModelMock(t)
		protocolContractsModelMock := data.NewProtocolContractsModelMock(t)
		svc := NewWasmIngestionService(protocolWasmModelMock, protocolContractsModelMock)

		err := svc.ProcessContractCode(ctx, hash)
		require.NoError(t, err)

		err = svc.ProcessContractCode(ctx, hash)
		require.NoError(t, err)

		assert.Len(t, svc.wasmHashes, 1, "duplicate hash should be deduplicated")

		// Verify PersistProtocolWasms produces 1 entry
		protocolWasmModelMock.On("BatchInsert", mock.Anything, mock.Anything,
			mock.MatchedBy(func(wasms []data.ProtocolWasm) bool {
				return len(wasms) == 1
			}),
		).Return(nil).Once()

		err = svc.PersistProtocolWasms(ctx, nil)
		require.NoError(t, err)
	})
}

func TestWasmIngestionService_PersistProtocolWasms(t *testing.T) {
	ctx := context.Background()

	t.Run("no_hashes_skips_insert", func(t *testing.T) {
		protocolWasmModelMock := data.NewProtocolWasmModelMock(t)
		protocolContractsModelMock := data.NewProtocolContractsModelMock(t)
		svc := NewWasmIngestionService(protocolWasmModelMock, protocolContractsModelMock)

		err := svc.PersistProtocolWasms(ctx, nil)
		require.NoError(t, err)
		protocolWasmModelMock.AssertNotCalled(t, "BatchInsert", mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("single_hash_persisted", func(t *testing.T) {
		protocolWasmModelMock := data.NewProtocolWasmModelMock(t)
		protocolContractsModelMock := data.NewProtocolContractsModelMock(t)
		hash := xdr.Hash{10, 20, 30}

		protocolWasmModelMock.On("BatchInsert", mock.Anything, mock.Anything,
			mock.MatchedBy(func(wasms []data.ProtocolWasm) bool {
				if len(wasms) != 1 {
					return false
				}
				return wasms[0].WasmHash == types.HashBytea(hex.EncodeToString(hash[:])) && wasms[0].ProtocolID == nil
			}),
		).Return(nil).Once()

		svc := NewWasmIngestionService(protocolWasmModelMock, protocolContractsModelMock)
		err := svc.ProcessContractCode(ctx, hash)
		require.NoError(t, err)

		err = svc.PersistProtocolWasms(ctx, nil)
		require.NoError(t, err)
	})

	t.Run("multiple_hashes_persisted", func(t *testing.T) {
		protocolWasmModelMock := data.NewProtocolWasmModelMock(t)
		protocolContractsModelMock := data.NewProtocolContractsModelMock(t)
		hash1 := xdr.Hash{1}
		hash2 := xdr.Hash{2}

		protocolWasmModelMock.On("BatchInsert", mock.Anything, mock.Anything,
			mock.MatchedBy(func(wasms []data.ProtocolWasm) bool {
				if len(wasms) != 2 {
					return false
				}
				found := make(map[string]bool)
				for _, w := range wasms {
					found[string(w.WasmHash)] = true
				}
				return found[hex.EncodeToString(hash1[:])] && found[hex.EncodeToString(hash2[:])]
			}),
		).Return(nil).Once()

		svc := NewWasmIngestionService(protocolWasmModelMock, protocolContractsModelMock)
		require.NoError(t, svc.ProcessContractCode(ctx, hash1))
		require.NoError(t, svc.ProcessContractCode(ctx, hash2))

		err := svc.PersistProtocolWasms(ctx, nil)
		require.NoError(t, err)
	})

	t.Run("batch_insert_error_propagated", func(t *testing.T) {
		protocolWasmModelMock := data.NewProtocolWasmModelMock(t)
		protocolContractsModelMock := data.NewProtocolContractsModelMock(t)
		hash := xdr.Hash{99}
		insertErr := errors.New("db connection lost")

		protocolWasmModelMock.On("BatchInsert", mock.Anything, mock.Anything, mock.Anything).
			Return(insertErr).Once()

		svc := NewWasmIngestionService(protocolWasmModelMock, protocolContractsModelMock)
		require.NoError(t, svc.ProcessContractCode(ctx, hash))

		err := svc.PersistProtocolWasms(ctx, nil)
		require.Error(t, err)
		assert.ErrorContains(t, err, "persisting protocol wasms")
		assert.ErrorIs(t, err, insertErr)
	})
}

func TestWasmIngestionService_ProcessContractData(t *testing.T) {
	ctx := context.Background()

	t.Run("non_instance_entry_skipped", func(t *testing.T) {
		protocolWasmModelMock := data.NewProtocolWasmModelMock(t)
		protocolContractsModelMock := data.NewProtocolContractsModelMock(t)
		svc := NewWasmIngestionService(protocolWasmModelMock, protocolContractsModelMock)

		// ContractData entry with a non-Instance key type (e.g., balance entry)
		contractHash := [32]byte{1, 2, 3}
		change := ingest.Change{
			Type: xdr.LedgerEntryTypeContractData,
			Post: &xdr.LedgerEntry{
				Data: xdr.LedgerEntryData{
					Type: xdr.LedgerEntryTypeContractData,
					ContractData: &xdr.ContractDataEntry{
						Contract: xdr.ScAddress{
							Type:       xdr.ScAddressTypeScAddressTypeContract,
							ContractId: (*xdr.ContractId)(&contractHash),
						},
						Key:        xdr.ScVal{Type: xdr.ScValTypeScvSymbol},
						Durability: xdr.ContractDataDurabilityPersistent,
					},
				},
			},
		}

		err := svc.ProcessContractData(ctx, change)
		require.NoError(t, err)
		assert.Empty(t, svc.contractIDsByWasmHash, "non-instance entry should be skipped")
	})

	t.Run("instance_without_contract_id_skipped", func(t *testing.T) {
		protocolWasmModelMock := data.NewProtocolWasmModelMock(t)
		protocolContractsModelMock := data.NewProtocolContractsModelMock(t)
		svc := NewWasmIngestionService(protocolWasmModelMock, protocolContractsModelMock)

		// Instance entry with account address (not contract) — GetContractId returns false
		change := ingest.Change{
			Type: xdr.LedgerEntryTypeContractData,
			Post: &xdr.LedgerEntry{
				Data: xdr.LedgerEntryData{
					Type: xdr.LedgerEntryTypeContractData,
					ContractData: &xdr.ContractDataEntry{
						Contract: xdr.ScAddress{
							Type: xdr.ScAddressTypeScAddressTypeAccount,
						},
						Key:        xdr.ScVal{Type: xdr.ScValTypeScvLedgerKeyContractInstance},
						Durability: xdr.ContractDataDurabilityPersistent,
					},
				},
			},
		}

		err := svc.ProcessContractData(ctx, change)
		require.NoError(t, err)
		assert.Empty(t, svc.contractIDsByWasmHash, "entry without contract ID should be skipped")
	})

	t.Run("sac_contract_skipped", func(t *testing.T) {
		protocolWasmModelMock := data.NewProtocolWasmModelMock(t)
		protocolContractsModelMock := data.NewProtocolContractsModelMock(t)
		svc := NewWasmIngestionService(protocolWasmModelMock, protocolContractsModelMock)

		// SAC contract — executable type is StellarAsset, not WASM
		contractHash := [32]byte{5, 6, 7}
		change := ingest.Change{
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
									Type: xdr.ContractExecutableTypeContractExecutableStellarAsset,
								},
							},
						},
					},
				},
			},
		}

		err := svc.ProcessContractData(ctx, change)
		require.NoError(t, err)
		assert.Empty(t, svc.contractIDsByWasmHash, "SAC contract should be skipped")
	})

	t.Run("wasm_contract_tracked", func(t *testing.T) {
		protocolWasmModelMock := data.NewProtocolWasmModelMock(t)
		protocolContractsModelMock := data.NewProtocolContractsModelMock(t)
		svc := NewWasmIngestionService(protocolWasmModelMock, protocolContractsModelMock)

		contractHash := [32]byte{10, 20, 30}
		wasmHash := xdr.Hash{40, 50, 60}
		change := makeContractInstanceChange(contractHash, wasmHash)

		err := svc.ProcessContractData(ctx, change)
		require.NoError(t, err)

		require.Contains(t, svc.contractIDsByWasmHash, wasmHash)
		expectedContractID := types.HashBytea(hex.EncodeToString(contractHash[:]))
		assert.Equal(t, []types.HashBytea{expectedContractID}, svc.contractIDsByWasmHash[wasmHash])
	})

	t.Run("multiple_contracts_same_wasm_hash", func(t *testing.T) {
		protocolWasmModelMock := data.NewProtocolWasmModelMock(t)
		protocolContractsModelMock := data.NewProtocolContractsModelMock(t)
		svc := NewWasmIngestionService(protocolWasmModelMock, protocolContractsModelMock)

		wasmHash := xdr.Hash{1, 2, 3}
		contractHash1 := [32]byte{10}
		contractHash2 := [32]byte{20}

		change1 := makeContractInstanceChange(contractHash1, wasmHash)
		change2 := makeContractInstanceChange(contractHash2, wasmHash)

		require.NoError(t, svc.ProcessContractData(ctx, change1))
		require.NoError(t, svc.ProcessContractData(ctx, change2))

		require.Contains(t, svc.contractIDsByWasmHash, wasmHash)
		assert.Len(t, svc.contractIDsByWasmHash[wasmHash], 2)

		// Check that both contract hashes are present as hex strings
		expectedID1 := types.HashBytea(hex.EncodeToString(contractHash1[:]))
		expectedID2 := types.HashBytea(hex.EncodeToString(contractHash2[:]))
		var foundAddr1, foundAddr2 bool
		for _, id := range svc.contractIDsByWasmHash[wasmHash] {
			if id == expectedID1 {
				foundAddr1 = true
			}
			if id == expectedID2 {
				foundAddr2 = true
			}
		}
		assert.True(t, foundAddr1, "contractHash1 should be tracked")
		assert.True(t, foundAddr2, "contractHash2 should be tracked")
	})
}

func TestWasmIngestionService_PersistProtocolContracts(t *testing.T) {
	ctx := context.Background()

	t.Run("empty_no_op", func(t *testing.T) {
		protocolWasmModelMock := data.NewProtocolWasmModelMock(t)
		protocolContractsModelMock := data.NewProtocolContractsModelMock(t)
		svc := NewWasmIngestionService(protocolWasmModelMock, protocolContractsModelMock)

		err := svc.PersistProtocolContracts(ctx, nil)
		require.NoError(t, err)
		protocolContractsModelMock.AssertNotCalled(t, "BatchInsert", mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("single_contract", func(t *testing.T) {
		protocolWasmModelMock := data.NewProtocolWasmModelMock(t)
		protocolContractsModelMock := data.NewProtocolContractsModelMock(t)
		svc := NewWasmIngestionService(protocolWasmModelMock, protocolContractsModelMock)

		contractHash := [32]byte{10, 20, 30}
		wasmHash := xdr.Hash{40, 50, 60}
		change := makeContractInstanceChange(contractHash, wasmHash)

		require.NoError(t, svc.ProcessContractCode(ctx, wasmHash))
		require.NoError(t, svc.ProcessContractData(ctx, change))

		protocolContractsModelMock.On("BatchInsert", mock.Anything, mock.Anything,
			mock.MatchedBy(func(contracts []data.ProtocolContracts) bool {
				if len(contracts) != 1 {
					return false
				}
				return contracts[0].ContractID == types.HashBytea(hex.EncodeToString(contractHash[:])) &&
					contracts[0].WasmHash == types.HashBytea(hex.EncodeToString(wasmHash[:])) &&
					contracts[0].Name == nil
			}),
		).Return(nil).Once()

		err := svc.PersistProtocolContracts(ctx, nil)
		require.NoError(t, err)
	})

	t.Run("multiple_contracts_across_hashes", func(t *testing.T) {
		protocolWasmModelMock := data.NewProtocolWasmModelMock(t)
		protocolContractsModelMock := data.NewProtocolContractsModelMock(t)
		svc := NewWasmIngestionService(protocolWasmModelMock, protocolContractsModelMock)

		wasmHash1 := xdr.Hash{1}
		wasmHash2 := xdr.Hash{2}
		contractHash1 := [32]byte{10}
		contractHash2 := [32]byte{20}
		contractHash3 := [32]byte{30}

		// Register WASM hashes first
		require.NoError(t, svc.ProcessContractCode(ctx, wasmHash1))
		require.NoError(t, svc.ProcessContractCode(ctx, wasmHash2))

		// Two contracts with wasmHash1, one with wasmHash2
		require.NoError(t, svc.ProcessContractData(ctx, makeContractInstanceChange(contractHash1, wasmHash1)))
		require.NoError(t, svc.ProcessContractData(ctx, makeContractInstanceChange(contractHash2, wasmHash1)))
		require.NoError(t, svc.ProcessContractData(ctx, makeContractInstanceChange(contractHash3, wasmHash2)))

		protocolContractsModelMock.On("BatchInsert", mock.Anything, mock.Anything,
			mock.MatchedBy(func(contracts []data.ProtocolContracts) bool {
				return len(contracts) == 3
			}),
		).Return(nil).Once()

		err := svc.PersistProtocolContracts(ctx, nil)
		require.NoError(t, err)
	})

	t.Run("contracts_with_missing_wasm_skipped", func(t *testing.T) {
		protocolWasmModelMock := data.NewProtocolWasmModelMock(t)
		protocolContractsModelMock := data.NewProtocolContractsModelMock(t)
		svc := NewWasmIngestionService(protocolWasmModelMock, protocolContractsModelMock)

		knownWasm := xdr.Hash{1}
		unknownWasm := xdr.Hash{2}
		contractHash1 := [32]byte{10}
		contractHash2 := [32]byte{20}

		// Only register one WASM hash
		require.NoError(t, svc.ProcessContractCode(ctx, knownWasm))

		// Add contracts — one with known WASM, one with unknown
		require.NoError(t, svc.ProcessContractData(ctx, makeContractInstanceChange(contractHash1, knownWasm)))
		require.NoError(t, svc.ProcessContractData(ctx, makeContractInstanceChange(contractHash2, unknownWasm)))

		protocolContractsModelMock.On("BatchInsert", mock.Anything, mock.Anything,
			mock.MatchedBy(func(contracts []data.ProtocolContracts) bool {
				return len(contracts) == 1 && contracts[0].WasmHash == types.HashBytea(hex.EncodeToString(knownWasm[:]))
			}),
		).Return(nil).Once()

		err := svc.PersistProtocolContracts(ctx, nil)
		require.NoError(t, err)
	})

	t.Run("batch_insert_error_propagated", func(t *testing.T) {
		protocolWasmModelMock := data.NewProtocolWasmModelMock(t)
		protocolContractsModelMock := data.NewProtocolContractsModelMock(t)
		svc := NewWasmIngestionService(protocolWasmModelMock, protocolContractsModelMock)

		contractHash := [32]byte{10}
		wasmHash := xdr.Hash{1}
		require.NoError(t, svc.ProcessContractCode(ctx, wasmHash))
		require.NoError(t, svc.ProcessContractData(ctx, makeContractInstanceChange(contractHash, wasmHash)))

		insertErr := errors.New("db connection lost")
		protocolContractsModelMock.On("BatchInsert", mock.Anything, mock.Anything, mock.Anything).
			Return(insertErr).Once()

		err := svc.PersistProtocolContracts(ctx, nil)
		require.Error(t, err)
		assert.ErrorContains(t, err, "persisting protocol contracts")
		assert.ErrorIs(t, err, insertErr)
	})
}
