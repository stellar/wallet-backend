package services

import (
	"context"
	"errors"
	"testing"

	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
)

func TestWasmIngestionService_ProcessContractCode(t *testing.T) {
	ctx := context.Background()
	hash := xdr.Hash{1, 2, 3}
	code := []byte{0xDE, 0xAD}

	t.Run("no_validators_tracks_hash", func(t *testing.T) {
		knownWasmModelMock := data.NewKnownWasmModelMock(t)
		svc := NewWasmIngestionService(knownWasmModelMock).(*wasmIngestionService)

		err := svc.ProcessContractCode(ctx, hash, code)
		require.NoError(t, err)

		_, tracked := svc.wasmHashes[hash]
		assert.True(t, tracked, "hash should be tracked")
	})

	t.Run("validator_match", func(t *testing.T) {
		knownWasmModelMock := data.NewKnownWasmModelMock(t)
		validatorMock := NewProtocolValidatorMock(t)
		validatorMock.On("Validate", mock.Anything, code).Return(true, nil).Once()
		validatorMock.On("ProtocolID").Return("test-protocol").Once()

		svc := NewWasmIngestionService(knownWasmModelMock, validatorMock).(*wasmIngestionService)

		err := svc.ProcessContractCode(ctx, hash, code)
		require.NoError(t, err)

		_, tracked := svc.wasmHashes[hash]
		assert.True(t, tracked)
	})

	t.Run("validator_no_match", func(t *testing.T) {
		knownWasmModelMock := data.NewKnownWasmModelMock(t)
		validatorMock := NewProtocolValidatorMock(t)
		validatorMock.On("Validate", mock.Anything, code).Return(false, nil).Once()

		svc := NewWasmIngestionService(knownWasmModelMock, validatorMock).(*wasmIngestionService)

		err := svc.ProcessContractCode(ctx, hash, code)
		require.NoError(t, err)

		_, tracked := svc.wasmHashes[hash]
		assert.True(t, tracked, "hash should still be tracked even without match")
	})

	t.Run("validator_error_continues", func(t *testing.T) {
		knownWasmModelMock := data.NewKnownWasmModelMock(t)
		validatorMock := NewProtocolValidatorMock(t)
		validatorMock.On("Validate", mock.Anything, code).Return(false, errors.New("validation failed")).Once()
		validatorMock.On("ProtocolID").Return("test-protocol").Once()

		svc := NewWasmIngestionService(knownWasmModelMock, validatorMock).(*wasmIngestionService)

		err := svc.ProcessContractCode(ctx, hash, code)
		require.NoError(t, err, "validator error should not propagate")

		_, tracked := svc.wasmHashes[hash]
		assert.True(t, tracked, "hash should still be tracked despite validator error")
	})

	t.Run("multiple_validators_all_run", func(t *testing.T) {
		knownWasmModelMock := data.NewKnownWasmModelMock(t)
		v1 := NewProtocolValidatorMock(t)
		v1.On("Validate", mock.Anything, code).Return(true, nil).Once()
		v1.On("ProtocolID").Return("protocol-1").Once()

		v2 := NewProtocolValidatorMock(t)
		v2.On("Validate", mock.Anything, code).Return(true, nil).Once()
		v2.On("ProtocolID").Return("protocol-2").Once()

		svc := NewWasmIngestionService(knownWasmModelMock, v1, v2).(*wasmIngestionService)

		err := svc.ProcessContractCode(ctx, hash, code)
		require.NoError(t, err)
		// Both validator expectations are asserted via t.Cleanup
	})

	t.Run("duplicate_hash_deduplicated", func(t *testing.T) {
		knownWasmModelMock := data.NewKnownWasmModelMock(t)
		svc := NewWasmIngestionService(knownWasmModelMock).(*wasmIngestionService)

		err := svc.ProcessContractCode(ctx, hash, code)
		require.NoError(t, err)

		err = svc.ProcessContractCode(ctx, hash, code)
		require.NoError(t, err)

		assert.Len(t, svc.wasmHashes, 1, "duplicate hash should be deduplicated")

		// Verify PersistKnownWasms produces 1 entry
		knownWasmModelMock.On("BatchInsert", mock.Anything, mock.Anything,
			mock.MatchedBy(func(wasms []data.KnownWasm) bool {
				return len(wasms) == 1
			}),
		).Return(nil).Once()

		err = svc.PersistKnownWasms(ctx, nil)
		require.NoError(t, err)
	})
}

func TestWasmIngestionService_PersistKnownWasms(t *testing.T) {
	ctx := context.Background()

	t.Run("no_hashes_skips_insert", func(t *testing.T) {
		knownWasmModelMock := data.NewKnownWasmModelMock(t)
		svc := NewWasmIngestionService(knownWasmModelMock)

		err := svc.PersistKnownWasms(ctx, nil)
		require.NoError(t, err)
		knownWasmModelMock.AssertNotCalled(t, "BatchInsert", mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("single_hash_persisted", func(t *testing.T) {
		knownWasmModelMock := data.NewKnownWasmModelMock(t)
		hash := xdr.Hash{10, 20, 30}

		knownWasmModelMock.On("BatchInsert", mock.Anything, mock.Anything,
			mock.MatchedBy(func(wasms []data.KnownWasm) bool {
				if len(wasms) != 1 {
					return false
				}
				return wasms[0].WasmHash == hash.HexString() && wasms[0].ProtocolID == nil
			}),
		).Return(nil).Once()

		svc := NewWasmIngestionService(knownWasmModelMock).(*wasmIngestionService)
		err := svc.ProcessContractCode(ctx, hash, []byte{0x01})
		require.NoError(t, err)

		err = svc.PersistKnownWasms(ctx, nil)
		require.NoError(t, err)
	})

	t.Run("multiple_hashes_persisted", func(t *testing.T) {
		knownWasmModelMock := data.NewKnownWasmModelMock(t)
		hash1 := xdr.Hash{1}
		hash2 := xdr.Hash{2}

		knownWasmModelMock.On("BatchInsert", mock.Anything, mock.Anything,
			mock.MatchedBy(func(wasms []data.KnownWasm) bool {
				if len(wasms) != 2 {
					return false
				}
				hashes := make(map[string]bool)
				for _, w := range wasms {
					hashes[w.WasmHash] = true
				}
				return hashes[hash1.HexString()] && hashes[hash2.HexString()]
			}),
		).Return(nil).Once()

		svc := NewWasmIngestionService(knownWasmModelMock).(*wasmIngestionService)
		require.NoError(t, svc.ProcessContractCode(ctx, hash1, []byte{0x01}))
		require.NoError(t, svc.ProcessContractCode(ctx, hash2, []byte{0x02}))

		err := svc.PersistKnownWasms(ctx, nil)
		require.NoError(t, err)
	})

	t.Run("batch_insert_error_propagated", func(t *testing.T) {
		knownWasmModelMock := data.NewKnownWasmModelMock(t)
		hash := xdr.Hash{99}
		insertErr := errors.New("db connection lost")

		knownWasmModelMock.On("BatchInsert", mock.Anything, mock.Anything, mock.Anything).
			Return(insertErr).Once()

		svc := NewWasmIngestionService(knownWasmModelMock).(*wasmIngestionService)
		require.NoError(t, svc.ProcessContractCode(ctx, hash, []byte{0x01}))

		err := svc.PersistKnownWasms(ctx, nil)
		require.Error(t, err)
		assert.ErrorContains(t, err, "persisting known wasms")
		assert.ErrorIs(t, err, insertErr)
	})
}
