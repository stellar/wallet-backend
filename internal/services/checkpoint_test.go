package services

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stellar/go-stellar-sdk/historyarchive"
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
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
					Balance: 100,
				},
			},
		},
	}
}

// setupCheckpointTest creates a checkpointService with mocked dependencies and a real DB pool.
// Returns the service, all mocks, and a cleanup function.
func setupCheckpointTest(t *testing.T) (
	*checkpointService,
	*ChangeReaderMock,
	*TokenIngestionServiceMock,
	*TokenProcessorMock,
	*WasmIngestionServiceMock,
	*ContractValidatorMock,
	db.ConnectionPool,
) {
	t.Helper()

	dbt := dbtest.Open(t)
	t.Cleanup(func() { dbt.Close() })
	dbPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	t.Cleanup(func() { dbPool.Close() })

	readerMock := NewChangeReaderMock(t)
	tokenIngestionServiceMock := NewTokenIngestionServiceMock(t)
	tokenProcessorMock := NewTokenProcessorMock(t)
	wasmIngestionServiceMock := NewWasmIngestionServiceMock(t)
	contractValidatorMock := NewContractValidatorMock(t)

	svc := &checkpointService{
		db:                    dbPool,
		archive:               &HistoryArchiveMock{}, // non-nil archive
		tokenIngestionService: tokenIngestionServiceMock,
		wasmIngestionService:  wasmIngestionServiceMock,
		contractValidator:     contractValidatorMock,
		readerFactory: func(_ context.Context, _ historyarchive.ArchiveInterface, _ uint32) (ingest.ChangeReader, error) {
			return readerMock, nil
		},
	}

	return svc, readerMock, tokenIngestionServiceMock, tokenProcessorMock, wasmIngestionServiceMock, contractValidatorMock, dbPool
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
	svc, readerMock, tokenIngestionServiceMock, tokenProcessorMock, wasmIngestionServiceMock, contractValidatorMock, _ := setupCheckpointTest(t)

	// Reader returns EOF immediately
	readerMock.On("Read").Return(ingest.Change{}, io.EOF).Once()
	readerMock.On("Close").Return(nil).Once()

	// Token processor creation and finalization
	tokenIngestionServiceMock.On("NewTokenProcessor", mock.Anything, uint32(100)).Return(tokenProcessorMock).Once()
	tokenProcessorMock.On("FlushRemainingBatch", mock.Anything).Return(nil).Once()
	tokenProcessorMock.On("Finalize", mock.Anything, mock.Anything).Return(nil).Once()

	// WASM persistence
	wasmIngestionServiceMock.On("PersistKnownWasms", mock.Anything, mock.Anything).Return(nil).Once()

	// Contract validator cleanup
	contractValidatorMock.On("Close", mock.Anything).Return(nil).Once()

	cursorsCalled := false
	err := svc.PopulateFromCheckpoint(context.Background(), 100, func(_ pgx.Tx) error {
		cursorsCalled = true
		return nil
	})
	require.NoError(t, err)
	assert.True(t, cursorsCalled, "initializeCursors should be called")
}

func TestCheckpointService_PopulateFromCheckpoint_ContractCodeEntry(t *testing.T) {
	svc, readerMock, tokenIngestionServiceMock, tokenProcessorMock, wasmIngestionServiceMock, contractValidatorMock, _ := setupCheckpointTest(t)

	hash := xdr.Hash{1, 2, 3}
	code := []byte{0xDE, 0xAD}
	change := makeContractCodeChange(hash, code)

	// Reader returns one ContractCode then EOF
	readerMock.On("Read").Return(change, nil).Once()
	readerMock.On("Read").Return(ingest.Change{}, io.EOF).Once()
	readerMock.On("Close").Return(nil).Once()

	tokenIngestionServiceMock.On("NewTokenProcessor", mock.Anything, uint32(100)).Return(tokenProcessorMock).Once()

	// Both services should receive the ContractCode
	wasmIngestionServiceMock.On("ProcessContractCode", mock.Anything, hash, code).Return(nil).Once()
	tokenProcessorMock.On("ProcessContractCode", mock.Anything, hash, code).Return(nil).Once()
	tokenProcessorMock.On("FlushBatchIfNeeded", mock.Anything).Return(nil).Once()

	// Finalization
	tokenProcessorMock.On("FlushRemainingBatch", mock.Anything).Return(nil).Once()
	tokenProcessorMock.On("Finalize", mock.Anything, mock.Anything).Return(nil).Once()
	wasmIngestionServiceMock.On("PersistKnownWasms", mock.Anything, mock.Anything).Return(nil).Once()
	contractValidatorMock.On("Close", mock.Anything).Return(nil).Once()

	err := svc.PopulateFromCheckpoint(context.Background(), 100, func(_ pgx.Tx) error { return nil })
	require.NoError(t, err)

	// ProcessEntry should NOT have been called
	tokenProcessorMock.AssertNotCalled(t, "ProcessEntry", mock.Anything, mock.Anything)
}

func TestCheckpointService_PopulateFromCheckpoint_NonContractCodeEntry(t *testing.T) {
	svc, readerMock, tokenIngestionServiceMock, tokenProcessorMock, wasmIngestionServiceMock, contractValidatorMock, _ := setupCheckpointTest(t)

	accountChange := makeAccountChange()

	// Reader returns one Account entry then EOF
	readerMock.On("Read").Return(accountChange, nil).Once()
	readerMock.On("Read").Return(ingest.Change{}, io.EOF).Once()
	readerMock.On("Close").Return(nil).Once()

	tokenIngestionServiceMock.On("NewTokenProcessor", mock.Anything, uint32(100)).Return(tokenProcessorMock).Once()

	// Only token processor ProcessEntry should be called
	tokenProcessorMock.On("ProcessEntry", mock.Anything, accountChange).Return(nil).Once()
	tokenProcessorMock.On("FlushBatchIfNeeded", mock.Anything).Return(nil).Once()

	// Finalization
	tokenProcessorMock.On("FlushRemainingBatch", mock.Anything).Return(nil).Once()
	tokenProcessorMock.On("Finalize", mock.Anything, mock.Anything).Return(nil).Once()
	wasmIngestionServiceMock.On("PersistKnownWasms", mock.Anything, mock.Anything).Return(nil).Once()
	contractValidatorMock.On("Close", mock.Anything).Return(nil).Once()

	err := svc.PopulateFromCheckpoint(context.Background(), 100, func(_ pgx.Tx) error { return nil })
	require.NoError(t, err)

	// WasmIngestionService.ProcessContractCode should NOT have been called
	wasmIngestionServiceMock.AssertNotCalled(t, "ProcessContractCode", mock.Anything, mock.Anything, mock.Anything)
}

func TestCheckpointService_PopulateFromCheckpoint_MixedEntries(t *testing.T) {
	svc, readerMock, tokenIngestionServiceMock, tokenProcessorMock, wasmIngestionServiceMock, contractValidatorMock, _ := setupCheckpointTest(t)

	hash1 := xdr.Hash{1}
	hash2 := xdr.Hash{2}
	code1 := []byte{0x01}
	code2 := []byte{0x02}
	contractCode1 := makeContractCodeChange(hash1, code1)
	accountChange := makeAccountChange()
	contractCode2 := makeContractCodeChange(hash2, code2)

	// Reader: ContractCode, Account, ContractCode, EOF
	readerMock.On("Read").Return(contractCode1, nil).Once()
	readerMock.On("Read").Return(accountChange, nil).Once()
	readerMock.On("Read").Return(contractCode2, nil).Once()
	readerMock.On("Read").Return(ingest.Change{}, io.EOF).Once()
	readerMock.On("Close").Return(nil).Once()

	tokenIngestionServiceMock.On("NewTokenProcessor", mock.Anything, uint32(100)).Return(tokenProcessorMock).Once()

	// 2 WASM calls
	wasmIngestionServiceMock.On("ProcessContractCode", mock.Anything, hash1, code1).Return(nil).Once()
	wasmIngestionServiceMock.On("ProcessContractCode", mock.Anything, hash2, code2).Return(nil).Once()

	// 2 token ProcessContractCode calls
	tokenProcessorMock.On("ProcessContractCode", mock.Anything, hash1, code1).Return(nil).Once()
	tokenProcessorMock.On("ProcessContractCode", mock.Anything, hash2, code2).Return(nil).Once()

	// 1 ProcessEntry call for Account
	tokenProcessorMock.On("ProcessEntry", mock.Anything, accountChange).Return(nil).Once()

	// 3 FlushBatchIfNeeded calls (one per entry)
	tokenProcessorMock.On("FlushBatchIfNeeded", mock.Anything).Return(nil).Times(3)

	// Finalization
	tokenProcessorMock.On("FlushRemainingBatch", mock.Anything).Return(nil).Once()
	tokenProcessorMock.On("Finalize", mock.Anything, mock.Anything).Return(nil).Once()
	wasmIngestionServiceMock.On("PersistKnownWasms", mock.Anything, mock.Anything).Return(nil).Once()
	contractValidatorMock.On("Close", mock.Anything).Return(nil).Once()

	err := svc.PopulateFromCheckpoint(context.Background(), 100, func(_ pgx.Tx) error { return nil })
	require.NoError(t, err)
}

func TestCheckpointService_PopulateFromCheckpoint_ErrorPropagation(t *testing.T) {
	tests := []struct {
		name           string
		setupMocks     func(*ChangeReaderMock, *TokenIngestionServiceMock, *TokenProcessorMock, *WasmIngestionServiceMock, *ContractValidatorMock) func(pgx.Tx) error
		expectedErrMsg string
	}{
		{
			name: "wasm_process_error",
			setupMocks: func(reader *ChangeReaderMock, tis *TokenIngestionServiceMock, tp *TokenProcessorMock, wis *WasmIngestionServiceMock, cv *ContractValidatorMock) func(pgx.Tx) error {
				change := makeContractCodeChange(xdr.Hash{1}, []byte{0x01})
				reader.On("Read").Return(change, nil).Once()
				reader.On("Close").Return(nil).Once()
				tis.On("NewTokenProcessor", mock.Anything, uint32(100)).Return(tp).Once()
				wis.On("ProcessContractCode", mock.Anything, mock.Anything, mock.Anything).Return(errors.New("wasm error")).Once()
				cv.On("Close", mock.Anything).Return(nil).Once()
				return func(_ pgx.Tx) error { return nil }
			},
			expectedErrMsg: "wasm service processing contract code",
		},
		{
			name: "token_process_contract_code_error",
			setupMocks: func(reader *ChangeReaderMock, tis *TokenIngestionServiceMock, tp *TokenProcessorMock, wis *WasmIngestionServiceMock, cv *ContractValidatorMock) func(pgx.Tx) error {
				change := makeContractCodeChange(xdr.Hash{1}, []byte{0x01})
				reader.On("Read").Return(change, nil).Once()
				reader.On("Close").Return(nil).Once()
				tis.On("NewTokenProcessor", mock.Anything, uint32(100)).Return(tp).Once()
				wis.On("ProcessContractCode", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
				tp.On("ProcessContractCode", mock.Anything, mock.Anything, mock.Anything).Return(errors.New("token contract code error")).Once()
				cv.On("Close", mock.Anything).Return(nil).Once()
				return func(_ pgx.Tx) error { return nil }
			},
			expectedErrMsg: "token processor processing contract code",
		},
		{
			name: "token_process_entry_error",
			setupMocks: func(reader *ChangeReaderMock, tis *TokenIngestionServiceMock, tp *TokenProcessorMock, wis *WasmIngestionServiceMock, cv *ContractValidatorMock) func(pgx.Tx) error {
				change := makeAccountChange()
				reader.On("Read").Return(change, nil).Once()
				reader.On("Close").Return(nil).Once()
				tis.On("NewTokenProcessor", mock.Anything, uint32(100)).Return(tp).Once()
				tp.On("ProcessEntry", mock.Anything, mock.Anything).Return(errors.New("process entry error")).Once()
				cv.On("Close", mock.Anything).Return(nil).Once()
				return func(_ pgx.Tx) error { return nil }
			},
			expectedErrMsg: "token processor processing entry",
		},
		{
			name: "flush_batch_error",
			setupMocks: func(reader *ChangeReaderMock, tis *TokenIngestionServiceMock, tp *TokenProcessorMock, wis *WasmIngestionServiceMock, cv *ContractValidatorMock) func(pgx.Tx) error {
				change := makeAccountChange()
				reader.On("Read").Return(change, nil).Once()
				reader.On("Close").Return(nil).Once()
				tis.On("NewTokenProcessor", mock.Anything, uint32(100)).Return(tp).Once()
				tp.On("ProcessEntry", mock.Anything, mock.Anything).Return(nil).Once()
				tp.On("FlushBatchIfNeeded", mock.Anything).Return(errors.New("flush error")).Once()
				cv.On("Close", mock.Anything).Return(nil).Once()
				return func(_ pgx.Tx) error { return nil }
			},
			expectedErrMsg: "flushing token batch",
		},
		{
			name: "flush_remaining_error",
			setupMocks: func(reader *ChangeReaderMock, tis *TokenIngestionServiceMock, tp *TokenProcessorMock, wis *WasmIngestionServiceMock, cv *ContractValidatorMock) func(pgx.Tx) error {
				reader.On("Read").Return(ingest.Change{}, io.EOF).Once()
				reader.On("Close").Return(nil).Once()
				tis.On("NewTokenProcessor", mock.Anything, uint32(100)).Return(tp).Once()
				tp.On("FlushRemainingBatch", mock.Anything).Return(errors.New("flush remaining error")).Once()
				cv.On("Close", mock.Anything).Return(nil).Once()
				return func(_ pgx.Tx) error { return nil }
			},
			expectedErrMsg: "flushing remaining token batch",
		},
		{
			name: "finalize_error",
			setupMocks: func(reader *ChangeReaderMock, tis *TokenIngestionServiceMock, tp *TokenProcessorMock, wis *WasmIngestionServiceMock, cv *ContractValidatorMock) func(pgx.Tx) error {
				reader.On("Read").Return(ingest.Change{}, io.EOF).Once()
				reader.On("Close").Return(nil).Once()
				tis.On("NewTokenProcessor", mock.Anything, uint32(100)).Return(tp).Once()
				tp.On("FlushRemainingBatch", mock.Anything).Return(nil).Once()
				tp.On("Finalize", mock.Anything, mock.Anything).Return(errors.New("finalize error")).Once()
				cv.On("Close", mock.Anything).Return(nil).Once()
				return func(_ pgx.Tx) error { return nil }
			},
			expectedErrMsg: "finalizing token processor",
		},
		{
			name: "persist_wasms_error",
			setupMocks: func(reader *ChangeReaderMock, tis *TokenIngestionServiceMock, tp *TokenProcessorMock, wis *WasmIngestionServiceMock, cv *ContractValidatorMock) func(pgx.Tx) error {
				reader.On("Read").Return(ingest.Change{}, io.EOF).Once()
				reader.On("Close").Return(nil).Once()
				tis.On("NewTokenProcessor", mock.Anything, uint32(100)).Return(tp).Once()
				tp.On("FlushRemainingBatch", mock.Anything).Return(nil).Once()
				tp.On("Finalize", mock.Anything, mock.Anything).Return(nil).Once()
				wis.On("PersistKnownWasms", mock.Anything, mock.Anything).Return(errors.New("persist error")).Once()
				cv.On("Close", mock.Anything).Return(nil).Once()
				return func(_ pgx.Tx) error { return nil }
			},
			expectedErrMsg: "persisting known wasms",
		},
		{
			name: "initialize_cursors_error",
			setupMocks: func(reader *ChangeReaderMock, tis *TokenIngestionServiceMock, tp *TokenProcessorMock, wis *WasmIngestionServiceMock, cv *ContractValidatorMock) func(pgx.Tx) error {
				reader.On("Read").Return(ingest.Change{}, io.EOF).Once()
				reader.On("Close").Return(nil).Once()
				tis.On("NewTokenProcessor", mock.Anything, uint32(100)).Return(tp).Once()
				tp.On("FlushRemainingBatch", mock.Anything).Return(nil).Once()
				tp.On("Finalize", mock.Anything, mock.Anything).Return(nil).Once()
				wis.On("PersistKnownWasms", mock.Anything, mock.Anything).Return(nil).Once()
				cv.On("Close", mock.Anything).Return(nil).Once()
				return func(_ pgx.Tx) error { return errors.New("cursor init failed") }
			},
			expectedErrMsg: "initializing cursors",
		},
		{
			name: "reader_read_error",
			setupMocks: func(reader *ChangeReaderMock, tis *TokenIngestionServiceMock, tp *TokenProcessorMock, wis *WasmIngestionServiceMock, cv *ContractValidatorMock) func(pgx.Tx) error {
				reader.On("Read").Return(ingest.Change{}, errors.New("network timeout")).Once()
				reader.On("Close").Return(nil).Once()
				tis.On("NewTokenProcessor", mock.Anything, uint32(100)).Return(tp).Once()
				cv.On("Close", mock.Anything).Return(nil).Once()
				return func(_ pgx.Tx) error { return nil }
			},
			expectedErrMsg: "reading checkpoint changes",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc, readerMock, tokenIngestionServiceMock, tokenProcessorMock, wasmIngestionServiceMock, contractValidatorMock, _ := setupCheckpointTest(t)
			initializeCursors := tt.setupMocks(readerMock, tokenIngestionServiceMock, tokenProcessorMock, wasmIngestionServiceMock, contractValidatorMock)

			err := svc.PopulateFromCheckpoint(context.Background(), 100, initializeCursors)
			require.Error(t, err)
			assert.ErrorContains(t, err, tt.expectedErrMsg)
		})
	}
}

func TestCheckpointService_PopulateFromCheckpoint_ContextCancellation(t *testing.T) {
	svc, readerMock, tokenIngestionServiceMock, tokenProcessorMock, _, _, _ := setupCheckpointTest(t)

	ctx, cancel := context.WithCancel(context.Background())

	readerMock.On("Close").Return(nil).Once()

	// Cancel context when NewTokenProcessor is called (after transaction starts, before loop)
	tokenIngestionServiceMock.On("NewTokenProcessor", mock.Anything, uint32(100)).
		Run(func(_ mock.Arguments) {
			cancel()
		}).
		Return(tokenProcessorMock).Once()

	// The contractValidator.Close is deferred and will be called with the cancelled context
	svc.contractValidator.(*ContractValidatorMock).On("Close", mock.Anything).Return(nil).Once()

	err := svc.PopulateFromCheckpoint(ctx, 100, func(_ pgx.Tx) error { return nil })
	require.Error(t, err)
	assert.ErrorContains(t, err, "checkpoint processing cancelled")
}
