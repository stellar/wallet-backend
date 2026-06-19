// Package indexer provides transaction indexing functionality for the wallet backend
package indexer

import (
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/alitto/pond/v2"
	set "github.com/deckarep/golang-set/v2"
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer/processors"
	"github.com/stellar/wallet-backend/internal/indexer/types"
)

const (
	// Test fixtures for ledger metadata
	ledgerMetadataWith0Tx = "AAAAAQAAAACB7Zh2o0NTFwl1nvs7xr3SJ7w8PpwnSRb8QyG9k6acEwAAABaeASPlzu/ZFxwyyWsxtGoj3KCrybm2yN7WOweR0BWdLYjyoO5BI41g1PFT+iHW68giP49Koo+q3VmH8I4GdtW2AAAAAGhTTB8AAAAAAAAAAQAAAAC1XRCyu30oTtXAOkel4bWQyQ9Xg1VHHMRQe76CBNI8iwAAAEDSH4sE7cL7UJyOqUo9ZZeNqPT7pt7su8iijHjWYg4MbeFUh/gkGf6N40bZjP/dlIuGXmuEhWoEX0VTV58xOB4C3z9hmASpL9tAVxktxD3XSOp3itxSvEmM6AUkwBS4ERm+pITz+1V1m+3/v6eaEKglCnon3a5xkn02sLltJ9CSzwAAEYIN4Lazp2QAAAAAAAMtYtQzAAAAAAAAAAAAAAAMAAAAZABMS0AAAADIXukLfWC53MCmzxKd/+LBbaYxQkgxATFDLI3hWj7EqWgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAGeASPlzu/ZFxwyyWsxtGoj3KCrybm2yN7WOweR0BWdLQAAAAIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA9yHMAAAAAAAAAAA=="
	ledgerMetadataWith1Tx = "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"
)

var (
	accountA = xdr.MustMuxedAddress("GBXGQJWVLWOYHFLVTKWV5FGHA3LNYY2JQKM7OAJAUEQFU6LPCSEFVXON")
	accountB = xdr.MustMuxedAddress("GCCOBXW2XQNUSL467IEILE6MMCNRR66SSVL4YQADUNYYNUVREF3FIV2Z")
	oneUnit  = xdr.Int64(1e7)

	testLcm = xdr.LedgerCloseMeta{
		V: int32(0),
		V0: &xdr.LedgerCloseMetaV0{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Header: xdr.LedgerHeader{
					LedgerVersion: 20,
					LedgerSeq:     xdr.Uint32(12345),
					ScpValue:      xdr.StellarValue{CloseTime: xdr.TimePoint(12345 * 100)},
				},
			},
			TxSet:              xdr.TransactionSet{},
			TxProcessing:       nil,
			UpgradesProcessing: nil,
			ScpInfo:            nil,
		},
		V1: nil,
	}

	testTx = ingest.LedgerTransaction{
		Index:  1,
		Ledger: testLcm,
		Hash:   xdr.Hash{1, 2, 3},
		Envelope: xdr.TransactionEnvelope{
			Type: xdr.EnvelopeTypeEnvelopeTypeTx,
			V1: &xdr.TransactionV1Envelope{
				Tx: xdr.Transaction{
					SourceAccount: xdr.MustMuxedAddress("GBF3XFXGBGNQDN3HOSZ7NVRF6TJ2JOD5U6ELIWJOOEI6T5WKMQT2YSXQ"),
					SeqNum:        xdr.SequenceNumber(54321),
				},
			},
		},
		Result: xdr.TransactionResultPair{
			TransactionHash: xdr.Hash{1, 2, 3},
			Result: xdr.TransactionResult{
				FeeCharged: xdr.Int64(100),
				Result: xdr.TransactionResultResult{
					Code:    xdr.TransactionResultCodeTxSuccess,
					Results: &[]xdr.OperationResult{},
				},
			},
		},
		UnsafeMeta: xdr.TransactionMeta{
			V: 3,
			V3: &xdr.TransactionMetaV3{
				Operations: []xdr.OperationMeta{{}},
			},
		},
	}

	testTx2 = ingest.LedgerTransaction{
		Index:  2,
		Ledger: testLcm,
		Hash:   xdr.Hash{4, 5, 6},
		Envelope: xdr.TransactionEnvelope{
			Type: xdr.EnvelopeTypeEnvelopeTypeTx,
			V1: &xdr.TransactionV1Envelope{
				Tx: xdr.Transaction{
					SourceAccount: xdr.MustMuxedAddress("GBXGQJWVLWOYHFLVTKWV5FGHA3LNYY2JQKM7OAJAUEQFU6LPCSEFVXON"),
					SeqNum:        xdr.SequenceNumber(54322),
				},
			},
		},
		Result: xdr.TransactionResultPair{
			TransactionHash: xdr.Hash{4, 5, 6},
			Result: xdr.TransactionResult{
				FeeCharged: xdr.Int64(200),
				Result: xdr.TransactionResultResult{
					Code:    xdr.TransactionResultCodeTxSuccess,
					Results: &[]xdr.OperationResult{},
				},
			},
		},
		UnsafeMeta: xdr.TransactionMeta{
			V: 3,
			V3: &xdr.TransactionMetaV3{
				Operations: []xdr.OperationMeta{{}, {}},
			},
		},
	}

	createAccountOp = xdr.Operation{
		SourceAccount: &accountA,
		Body: xdr.OperationBody{
			Type: xdr.OperationTypeCreateAccount,
			CreateAccountOp: &xdr.CreateAccountOp{
				Destination:     accountB.ToAccountId(),
				StartingBalance: 100 * oneUnit,
			},
		},
	}

	paymentOp = xdr.Operation{
		SourceAccount: &accountB,
		Body: xdr.OperationBody{
			Type: xdr.OperationTypePayment,
			PaymentOp: &xdr.PaymentOp{
				Destination: accountA,
				Asset:       xdr.Asset{Type: xdr.AssetTypeAssetTypeNative},
				Amount:      50 * oneUnit,
			},
		},
	}
)

func TestIndexer_NewIndexer(t *testing.T) {
	networkPassphrase := network.TestNetworkPassphrase
	pool := pond.NewPool(runtime.NumCPU())

	indexer := NewIndexer(networkPassphrase, pool, nil)

	require.NotNil(t, indexer)
	assert.NotNil(t, indexer.participantsProcessor)
	assert.NotNil(t, indexer.tokenTransferProcessor)
	assert.NotNil(t, indexer.processors)
	assert.NotNil(t, indexer.pool)
	assert.Len(t, indexer.processors, 3) // effects, contract deploy, SAC events
}

func TestIndexer_ProcessLedgerTransactions(t *testing.T) {
	t.Run("🟢 single transaction with participants", func(t *testing.T) {
		// Create mocks
		mockParticipants := &MockParticipantsProcessor{}
		mockTokenTransfer := &MockTokenTransferProcessor{}
		mockEffects := &MockOperationProcessor{}
		mockContractDeploy := &MockOperationProcessor{}
		mockSACEvents := &MockOperationProcessor{}
		mockTrustlines := &MockTrustlinesProcessor{}
		mockAccounts := &MockAccountsProcessor{}
		mockSACBalances := &MockSACBalancesProcessor{}
		mockSACInstances := &MockSACInstancesProcessor{}
		mockProtocolWasms := &MockProtocolWasmsProcessor{}
		mockProtocolContracts := &MockProtocolContractsProcessor{}

		// Setup mock expectations
		txParticipants := set.NewSet("alice", "bob")
		mockParticipants.On("GetTransactionParticipants", testTx).Return(txParticipants, nil)

		opParticipants := map[int64]processors.OperationParticipants{
			1: {
				OpWrapper: &processors.TransactionOperationWrapper{
					Index:          0,
					Operation:      createAccountOp,
					Network:        network.TestNetworkPassphrase,
					LedgerSequence: 12345,
				},
				Participants: set.NewSet("alice"),
			},
		}
		mockParticipants.On("GetOperationsParticipants", testTx).Return(opParticipants, nil)

		mockEffects.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.StateChange{
			{ToID: 1, AccountID: "alice", OperationID: 1},
		}, nil)
		mockContractDeploy.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.StateChange{
			{ToID: 2, AccountID: "charlie", OperationID: 1},
		}, nil)
		mockSACEvents.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.StateChange{}, nil)

		mockTokenTransfer.On("ProcessTransaction", mock.Anything, testTx).Return([]types.StateChange{
			{ToID: 3, AccountID: "dave", OperationID: 0},
		}, nil)

		mockTrustlines.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.TrustlineChange{}, nil)
		mockAccounts.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.AccountChange{}, nil)
		mockSACBalances.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.SACBalanceChange{}, nil)
		mockSACInstances.On("ProcessOperation", mock.Anything, mock.Anything).Return([]*data.Contract{}, nil)
		mockProtocolWasms.On("ProcessOperation", mock.Anything, mock.Anything).Return([]processors.ProtocolWasmObservation{}, nil)
		mockProtocolContracts.On("ProcessOperation", mock.Anything, mock.Anything).Return([]data.ProtocolContracts{}, nil)

		// Create indexer
		indexer := &Indexer{
			participantsProcessor:      mockParticipants,
			tokenTransferProcessor:     mockTokenTransfer,
			trustlinesProcessor:        mockTrustlines,
			accountsProcessor:          mockAccounts,
			sacBalancesProcessor:       mockSACBalances,
			sacInstancesProcessor:      mockSACInstances,
			protocolWasmsProcessor:     mockProtocolWasms,
			protocolContractsProcessor: mockProtocolContracts,
			processors:                 []OperationProcessorInterface{mockEffects, mockContractDeploy, mockSACEvents},
			pool:                       pond.NewPool(runtime.NumCPU()),
			networkPassphrase:          network.TestNetworkPassphrase,
		}

		// Test ProcessLedgerTransactions
		buffer := NewIndexerBuffer()
		participantCount, err := indexer.ProcessLedgerTransactions(context.Background(), []ingest.LedgerTransaction{testTx}, buffer)

		// Assert results
		require.NoError(t, err)
		assert.Equal(t, 4, participantCount) // alice, bob, charlie, dave

		// Verify transactions
		allTxs := buffer.GetTransactions()
		require.Len(t, allTxs, 1, "should have 1 transaction")

		// Verify transaction participants
		txParticipantsMap := buffer.GetTransactionsParticipants()
		toID := allTxs[0].ToID
		assert.True(t, txParticipantsMap[toID].Contains("alice"), "alice should be in tx participants")
		assert.True(t, txParticipantsMap[toID].Contains("bob"), "bob should be in tx participants")

		// Verify operations
		allOps := buffer.GetOperations()
		require.Len(t, allOps, 1, "should have 1 operation")

		// Verify state changes
		stateChanges := buffer.GetStateChanges()
		assert.Len(t, stateChanges, 3, "should have 3 state changes")

		// Verify mock expectations
		mockParticipants.AssertExpectations(t)
		mockTokenTransfer.AssertExpectations(t)
		mockEffects.AssertExpectations(t)
		mockContractDeploy.AssertExpectations(t)
		mockSACEvents.AssertExpectations(t)
		mockTrustlines.AssertExpectations(t)
		mockAccounts.AssertExpectations(t)
	})

	t.Run("🟢 multiple transactions with overlapping participants", func(t *testing.T) {
		// Create mocks
		mockParticipants := &MockParticipantsProcessor{}
		mockTokenTransfer := &MockTokenTransferProcessor{}
		mockEffects := &MockOperationProcessor{}
		mockContractDeploy := &MockOperationProcessor{}
		mockSACEvents := &MockOperationProcessor{}
		mockTrustlines := &MockTrustlinesProcessor{}
		mockAccounts := &MockAccountsProcessor{}
		mockSACBalances := &MockSACBalancesProcessor{}
		mockSACInstances := &MockSACInstancesProcessor{}
		mockProtocolWasms := &MockProtocolWasmsProcessor{}
		mockProtocolContracts := &MockProtocolContractsProcessor{}

		// Setup mocks for first transaction
		txParticipants1 := set.NewSet("alice", "bob")
		mockParticipants.On("GetTransactionParticipants", testTx).Return(txParticipants1, nil)
		opParticipants1 := map[int64]processors.OperationParticipants{
			1: {
				OpWrapper: &processors.TransactionOperationWrapper{
					Index:          0,
					Operation:      createAccountOp,
					Network:        network.TestNetworkPassphrase,
					LedgerSequence: 12345,
				},
				Participants: set.NewSet("alice"),
			},
		}
		mockParticipants.On("GetOperationsParticipants", testTx).Return(opParticipants1, nil)

		// Setup mocks for second transaction
		txParticipants2 := set.NewSet("bob", "charlie")
		mockParticipants.On("GetTransactionParticipants", testTx2).Return(txParticipants2, nil)
		opParticipants2 := map[int64]processors.OperationParticipants{
			2: {
				OpWrapper: &processors.TransactionOperationWrapper{
					Index:          0,
					Operation:      paymentOp,
					Network:        network.TestNetworkPassphrase,
					LedgerSequence: 12345,
				},
				Participants: set.NewSet("charlie"),
			},
		}
		mockParticipants.On("GetOperationsParticipants", testTx2).Return(opParticipants2, nil)

		mockEffects.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.StateChange{}, nil)
		mockContractDeploy.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.StateChange{}, nil)
		mockSACEvents.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.StateChange{}, nil)

		mockTokenTransfer.On("ProcessTransaction", mock.Anything, testTx).Return([]types.StateChange{}, nil)
		mockTokenTransfer.On("ProcessTransaction", mock.Anything, testTx2).Return([]types.StateChange{}, nil)

		mockTrustlines.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.TrustlineChange{}, nil)
		mockAccounts.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.AccountChange{}, nil)
		mockSACBalances.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.SACBalanceChange{}, nil)
		mockSACInstances.On("ProcessOperation", mock.Anything, mock.Anything).Return([]*data.Contract{}, nil)
		mockProtocolWasms.On("ProcessOperation", mock.Anything, mock.Anything).Return([]processors.ProtocolWasmObservation{}, nil)
		mockProtocolContracts.On("ProcessOperation", mock.Anything, mock.Anything).Return([]data.ProtocolContracts{}, nil)

		// Create indexer
		indexer := &Indexer{
			participantsProcessor:      mockParticipants,
			tokenTransferProcessor:     mockTokenTransfer,
			trustlinesProcessor:        mockTrustlines,
			accountsProcessor:          mockAccounts,
			sacBalancesProcessor:       mockSACBalances,
			sacInstancesProcessor:      mockSACInstances,
			protocolWasmsProcessor:     mockProtocolWasms,
			protocolContractsProcessor: mockProtocolContracts,
			processors:                 []OperationProcessorInterface{mockEffects, mockContractDeploy, mockSACEvents},
			pool:                       pond.NewPool(runtime.NumCPU()),
			networkPassphrase:          network.TestNetworkPassphrase,
		}

		// Test ProcessLedgerTransactions
		buffer := NewIndexerBuffer()
		participantCount, err := indexer.ProcessLedgerTransactions(context.Background(), []ingest.LedgerTransaction{testTx, testTx2}, buffer)

		// Assert results
		require.NoError(t, err)
		// alice, bob from tx1 + bob, charlie from tx2 = 2+2=4 (bob counted twice since per-tx)
		assert.Equal(t, 4, participantCount)

		// Verify transactions
		allTxs := buffer.GetTransactions()
		require.Len(t, allTxs, 2, "should have 2 transactions")

		// Verify mock expectations
		mockParticipants.AssertExpectations(t)
		mockTokenTransfer.AssertExpectations(t)
		mockEffects.AssertExpectations(t)
		mockContractDeploy.AssertExpectations(t)
		mockSACEvents.AssertExpectations(t)
		mockTrustlines.AssertExpectations(t)
		mockAccounts.AssertExpectations(t)
	})

	t.Run("🟢 empty transaction list", func(t *testing.T) {
		// Create mocks
		mockParticipants := &MockParticipantsProcessor{}
		mockTokenTransfer := &MockTokenTransferProcessor{}
		mockEffects := &MockOperationProcessor{}
		mockContractDeploy := &MockOperationProcessor{}
		mockSACEvents := &MockOperationProcessor{}

		// Create indexer
		indexer := &Indexer{
			participantsProcessor:  mockParticipants,
			tokenTransferProcessor: mockTokenTransfer,
			processors:             []OperationProcessorInterface{mockEffects, mockContractDeploy, mockSACEvents},
			pool:                   pond.NewPool(runtime.NumCPU()),
			networkPassphrase:      network.TestNetworkPassphrase,
		}

		// Test ProcessLedgerTransactions
		buffer := NewIndexerBuffer()
		participantCount, err := indexer.ProcessLedgerTransactions(context.Background(), []ingest.LedgerTransaction{}, buffer)

		// Assert results
		require.NoError(t, err)
		assert.Equal(t, 0, participantCount)
		assert.Equal(t, 0, buffer.GetNumberOfTransactions())

		// Verify mock expectations
		mockParticipants.AssertExpectations(t)
		mockTokenTransfer.AssertExpectations(t)
		mockEffects.AssertExpectations(t)
		mockContractDeploy.AssertExpectations(t)
		mockSACEvents.AssertExpectations(t)
	})

	t.Run("🟢 transaction with no participants", func(t *testing.T) {
		// Create mocks
		mockParticipants := &MockParticipantsProcessor{}
		mockTokenTransfer := &MockTokenTransferProcessor{}
		mockEffects := &MockOperationProcessor{}
		mockContractDeploy := &MockOperationProcessor{}
		mockSACEvents := &MockOperationProcessor{}

		// Setup mocks
		mockParticipants.On("GetTransactionParticipants", testTx).Return(set.NewSet[string](), nil)
		mockParticipants.On("GetOperationsParticipants", testTx).Return(map[int64]processors.OperationParticipants{}, nil)
		mockTokenTransfer.On("ProcessTransaction", mock.Anything, testTx).Return([]types.StateChange{}, nil)

		// Create indexer
		indexer := &Indexer{
			participantsProcessor:  mockParticipants,
			tokenTransferProcessor: mockTokenTransfer,
			processors:             []OperationProcessorInterface{mockEffects, mockContractDeploy, mockSACEvents},
			pool:                   pond.NewPool(runtime.NumCPU()),
			networkPassphrase:      network.TestNetworkPassphrase,
		}

		// Test ProcessLedgerTransactions
		buffer := NewIndexerBuffer()
		participantCount, err := indexer.ProcessLedgerTransactions(context.Background(), []ingest.LedgerTransaction{testTx}, buffer)

		// Assert results
		require.NoError(t, err)
		assert.Equal(t, 0, participantCount)

		// Verify mock expectations
		mockParticipants.AssertExpectations(t)
		mockTokenTransfer.AssertExpectations(t)
		mockEffects.AssertExpectations(t)
		mockContractDeploy.AssertExpectations(t)
		mockSACEvents.AssertExpectations(t)
	})

	t.Run("🔴 error getting transaction participants", func(t *testing.T) {
		// Create mocks
		mockParticipants := &MockParticipantsProcessor{}
		mockTokenTransfer := &MockTokenTransferProcessor{}
		mockEffects := &MockOperationProcessor{}
		mockContractDeploy := &MockOperationProcessor{}
		mockSACEvents := &MockOperationProcessor{}

		// Setup mocks
		mockParticipants.On("GetTransactionParticipants", testTx).Return(set.NewSet[string](), errors.New("participant error"))

		// Create indexer
		indexer := &Indexer{
			participantsProcessor:  mockParticipants,
			tokenTransferProcessor: mockTokenTransfer,
			processors:             []OperationProcessorInterface{mockEffects, mockContractDeploy, mockSACEvents},
			pool:                   pond.NewPool(runtime.NumCPU()),
			networkPassphrase:      network.TestNetworkPassphrase,
		}

		// Test ProcessLedgerTransactions
		buffer := NewIndexerBuffer()
		participantCount, err := indexer.ProcessLedgerTransactions(context.Background(), []ingest.LedgerTransaction{testTx}, buffer)

		// Assert results
		require.Error(t, err)
		assert.Contains(t, err.Error(), "getting transaction participants: participant error")
		assert.Equal(t, 0, participantCount)

		// Verify mock expectations
		mockParticipants.AssertExpectations(t)
		mockTokenTransfer.AssertExpectations(t)
		mockEffects.AssertExpectations(t)
		mockContractDeploy.AssertExpectations(t)
		mockSACEvents.AssertExpectations(t)
	})

	t.Run("🔴 error getting operations participants", func(t *testing.T) {
		// Create mocks
		mockParticipants := &MockParticipantsProcessor{}
		mockTokenTransfer := &MockTokenTransferProcessor{}
		mockEffects := &MockOperationProcessor{}
		mockContractDeploy := &MockOperationProcessor{}
		mockSACEvents := &MockOperationProcessor{}

		// Setup mocks
		mockParticipants.On("GetTransactionParticipants", testTx).Return(set.NewSet[string](), nil)
		mockParticipants.On("GetOperationsParticipants", testTx).Return(map[int64]processors.OperationParticipants{}, errors.New("operations error"))

		// Create indexer
		indexer := &Indexer{
			participantsProcessor:  mockParticipants,
			tokenTransferProcessor: mockTokenTransfer,
			processors:             []OperationProcessorInterface{mockEffects, mockContractDeploy, mockSACEvents},
			pool:                   pond.NewPool(runtime.NumCPU()),
			networkPassphrase:      network.TestNetworkPassphrase,
		}

		// Test ProcessLedgerTransactions
		buffer := NewIndexerBuffer()
		participantCount, err := indexer.ProcessLedgerTransactions(context.Background(), []ingest.LedgerTransaction{testTx}, buffer)

		// Assert results
		require.Error(t, err)
		assert.Contains(t, err.Error(), "getting operations participants: operations error")
		assert.Equal(t, 0, participantCount)

		// Verify mock expectations
		mockParticipants.AssertExpectations(t)
		mockTokenTransfer.AssertExpectations(t)
		mockEffects.AssertExpectations(t)
		mockContractDeploy.AssertExpectations(t)
		mockSACEvents.AssertExpectations(t)
	})

	t.Run("🔴 error in token transfer processor", func(t *testing.T) {
		// Create mocks
		mockParticipants := &MockParticipantsProcessor{}
		mockTokenTransfer := &MockTokenTransferProcessor{}
		mockEffects := &MockOperationProcessor{}
		mockContractDeploy := &MockOperationProcessor{}
		mockSACEvents := &MockOperationProcessor{}

		// Setup mocks
		mockParticipants.On("GetTransactionParticipants", testTx).Return(set.NewSet[string](), nil)
		mockParticipants.On("GetOperationsParticipants", testTx).Return(map[int64]processors.OperationParticipants{}, nil)
		mockTokenTransfer.On("ProcessTransaction", mock.Anything, testTx).Return([]types.StateChange{}, errors.New("token transfer error"))

		// Create indexer
		indexer := &Indexer{
			participantsProcessor:  mockParticipants,
			tokenTransferProcessor: mockTokenTransfer,
			processors:             []OperationProcessorInterface{mockEffects, mockContractDeploy, mockSACEvents},
			pool:                   pond.NewPool(runtime.NumCPU()),
			networkPassphrase:      network.TestNetworkPassphrase,
		}

		// Test ProcessLedgerTransactions
		buffer := NewIndexerBuffer()
		participantCount, err := indexer.ProcessLedgerTransactions(context.Background(), []ingest.LedgerTransaction{testTx}, buffer)

		// Assert results
		require.Error(t, err)
		assert.Contains(t, err.Error(), "processing token transfer state changes: token transfer error")
		assert.Equal(t, 0, participantCount)

		// Verify mock expectations
		mockParticipants.AssertExpectations(t)
		mockTokenTransfer.AssertExpectations(t)
		mockEffects.AssertExpectations(t)
		mockContractDeploy.AssertExpectations(t)
		mockSACEvents.AssertExpectations(t)
	})

	t.Run("🔴 error in operation processor", func(t *testing.T) {
		// Create mocks
		mockParticipants := &MockParticipantsProcessor{}
		mockTokenTransfer := &MockTokenTransferProcessor{}
		mockEffects := &MockOperationProcessor{}
		mockContractDeploy := &MockOperationProcessor{}
		mockSACEvents := &MockOperationProcessor{}

		// Setup mocks
		mockParticipants.On("GetTransactionParticipants", testTx).Return(set.NewSet[string](), nil)
		opParticipants := map[int64]processors.OperationParticipants{
			1: {
				OpWrapper: &processors.TransactionOperationWrapper{
					Index:          0,
					Operation:      createAccountOp,
					Network:        network.TestNetworkPassphrase,
					LedgerSequence: 12345,
				},
				Participants: set.NewSet("alice"),
			},
		}
		mockParticipants.On("GetOperationsParticipants", testTx).Return(opParticipants, nil)
		mockEffects.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.StateChange{}, errors.New("effects error"))
		mockEffects.On("Name").Return("effects")

		// Create indexer
		indexer := &Indexer{
			participantsProcessor:  mockParticipants,
			tokenTransferProcessor: mockTokenTransfer,
			processors:             []OperationProcessorInterface{mockEffects, mockContractDeploy, mockSACEvents},
			pool:                   pond.NewPool(runtime.NumCPU()),
			networkPassphrase:      network.TestNetworkPassphrase,
		}

		// Test ProcessLedgerTransactions
		buffer := NewIndexerBuffer()
		participantCount, err := indexer.ProcessLedgerTransactions(context.Background(), []ingest.LedgerTransaction{testTx}, buffer)

		// Assert results
		require.Error(t, err)
		assert.Contains(t, err.Error(), "processing effects state changes: effects error")
		assert.Equal(t, 0, participantCount)

		// Verify mock expectations
		mockParticipants.AssertExpectations(t)
		mockTokenTransfer.AssertExpectations(t)
		mockEffects.AssertExpectations(t)
		mockContractDeploy.AssertExpectations(t)
		mockSACEvents.AssertExpectations(t)
	})

	t.Run("🟢 multiple state changes per operation", func(t *testing.T) {
		// Create mocks
		mockParticipants := &MockParticipantsProcessor{}
		mockTokenTransfer := &MockTokenTransferProcessor{}
		mockEffects := &MockOperationProcessor{}
		mockContractDeploy := &MockOperationProcessor{}
		mockSACEvents := &MockOperationProcessor{}
		mockTrustlines := &MockTrustlinesProcessor{}
		mockAccounts := &MockAccountsProcessor{}
		mockSACBalances := &MockSACBalancesProcessor{}
		mockSACInstances := &MockSACInstancesProcessor{}
		mockProtocolWasms := &MockProtocolWasmsProcessor{}
		mockProtocolContracts := &MockProtocolContractsProcessor{}

		// Setup mock expectations
		txParticipants := set.NewSet("alice")
		mockParticipants.On("GetTransactionParticipants", testTx).Return(txParticipants, nil)

		opParticipants := map[int64]processors.OperationParticipants{
			1: {
				OpWrapper: &processors.TransactionOperationWrapper{
					Index:          0,
					Operation:      createAccountOp,
					Network:        network.TestNetworkPassphrase,
					LedgerSequence: 12345,
				},
				Participants: set.NewSet("alice"),
			},
		}
		mockParticipants.On("GetOperationsParticipants", testTx).Return(opParticipants, nil)

		mockEffects.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.StateChange{
			{ToID: 1, AccountID: "alice", OperationID: 1},
			{ToID: 2, AccountID: "alice", OperationID: 1},
			{ToID: 3, AccountID: "alice", OperationID: 1},
		}, nil)
		mockContractDeploy.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.StateChange{}, nil)
		mockSACEvents.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.StateChange{}, nil)

		mockTokenTransfer.On("ProcessTransaction", mock.Anything, testTx).Return([]types.StateChange{}, nil)

		mockTrustlines.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.TrustlineChange{}, nil)
		mockAccounts.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.AccountChange{}, nil)
		mockSACBalances.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.SACBalanceChange{}, nil)
		mockSACInstances.On("ProcessOperation", mock.Anything, mock.Anything).Return([]*data.Contract{}, nil)
		mockProtocolWasms.On("ProcessOperation", mock.Anything, mock.Anything).Return([]processors.ProtocolWasmObservation{}, nil)
		mockProtocolContracts.On("ProcessOperation", mock.Anything, mock.Anything).Return([]data.ProtocolContracts{}, nil)

		// Create indexer
		indexer := &Indexer{
			participantsProcessor:      mockParticipants,
			tokenTransferProcessor:     mockTokenTransfer,
			trustlinesProcessor:        mockTrustlines,
			accountsProcessor:          mockAccounts,
			sacBalancesProcessor:       mockSACBalances,
			sacInstancesProcessor:      mockSACInstances,
			protocolWasmsProcessor:     mockProtocolWasms,
			protocolContractsProcessor: mockProtocolContracts,
			processors:                 []OperationProcessorInterface{mockEffects, mockContractDeploy, mockSACEvents},
			pool:                       pond.NewPool(runtime.NumCPU()),
			networkPassphrase:          network.TestNetworkPassphrase,
		}

		// Test ProcessLedgerTransactions
		buffer := NewIndexerBuffer()
		participantCount, err := indexer.ProcessLedgerTransactions(context.Background(), []ingest.LedgerTransaction{testTx}, buffer)

		// Assert
		require.NoError(t, err)
		assert.Equal(t, 1, participantCount)

		// Verify state changes are all present (no ordering guarantee)
		stateChanges := buffer.GetStateChanges()
		require.Len(t, stateChanges, 3, "should have 3 state changes")

		for _, sc := range stateChanges {
			assert.Equal(t, "alice", sc.AccountID.String())
			assert.Equal(t, int64(1), sc.OperationID)
		}

		// Verify mock expectations
		mockParticipants.AssertExpectations(t)
		mockTokenTransfer.AssertExpectations(t)
		mockEffects.AssertExpectations(t)
		mockContractDeploy.AssertExpectations(t)
		mockSACEvents.AssertExpectations(t)
		mockTrustlines.AssertExpectations(t)
		mockAccounts.AssertExpectations(t)
	})
}

func TestIndexer_getTransactionStateChanges(t *testing.T) {
	t.Run("🟢 state changes from all processors", func(t *testing.T) {
		// Create mocks
		mockTokenTransfer := &MockTokenTransferProcessor{}
		mockEffects := &MockOperationProcessor{}
		mockContractDeploy := &MockOperationProcessor{}
		mockSACEvents := &MockOperationProcessor{}

		// Setup mocks
		opsParticipants := map[int64]processors.OperationParticipants{
			1: {
				OpWrapper: &processors.TransactionOperationWrapper{
					Index:          0,
					Operation:      createAccountOp,
					Network:        network.TestNetworkPassphrase,
					LedgerSequence: 12345,
				},
				Participants: set.NewSet("alice"),
			},
		}

		mockEffects.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.StateChange{
			{ToID: 1, AccountID: "alice", OperationID: 1},
		}, nil)
		mockContractDeploy.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.StateChange{
			{ToID: 2, AccountID: "bob", OperationID: 1},
		}, nil)
		mockSACEvents.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.StateChange{}, nil)

		mockTokenTransfer.On("ProcessTransaction", mock.Anything, testTx).Return([]types.StateChange{
			{ToID: 3, AccountID: "charlie", OperationID: 0},
		}, nil)

		// Create indexer
		indexer := &Indexer{
			participantsProcessor:  &MockParticipantsProcessor{},
			tokenTransferProcessor: mockTokenTransfer,
			processors:             []OperationProcessorInterface{mockEffects, mockContractDeploy, mockSACEvents},
			pool:                   pond.NewPool(runtime.NumCPU()),
		}

		// Test getTransactionStateChanges
		stateChanges, err := indexer.getTransactionStateChanges(context.Background(), testTx, opsParticipants)

		// Assert
		require.NoError(t, err)
		assert.Len(t, stateChanges, 3)

		// Verify specific state changes by AccountID
		foundAlice := false
		foundBob := false
		foundCharlie := false
		for _, sc := range stateChanges {
			switch sc.AccountID.String() {
			case "alice":
				assert.Equal(t, int64(1), sc.ToID)
				assert.Equal(t, int64(1), sc.OperationID)
				foundAlice = true
			case "bob":
				assert.Equal(t, int64(2), sc.ToID)
				assert.Equal(t, int64(1), sc.OperationID)
				foundBob = true
			case "charlie":
				assert.Equal(t, int64(3), sc.ToID)
				assert.Equal(t, int64(0), sc.OperationID)
				foundCharlie = true
			}
		}
		assert.True(t, foundAlice, "Should have state change for alice")
		assert.True(t, foundBob, "Should have state change for bob")
		assert.True(t, foundCharlie, "Should have state change for charlie")

		// Verify mock expectations
		mockTokenTransfer.AssertExpectations(t)
		mockEffects.AssertExpectations(t)
		mockContractDeploy.AssertExpectations(t)
		mockSACEvents.AssertExpectations(t)
	})

	t.Run("🟢 no operations", func(t *testing.T) {
		// Create mocks
		mockTokenTransfer := &MockTokenTransferProcessor{}
		mockEffects := &MockOperationProcessor{}
		mockContractDeploy := &MockOperationProcessor{}
		mockSACEvents := &MockOperationProcessor{}

		// Setup mocks
		opsParticipants := map[int64]processors.OperationParticipants{}
		mockTokenTransfer.On("ProcessTransaction", mock.Anything, testTx).Return([]types.StateChange{}, nil)

		// Create indexer
		indexer := &Indexer{
			participantsProcessor:  &MockParticipantsProcessor{},
			tokenTransferProcessor: mockTokenTransfer,
			processors:             []OperationProcessorInterface{mockEffects, mockContractDeploy, mockSACEvents},
			pool:                   pond.NewPool(runtime.NumCPU()),
		}

		// Test getTransactionStateChanges
		stateChanges, err := indexer.getTransactionStateChanges(context.Background(), testTx, opsParticipants)

		// Assert
		require.NoError(t, err)
		assert.Len(t, stateChanges, 0)

		// Verify mock expectations
		mockTokenTransfer.AssertExpectations(t)
		mockEffects.AssertExpectations(t)
		mockContractDeploy.AssertExpectations(t)
		mockSACEvents.AssertExpectations(t)
	})

	t.Run("🔴 error in operation processor", func(t *testing.T) {
		// Create mocks
		mockTokenTransfer := &MockTokenTransferProcessor{}
		mockEffects := &MockOperationProcessor{}
		mockContractDeploy := &MockOperationProcessor{}
		mockSACEvents := &MockOperationProcessor{}

		// Setup mocks
		opsParticipants := map[int64]processors.OperationParticipants{
			1: {
				OpWrapper: &processors.TransactionOperationWrapper{
					Index:          0,
					Operation:      createAccountOp,
					Network:        network.TestNetworkPassphrase,
					LedgerSequence: 12345,
				},
				Participants: set.NewSet("alice"),
			},
		}

		mockEffects.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.StateChange{}, errors.New("processor error"))
		mockEffects.On("Name").Return("effects")

		// Create indexer
		indexer := &Indexer{
			participantsProcessor:  &MockParticipantsProcessor{},
			tokenTransferProcessor: mockTokenTransfer,
			processors:             []OperationProcessorInterface{mockEffects, mockContractDeploy, mockSACEvents},
			pool:                   pond.NewPool(runtime.NumCPU()),
		}

		// Test getTransactionStateChanges
		stateChanges, err := indexer.getTransactionStateChanges(context.Background(), testTx, opsParticipants)

		// Assert
		require.Error(t, err)
		assert.Contains(t, err.Error(), "processing effects state changes: processor error")
		assert.Nil(t, stateChanges)

		// Verify mock expectations
		mockTokenTransfer.AssertExpectations(t)
		mockEffects.AssertExpectations(t)
		mockContractDeploy.AssertExpectations(t)
		mockSACEvents.AssertExpectations(t)
	})

	t.Run("🔴 error in token transfer processor", func(t *testing.T) {
		// Create mocks
		mockTokenTransfer := &MockTokenTransferProcessor{}
		mockEffects := &MockOperationProcessor{}
		mockContractDeploy := &MockOperationProcessor{}
		mockSACEvents := &MockOperationProcessor{}

		// Setup mocks
		opsParticipants := map[int64]processors.OperationParticipants{}
		mockTokenTransfer.On("ProcessTransaction", mock.Anything, testTx).Return([]types.StateChange{}, errors.New("token transfer error"))

		// Create indexer
		indexer := &Indexer{
			participantsProcessor:  &MockParticipantsProcessor{},
			tokenTransferProcessor: mockTokenTransfer,
			processors:             []OperationProcessorInterface{mockEffects, mockContractDeploy, mockSACEvents},
			pool:                   pond.NewPool(runtime.NumCPU()),
		}

		// Test getTransactionStateChanges
		stateChanges, err := indexer.getTransactionStateChanges(context.Background(), testTx, opsParticipants)

		// Assert
		require.Error(t, err)
		assert.Contains(t, err.Error(), "processing token transfer state changes: token transfer error")
		assert.Nil(t, stateChanges)

		// Verify mock expectations
		mockTokenTransfer.AssertExpectations(t)
		mockEffects.AssertExpectations(t)
		mockContractDeploy.AssertExpectations(t)
		mockSACEvents.AssertExpectations(t)
	})

	t.Run("🟢 ErrInvalidOpType is ignored", func(t *testing.T) {
		// Create mocks
		mockTokenTransfer := &MockTokenTransferProcessor{}
		mockEffects := &MockOperationProcessor{}
		mockContractDeploy := &MockOperationProcessor{}
		mockSACEvents := &MockOperationProcessor{}

		// Setup mocks
		opsParticipants := map[int64]processors.OperationParticipants{
			1: {
				OpWrapper: &processors.TransactionOperationWrapper{
					Index:          0,
					Operation:      createAccountOp,
					Network:        network.TestNetworkPassphrase,
					LedgerSequence: 12345,
				},
				Participants: set.NewSet("alice"),
			},
		}

		// One processor returns ErrInvalidOpType (should be ignored)
		mockEffects.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.StateChange{}, processors.ErrInvalidOpType)
		// Other processors work normally
		mockContractDeploy.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.StateChange{
			{ToID: 1, AccountID: "alice", OperationID: 1},
		}, nil)
		mockSACEvents.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.StateChange{}, nil)

		mockTokenTransfer.On("ProcessTransaction", mock.Anything, testTx).Return([]types.StateChange{}, nil)

		// Create indexer
		indexer := &Indexer{
			participantsProcessor:  &MockParticipantsProcessor{},
			tokenTransferProcessor: mockTokenTransfer,
			processors:             []OperationProcessorInterface{mockEffects, mockContractDeploy, mockSACEvents},
			pool:                   pond.NewPool(runtime.NumCPU()),
		}

		// Test getTransactionStateChanges
		stateChanges, err := indexer.getTransactionStateChanges(context.Background(), testTx, opsParticipants)

		// Assert
		require.NoError(t, err)
		assert.Len(t, stateChanges, 1)

		// Verify it's the correct state change
		sc := stateChanges[0]
		assert.Equal(t, "alice", sc.AccountID.String())
		assert.Equal(t, int64(1), sc.ToID)
		assert.Equal(t, int64(1), sc.OperationID)

		// Verify mock expectations
		mockTokenTransfer.AssertExpectations(t)
		mockEffects.AssertExpectations(t)
		mockContractDeploy.AssertExpectations(t)
		mockSACEvents.AssertExpectations(t)
	})
}

func TestIndexer_GetLedgerTransactions(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name                    string
		inputLedgerCloseMetaStr string
		wantErrContains         string
		wantResultTxHashes      []string
	}{
		{
			name:                    "🟢successful_transaction_reading_0_tx",
			inputLedgerCloseMetaStr: ledgerMetadataWith0Tx,
		},
		{
			name:                    "🟢successful_transaction_reading_1_tx",
			inputLedgerCloseMetaStr: ledgerMetadataWith1Tx,
			wantErrContains:         "",
			wantResultTxHashes:      []string{"5c54a259cc690d16bc7eb06cd706c468c530e2131eedcb45b5dc370a2ef7204c"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var xdrLedgerCloseMeta xdr.LedgerCloseMeta
			err := xdr.SafeUnmarshalBase64(tc.inputLedgerCloseMetaStr, &xdrLedgerCloseMeta)
			require.NoError(t, err)
			transactions, err := GetLedgerTransactions(ctx, network.TestNetworkPassphrase, xdrLedgerCloseMeta)

			// Verify results
			if tc.wantErrContains != "" {
				require.Error(t, err)
				assert.ErrorContains(t, err, tc.wantErrContains)
			} else {
				require.NoError(t, err)
				assert.Len(t, transactions, len(tc.wantResultTxHashes))

				// Verify transaction hashes if we have expected results
				if len(tc.wantResultTxHashes) > 0 {
					for i, expectedHash := range tc.wantResultTxHashes {
						assert.Equal(t, expectedHash, transactions[i].Hash.HexString())
					}
				}
			}
		})
	}
}

// extractContractEventsViaReader is the reader-based reference implementation of
// ExtractContractEventsForLedger, kept as the oracle for the differential
// equivalence test. It constructs a LedgerTransactionReader (which re-hashes
// every transaction envelope) and reads events through the resulting
// LedgerTransaction values. The production function must produce output equal to
// this oracle for every committed ledger fixture.
func extractContractEventsViaReader(ctx context.Context, networkPassphrase string, ledgerMeta xdr.LedgerCloseMeta) (map[ContractEventKey][]xdr.ContractEvent, error) {
	transactions, err := GetLedgerTransactions(ctx, networkPassphrase, ledgerMeta)
	if err != nil {
		return nil, fmt.Errorf("getting transactions for ledger %d: %w", ledgerMeta.LedgerSequence(), err)
	}

	out := make(map[ContractEventKey][]xdr.ContractEvent)
	for _, tx := range transactions {
		if !tx.Result.Successful() {
			continue
		}
		for opIdx, op := range tx.Envelope.Operations() {
			if op.Body.Type != xdr.OperationTypeInvokeHostFunction {
				continue
			}
			events, evErr := tx.GetContractEventsForOperation(uint32(opIdx))
			if evErr != nil {
				return nil, fmt.Errorf("extracting contract events for ledger %d tx %d op %d: %w",
					ledgerMeta.LedgerSequence(), tx.Index, opIdx, evErr)
			}
			if len(events) == 0 {
				continue
			}
			out[ContractEventKey{TxIdx: tx.Index, OpIdx: uint32(opIdx)}] = events
		}
	}
	return out, nil
}

// loadLedgerFixture reads a gzip-compressed XDR LedgerCloseMeta from testdata/.
//
// Fixtures are pubnet LedgerCloseMeta captured from the public data lake
// (config/datastore-pubnet.toml). To refresh or expand the corpus, run a small
// program from the repo root that builds an ingest.NewLedgerBackend with
// LedgerBackendType=datastore + NetworkPassphrase=PublicNetworkPassphrase,
// discovers the lake tip via datastore.FindLatestLedgerSequence, then for each
// sequence calls backend.GetLedger(seq), lcm.MarshalBinary(), gzips the bytes,
// and writes testdata/ledger-<seq>.xdr.gz.
func loadLedgerFixture(t *testing.T, path string) xdr.LedgerCloseMeta {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()

	gz, err := gzip.NewReader(f)
	require.NoError(t, err)
	defer gz.Close()

	raw, err := io.ReadAll(gz)
	require.NoError(t, err)

	var lcm xdr.LedgerCloseMeta
	require.NoError(t, lcm.UnmarshalBinary(raw))
	return lcm
}

func TestExtractContractEventsForLedger_EquivalenceOnRealLedgers(t *testing.T) {
	ctx := context.Background()

	paths, err := filepath.Glob("testdata/*.xdr.gz")
	require.NoError(t, err)
	require.NotEmpty(t, paths, "no ledger fixtures under testdata/ — regenerate per the loadLedgerFixture recipe")

	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			lcm := loadLedgerFixture(t, path)

			want, err := extractContractEventsViaReader(ctx, network.PublicNetworkPassphrase, lcm)
			require.NoError(t, err)

			got, err := ExtractContractEventsForLedger(ctx, network.PublicNetworkPassphrase, lcm)
			require.NoError(t, err)

			require.Equal(t, want, got)
		})
	}
}

// newSyntheticLedgerCloseMeta builds a minimal single-transaction V0
// LedgerCloseMeta carrying a successful result with the given operation results
// and apply meta. It omits the TxSet/envelopes: the meta-only extractor never
// reads them, so this isolates op-result + apply-meta behavior that real
// fixtures can't produce on demand.
func newSyntheticLedgerCloseMeta(seq uint32, opResults []xdr.OperationResult, applyMeta xdr.TransactionMeta) xdr.LedgerCloseMeta {
	results := opResults
	return xdr.LedgerCloseMeta{
		V: 0,
		V0: &xdr.LedgerCloseMetaV0{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Header: xdr.LedgerHeader{LedgerSeq: xdr.Uint32(seq)},
			},
			TxProcessing: []xdr.TransactionResultMeta{
				{
					Result: xdr.TransactionResultPair{
						Result: xdr.TransactionResult{
							Result: xdr.TransactionResultResult{
								Code:    xdr.TransactionResultCodeTxSuccess,
								Results: &results,
							},
						},
					},
					TxApplyProcessing: applyMeta,
				},
			},
		},
	}
}

// TestExtractContractEventsForLedger_V4OperationsShorterThanResults proves the
// result-Tr.Type filter prevents indexing past the end of TransactionMetaV4.Operations.
// The V4 apply meta has ONE operation meta entry (index 0), but the tx has TWO
// operation results: [0] InvokeHostFunction, [1] Payment. A walk that indexed
// Operations by every result index would panic on Operations[1]; the filter
// skips the non-InvokeHostFunction result before it is ever used to index.
func TestExtractContractEventsForLedger_V4OperationsShorterThanResults(t *testing.T) {
	ctx := context.Background()

	applyMeta := xdr.TransactionMeta{
		V: 4,
		V4: &xdr.TransactionMetaV4{
			Operations: []xdr.OperationMetaV2{
				{Events: []xdr.ContractEvent{{Type: xdr.ContractEventTypeContract}}}, // index 0 only
			},
		},
	}
	opResults := []xdr.OperationResult{
		{Code: xdr.OperationResultCodeOpInner, Tr: &xdr.OperationResultTr{Type: xdr.OperationTypeInvokeHostFunction}},
		{Code: xdr.OperationResultCodeOpInner, Tr: &xdr.OperationResultTr{Type: xdr.OperationTypePayment}},
	}

	lcm := newSyntheticLedgerCloseMeta(100, opResults, applyMeta)

	out, err := ExtractContractEventsForLedger(ctx, network.PublicNetworkPassphrase, lcm)
	require.NoError(t, err)
	require.Len(t, out, 1)
	events, ok := out[ContractEventKey{TxIdx: 1, OpIdx: 0}]
	require.True(t, ok, "expected events at tx 1 op 0")
	require.Len(t, events, 1)
}

// TestExtractContractEventsForLedger_UnknownMetaVersionErrors confirms the one
// intentional behavior change: an unsupported TransactionMeta version surfaces
// as a propagated error (fail loud) rather than being silently dropped.
func TestExtractContractEventsForLedger_UnknownMetaVersionErrors(t *testing.T) {
	ctx := context.Background()

	opResults := []xdr.OperationResult{
		{Code: xdr.OperationResultCodeOpInner, Tr: &xdr.OperationResultTr{Type: xdr.OperationTypeInvokeHostFunction}},
	}
	applyMeta := xdr.TransactionMeta{V: 99} // unsupported; all arm pointers nil

	lcm := newSyntheticLedgerCloseMeta(101, opResults, applyMeta)

	_, err := ExtractContractEventsForLedger(ctx, network.PublicNetworkPassphrase, lcm)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported TransactionMeta version")
}
