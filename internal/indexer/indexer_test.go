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
	ledgerMetadataWith1Tx = "AAAAAQAAAAD8G2qemHnBKFkbq90RTagxAypNnA7DXDc63Giipq9mNwAAABYLEZ5DrTv6njXTOAFEdOO0yeLtJjCRyH4ryJkgpRh7VPJvwbisrc9A0yzFxxCdkICgB3Gv7qHOi8ZdsK2CNks2AAAAAGhTTAsAAAAAAAAAAQAAAACoJM0YvJ11Bk0pmltbrKQ7w6ovMmk4FT2ML5u1y23wMwAAAEAunZtorOSbnRpgnykoDe4kzAvLwNXefncy1R/1ynBWyDv0DfdnqJ6Hcy/0AJf6DkBZlRayg775h3HjV0GKF/oPua7l8wkLlJBtSk1kRDt55qSf6btSrgcupB/8bnpJfUUgZJ76saUrj29HukYHS1bq7SyuoCAY+5F9iBYTmW1G9QAAEX4N4Lazp2QAAAAAAAMtS3veAAAAAAAAAAAAAAAMAAAAZABMS0AAAADIXukLfWC53MCmzxKd/+LBbaYxQkgxATFDLI3hWj7EqWgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAELEZ5DrTv6njXTOAFEdOO0yeLtJjCRyH4ryJkgpRh7VAAAAAIAAAAAAAAAAQAAAAAAAAABAAAAAAAAAGQAAAABAAAAAgAAAADg4mtiLKjJVgrmOpO9+Ff3XAmnycHyNUKu/v9KhHevAAAAAGQAAA7FAAAAGgAAAAAAAAAAAAAAAQAAAAAAAAABAAAAALvqzdVyRxgBMcLzbw1wNWcJYHPNPok1GdVSgmy4sjR2AAAAAVVTREMAAAAA4OJrYiyoyVYK5jqTvfhX91wJp8nB8jVCrv7/SoR3rwAAAAACVAvkAAAAAAAAAAABhHevAAAAAEDq2yIDzXUoLboBHQkbr8U2oKqLzf0gfpwXbmRPLB6Ek3G8uCEYyry1vt5Sb+LCEd81fefFQcQN0nydr1FmiXcDAAAAAAAAAAAAAAABXFSiWcxpDRa8frBs1wbEaMUw4hMe7ctFtdw3Ci73IEwAAAAAAAAAZAAAAAAAAAABAAAAAAAAAAEAAAAAAAAAAAAAAAIAAAADAAARfQAAAAAAAAAA4OJrYiyoyVYK5jqTvfhX91wJp8nB8jVCrv7/SoR3rwAAAAAukO3GPAAADsUAAAAZAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAACAAAAAAAAAAAAAAAAAAAAAwAAAAAAABF9AAAAAGhTTAYAAAAAAAAAAQAAEX4AAAAAAAAAAODia2IsqMlWCuY6k734V/dcCafJwfI1Qq7+/0qEd68AAAAALpDtxdgAAA7FAAAAGQAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAgAAAAAAAAAAAAAAAAAAAAMAAAAAAAARfQAAAABoU0wGAAAAAAAAAAMAAAAAAAAAAgAAAAMAABF+AAAAAAAAAADg4mtiLKjJVgrmOpO9+Ff3XAmnycHyNUKu/v9KhHevAAAAAC6Q7cXYAAAOxQAAABkAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAAAAAAAAAAAADAAAAAAAAEX0AAAAAaFNMBgAAAAAAAAABAAARfgAAAAAAAAAA4OJrYiyoyVYK5jqTvfhX91wJp8nB8jVCrv7/SoR3rwAAAAAukO3F2AAADsUAAAAaAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAACAAAAAAAAAAAAAAAAAAAAAwAAAAAAABF+AAAAAGhTTAsAAAAAAAAAAQAAAAIAAAADAAARcwAAAAEAAAAAu+rN1XJHGAExwvNvDXA1Zwlgc80+iTUZ1VKCbLiyNHYAAAABVVNEQwAAAADg4mtiLKjJVgrmOpO9+Ff3XAmnycHyNUKu/v9KhHevAAAAAAlQL5AAf/////////8AAAABAAAAAAAAAAAAAAABAAARfgAAAAEAAAAAu+rN1XJHGAExwvNvDXA1Zwlgc80+iTUZ1VKCbLiyNHYAAAABVVNEQwAAAADg4mtiLKjJVgrmOpO9+Ff3XAmnycHyNUKu/v9KhHevAAAAAAukO3QAf/////////8AAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA8RxEAAAAAAAAAAA=="
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

	indexer, err := NewIndexer(networkPassphrase, pool, nil)

	require.NoError(t, err)
	require.NotNil(t, indexer)
	assert.NotNil(t, indexer.participantsProcessor)
	assert.NotNil(t, indexer.tokenTransferProcessor)
	assert.NotNil(t, indexer.processors)
	assert.NotNil(t, indexer.pool)
	assert.Len(t, indexer.processors, 3) // effects, contract deploy, SAC events
}

func TestValidateStateChangeSubBases(t *testing.T) {
	newProc := func(name string, subBase int64) *MockOperationProcessor {
		p := &MockOperationProcessor{SubBase: subBase}
		p.On("Name").Return(name).Maybe()
		return p
	}

	testCases := []struct {
		name            string
		procs           []OperationProcessorInterface
		wantErrContains string
	}{
		{
			name: "🟢 distinct aligned sub-bases",
			procs: []OperationProcessorInterface{
				newProc("effects", types.StateChangeSubBaseEffects),
				newProc("contract_deploy", types.StateChangeSubBaseContractDeploy),
			},
		},
		{
			name: "🔴 duplicate sub-base between two processors",
			procs: []OperationProcessorInterface{
				newProc("effects", types.StateChangeSubBaseEffects),
				newProc("copy_pasted", types.StateChangeSubBaseEffects),
			},
			wantErrContains: `indexer streams "effects" and "copy_pasted" share state_change_id sub-base`,
		},
		{
			name:            "🔴 sub-base 0 is reserved for the token transfer stream",
			procs:           []OperationProcessorInterface{newProc("zero", 0)},
			wantErrContains: `processor "zero" has invalid state_change_id sub-base 0`,
		},
		{
			name:            "🔴 sub-base not a multiple of the sub-namespace width",
			procs:           []OperationProcessorInterface{newProc("misaligned", types.StateChangeSubNamespaceWidth+1)},
			wantErrContains: "invalid state_change_id sub-base",
		},
		{
			name:            "🔴 negative sub-base",
			procs:           []OperationProcessorInterface{newProc("negative", -types.StateChangeSubNamespaceWidth)},
			wantErrContains: "invalid state_change_id sub-base",
		},
		{
			name:            "🔴 sub-base outside the indexer emitter namespace",
			procs:           []OperationProcessorInterface{newProc("oversized", types.StateChangeOrdinalNamespaceWidth)},
			wantErrContains: "invalid state_change_id sub-base",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateStateChangeSubBases(tc.procs)
			if tc.wantErrContains == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.wantErrContains)
			}
		})
	}
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
		mockLPShares := &MockLiquidityPoolSharesProcessor{}
		mockLPPools := &MockLiquidityPoolsProcessor{}
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
		mockAccounts.On("ProcessTransactionFees", mock.Anything, mock.Anything).Return([]types.AccountChange{}, nil)
		mockSACBalances.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.SACBalanceChange{}, nil)
		mockLPShares.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.LiquidityPoolShareChange{}, nil)
		mockLPPools.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.LiquidityPoolChange{}, nil)
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
			lpSharesProcessor:          mockLPShares,
			lpProcessor:                mockLPPools,
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
		assert.Contains(t, txParticipantsMap[toID], "alice", "alice should be in tx participants")
		assert.Contains(t, txParticipantsMap[toID], "bob", "bob should be in tx participants")

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
		mockLPShares := &MockLiquidityPoolSharesProcessor{}
		mockLPPools := &MockLiquidityPoolsProcessor{}
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
		mockAccounts.On("ProcessTransactionFees", mock.Anything, mock.Anything).Return([]types.AccountChange{}, nil)
		mockSACBalances.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.SACBalanceChange{}, nil)
		mockLPShares.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.LiquidityPoolShareChange{}, nil)
		mockLPPools.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.LiquidityPoolChange{}, nil)
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
			lpSharesProcessor:          mockLPShares,
			lpProcessor:                mockLPPools,
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
		mockAccounts := &MockAccountsProcessor{}

		mockParticipants.On("GetTransactionParticipants", testTx).Return(set.NewSet[string](), nil)
		mockParticipants.On("GetOperationsParticipants", testTx).Return(map[int64]processors.OperationParticipants{}, nil)
		mockTokenTransfer.On("ProcessTransaction", mock.Anything, testTx).Return([]types.StateChange{}, nil)
		mockAccounts.On("ProcessTransactionFees", mock.Anything, testTx).Return([]types.AccountChange{}, nil)

		// Create indexer
		indexer := &Indexer{
			participantsProcessor:  mockParticipants,
			tokenTransferProcessor: mockTokenTransfer,
			accountsProcessor:      mockAccounts,
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
		mockLPShares := &MockLiquidityPoolSharesProcessor{}
		mockLPPools := &MockLiquidityPoolsProcessor{}
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
		mockAccounts.On("ProcessTransactionFees", mock.Anything, mock.Anything).Return([]types.AccountChange{}, nil)
		mockSACBalances.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.SACBalanceChange{}, nil)
		mockLPShares.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.LiquidityPoolShareChange{}, nil)
		mockLPPools.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.LiquidityPoolChange{}, nil)
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
			lpSharesProcessor:          mockLPShares,
			lpProcessor:                mockLPPools,
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

func TestIndexer_ProcessLedgerTransactions_FeePhaseBalances(t *testing.T) {
	// End-to-end through the real indexer pipeline (issue #637): an account whose
	// balance moves ONLY in the fee phase — e.g. a fee-bump fee source — is touched by
	// no operation, so the per-operation path never produces a change for it. Before
	// the fix it never appeared in GetAccountChanges(); now ProcessTransactionFees
	// captures it.
	feeSource := accountA.ToAccountId()
	feeSourceAddr := accountA.Address()

	accountEntry := func(balance int64) *xdr.LedgerEntry {
		return &xdr.LedgerEntry{
			Data: xdr.LedgerEntryData{
				Type: xdr.LedgerEntryTypeAccount,
				Account: &xdr.AccountEntry{
					AccountId: feeSource,
					Balance:   xdr.Int64(balance),
					SeqNum:    xdr.SequenceNumber(1),
				},
			},
		}
	}

	// Fee debit: 100 XLM → 100 XLM minus 100 stroops. No operation touches feeSource.
	feeTx := ingest.LedgerTransaction{
		Index:  1,
		Ledger: testLcm,
		Hash:   xdr.Hash{7, 8, 9},
		Envelope: xdr.TransactionEnvelope{
			Type: xdr.EnvelopeTypeEnvelopeTypeTx,
			V1: &xdr.TransactionV1Envelope{
				Tx: xdr.Transaction{
					SourceAccount: accountA,
					SeqNum:        xdr.SequenceNumber(1),
				},
			},
		},
		Result: xdr.TransactionResultPair{
			TransactionHash: xdr.Hash{7, 8, 9},
			Result: xdr.TransactionResult{
				FeeCharged: xdr.Int64(100),
				Result: xdr.TransactionResultResult{
					Code:    xdr.TransactionResultCodeTxSuccess,
					Results: &[]xdr.OperationResult{},
				},
			},
		},
		FeeChanges: xdr.LedgerEntryChanges{
			{Type: xdr.LedgerEntryChangeTypeLedgerEntryState, State: accountEntry(1_000_000_000)},
			{Type: xdr.LedgerEntryChangeTypeLedgerEntryUpdated, Updated: accountEntry(999_999_900)},
		},
		UnsafeMeta: xdr.TransactionMeta{
			V:  3,
			V3: &xdr.TransactionMetaV3{Operations: []xdr.OperationMeta{}},
		},
	}

	indexer, err := NewIndexer(network.TestNetworkPassphrase, pond.NewPool(runtime.NumCPU()), nil)
	require.NoError(t, err)
	buffer := NewIndexerBuffer()
	_, err = indexer.ProcessLedgerTransactions(context.Background(), []ingest.LedgerTransaction{feeTx}, buffer)
	require.NoError(t, err)

	accountChanges := buffer.GetAccountChanges()
	require.Contains(t, accountChanges, feeSourceAddr, "fee-only account must be captured")
	assert.Equal(t, int64(999_999_900), accountChanges[feeSourceAddr].Balance)
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
// Fixtures are pubnet ledgers from the public data lake (bucket
// aws-public-blockchain/v1.1/stellar/ledgers/pubnet, us-east-2). The lake re-encodes
// all history to TransactionMetaV4, so every fixture — like everything production
// reads — is V4, even ledgers that closed under older protocols. What does vary is
// the LedgerCloseMeta wrapper version (V1 for Protocol 20-22, V2 for 23+), which the
// walk switches on, so the corpus keeps both, with a mix of tx shapes (plain and
// fee-bumped invocations, failed txs, multi-event ledgers). The V3 meta branch isn't
// in the lake and is covered by synthetic tests.
//
// To add fixtures: build ingest.NewLedgerBackend (datastore + PublicNetworkPassphrase),
// then per sequence call GetLedger, MarshalBinary, gzip, and write
// testdata/ledger-<seq>.xdr.gz. Keep both wrapper versions represented.
func loadLedgerFixture(t testing.TB, path string) xdr.LedgerCloseMeta {
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

			got, err := ExtractContractEventsForLedger(lcm)
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
	return newSyntheticLedgerCloseMetaWithResult(seq, xdr.TransactionResult{
		Result: xdr.TransactionResultResult{
			Code:    xdr.TransactionResultCodeTxSuccess,
			Results: &results,
		},
	}, applyMeta)
}

// newSyntheticLedgerCloseMetaWithResult is newSyntheticLedgerCloseMeta with a
// full TransactionResult, for modelling fee-bump (inner-result) shapes.
func newSyntheticLedgerCloseMetaWithResult(seq uint32, result xdr.TransactionResult, applyMeta xdr.TransactionMeta) xdr.LedgerCloseMeta {
	return xdr.LedgerCloseMeta{
		V: 0,
		V0: &xdr.LedgerCloseMetaV0{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Header: xdr.LedgerHeader{LedgerSeq: xdr.Uint32(seq)},
			},
			TxProcessing: []xdr.TransactionResultMeta{
				{
					Result:            xdr.TransactionResultPair{Result: result},
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

	out, err := ExtractContractEventsForLedger(lcm)
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
	opResults := []xdr.OperationResult{
		{Code: xdr.OperationResultCodeOpInner, Tr: &xdr.OperationResultTr{Type: xdr.OperationTypeInvokeHostFunction}},
	}
	applyMeta := xdr.TransactionMeta{V: 99} // unsupported; all arm pointers nil

	lcm := newSyntheticLedgerCloseMeta(101, opResults, applyMeta)

	_, err := ExtractContractEventsForLedger(lcm)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported TransactionMeta version")
}

// In V3 meta, Soroban events live at the transaction level (SorobanMeta.Events)
// and the op index is ignored. This checks the walk surfaces them at op 0.
//
// It's our only V3 coverage: the data lake re-encodes all history to V4, so a real
// V3 fixture can't be fetched.
func TestExtractContractEventsForLedger_V3SorobanMetaEvents(t *testing.T) {
	ev1 := xdr.ContractEvent{Type: xdr.ContractEventTypeContract}
	ev2 := xdr.ContractEvent{Type: xdr.ContractEventTypeSystem}
	applyMeta := xdr.TransactionMeta{
		V: 3,
		V3: &xdr.TransactionMetaV3{
			SorobanMeta: &xdr.SorobanTransactionMeta{Events: []xdr.ContractEvent{ev1, ev2}},
		},
	}
	opResults := []xdr.OperationResult{
		{Code: xdr.OperationResultCodeOpInner, Tr: &xdr.OperationResultTr{Type: xdr.OperationTypeInvokeHostFunction}},
	}
	lcm := newSyntheticLedgerCloseMeta(103, opResults, applyMeta)

	out, err := ExtractContractEventsForLedger(lcm)
	require.NoError(t, err)
	require.Len(t, out, 1)
	got, ok := out[ContractEventKey{TxIdx: 1, OpIdx: 0}]
	require.True(t, ok, "expected events at tx 1 op 0")
	require.Equal(t, []xdr.ContractEvent{ev1, ev2}, got)
}

// A fee-bump's operation results live in its inner transaction. This checks the
// walk unwraps to those inner results instead of reading the empty outer result.
// Real fee-bumped invocations are also in the fixture corpus.
func TestExtractContractEventsForLedger_FeeBumpUnwrapsInnerResults(t *testing.T) {
	ev := xdr.ContractEvent{Type: xdr.ContractEventTypeContract}
	applyMeta := xdr.TransactionMeta{
		V: 3,
		V3: &xdr.TransactionMetaV3{
			SorobanMeta: &xdr.SorobanTransactionMeta{Events: []xdr.ContractEvent{ev}},
		},
	}
	innerOpResults := []xdr.OperationResult{
		{Code: xdr.OperationResultCodeOpInner, Tr: &xdr.OperationResultTr{Type: xdr.OperationTypeInvokeHostFunction}},
	}
	feeBump := xdr.TransactionResult{
		Result: xdr.TransactionResultResult{
			Code: xdr.TransactionResultCodeTxFeeBumpInnerSuccess,
			InnerResultPair: &xdr.InnerTransactionResultPair{
				Result: xdr.InnerTransactionResult{
					Result: xdr.InnerTransactionResultResult{
						Code:    xdr.TransactionResultCodeTxSuccess,
						Results: &innerOpResults,
					},
				},
			},
		},
	}
	lcm := newSyntheticLedgerCloseMetaWithResult(104, feeBump, applyMeta)

	out, err := ExtractContractEventsForLedger(lcm)
	require.NoError(t, err)
	require.Len(t, out, 1, "fee-bump inner op results must be unwrapped")
	got, ok := out[ContractEventKey{TxIdx: 1, OpIdx: 0}]
	require.True(t, ok)
	require.Equal(t, []xdr.ContractEvent{ev}, got)
}

// RestoreFootprint and ExtendFootprintTtl ops carry no contract events, so the
// walk should skip them and return events only for the InvokeHostFunction op.
// Done synthetically since these ops weren't found in the scanned pubnet ledgers.
func TestExtractContractEventsForLedger_FootprintOpsSkipped(t *testing.T) {
	applyMeta := xdr.TransactionMeta{
		V: 4,
		V4: &xdr.TransactionMetaV4{
			Operations: []xdr.OperationMetaV2{
				{Events: []xdr.ContractEvent{{Type: xdr.ContractEventTypeContract}}}, // index 0 (invoke)
			},
		},
	}
	opResults := []xdr.OperationResult{
		{Code: xdr.OperationResultCodeOpInner, Tr: &xdr.OperationResultTr{Type: xdr.OperationTypeInvokeHostFunction}},
		{Code: xdr.OperationResultCodeOpInner, Tr: &xdr.OperationResultTr{Type: xdr.OperationTypeExtendFootprintTtl}},
		{Code: xdr.OperationResultCodeOpInner, Tr: &xdr.OperationResultTr{Type: xdr.OperationTypeRestoreFootprint}},
	}
	lcm := newSyntheticLedgerCloseMeta(105, opResults, applyMeta)

	out, err := ExtractContractEventsForLedger(lcm)
	require.NoError(t, err)
	require.Len(t, out, 1, "only the InvokeHostFunction op yields events; footprint ops are skipped")
	_, ok := out[ContractEventKey{TxIdx: 1, OpIdx: 0}]
	require.True(t, ok)
}

// Guards that the corpus keeps real fixtures for both LedgerCloseMeta wrapper
// versions the lake serves with events: V1 (Protocol 20-22) and V2 (Protocol 23+).
// The walk switches on this version, so dropping either set would quietly lose
// coverage of an older history window.
//
// All lake fixtures are TransactionMetaV4; the V3 branch is covered by the
// synthetic tests above.
func TestExtractContractEventsForLedger_CorpusCoversWrapperVersions(t *testing.T) {
	paths, err := filepath.Glob("testdata/*.xdr.gz")
	require.NoError(t, err)
	require.NotEmpty(t, paths)

	wrapperWithEvents := map[int32]bool{} // LedgerCloseMeta wrapper version -> a ledger of it yielded events
	for _, path := range paths {
		lcm := loadLedgerFixture(t, path)
		events, evErr := ExtractContractEventsForLedger(lcm)
		require.NoError(t, evErr)
		if len(events) > 0 {
			wrapperWithEvents[lcm.V] = true
		}
	}
	require.True(t, wrapperWithEvents[1], "corpus must include a wrapper-V1 (Protocol 20-22) ledger with extracted contract events")
	require.True(t, wrapperWithEvents[2], "corpus must include a wrapper-V2 (Protocol 23+) ledger with extracted contract events")
}

func TestAssignStateChangeStream_SubNamespaces(t *testing.T) {
	effects := []types.StateChange{
		{OperationID: 42, AccountID: "GA"},
		{OperationID: 42, AccountID: "GB"},
		{OperationID: 43, AccountID: "GC"},
	}
	tokenTransfers := []types.StateChange{
		{OperationID: 42, AccountID: "GD"},
		{OperationID: 42, AccountID: ""}, // no account — dropped before assignment
		{OperationID: 42, AccountID: "GE"},
	}

	var merged []types.StateChange
	merged = assignStateChangeStream(merged, effects, types.StateChangeSubBaseEffects)
	merged = assignStateChangeStream(merged, tokenTransfers, types.StateChangeSubBaseTokenTransfer)

	// Each stream numbers its own emissions 1..N per operation inside its own
	// sub-namespace; the account-less entry is filtered out first so ordinals
	// stay gap-free.
	require.Len(t, merged, 5)
	assert.Equal(t, types.StateChangeSubBaseEffects+1, merged[0].StateChangeID)
	assert.Equal(t, types.StateChangeSubBaseEffects+2, merged[1].StateChangeID)
	assert.Equal(t, types.StateChangeSubBaseEffects+1, merged[2].StateChangeID) // op 43: its own group
	assert.Equal(t, types.StateChangeSubBaseTokenTransfer+1, merged[3].StateChangeID)
	assert.Equal(t, types.StateChangeSubBaseTokenTransfer+2, merged[4].StateChangeID)

	// The IDs a stream gets are a function of that stream alone: assigning the
	// same effects emissions with no other stream present yields identical IDs.
	// This is the property that makes IDs immune to processors being added,
	// removed, or reordered around this one.
	alone := assignStateChangeStream(nil, []types.StateChange{
		{OperationID: 42, AccountID: "GA"},
		{OperationID: 42, AccountID: "GB"},
		{OperationID: 43, AccountID: "GC"},
	}, types.StateChangeSubBaseEffects)
	require.Len(t, alone, 3)
	for i := range alone {
		assert.Equal(t, merged[i].StateChangeID, alone[i].StateChangeID)
	}

	// All IDs across streams sharing operation 42 are distinct.
	seen := map[int64]bool{}
	for _, sc := range merged {
		if sc.OperationID != 42 {
			continue
		}
		assert.False(t, seen[sc.StateChangeID], "duplicate state_change_id %d", sc.StateChangeID)
		seen[sc.StateChangeID] = true
	}
}

// TestIndexer_ProcessLedgerTransactions_RealLedgerParallel drives every real
// pubnet fixture through the parallel indexing path under the race detector.
//
// It enforces the single-owner buffer convention: ProcessLedgerTransactions
// builds a per-transaction TransactionResult in parallel pool workers with no
// shared state, then folds those results into the one shared IndexerBuffer
// serially. Workers must never touch the shared buffer. If that regresses —
// any worker writing to the buffer — the race detector (make unit-test runs
// go test -race) fails this test.
func TestIndexer_ProcessLedgerTransactions_RealLedgerParallel(t *testing.T) {
	ctx := context.Background()

	paths, err := filepath.Glob("testdata/*.xdr.gz")
	require.NoError(t, err)
	require.NotEmpty(t, paths, "no ledger fixtures under testdata/ — regenerate per the loadLedgerFixture recipe")

	idx, err := NewIndexer(network.PublicNetworkPassphrase, pond.NewPool(runtime.NumCPU()), nil)
	require.NoError(t, err)

	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			lcm := loadLedgerFixture(t, path)
			txs, err := GetLedgerTransactions(ctx, network.PublicNetworkPassphrase, lcm)
			require.NoError(t, err)
			if len(txs) < 2 {
				t.Skipf("fixture has %d transaction(s); need ≥2 to exercise parallelism", len(txs))
			}

			buffer := NewIndexerBuffer()
			participantCount, err := idx.ProcessLedgerTransactions(ctx, txs, buffer)
			require.NoError(t, err)

			assert.Positive(t, participantCount)
			assert.Positive(t, buffer.GetNumberOfTransactions())
			assert.Equal(t, buffer.GetNumberOfTransactions(), len(buffer.GetTransactions()))
			assert.Positive(t, buffer.GetNumberOfOperations())
			assert.NotEmpty(t, buffer.GetStateChanges())
		})
	}
}

// TestIndexer_ProcessTransaction_EmitsChangesInOpOrder is the regression test for issue #653:
// pushWithTombstone's create+remove netting requires each change family to reach the fold in
// ascending order-value per key (CREATE before REMOVE), so processTransaction must walk
// operations in ascending opID order. Under map-order iteration this test fails with
// overwhelming probability (12 ops → 1/12! chance of accidentally sorted output).
func TestIndexer_ProcessTransaction_EmitsChangesInOpOrder(t *testing.T) {
	const numOps = 12
	const asset = "USDC:GBWAH7AOBZYAYLT76Z7MQDDRRJCCERRVRSCJ4GAEGV2S5W474ZLEOH4U"

	mockParticipants := &MockParticipantsProcessor{}
	mockTokenTransfer := &MockTokenTransferProcessor{}
	mockEffects := &MockOperationProcessor{}
	mockTrustlines := &MockTrustlinesProcessor{}
	mockAccounts := &MockAccountsProcessor{}
	mockSACBalances := &MockSACBalancesProcessor{}
	mockLPShares := &MockLiquidityPoolSharesProcessor{}
	mockLPPools := &MockLiquidityPoolsProcessor{}
	mockSACInstances := &MockSACInstancesProcessor{}
	mockProtocolWasms := &MockProtocolWasmsProcessor{}
	mockProtocolContracts := &MockProtocolContractsProcessor{}

	mockParticipants.On("GetTransactionParticipants", testTx).Return(set.NewSet("alice"), nil)

	// Op 1 adds a trustline for the shared key and op 12 removes it again within the same
	// transaction; every other op updates a distinct account's trustline on the same asset.
	opsParticipants := make(map[int64]processors.OperationParticipants, numOps)
	for i := 1; i <= numOps; i++ {
		opID := int64(i)
		wrapper := &processors.TransactionOperationWrapper{
			Index:          uint32(i - 1),
			Operation:      createAccountOp,
			Network:        network.TestNetworkPassphrase,
			LedgerSequence: 12345,
		}
		opsParticipants[opID] = processors.OperationParticipants{
			OpWrapper:    wrapper,
			Participants: set.NewSet("alice"),
		}

		change := types.TrustlineChange{
			AccountID:   fmt.Sprintf("account-%02d", i),
			Asset:       asset,
			OperationID: opID,
			Operation:   types.TrustlineOpUpdate,
		}
		switch i {
		case 1:
			change.AccountID = "shared"
			change.Operation = types.TrustlineOpAdd
		case numOps:
			change.AccountID = "shared"
			change.Operation = types.TrustlineOpRemove
		}
		mockTrustlines.On("ProcessOperation", mock.Anything, wrapper).Return([]types.TrustlineChange{change}, nil)
	}
	mockParticipants.On("GetOperationsParticipants", testTx).Return(opsParticipants, nil)

	mockEffects.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.StateChange{}, nil)
	mockTokenTransfer.On("ProcessTransaction", mock.Anything, testTx).Return([]types.StateChange{}, nil)
	mockAccounts.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.AccountChange{}, nil)
	mockAccounts.On("ProcessTransactionFees", mock.Anything, mock.Anything).Return([]types.AccountChange{}, nil)
	mockSACBalances.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.SACBalanceChange{}, nil)
	mockLPShares.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.LiquidityPoolShareChange{}, nil)
	mockLPPools.On("ProcessOperation", mock.Anything, mock.Anything).Return([]types.LiquidityPoolChange{}, nil)
	mockSACInstances.On("ProcessOperation", mock.Anything, mock.Anything).Return([]*data.Contract{}, nil)
	mockProtocolWasms.On("ProcessOperation", mock.Anything, mock.Anything).Return([]processors.ProtocolWasmObservation{}, nil)
	mockProtocolContracts.On("ProcessOperation", mock.Anything, mock.Anything).Return([]data.ProtocolContracts{}, nil)

	idx := &Indexer{
		participantsProcessor:      mockParticipants,
		tokenTransferProcessor:     mockTokenTransfer,
		trustlinesProcessor:        mockTrustlines,
		accountsProcessor:          mockAccounts,
		sacBalancesProcessor:       mockSACBalances,
		lpSharesProcessor:          mockLPShares,
		lpProcessor:                mockLPPools,
		sacInstancesProcessor:      mockSACInstances,
		protocolWasmsProcessor:     mockProtocolWasms,
		protocolContractsProcessor: mockProtocolContracts,
		processors:                 []OperationProcessorInterface{mockEffects},
		pool:                       pond.NewPool(runtime.NumCPU()),
		networkPassphrase:          network.TestNetworkPassphrase,
	}

	result, err := idx.processTransaction(context.Background(), testTx)
	require.NoError(t, err)

	// Change families arrive in ascending op order...
	require.Len(t, result.TrustlineChanges, numOps)
	for i := 1; i < len(result.TrustlineChanges); i++ {
		assert.Less(t, result.TrustlineChanges[i-1].OperationID, result.TrustlineChanges[i].OperationID,
			"trustline changes must be emitted in ascending opID order")
	}

	// ...so the fold nets the same-tx ADD→REMOVE to nothing instead of keeping a spurious REMOVE.
	buffer := NewIndexerBuffer()
	buffer.IngestTransactionResult(result)
	trustlines := buffer.GetTrustlineChanges()
	assert.Len(t, trustlines, numOps-2, "shared-key ADD→REMOVE should net to nothing")
	for _, change := range trustlines {
		assert.NotEqual(t, "shared", change.AccountID, "spurious REMOVE for the netted key must not persist")
	}
}
