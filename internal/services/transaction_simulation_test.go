package services

import (
	"context"
	"math/big"
	"testing"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/support/contractevents"
	"github.com/stellar/go-stellar-sdk/txnbuild"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/indexer/types"
)

func TestTransactionSimulationService_SimulateStateChanges_errors(t *testing.T) {
	svc, err := NewTransactionSimulationService(&RPCServiceMock{}, network.TestNetworkPassphrase)
	require.NoError(t, err)
	ctx := context.Background()

	t.Run("🔴 invalid XDR", func(t *testing.T) {
		_, err := svc.SimulateStateChanges(ctx, "not-xdr")
		assert.ErrorIs(t, err, ErrInvalidTransactionXDR)
	})

	t.Run("🔴 classic transaction unsupported", func(t *testing.T) {
		_, err := svc.SimulateStateChanges(ctx, buildTxXDR(t, &txnbuild.Payment{
			Destination: keypair.MustRandom().Address(),
			Amount:      "1",
			Asset:       txnbuild.NativeAsset{},
		}))
		assert.ErrorIs(t, err, ErrUnsupportedTransaction)
	})

	t.Run("🔴 RPC simulation error surfaced", func(t *testing.T) {
		rpcMock := &RPCServiceMock{}
		rpcMock.On("SimulateTransaction", mock.Anything, mock.Anything).
			Return(entities.RPCSimulateTransactionResult{Error: "contract trapped"}, nil).Once()
		errSvc, err := NewTransactionSimulationService(rpcMock, network.TestNetworkPassphrase)
		require.NoError(t, err)

		_, err = errSvc.SimulateStateChanges(ctx, nativeSACTransferXDR(t, keypair.MustRandom().Address()))
		assert.ErrorIs(t, err, ErrSimulationFailed)
		rpcMock.AssertExpectations(t)
	})
}

// TestTransactionSimulationService_SimulateStateChanges_soroban drives the full
// Phase 1 path: a canned RPC simulateTransaction result (a native-SAC transfer
// event) is synthesized into a ledger transaction and run through the real
// processors, which must emit a DEBIT for the sender and a CREDIT for the
// receiver, the same state changes history would show.
func TestTransactionSimulationService_SimulateStateChanges_soroban(t *testing.T) {
	from := keypair.MustRandom().Address()
	to := keypair.MustRandom().Address()
	amount := big.NewInt(10_000_000)
	nativeAsset := xdr.Asset{Type: xdr.AssetTypeAssetTypeNative}

	transferEvent := contractevents.GenerateEvent(
		contractevents.EventTypeTransfer,
		from, to, "",
		nativeAsset,
		amount,
		network.TestNetworkPassphrase,
	)

	diagnosticB64, err := xdr.MarshalBase64(xdr.DiagnosticEvent{InSuccessfulContractCall: true, Event: transferEvent})
	require.NoError(t, err)

	txXDR := nativeSACTransferXDR(t, from)

	rpcMock := &RPCServiceMock{}
	rpcMock.On("SimulateTransaction", txXDR, entities.RPCResourceConfig{}).
		Return(entities.RPCSimulateTransactionResult{
			LatestLedger:   2900148,
			MinResourceFee: "100",
			Events:         []string{diagnosticB64},
		}, nil).Once()

	svc, err := NewTransactionSimulationService(rpcMock, network.TestNetworkPassphrase)
	require.NoError(t, err)

	result, err := svc.SimulateStateChanges(context.Background(), txXDR)
	require.NoError(t, err)
	assert.Equal(t, uint32(2900148), result.LatestLedger)

	nativeContractID, err := nativeAsset.ContractID(network.TestNetworkPassphrase)
	require.NoError(t, err)
	nativeContractAddress, err := strkeyContractID(nativeContractID)
	require.NoError(t, err)

	byReason := map[types.StateChangeReason]types.StateChange{}
	for _, sc := range result.StateChanges {
		if sc.StateChangeCategory == types.StateChangeCategoryBalance {
			byReason[sc.StateChangeReason] = sc
		}
	}

	debit, ok := byReason[types.StateChangeReasonDebit]
	require.True(t, ok, "expected a DEBIT balance change for the sender")
	assert.Equal(t, from, string(debit.AccountID))
	assert.Equal(t, "10000000", debit.Amount.String)
	assert.Equal(t, nativeContractAddress, debit.TokenID.String())

	credit, ok := byReason[types.StateChangeReasonCredit]
	require.True(t, ok, "expected a CREDIT balance change for the receiver")
	assert.Equal(t, to, string(credit.AccountID))
	assert.Equal(t, "10000000", credit.Amount.String)
	assert.Equal(t, nativeContractAddress, credit.TokenID.String())

	rpcMock.AssertExpectations(t)
}

// nativeSACTransferXDR builds an unsigned InvokeHostFunction envelope invoking
// the native SAC's transfer: a minimal, fully-encodable Soroban transaction.
func nativeSACTransferXDR(t *testing.T, sourceAccount string) string {
	t.Helper()
	nativeContractID, err := xdr.Asset{Type: xdr.AssetTypeAssetTypeNative}.ContractID(network.TestNetworkPassphrase)
	require.NoError(t, err)
	contractID := xdr.ContractId(nativeContractID)
	return buildTxXDRFrom(t, sourceAccount, &txnbuild.InvokeHostFunction{
		HostFunction: xdr.HostFunction{
			Type: xdr.HostFunctionTypeHostFunctionTypeInvokeContract,
			InvokeContract: &xdr.InvokeContractArgs{
				ContractAddress: xdr.ScAddress{Type: xdr.ScAddressTypeScAddressTypeContract, ContractId: &contractID},
				FunctionName:    "transfer",
			},
		},
	})
}

// TestTransactionSimulationService_walkingSkeleton proves Phase 0's core
// assumption: a synthesized ingest.LedgerTransaction — the shape the Soroban
// source will build from RPC simulateTransaction results — runs through the
// real ingestion processors in-memory (no DB) and yields the same state
// changes ingestion would produce. It hardcodes a native-SAC transfer: the
// token-transfer processor must emit a DEBIT for the sender and a CREDIT for
// the receiver.
func TestTransactionSimulationService_walkingSkeleton(t *testing.T) {
	svc, err := NewTransactionSimulationService(&RPCServiceMock{}, network.TestNetworkPassphrase)
	require.NoError(t, err)

	from := keypair.MustRandom().Address()
	to := keypair.MustRandom().Address()
	amount := big.NewInt(10_000_000) // 1 XLM in stroops
	nativeAsset := xdr.Asset{Type: xdr.AssetTypeAssetTypeNative}

	transferEvent := contractevents.GenerateEvent(
		contractevents.EventTypeTransfer,
		from, to, "",
		nativeAsset,
		amount,
		network.TestNetworkPassphrase,
	)
	tx := synthesizeSorobanTransaction(t, from, transferEvent)

	stateChanges, err := svc.stateChangesForTransaction(context.Background(), tx)
	require.NoError(t, err)
	require.NotEmpty(t, stateChanges, "the pipeline must emit state changes for the synthesized transfer")

	nativeContractID, err := nativeAsset.ContractID(network.TestNetworkPassphrase)
	require.NoError(t, err)
	nativeContractAddress, err := strkeyContractID(nativeContractID)
	require.NoError(t, err)

	byReason := map[types.StateChangeReason]types.StateChange{}
	for _, sc := range stateChanges {
		if sc.StateChangeCategory == types.StateChangeCategoryBalance {
			byReason[sc.StateChangeReason] = sc
		}
	}

	debit, ok := byReason[types.StateChangeReasonDebit]
	require.True(t, ok, "expected a DEBIT balance change for the sender")
	assert.Equal(t, from, string(debit.AccountID))
	assert.Equal(t, "10000000", debit.Amount.String)
	assert.Equal(t, nativeContractAddress, debit.TokenID.String())

	credit, ok := byReason[types.StateChangeReasonCredit]
	require.True(t, ok, "expected a CREDIT balance change for the receiver")
	assert.Equal(t, to, string(credit.AccountID))
	assert.Equal(t, "10000000", credit.Amount.String)
	assert.Equal(t, nativeContractAddress, credit.TokenID.String())
}

// synthesizeSorobanTransaction assembles the minimal ingest.LedgerTransaction
// the processors need: an InvokeHostFunction envelope, a successful result,
// and a V3 meta carrying the given contract events — the same synthesis the
// Soroban source will perform on RPC simulateTransaction output. The envelope
// operation must be fully encodable (the pipeline marshals it), so it carries
// real invoke-contract args.
func synthesizeSorobanTransaction(t *testing.T, sourceAccount string, events ...xdr.ContractEvent) ingest.LedgerTransaction {
	t.Helper()

	nativeContractID, err := xdr.Asset{Type: xdr.AssetTypeAssetTypeNative}.ContractID(network.TestNetworkPassphrase)
	require.NoError(t, err)
	contractID := xdr.ContractId(nativeContractID)

	sourceAID := xdr.MustAddress(sourceAccount)
	return ingest.LedgerTransaction{
		Index: 1,
		Ledger: xdr.LedgerCloseMeta{
			V: 0,
			V0: &xdr.LedgerCloseMetaV0{
				LedgerHeader: xdr.LedgerHeaderHistoryEntry{
					Header: xdr.LedgerHeader{
						LedgerSeq: xdr.Uint32(12345),
						ScpValue:  xdr.StellarValue{CloseTime: xdr.TimePoint(1234500)},
					},
				},
			},
		},
		Hash: xdr.Hash{1},
		Envelope: xdr.TransactionEnvelope{
			Type: xdr.EnvelopeTypeEnvelopeTypeTx,
			V1: &xdr.TransactionV1Envelope{
				Tx: xdr.Transaction{
					SourceAccount: sourceAID.ToMuxedAccount(),
					SeqNum:        xdr.SequenceNumber(1),
					Operations: []xdr.Operation{{
						Body: xdr.OperationBody{
							Type: xdr.OperationTypeInvokeHostFunction,
							InvokeHostFunctionOp: &xdr.InvokeHostFunctionOp{
								HostFunction: xdr.HostFunction{
									Type: xdr.HostFunctionTypeHostFunctionTypeInvokeContract,
									InvokeContract: &xdr.InvokeContractArgs{
										ContractAddress: xdr.ScAddress{Type: xdr.ScAddressTypeScAddressTypeContract, ContractId: &contractID},
										FunctionName:    "transfer",
									},
								},
							},
						},
					}},
				},
			},
		},
		Result: xdr.TransactionResultPair{
			TransactionHash: xdr.Hash{1},
			Result: xdr.TransactionResult{
				FeeCharged: xdr.Int64(100),
				Result: xdr.TransactionResultResult{
					Code: xdr.TransactionResultCodeTxSuccess,
					// One result per operation: the token-transfer processor
					// indexes operation results positionally.
					Results: &[]xdr.OperationResult{{
						Code: xdr.OperationResultCodeOpInner,
						Tr: &xdr.OperationResultTr{
							Type: xdr.OperationTypeInvokeHostFunction,
							InvokeHostFunctionResult: &xdr.InvokeHostFunctionResult{
								Code:    xdr.InvokeHostFunctionResultCodeInvokeHostFunctionSuccess,
								Success: &xdr.Hash{},
							},
						},
					}},
				},
			},
		},
		UnsafeMeta: xdr.TransactionMeta{
			V: 3,
			V3: &xdr.TransactionMetaV3{
				Operations: []xdr.OperationMeta{{}},
				SorobanMeta: &xdr.SorobanTransactionMeta{
					Events: events,
				},
			},
		},
	}
}

// buildTxXDR builds an unsigned single-op transaction envelope from a random source.
func buildTxXDR(t *testing.T, op txnbuild.Operation) string {
	t.Helper()
	return buildTxXDRFrom(t, keypair.MustRandom().Address(), op)
}

// buildTxXDRFrom builds an unsigned single-op transaction envelope from the given source.
func buildTxXDRFrom(t *testing.T, sourceAccount string, op txnbuild.Operation) string {
	t.Helper()
	src := txnbuild.SimpleAccount{AccountID: sourceAccount, Sequence: 1}
	tx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount:        &src,
		Operations:           []txnbuild.Operation{op},
		BaseFee:              txnbuild.MinBaseFee,
		Preconditions:        txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(300)},
		IncrementSequenceNum: true,
	})
	require.NoError(t, err)
	b64, err := tx.Base64()
	require.NoError(t, err)
	return b64
}

// strkeyContractID renders a 32-byte contract id as a C... strkey address.
func strkeyContractID(contractID [32]byte) (string, error) {
	addr := xdr.ScAddress{Type: xdr.ScAddressTypeScAddressTypeContract, ContractId: (*xdr.ContractId)(&contractID)}
	return addr.String()
}

func TestLedgerEntryChangesFromSimulation(t *testing.T) {
	accountEntry := xdr.LedgerEntry{
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeAccount,
			Account: &xdr.AccountEntry{
				AccountId: xdr.MustAddress("GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"),
				Balance:   100,
			},
		},
	}
	entryB64, err := xdr.MarshalBase64(accountEntry)
	require.NoError(t, err)

	accountKey := xdr.LedgerKey{
		Type: xdr.LedgerEntryTypeAccount,
		Account: &xdr.LedgerKeyAccount{
			AccountId: xdr.MustAddress("GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"),
		},
	}
	keyB64, err := xdr.MarshalBase64(accountKey)
	require.NoError(t, err)

	t.Run("created (after only) becomes a Created change", func(t *testing.T) {
		changes, err := ledgerEntryChangesFromSimulation([]entities.RPCSimulateStateChange{
			{Type: "created", After: &entryB64},
		})
		require.NoError(t, err)
		require.Len(t, changes, 1)
		assert.Equal(t, xdr.LedgerEntryChangeTypeLedgerEntryCreated, changes[0].Type)
		require.NotNil(t, changes[0].Created)
		assert.Equal(t, accountEntry, *changes[0].Created)
	})

	t.Run("updated (before+after) becomes State + Updated", func(t *testing.T) {
		changes, err := ledgerEntryChangesFromSimulation([]entities.RPCSimulateStateChange{
			{Type: "updated", Before: &entryB64, After: &entryB64},
		})
		require.NoError(t, err)
		require.Len(t, changes, 2)
		assert.Equal(t, xdr.LedgerEntryChangeTypeLedgerEntryState, changes[0].Type)
		assert.Equal(t, xdr.LedgerEntryChangeTypeLedgerEntryUpdated, changes[1].Type)
	})

	t.Run("removed (before only) becomes State + Removed with the decoded key", func(t *testing.T) {
		changes, err := ledgerEntryChangesFromSimulation([]entities.RPCSimulateStateChange{
			{Type: "deleted", Before: &entryB64, Key: keyB64},
		})
		require.NoError(t, err)
		require.Len(t, changes, 2)
		assert.Equal(t, xdr.LedgerEntryChangeTypeLedgerEntryState, changes[0].Type)
		assert.Equal(t, xdr.LedgerEntryChangeTypeLedgerEntryRemoved, changes[1].Type)
		require.NotNil(t, changes[1].Removed)
		assert.Equal(t, accountKey, *changes[1].Removed)
	})

	t.Run("invalid before XDR is an error", func(t *testing.T) {
		bad := "not-valid-xdr"
		_, err := ledgerEntryChangesFromSimulation([]entities.RPCSimulateStateChange{
			{Type: "updated", Before: &bad, After: &entryB64},
		})
		require.Error(t, err)
	})

	t.Run("invalid after XDR is an error", func(t *testing.T) {
		bad := "not-valid-xdr"
		_, err := ledgerEntryChangesFromSimulation([]entities.RPCSimulateStateChange{
			{Type: "created", After: &bad},
		})
		require.Error(t, err)
	})

	t.Run("invalid removed key XDR is an error", func(t *testing.T) {
		_, err := ledgerEntryChangesFromSimulation([]entities.RPCSimulateStateChange{
			{Type: "deleted", Before: &entryB64, Key: "not-valid-xdr"},
		})
		require.Error(t, err)
	})
}
