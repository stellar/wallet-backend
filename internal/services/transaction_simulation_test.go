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
	"github.com/stretchr/testify/require"

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

	t.Run("🔴 soroban transaction not implemented yet", func(t *testing.T) {
		nativeContractID, err := xdr.Asset{Type: xdr.AssetTypeAssetTypeNative}.ContractID(network.TestNetworkPassphrase)
		require.NoError(t, err)
		contractID := xdr.ContractId(nativeContractID)
		_, err = svc.SimulateStateChanges(ctx, buildTxXDR(t, &txnbuild.InvokeHostFunction{
			HostFunction: xdr.HostFunction{
				Type: xdr.HostFunctionTypeHostFunctionTypeInvokeContract,
				InvokeContract: &xdr.InvokeContractArgs{
					ContractAddress: xdr.ScAddress{Type: xdr.ScAddressTypeScAddressTypeContract, ContractId: &contractID},
					FunctionName:    "transfer",
				},
			},
		}))
		assert.ErrorIs(t, err, ErrUnsupportedTransaction)
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

// buildTxXDR builds an unsigned single-op transaction envelope.
func buildTxXDR(t *testing.T, op txnbuild.Operation) string {
	t.Helper()
	src := txnbuild.SimpleAccount{AccountID: keypair.MustRandom().Address(), Sequence: 1}
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
