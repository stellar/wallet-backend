package contracts

import (
	"testing"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/indexer/types"
)

var (
	networkPassphrase = "Public Global Stellar Network ; September 2015"
	someTxAccount     = xdr.MustMuxedAddress("GBF3XFXGBGNQDN3HOSZ7NVRF6TJ2JOD5U6ELIWJOOEI6T5WKMQT2YSXQ")

	someLcm = xdr.LedgerCloseMeta{
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
)

func createSACInvocationTxV3(
	account string,
	admin string,
	asset xdr.Asset,
	isAuthorized bool,
) ingest.LedgerTransaction {
	rawContractID, err := asset.ContractID(networkPassphrase)
	if err != nil {
		panic(err)
	}
	contractID := xdr.ContractId(rawContractID)
	data := makeBooleanData(isAuthorized)

	// V3 format: ["set_authorized", admin: Address, id: Address, sep0011_asset: String]
	topics := []xdr.ScVal{
		makeSymbol("set_authorized"),
		makeAddressFromString(admin),
		makeAddressFromString(account),
		makeAssetString(asset),
	}

	metaV3 := xdr.TransactionMetaV3{
		Operations: []xdr.OperationMeta{},
		SorobanMeta: &xdr.SorobanTransactionMeta{
			Events: []xdr.ContractEvent{
				{
					Type:       xdr.ContractEventTypeContract,
					ContractId: &contractID,
					Body: xdr.ContractEventBody{
						V: 0,
						V0: &xdr.ContractEventV0{
							Topics: xdr.ScVec(topics),
							Data:   data,
						},
					},
				},
			},
		},
	}

	envelope := xdr.TransactionV1Envelope{
		Tx: xdr.Transaction{
			SourceAccount: someTxAccount,
			SeqNum:        xdr.SequenceNumber(54321),
			Operations: []xdr.Operation{
				{
					SourceAccount: xdr.MustMuxedAddressPtr(admin),
					Body: xdr.OperationBody{
						Type:                 xdr.OperationTypeInvokeHostFunction,
						InvokeHostFunctionOp: &xdr.InvokeHostFunctionOp{},
					},
				},
			},
		},
	}

	resp := ingest.LedgerTransaction{
		Index:  0,
		Ledger: someLcm,
		Envelope: xdr.TransactionEnvelope{
			Type: xdr.EnvelopeTypeEnvelopeTypeTx,
			V1:   &envelope,
		},
		Result: xdr.TransactionResultPair{
			TransactionHash: xdr.Hash([32]byte{}),
			Result: xdr.TransactionResult{
				FeeCharged: 1234,
				Result: xdr.TransactionResultResult{
					Code:    xdr.TransactionResultCodeTxSuccess,
					Results: &[]xdr.OperationResult{},
				},
			},
		},
		UnsafeMeta: xdr.TransactionMeta{V: 3, V3: &metaV3},
	}
	return resp
}

func createSACInvocationTxV4(
	account string,
	admin string,
	asset xdr.Asset,
	isAuthorized bool,
) ingest.LedgerTransaction {
	rawContractID, err := asset.ContractID(networkPassphrase)
	if err != nil {
		panic(err)
	}
	contractID := xdr.ContractId(rawContractID)
	data := makeBooleanData(isAuthorized)

	// V4 format: ["set_authorized", id: Address, sep0011_asset: String]
	topics := []xdr.ScVal{
		makeSymbol("set_authorized"),
		makeAddressFromString(account),
		makeAssetString(asset),
	}

	metaV4 := xdr.TransactionMetaV4{
		Operations: []xdr.OperationMetaV2{
			{
				Events: []xdr.ContractEvent{
					{
						Type:       xdr.ContractEventTypeContract,
						ContractId: &contractID,
						Body: xdr.ContractEventBody{
							V: 0,
							V0: &xdr.ContractEventV0{
								Topics: xdr.ScVec(topics),
								Data:   data,
							},
						},
					},
				},
			},
		},
	}

	envelope := xdr.TransactionV1Envelope{
		Tx: xdr.Transaction{
			SourceAccount: someTxAccount,
			SeqNum:        xdr.SequenceNumber(54321),
			Operations: []xdr.Operation{
				{
					SourceAccount: xdr.MustMuxedAddressPtr(admin),
					Body: xdr.OperationBody{
						Type:                 xdr.OperationTypeInvokeHostFunction,
						InvokeHostFunctionOp: &xdr.InvokeHostFunctionOp{},
					},
				},
			},
		},
	}

	resp := ingest.LedgerTransaction{
		Index:  0,
		Ledger: someLcm,
		Envelope: xdr.TransactionEnvelope{
			Type: xdr.EnvelopeTypeEnvelopeTypeTx,
			V1:   &envelope,
		},
		Result: xdr.TransactionResultPair{
			TransactionHash: xdr.Hash([32]byte{}),
			Result: xdr.TransactionResult{
				FeeCharged: 1234,
				Result: xdr.TransactionResultResult{
					Code:    xdr.TransactionResultCodeTxSuccess,
					Results: &[]xdr.OperationResult{},
				},
			},
		},
		UnsafeMeta: xdr.TransactionMeta{V: 4, V4: &metaV4}, // V4 format
	}
	return resp
}

// createInvalidContractEventTx creates a transaction with invalid contract event structure
func createInvalidContractEventTx(
	account string,
	admin string,
	asset xdr.Asset,
	isAuthorized bool,
) ingest.LedgerTransaction {
	rawContractID, err := asset.ContractID(networkPassphrase)
	if err != nil {
		panic(err)
	}
	contractID := xdr.ContractId(rawContractID)

	// Create invalid event with wrong event type
	event := xdr.ContractEvent{
		Type:       xdr.ContractEventTypeSystem, // Invalid type
		ContractId: &contractID,
		Body: xdr.ContractEventBody{
			V: 0,
			V0: &xdr.ContractEventV0{
				Topics: xdr.ScVec([]xdr.ScVal{
					makeSymbol("set_authorized"),
					makeAddressFromString(admin),
					makeAddressFromString(account),
					makeAssetString(asset),
				}),
				Data: makeBooleanData(isAuthorized),
			},
		},
	}

	metaV3 := xdr.TransactionMetaV3{
		Operations: []xdr.OperationMeta{},
		SorobanMeta: &xdr.SorobanTransactionMeta{
			Events: []xdr.ContractEvent{event},
		},
	}

	envelope := xdr.TransactionV1Envelope{
		Tx: xdr.Transaction{
			SourceAccount: someTxAccount,
			SeqNum:        xdr.SequenceNumber(54321),
			Operations: []xdr.Operation{
				{
					SourceAccount: xdr.MustMuxedAddressPtr(admin),
					Body: xdr.OperationBody{
						Type:                 xdr.OperationTypeInvokeHostFunction,
						InvokeHostFunctionOp: &xdr.InvokeHostFunctionOp{},
					},
				},
			},
		},
	}

	return ingest.LedgerTransaction{
		Index:  0,
		Ledger: someLcm,
		Envelope: xdr.TransactionEnvelope{
			Type: xdr.EnvelopeTypeEnvelopeTypeTx,
			V1:   &envelope,
		},
		Result: xdr.TransactionResultPair{
			TransactionHash: xdr.Hash([32]byte{}),
			Result: xdr.TransactionResult{
				FeeCharged: 1234,
				Result: xdr.TransactionResultResult{
					Code:    xdr.TransactionResultCodeTxSuccess,
					Results: &[]xdr.OperationResult{},
				},
			},
		},
		UnsafeMeta: xdr.TransactionMeta{V: 3, V3: &metaV3},
	}
}

// createInsufficientTopicsTx creates a transaction with insufficient topics for set_authorized event
func createInsufficientTopicsTx(
	account string,
	admin string,
	asset xdr.Asset,
	isAuthorized bool,
) ingest.LedgerTransaction {
	rawContractID, err := asset.ContractID(networkPassphrase)
	if err != nil {
		panic(err)
	}
	contractID := xdr.ContractId(rawContractID)

	// Create set_authorized event with only 2 topics (V3 needs 4, V4 needs 3)
	topics := []xdr.ScVal{
		makeSymbol("set_authorized"),
		makeAddressFromString(account),
		// Missing other required topics
	}

	event := xdr.ContractEvent{
		Type:       xdr.ContractEventTypeContract,
		ContractId: &contractID,
		Body: xdr.ContractEventBody{
			V: 0,
			V0: &xdr.ContractEventV0{
				Topics: xdr.ScVec(topics),
				Data:   makeBooleanData(isAuthorized),
			},
		},
	}

	metaV3 := xdr.TransactionMetaV3{
		Operations: []xdr.OperationMeta{},
		SorobanMeta: &xdr.SorobanTransactionMeta{
			Events: []xdr.ContractEvent{event},
		},
	}

	envelope := xdr.TransactionV1Envelope{
		Tx: xdr.Transaction{
			SourceAccount: someTxAccount,
			SeqNum:        xdr.SequenceNumber(54321),
			Operations: []xdr.Operation{
				{
					SourceAccount: xdr.MustMuxedAddressPtr(admin),
					Body: xdr.OperationBody{
						Type:                 xdr.OperationTypeInvokeHostFunction,
						InvokeHostFunctionOp: &xdr.InvokeHostFunctionOp{},
					},
				},
			},
		},
	}

	return ingest.LedgerTransaction{
		Index:  0,
		Ledger: someLcm,
		Envelope: xdr.TransactionEnvelope{
			Type: xdr.EnvelopeTypeEnvelopeTypeTx,
			V1:   &envelope,
		},
		Result: xdr.TransactionResultPair{
			TransactionHash: xdr.Hash([32]byte{}),
			Result: xdr.TransactionResult{
				FeeCharged: 1234,
				Result: xdr.TransactionResultResult{
					Code:    xdr.TransactionResultCodeTxSuccess,
					Results: &[]xdr.OperationResult{},
				},
			},
		},
		UnsafeMeta: xdr.TransactionMeta{V: 3, V3: &metaV3},
	}
}

// createNonSACEventTx creates a transaction with non-set_authorized events
func createNonSACEventTx(
	account string,
	admin string,
	asset xdr.Asset,
	isAuthorized bool,
) ingest.LedgerTransaction {
	rawContractID, err := asset.ContractID(networkPassphrase)
	if err != nil {
		panic(err)
	}
	contractID := xdr.ContractId(rawContractID)

	// Create transfer event instead of set_authorized
	topics := []xdr.ScVal{
		makeSymbol("transfer"), // Different function name
		makeAddressFromString(admin),
		makeAddressFromString(account),
		makeAssetString(asset),
	}

	event := xdr.ContractEvent{
		Type:       xdr.ContractEventTypeContract,
		ContractId: &contractID,
		Body: xdr.ContractEventBody{
			V: 0,
			V0: &xdr.ContractEventV0{
				Topics: xdr.ScVec(topics),
				Data:   makeBooleanData(isAuthorized),
			},
		},
	}

	metaV3 := xdr.TransactionMetaV3{
		Operations: []xdr.OperationMeta{},
		SorobanMeta: &xdr.SorobanTransactionMeta{
			Events: []xdr.ContractEvent{event},
		},
	}

	envelope := xdr.TransactionV1Envelope{
		Tx: xdr.Transaction{
			SourceAccount: someTxAccount,
			SeqNum:        xdr.SequenceNumber(54321),
			Operations: []xdr.Operation{
				{
					SourceAccount: xdr.MustMuxedAddressPtr(admin),
					Body: xdr.OperationBody{
						Type:                 xdr.OperationTypeInvokeHostFunction,
						InvokeHostFunctionOp: &xdr.InvokeHostFunctionOp{},
					},
				},
			},
		},
	}

	return ingest.LedgerTransaction{
		Index:  0,
		Ledger: someLcm,
		Envelope: xdr.TransactionEnvelope{
			Type: xdr.EnvelopeTypeEnvelopeTypeTx,
			V1:   &envelope,
		},
		Result: xdr.TransactionResultPair{
			TransactionHash: xdr.Hash([32]byte{}),
			Result: xdr.TransactionResult{
				FeeCharged: 1234,
				Result: xdr.TransactionResultResult{
					Code:    xdr.TransactionResultCodeTxSuccess,
					Results: &[]xdr.OperationResult{},
				},
			},
		},
		UnsafeMeta: xdr.TransactionMeta{V: 3, V3: &metaV3},
	}
}

func makeSymbol(sym string) xdr.ScVal {
	symbol := xdr.ScSymbol(sym)
	return xdr.ScVal{
		Type: xdr.ScValTypeScvSymbol,
		Sym:  &symbol,
	}
}

func makeBooleanData(value bool) xdr.ScVal {
	return xdr.ScVal{
		Type: xdr.ScValTypeScvBool,
		B:    &value,
	}
}

func makeAddressFromString(address string) xdr.ScVal {
	addr := xdr.MustAddress(address)
	return xdr.ScVal{
		Type: xdr.ScValTypeScvAddress,
		Address: &xdr.ScAddress{
			Type:      xdr.ScAddressTypeScAddressTypeAccount,
			AccountId: &addr,
		},
	}
}

func makeAssetString(asset xdr.Asset) xdr.ScVal {
	assetStr := asset.StringCanonical()
	assetStrXdr := xdr.ScString(assetStr)
	return xdr.ScVal{
		Type: xdr.ScValTypeScvString,
		Str:  &assetStrXdr,
	}
}

func assertContractEvent(t *testing.T, change types.StateChange, reason types.StateChangeReason, expectedAccount string, expectedContractID string) {
	t.Helper()
	require.Equal(t, types.StateChangeCategoryBalanceAuthorization, change.StateChangeCategory)
	require.Equal(t, expectedAccount, change.AccountID)
	if expectedContractID != "" {
		require.NotNil(t, change.TokenID)
		require.Equal(t, expectedContractID, change.TokenID.String)
	}
	require.Equal(t, reason, *change.StateChangeReason)
}
