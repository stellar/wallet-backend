package contracts

import (
	"testing"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/strkey"
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
	if len(address) > 0 && address[0] == 'C' {
		// Contract address (C...)
		contractIDBytes, err := strkey.Decode(strkey.VersionByteContract, address)
		if err != nil {
			panic(err)
		}
		var contractHash xdr.Hash
		copy(contractHash[:], contractIDBytes)
		contractID := xdr.ContractId(contractHash)
		return xdr.ScVal{
			Type: xdr.ScValTypeScvAddress,
			Address: &xdr.ScAddress{
				Type:       xdr.ScAddressTypeScAddressTypeContract,
				ContractId: &contractID,
			},
		}
	} else {
		// Account address (G...)
		addr := xdr.MustAddress(address)
		return xdr.ScVal{
			Type: xdr.ScValTypeScvAddress,
			Address: &xdr.ScAddress{
				Type:      xdr.ScAddressTypeScAddressTypeAccount,
				AccountId: &addr,
			},
		}
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

func createSACInvocationTxWithMismatchedTrustlineChanges(
	account string,
	admin string,
	asset xdr.Asset,
	isAuthorized bool,
	version int,
) ingest.LedgerTransaction {
	tx := createSACInvocationTxWithTrustlineChanges(account, admin, asset, isAuthorized, false, false, version)
	altAsset := xdr.MustNewCreditAsset("ALTASSET", admin)
	overrideTrustlineAsset(&tx, altAsset.ToTrustLineAsset(), version)
	return tx
}

func overrideTrustlineAsset(tx *ingest.LedgerTransaction, trustlineAsset xdr.TrustLineAsset, version int) {
	updateEntry := func(entry *xdr.LedgerEntry) {
		if entry == nil {
			return
		}
		if entry.Data.Type != xdr.LedgerEntryTypeTrustline || entry.Data.TrustLine == nil {
			return
		}
		entry.Data.TrustLine.Asset = trustlineAsset
	}

	if version == 3 {
		for i := range tx.UnsafeMeta.V3.Operations {
			op := &tx.UnsafeMeta.V3.Operations[i]
			for j := range op.Changes {
				change := &op.Changes[j]
				updateEntry(change.State)
				updateEntry(change.Updated)
			}
		}
	} else {
		for i := range tx.UnsafeMeta.V4.Operations {
			op := &tx.UnsafeMeta.V4.Operations[i]
			for j := range op.Changes {
				change := &op.Changes[j]
				updateEntry(change.State)
				updateEntry(change.Updated)
			}
		}
	}
}

func createSACInvocationTxWithMismatchedContractDataChanges(
	contractAccount string,
	admin string,
	asset xdr.Asset,
	isAuthorized bool,
	previouslyAuthorized bool,
	version int,
) ingest.LedgerTransaction {
	tx := createSACInvocationTxWithContractDataChanges(contractAccount, admin, asset, isAuthorized, previouslyAuthorized, version)
	altAsset := xdr.MustNewCreditAsset("ALTASSET", admin)
	altContractIDBytes, err := altAsset.ContractID(networkPassphrase)
	if err != nil {
		panic(err)
	}
	altContractID := xdr.ContractId(altContractIDBytes)
	overrideContractDataContract(&tx, altContractID, version)
	return tx
}

func overrideContractDataContract(tx *ingest.LedgerTransaction, contractID xdr.ContractId, version int) {
	setContract := func(entry *xdr.LedgerEntry) {
		if entry == nil {
			return
		}
		if entry.Data.Type != xdr.LedgerEntryTypeContractData || entry.Data.ContractData == nil {
			return
		}
		if entry.Data.ContractData.Contract.Type == xdr.ScAddressTypeScAddressTypeContract && entry.Data.ContractData.Contract.ContractId != nil {
			*entry.Data.ContractData.Contract.ContractId = contractID
			return
		}
		contractHash := contractID
		entry.Data.ContractData.Contract = xdr.ScAddress{
			Type:       xdr.ScAddressTypeScAddressTypeContract,
			ContractId: &contractHash,
		}
	}

	if version == 3 {
		for i := range tx.UnsafeMeta.V3.Operations {
			op := &tx.UnsafeMeta.V3.Operations[i]
			for j := range op.Changes {
				change := &op.Changes[j]
				setContract(change.State)
				setContract(change.Updated)
			}
		}
	} else {
		for i := range tx.UnsafeMeta.V4.Operations {
			op := &tx.UnsafeMeta.V4.Operations[i]
			for j := range op.Changes {
				change := &op.Changes[j]
				setContract(change.State)
				setContract(change.Updated)
			}
		}
	}
}

// createSACInvocationTxWithTrustlineChanges creates a transaction with both SAC events and trustline changes
func createSACInvocationTxWithTrustlineChanges(
	account string,
	admin string,
	asset xdr.Asset,
	isAuthorized bool,
	previouslyAuthorized bool,
	previouslyMaintainLiabilities bool,
	version int,
) ingest.LedgerTransaction {
	// Create the base transaction
	var tx ingest.LedgerTransaction
	if version == 3 {
		tx = createSACInvocationTxV3(account, admin, asset, isAuthorized)
	} else {
		tx = createSACInvocationTxV4(account, admin, asset, isAuthorized)
	}

	// Create trustline changes
	accountID := xdr.MustAddress(account)

	// Previous trustline state
	var prevFlags xdr.Uint32
	if previouslyAuthorized {
		prevFlags |= xdr.Uint32(xdr.TrustLineFlagsAuthorizedFlag)
	}
	if previouslyMaintainLiabilities {
		prevFlags |= xdr.Uint32(xdr.TrustLineFlagsAuthorizedToMaintainLiabilitiesFlag)
	}

	// New trustline state
	var newFlags xdr.Uint32
	if isAuthorized {
		newFlags |= xdr.Uint32(xdr.TrustLineFlagsAuthorizedFlag)
		// Clear maintain liabilities when authorizing
	} else {
		// Clear authorized when deauthorizing, set maintain liabilities
		newFlags |= xdr.Uint32(xdr.TrustLineFlagsAuthorizedToMaintainLiabilitiesFlag)
	}

	prevTrustline := xdr.TrustLineEntry{
		AccountId: accountID,
		Asset:     asset.ToTrustLineAsset(),
		Balance:   1000000,
		Limit:     9223372036854775807,
		Flags:     prevFlags,
	}

	newTrustline := prevTrustline
	newTrustline.Flags = newFlags

	// Create proper before/after changes as expected by stellar-go
	stateChange := xdr.LedgerEntryChange{
		Type: xdr.LedgerEntryChangeTypeLedgerEntryState,
		State: &xdr.LedgerEntry{
			LastModifiedLedgerSeq: 12344,
			Data: xdr.LedgerEntryData{
				Type:      xdr.LedgerEntryTypeTrustline,
				TrustLine: &prevTrustline,
			},
		},
	}

	updateChange := xdr.LedgerEntryChange{
		Type: xdr.LedgerEntryChangeTypeLedgerEntryUpdated,
		Updated: &xdr.LedgerEntry{
			LastModifiedLedgerSeq: 12345,
			Data: xdr.LedgerEntryData{
				Type:      xdr.LedgerEntryTypeTrustline,
				TrustLine: &newTrustline,
			},
		},
	}

	changes := []xdr.LedgerEntryChange{stateChange, updateChange}

	// Add the trustline changes to the operation meta
	if version == 3 {
		if tx.UnsafeMeta.V3.Operations == nil {
			tx.UnsafeMeta.V3.Operations = []xdr.OperationMeta{}
		}
		tx.UnsafeMeta.V3.Operations = append(tx.UnsafeMeta.V3.Operations, xdr.OperationMeta{
			Changes: changes,
		})
	} else {
		if len(tx.UnsafeMeta.V4.Operations) > 0 {
			tx.UnsafeMeta.V4.Operations[0].Changes = changes
		}
	}

	return tx
}

// createTxWithoutTrustlineChanges creates a transaction with SAC events but no trustline changes
// This simulates the error case where trustline changes are not found
func createTxWithoutTrustlineChanges(
	account string,
	admin string,
	asset xdr.Asset,
	isAuthorized bool,
	version int,
) ingest.LedgerTransaction {
	if version == 3 {
		return createSACInvocationTxV3(account, admin, asset, isAuthorized)
	}
	return createSACInvocationTxV4(account, admin, asset, isAuthorized)
}

// createSACInvocationTxWithTrustlineCreation creates a transaction where the trustline was just created
// so only a Post image exists for the ledger entry change.
func createSACInvocationTxWithTrustlineCreation(
	account string,
	admin string,
	asset xdr.Asset,
	isAuthorized bool,
	version int,
) ingest.LedgerTransaction {
	tx := createTxWithoutTrustlineChanges(account, admin, asset, isAuthorized, version)

	accountID := xdr.MustAddress(account)

	var newFlags xdr.Uint32
	if isAuthorized {
		newFlags |= xdr.Uint32(xdr.TrustLineFlagsAuthorizedFlag)
	} else {
		newFlags |= xdr.Uint32(xdr.TrustLineFlagsAuthorizedToMaintainLiabilitiesFlag)
	}

	trustline := xdr.TrustLineEntry{
		AccountId: accountID,
		Asset:     asset.ToTrustLineAsset(),
		Balance:   1000000,
		Limit:     9223372036854775807,
		Flags:     newFlags,
	}

	createChange := xdr.LedgerEntryChange{
		Type: xdr.LedgerEntryChangeTypeLedgerEntryCreated,
		Created: &xdr.LedgerEntry{
			LastModifiedLedgerSeq: 12345,
			Data: xdr.LedgerEntryData{
				Type:      xdr.LedgerEntryTypeTrustline,
				TrustLine: &trustline,
			},
		},
	}

	if version == 3 {
		tx.UnsafeMeta.V3.Operations = append(tx.UnsafeMeta.V3.Operations, xdr.OperationMeta{
			Changes: []xdr.LedgerEntryChange{createChange},
		})
	} else if len(tx.UnsafeMeta.V4.Operations) > 0 {
		tx.UnsafeMeta.V4.Operations[0].Changes = []xdr.LedgerEntryChange{createChange}
	}

	return tx
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

// makeContractAddress creates a contract address ScVal from a contract address string (C...)
func makeContractAddress(contractAddress string) xdr.ScVal {
	// Parse the contract address (C...) to get the raw bytes
	contractIDBytes, err := strkey.Decode(strkey.VersionByteContract, contractAddress)
	if err != nil {
		panic(err)
	}
	var contractHash xdr.Hash
	copy(contractHash[:], contractIDBytes)
	contractID := xdr.ContractId(contractHash)
	return xdr.ScVal{
		Type: xdr.ScValTypeScvAddress,
		Address: &xdr.ScAddress{
			Type:       xdr.ScAddressTypeScAddressTypeContract,
			ContractId: &contractID,
		},
	}
}

// generateContractAddress generates a valid contract address string (C...) from asset
func generateContractAddress(asset xdr.Asset) string {
	contractID, err := asset.ContractID(networkPassphrase)
	if err != nil {
		panic(err)
	}
	return strkey.MustEncode(strkey.VersionByteContract, contractID[:])
}

// makeInt128 creates an ScVal with Int128 type for amount values
func makeInt128(value int64) xdr.ScVal {
	// Convert int64 to Int128Parts (high=0, low=value for positive values)
	i128 := xdr.Int128Parts{
		Lo: xdr.Uint64(value),
		Hi: xdr.Int64(0),
	}
	return xdr.ScVal{
		Type: xdr.ScValTypeScvI128,
		I128: &i128,
	}
}

// makeBalanceValueMap creates a BalanceValue ScMap with {amount, authorized, clawback}
func makeBalanceValueMap(amount int64, authorized bool, clawback bool) xdr.ScMap {
	return xdr.ScMap{
		xdr.ScMapEntry{
			Key: makeSymbol("amount"),
			Val: makeInt128(amount),
		},
		xdr.ScMapEntry{
			Key: makeSymbol("authorized"),
			Val: makeBooleanData(authorized),
		},
		xdr.ScMapEntry{
			Key: makeSymbol("clawback"),
			Val: makeBooleanData(clawback),
		},
	}
}

// makeBalanceKey creates a ["Balance", contractAddress] key for contract data
func makeBalanceKey(contractAddress string) xdr.ScVal {
	balanceVec := xdr.ScVec{
		makeSymbol("Balance"),
		makeContractAddress(contractAddress),
	}
	balanceVecPtr := &balanceVec
	return xdr.ScVal{
		Type: xdr.ScValTypeScvVec,
		Vec:  &balanceVecPtr,
	}
}

// createSACInvocationTxWithContractDataChanges creates a transaction with SAC events and contract data changes
func createSACInvocationTxWithContractDataChanges(
	contractAccount string,
	admin string,
	asset xdr.Asset,
	isAuthorized bool,
	previouslyAuthorized bool,
	version int,
) ingest.LedgerTransaction {
	// Create the base transaction
	var tx ingest.LedgerTransaction
	if version == 3 {
		tx = createSACInvocationTxV3(contractAccount, admin, asset, isAuthorized)
	} else {
		tx = createSACInvocationTxV4(contractAccount, admin, asset, isAuthorized)
	}

	// Create contract data changes for BalanceValue
	assetContractIDBytes, err := asset.ContractID(networkPassphrase)
	if err != nil {
		panic(err)
	}
	assetContractID := xdr.Hash(assetContractIDBytes)

	// Previous balance value
	prevBalanceMap := makeBalanceValueMap(1000000, previouslyAuthorized, false)
	prevBalanceMapPtr := &prevBalanceMap
	prevBalanceVal := xdr.ScVal{
		Type: xdr.ScValTypeScvMap,
		Map:  &prevBalanceMapPtr,
	}

	// New balance value
	newBalanceMap := makeBalanceValueMap(1000000, isAuthorized, false)
	newBalanceMapPtr := &newBalanceMap
	newBalanceVal := xdr.ScVal{
		Type: xdr.ScValTypeScvMap,
		Map:  &newBalanceMapPtr,
	}

	// Create contract data entry
	contractDataKey := makeBalanceKey(contractAccount)

	assetContractIDConverted := xdr.ContractId(assetContractID)
	prevContractData := xdr.ContractDataEntry{
		Ext:        xdr.ExtensionPoint{V: 0},
		Contract:   xdr.ScAddress{Type: xdr.ScAddressTypeScAddressTypeContract, ContractId: &assetContractIDConverted},
		Key:        contractDataKey,
		Durability: xdr.ContractDataDurabilityPersistent,
		Val:        prevBalanceVal,
	}

	newContractData := prevContractData
	newContractData.Val = newBalanceVal

	// Create proper before/after changes
	stateChange := xdr.LedgerEntryChange{
		Type: xdr.LedgerEntryChangeTypeLedgerEntryState,
		State: &xdr.LedgerEntry{
			LastModifiedLedgerSeq: 12344,
			Data: xdr.LedgerEntryData{
				Type:         xdr.LedgerEntryTypeContractData,
				ContractData: &prevContractData,
			},
		},
	}

	updateChange := xdr.LedgerEntryChange{
		Type: xdr.LedgerEntryChangeTypeLedgerEntryUpdated,
		Updated: &xdr.LedgerEntry{
			LastModifiedLedgerSeq: 12345,
			Data: xdr.LedgerEntryData{
				Type:         xdr.LedgerEntryTypeContractData,
				ContractData: &newContractData,
			},
		},
	}

	changes := []xdr.LedgerEntryChange{stateChange, updateChange}

	// Add the contract data changes to the operation meta
	if version == 3 {
		if tx.UnsafeMeta.V3.Operations == nil {
			tx.UnsafeMeta.V3.Operations = []xdr.OperationMeta{}
		}
		tx.UnsafeMeta.V3.Operations = append(tx.UnsafeMeta.V3.Operations, xdr.OperationMeta{
			Changes: changes,
		})
	} else {
		if len(tx.UnsafeMeta.V4.Operations) > 0 {
			tx.UnsafeMeta.V4.Operations[0].Changes = changes
		}
	}

	return tx
}

// createTxWithoutContractDataChanges creates a transaction with SAC events but no contract data changes
func createTxWithoutContractDataChanges(
	contractAccount string,
	admin string,
	asset xdr.Asset,
	isAuthorized bool,
	version int,
) ingest.LedgerTransaction {
	if version == 3 {
		return createSACInvocationTxV3(contractAccount, admin, asset, isAuthorized)
	}
	return createSACInvocationTxV4(contractAccount, admin, asset, isAuthorized)
}

// createSACInvocationTxWithContractDataCreation creates a transaction where the contract balance entry
// is newly created so only a Post image exists for the ledger entry change.
func createSACInvocationTxWithContractDataCreation(
	contractAccount string,
	admin string,
	asset xdr.Asset,
	isAuthorized bool,
	version int,
) ingest.LedgerTransaction {
	tx := createTxWithoutContractDataChanges(contractAccount, admin, asset, isAuthorized, version)

	assetContractIDBytes, err := asset.ContractID(networkPassphrase)
	if err != nil {
		panic(err)
	}
	assetContractID := xdr.ContractId(assetContractIDBytes)

	balanceMap := makeBalanceValueMap(1000000, isAuthorized, false)
	balanceMapPtr := &balanceMap
	balanceVal := xdr.ScVal{
		Type: xdr.ScValTypeScvMap,
		Map:  &balanceMapPtr,
	}

	contractDataKey := makeBalanceKey(contractAccount)

	contractData := xdr.ContractDataEntry{
		Ext:        xdr.ExtensionPoint{V: 0},
		Contract:   xdr.ScAddress{Type: xdr.ScAddressTypeScAddressTypeContract, ContractId: &assetContractID},
		Key:        contractDataKey,
		Durability: xdr.ContractDataDurabilityPersistent,
		Val:        balanceVal,
	}

	createChange := xdr.LedgerEntryChange{
		Type: xdr.LedgerEntryChangeTypeLedgerEntryCreated,
		Created: &xdr.LedgerEntry{
			LastModifiedLedgerSeq: 12345,
			Data: xdr.LedgerEntryData{
				Type:         xdr.LedgerEntryTypeContractData,
				ContractData: &contractData,
			},
		},
	}

	if version == 3 {
		tx.UnsafeMeta.V3.Operations = append(tx.UnsafeMeta.V3.Operations, xdr.OperationMeta{
			Changes: []xdr.LedgerEntryChange{createChange},
		})
	} else if len(tx.UnsafeMeta.V4.Operations) > 0 {
		tx.UnsafeMeta.V4.Operations[0].Changes = []xdr.LedgerEntryChange{createChange}
	}

	return tx
}

// createInvalidBalanceMapTx creates a transaction with invalid balance map structure
func createInvalidBalanceMapTx(
	contractAccount string,
	admin string,
	asset xdr.Asset,
	isAuthorized bool,
	mapType string,
) ingest.LedgerTransaction {
	// Create base transaction
	tx := createSACInvocationTxV4(contractAccount, admin, asset, isAuthorized)

	// Create contract data changes with invalid balance map
	assetContractIDBytes, err := asset.ContractID(networkPassphrase)
	if err != nil {
		panic(err)
	}
	assetContractID := xdr.Hash(assetContractIDBytes)

	var invalidBalanceMap xdr.ScMap
	switch mapType {
	case "wrong_entry_count":
		// Create map with wrong number of entries (only 2 instead of 3)
		invalidBalanceMap = xdr.ScMap{
			xdr.ScMapEntry{Key: makeSymbol("amount"), Val: makeInt128(1000000)},
			xdr.ScMapEntry{Key: makeSymbol("authorized"), Val: makeBooleanData(true)},
			// Missing clawback entry
		}
	case "missing_authorized_key":
		// Create map without 'authorized' key
		invalidBalanceMap = xdr.ScMap{
			xdr.ScMapEntry{Key: makeSymbol("amount"), Val: makeInt128(1000000)},
			xdr.ScMapEntry{Key: makeSymbol("clawback"), Val: makeBooleanData(false)},
			xdr.ScMapEntry{Key: makeSymbol("other"), Val: makeBooleanData(true)},
		}
	case "wrong_authorized_type":
		// Create map with wrong type for 'authorized' value
		invalidBalanceMap = xdr.ScMap{
			xdr.ScMapEntry{Key: makeSymbol("amount"), Val: makeInt128(1000000)},
			xdr.ScMapEntry{Key: makeSymbol("authorized"), Val: makeInt128(1)}, // Wrong type - should be bool
			xdr.ScMapEntry{Key: makeSymbol("clawback"), Val: makeBooleanData(false)},
		}
	default:
		// Default to valid map if unknown type
		invalidBalanceMap = makeBalanceValueMap(1000000, true, false)
	}

	invalidBalanceMapPtr := &invalidBalanceMap
	invalidBalanceVal := xdr.ScVal{
		Type: xdr.ScValTypeScvMap,
		Map:  &invalidBalanceMapPtr,
	}

	// Create contract data entry with invalid balance map
	contractDataKey := makeBalanceKey(contractAccount)
	assetContractIDConverted := xdr.ContractId(assetContractID)
	contractData := xdr.ContractDataEntry{
		Ext:        xdr.ExtensionPoint{V: 0},
		Contract:   xdr.ScAddress{Type: xdr.ScAddressTypeScAddressTypeContract, ContractId: &assetContractIDConverted},
		Key:        contractDataKey,
		Durability: xdr.ContractDataDurabilityPersistent,
		Val:        invalidBalanceVal,
	}

	// Create ledger entry changes with the invalid balance map
	stateChange := xdr.LedgerEntryChange{
		Type: xdr.LedgerEntryChangeTypeLedgerEntryState,
		State: &xdr.LedgerEntry{
			LastModifiedLedgerSeq: 12344,
			Data: xdr.LedgerEntryData{
				Type:         xdr.LedgerEntryTypeContractData,
				ContractData: &contractData,
			},
		},
	}

	changes := []xdr.LedgerEntryChange{stateChange}

	// Add the invalid contract data changes to the operation meta
	if len(tx.UnsafeMeta.V4.Operations) > 0 {
		tx.UnsafeMeta.V4.Operations[0].Changes = changes
	}

	return tx
}
