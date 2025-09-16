package contracts

import (
	"testing"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/indexer/types"
)

// TxBuilder provides a fluent interface for building test transactions
type testTxBuilder struct {
	account      string
	admin        string
	asset        xdr.Asset
	isAuthorized bool
	version      int

	// For trustline tests
	previouslyAuthorized          bool
	previouslyMaintainLiabilities bool

	// For contract tests
	contractAccount              string
	previouslyContractAuthorized bool

	// Event modifications
	invalidEventType   bool
	insufficientTopics bool
	nonSACEvent        bool
	mismatchedAsset    bool
	invalidBalanceMap  string
	missingChanges     bool
	creationOnly       bool
}

// NewTxBuilder creates a new transaction builder
func newTestTxBuilder(account, admin string, asset xdr.Asset, isAuthorized bool, version int) *testTxBuilder {
	return &testTxBuilder{
		account:      account,
		admin:        admin,
		asset:        asset,
		isAuthorized: isAuthorized,
		version:      version,
	}
}

// WithTrustlineState sets previous trustline flag states
func (b *TxBuilder) WithTrustlineState(wasAuthorized, wasMaintainLiabilities bool) *TxBuilder {
	b.previouslyAuthorized = wasAuthorized
	b.previouslyMaintainLiabilities = wasMaintainLiabilities
	return b
}

// WithContractState sets contract-specific state
func (b *TxBuilder) WithContractState(contractAccount string, wasAuthorized bool) *TxBuilder {
	b.contractAccount = contractAccount
	b.previouslyContractAuthorized = wasAuthorized
	return b
}

// WithInvalidEventType makes the event have wrong type
func (b *TxBuilder) WithInvalidEventType() *TxBuilder {
	b.invalidEventType = true
	return b
}

// WithInsufficientTopics makes the event have insufficient topics
func (b *TxBuilder) WithInsufficientTopics() *TxBuilder {
	b.insufficientTopics = true
	return b
}

// WithNonSACEvent makes the event be a transfer instead of set_authorized
func (b *TxBuilder) WithNonSACEvent() *TxBuilder {
	b.nonSACEvent = true
	return b
}

// WithMismatchedAsset makes the ledger changes use a different asset
func (b *TxBuilder) WithMismatchedAsset() *TxBuilder {
	b.mismatchedAsset = true
	return b
}

// WithInvalidBalanceMap sets invalid balance map structure for contract tests
func (b *TxBuilder) WithInvalidBalanceMap(mapType string) *TxBuilder {
	b.invalidBalanceMap = mapType
	return b
}

// WithMissingChanges creates transaction without any ledger changes
func (b *TxBuilder) WithMissingChanges() *TxBuilder {
	b.missingChanges = true
	return b
}

// WithCreationOnly creates only creation changes (no previous state)
func (b *TxBuilder) WithCreationOnly() *TxBuilder {
	b.creationOnly = true
	return b
}

// BuildTrustline builds a transaction with trustline changes
func (b *TxBuilder) BuildTrustline() ingest.LedgerTransaction {
	return b.buildWithChanges(b.createTrustlineChanges())
}

// BuildContract builds a transaction with contract data changes
func (b *TxBuilder) BuildContract() ingest.LedgerTransaction {
	return b.buildWithChanges(b.createContractDataChanges())
}

// BuildEventOnly builds a transaction with only contract events (no ledger changes)
func (b *TxBuilder) BuildEventOnly() ingest.LedgerTransaction {
	return b.buildWithChanges(nil)
}

// buildWithChanges creates the base transaction and applies ledger changes
func (b *TxBuilder) buildWithChanges(changes []xdr.LedgerEntryChange) ingest.LedgerTransaction {
	// Create base transaction with contract event
	tx := b.createBaseTx()

	// Apply ledger changes if any
	if changes != nil && !b.missingChanges {
		b.addChangesToTx(&tx, changes)
	}

	return tx
}

// createBaseTx creates the base transaction structure with contract event
func (b *TxBuilder) createBaseTx() ingest.LedgerTransaction {
	rawContractID, err := b.asset.ContractID(networkPassphrase)
	if err != nil {
		panic(err)
	}
	contractID := xdr.ContractId(rawContractID)

	// Create contract event
	event := b.createContractEvent(contractID)

	// Create transaction metadata based on version
	var unsafeMeta xdr.TransactionMeta
	if b.version == 3 {
		unsafeMeta = xdr.TransactionMeta{
			V: 3,
			V3: &xdr.TransactionMetaV3{
				Operations: []xdr.OperationMeta{},
				SorobanMeta: &xdr.SorobanTransactionMeta{
					Events: []xdr.ContractEvent{event},
				},
			},
		}
	} else {
		unsafeMeta = xdr.TransactionMeta{
			V: 4,
			V4: &xdr.TransactionMetaV4{
				Operations: []xdr.OperationMetaV2{{
					Events: []xdr.ContractEvent{event},
				}},
			},
		}
	}

	// Create transaction envelope
	envelope := xdr.TransactionV1Envelope{
		Tx: xdr.Transaction{
			SourceAccount: someTxAccount,
			SeqNum:        xdr.SequenceNumber(54321),
			Operations: []xdr.Operation{{
				SourceAccount: xdr.MustMuxedAddressPtr(b.admin),
				Body: xdr.OperationBody{
					Type:                 xdr.OperationTypeInvokeHostFunction,
					InvokeHostFunctionOp: &xdr.InvokeHostFunctionOp{},
				},
			}},
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
		UnsafeMeta: unsafeMeta,
	}
}

// createContractEvent creates the contract event based on builder settings
func (b *TxBuilder) createContractEvent(contractID xdr.ContractId) xdr.ContractEvent {
	// Handle event type modification
	eventType := xdr.ContractEventTypeContract
	if b.invalidEventType {
		eventType = xdr.ContractEventTypeSystem
	}

	// Create topics based on version and modifications
	var topics []xdr.ScVal
	functionName := "set_authorized"
	if b.nonSACEvent {
		functionName = "transfer"
	}

	if b.insufficientTopics {
		// Only 2 topics (insufficient for both V3 and V4)
		topics = []xdr.ScVal{
			makeSymbol(functionName),
			makeAddressFromString(b.getTargetAccount()),
		}
	} else if b.version == 3 {
		// V3 format: [function, admin, account, asset]
		topics = []xdr.ScVal{
			makeSymbol(functionName),
			makeAddressFromString(b.admin),
			makeAddressFromString(b.getTargetAccount()),
			makeAssetString(b.asset),
		}
	} else {
		// V4 format: [function, account, asset]
		topics = []xdr.ScVal{
			makeSymbol(functionName),
			makeAddressFromString(b.getTargetAccount()),
			makeAssetString(b.asset),
		}
	}

	return xdr.ContractEvent{
		Type:       eventType,
		ContractId: &contractID,
		Body: xdr.ContractEventBody{
			V: 0,
			V0: &xdr.ContractEventV0{
				Topics: xdr.ScVec(topics),
				Data:   makeBooleanData(b.isAuthorized),
			},
		},
	}
}

// getTargetAccount returns the appropriate target account (contract or regular)
func (b *TxBuilder) getTargetAccount() string {
	if b.contractAccount != "" {
		return b.contractAccount
	}
	return b.account
}

// createTrustlineChanges creates trustline ledger entry changes
func (b *TxBuilder) createTrustlineChanges() []xdr.LedgerEntryChange {
	if b.creationOnly {
		return b.createTrustlineCreationChanges()
	}

	accountID := xdr.MustAddress(b.account)
	asset := b.asset
	if b.mismatchedAsset {
		asset = xdr.MustNewCreditAsset("ALTASSET", b.admin)
	}

	// Calculate previous flags
	var prevFlags xdr.Uint32
	if b.previouslyAuthorized {
		prevFlags |= xdr.Uint32(xdr.TrustLineFlagsAuthorizedFlag)
	}
	if b.previouslyMaintainLiabilities {
		prevFlags |= xdr.Uint32(xdr.TrustLineFlagsAuthorizedToMaintainLiabilitiesFlag)
	}

	// Calculate new flags based on authorization state
	var newFlags xdr.Uint32
	if b.isAuthorized {
		newFlags |= xdr.Uint32(xdr.TrustLineFlagsAuthorizedFlag)
	} else {
		newFlags |= xdr.Uint32(xdr.TrustLineFlagsAuthorizedToMaintainLiabilitiesFlag)
	}

	// Create trustline entries
	prevTrustline := xdr.TrustLineEntry{
		AccountId: accountID,
		Asset:     asset.ToTrustLineAsset(),
		Balance:   1000000,
		Limit:     9223372036854775807,
		Flags:     prevFlags,
	}

	newTrustline := prevTrustline
	newTrustline.Flags = newFlags

	return []xdr.LedgerEntryChange{
		{
			Type: xdr.LedgerEntryChangeTypeLedgerEntryState,
			State: &xdr.LedgerEntry{
				LastModifiedLedgerSeq: 12344,
				Data: xdr.LedgerEntryData{
					Type:      xdr.LedgerEntryTypeTrustline,
					TrustLine: &prevTrustline,
				},
			},
		},
		{
			Type: xdr.LedgerEntryChangeTypeLedgerEntryUpdated,
			Updated: &xdr.LedgerEntry{
				LastModifiedLedgerSeq: 12345,
				Data: xdr.LedgerEntryData{
					Type:      xdr.LedgerEntryTypeTrustline,
					TrustLine: &newTrustline,
				},
			},
		},
	}
}

// createTrustlineCreationChanges creates only creation changes for trustline
func (b *TxBuilder) createTrustlineCreationChanges() []xdr.LedgerEntryChange {
	accountID := xdr.MustAddress(b.account)

	var newFlags xdr.Uint32
	if b.isAuthorized {
		newFlags |= xdr.Uint32(xdr.TrustLineFlagsAuthorizedFlag)
	} else {
		newFlags |= xdr.Uint32(xdr.TrustLineFlagsAuthorizedToMaintainLiabilitiesFlag)
	}

	trustline := xdr.TrustLineEntry{
		AccountId: accountID,
		Asset:     b.asset.ToTrustLineAsset(),
		Balance:   1000000,
		Limit:     9223372036854775807,
		Flags:     newFlags,
	}

	return []xdr.LedgerEntryChange{{
		Type: xdr.LedgerEntryChangeTypeLedgerEntryCreated,
		Created: &xdr.LedgerEntry{
			LastModifiedLedgerSeq: 12345,
			Data: xdr.LedgerEntryData{
				Type:      xdr.LedgerEntryTypeTrustline,
				TrustLine: &trustline,
			},
		},
	}}
}

// createContractDataChanges creates contract data ledger entry changes
func (b *TxBuilder) createContractDataChanges() []xdr.LedgerEntryChange {
	if b.creationOnly {
		return b.createContractDataCreationChanges()
	}
	if b.invalidBalanceMap != "" {
		return b.createInvalidContractDataChanges()
	}

	assetContractIDBytes, err := b.asset.ContractID(networkPassphrase)
	if err != nil {
		panic(err)
	}
	assetContractID := xdr.ContractId(assetContractIDBytes)
	if b.mismatchedAsset {
		altAsset := xdr.MustNewCreditAsset("ALTASSET", b.admin)
		altContractIDBytes, err := altAsset.ContractID(networkPassphrase)
		if err != nil {
			panic(err)
		}
		assetContractID = xdr.ContractId(altContractIDBytes)
	}

	// Create balance values
	prevBalanceMap := makeBalanceValueMap(b.previouslyContractAuthorized)
	newBalanceMap := makeBalanceValueMap(b.isAuthorized)

	prevBalanceMapPtr := &prevBalanceMap
	newBalanceMapPtr := &newBalanceMap

	prevBalanceVal := xdr.ScVal{Type: xdr.ScValTypeScvMap, Map: &prevBalanceMapPtr}
	newBalanceVal := xdr.ScVal{Type: xdr.ScValTypeScvMap, Map: &newBalanceMapPtr}

	// Create contract data entries
	contractDataKey := makeBalanceKey(b.contractAccount)
	prevContractData := xdr.ContractDataEntry{
		Ext:        xdr.ExtensionPoint{V: 0},
		Contract:   xdr.ScAddress{Type: xdr.ScAddressTypeScAddressTypeContract, ContractId: &assetContractID},
		Key:        contractDataKey,
		Durability: xdr.ContractDataDurabilityPersistent,
		Val:        prevBalanceVal,
	}

	newContractData := prevContractData
	newContractData.Val = newBalanceVal

	return []xdr.LedgerEntryChange{
		{
			Type: xdr.LedgerEntryChangeTypeLedgerEntryState,
			State: &xdr.LedgerEntry{
				LastModifiedLedgerSeq: 12344,
				Data: xdr.LedgerEntryData{
					Type:         xdr.LedgerEntryTypeContractData,
					ContractData: &prevContractData,
				},
			},
		},
		{
			Type: xdr.LedgerEntryChangeTypeLedgerEntryUpdated,
			Updated: &xdr.LedgerEntry{
				LastModifiedLedgerSeq: 12345,
				Data: xdr.LedgerEntryData{
					Type:         xdr.LedgerEntryTypeContractData,
					ContractData: &newContractData,
				},
			},
		},
	}
}

// createContractDataCreationChanges creates only creation changes for contract data
func (b *TxBuilder) createContractDataCreationChanges() []xdr.LedgerEntryChange {
	assetContractIDBytes, err := b.asset.ContractID(networkPassphrase)
	if err != nil {
		panic(err)
	}
	assetContractID := xdr.ContractId(assetContractIDBytes)

	balanceMap := makeBalanceValueMap(b.isAuthorized)
	balanceMapPtr := &balanceMap
	balanceVal := xdr.ScVal{Type: xdr.ScValTypeScvMap, Map: &balanceMapPtr}

	contractDataKey := makeBalanceKey(b.contractAccount)
	contractData := xdr.ContractDataEntry{
		Ext:        xdr.ExtensionPoint{V: 0},
		Contract:   xdr.ScAddress{Type: xdr.ScAddressTypeScAddressTypeContract, ContractId: &assetContractID},
		Key:        contractDataKey,
		Durability: xdr.ContractDataDurabilityPersistent,
		Val:        balanceVal,
	}

	return []xdr.LedgerEntryChange{{
		Type: xdr.LedgerEntryChangeTypeLedgerEntryCreated,
		Created: &xdr.LedgerEntry{
			LastModifiedLedgerSeq: 12345,
			Data: xdr.LedgerEntryData{
				Type:         xdr.LedgerEntryTypeContractData,
				ContractData: &contractData,
			},
		},
	}}
}

// createInvalidContractDataChanges creates contract data with invalid balance map
func (b *TxBuilder) createInvalidContractDataChanges() []xdr.LedgerEntryChange {
	assetContractIDBytes, err := b.asset.ContractID(networkPassphrase)
	if err != nil {
		panic(err)
	}
	assetContractID := xdr.ContractId(assetContractIDBytes)

	var invalidBalanceMap xdr.ScMap
	switch b.invalidBalanceMap {
	case "wrong_entry_count":
		invalidBalanceMap = xdr.ScMap{
			xdr.ScMapEntry{Key: makeSymbol("amount"), Val: makeInt128(1000000)},
			xdr.ScMapEntry{Key: makeSymbol("authorized"), Val: makeBooleanData(true)},
		}
	case "missing_authorized_key":
		invalidBalanceMap = xdr.ScMap{
			xdr.ScMapEntry{Key: makeSymbol("amount"), Val: makeInt128(1000000)},
			xdr.ScMapEntry{Key: makeSymbol("clawback"), Val: makeBooleanData(false)},
			xdr.ScMapEntry{Key: makeSymbol("other"), Val: makeBooleanData(true)},
		}
	case "wrong_authorized_type":
		invalidBalanceMap = xdr.ScMap{
			xdr.ScMapEntry{Key: makeSymbol("amount"), Val: makeInt128(1000000)},
			xdr.ScMapEntry{Key: makeSymbol("authorized"), Val: makeInt128(1)},
			xdr.ScMapEntry{Key: makeSymbol("clawback"), Val: makeBooleanData(false)},
		}
	default:
		invalidBalanceMap = makeBalanceValueMap(true)
	}

	invalidBalanceMapPtr := &invalidBalanceMap
	invalidBalanceVal := xdr.ScVal{Type: xdr.ScValTypeScvMap, Map: &invalidBalanceMapPtr}

	contractDataKey := makeBalanceKey(b.contractAccount)
	contractData := xdr.ContractDataEntry{
		Ext:        xdr.ExtensionPoint{V: 0},
		Contract:   xdr.ScAddress{Type: xdr.ScAddressTypeScAddressTypeContract, ContractId: &assetContractID},
		Key:        contractDataKey,
		Durability: xdr.ContractDataDurabilityPersistent,
		Val:        invalidBalanceVal,
	}

	return []xdr.LedgerEntryChange{{
		Type: xdr.LedgerEntryChangeTypeLedgerEntryState,
		State: &xdr.LedgerEntry{
			LastModifiedLedgerSeq: 12344,
			Data: xdr.LedgerEntryData{
				Type:         xdr.LedgerEntryTypeContractData,
				ContractData: &contractData,
			},
		},
	}}
}

// addChangesToTx adds ledger entry changes to the transaction
func (b *TxBuilder) addChangesToTx(tx *ingest.LedgerTransaction, changes []xdr.LedgerEntryChange) {
	if b.version == 3 {
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
}

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

// Legacy functions for backward compatibility - now use TxBuilder internally

func createSACInvocationTxV3(account, admin string, asset xdr.Asset, isAuthorized bool) ingest.LedgerTransaction {
	return NewTxBuilder(account, admin, asset, isAuthorized, 3).BuildEventOnly()
}

func createInvalidContractEventTx(account, admin string, asset xdr.Asset, isAuthorized bool) ingest.LedgerTransaction {
	return NewTxBuilder(account, admin, asset, isAuthorized, 3).WithInvalidEventType().BuildEventOnly()
}

func createInsufficientTopicsTx(account, admin string, asset xdr.Asset, isAuthorized bool) ingest.LedgerTransaction {
	return NewTxBuilder(account, admin, asset, isAuthorized, 3).WithInsufficientTopics().BuildEventOnly()
}

func createNonSACEventTx(account, admin string, asset xdr.Asset, isAuthorized bool) ingest.LedgerTransaction {
	return NewTxBuilder(account, admin, asset, isAuthorized, 3).WithNonSACEvent().BuildEventOnly()
}

func createTxWithMismatchedTrustlineChanges(account, admin string, asset xdr.Asset, isAuthorized bool, version int) ingest.LedgerTransaction {
	return NewTxBuilder(account, admin, asset, isAuthorized, version).
		WithTrustlineState(false, false).
		WithMismatchedAsset().
		BuildTrustline()
}

func createTxWithMismatchedContractDataChanges(contractAccount, admin string, asset xdr.Asset, isAuthorized, previouslyAuthorized bool, version int) ingest.LedgerTransaction {
	return NewTxBuilder("", admin, asset, isAuthorized, version).
		WithContractState(contractAccount, previouslyAuthorized).
		WithMismatchedAsset().
		BuildContract()
}

func createTxWithTrustlineChanges(account, admin string, asset xdr.Asset, isAuthorized, previouslyAuthorized, previouslyMaintainLiabilities bool, version int) ingest.LedgerTransaction {
	return NewTxBuilder(account, admin, asset, isAuthorized, version).
		WithTrustlineState(previouslyAuthorized, previouslyMaintainLiabilities).
		BuildTrustline()
}

func createTxWithoutTrustlineChanges(account, admin string, asset xdr.Asset, isAuthorized bool, version int) ingest.LedgerTransaction {
	return NewTxBuilder(account, admin, asset, isAuthorized, version).WithMissingChanges().BuildEventOnly()
}

func createTxWithTrustlineCreation(account, admin string, asset xdr.Asset, isAuthorized bool, version int) ingest.LedgerTransaction {
	return NewTxBuilder(account, admin, asset, isAuthorized, version).WithCreationOnly().BuildTrustline()
}

func createTxWithContractDataChanges(contractAccount, admin string, asset xdr.Asset, isAuthorized, previouslyAuthorized bool, version int) ingest.LedgerTransaction {
	return NewTxBuilder("", admin, asset, isAuthorized, version).
		WithContractState(contractAccount, previouslyAuthorized).
		BuildContract()
}

func createTxWithoutContractDataChanges(contractAccount, admin string, asset xdr.Asset, isAuthorized bool, version int) ingest.LedgerTransaction {
	return NewTxBuilder(contractAccount, admin, asset, isAuthorized, version).WithMissingChanges().BuildEventOnly()
}

func createTxWithContractDataCreation(contractAccount, admin string, asset xdr.Asset, isAuthorized bool, version int) ingest.LedgerTransaction {
	return NewTxBuilder("", admin, asset, isAuthorized, version).
		WithContractState(contractAccount, false).
		WithCreationOnly().
		BuildContract()
}

func createInvalidBalanceMapTx(contractAccount, admin string, asset xdr.Asset, isAuthorized bool, mapType string) ingest.LedgerTransaction {
	return NewTxBuilder("", admin, asset, isAuthorized, 4).
		WithContractState(contractAccount, false).
		WithInvalidBalanceMap(mapType).
		BuildContract()
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
func makeBalanceValueMap(authorized bool) xdr.ScMap {
	return xdr.ScMap{
		xdr.ScMapEntry{
			Key: makeSymbol("amount"),
			Val: makeInt128(1000000),
		},
		xdr.ScMapEntry{
			Key: makeSymbol("authorized"),
			Val: makeBooleanData(authorized),
		},
		xdr.ScMapEntry{
			Key: makeSymbol("clawback"),
			Val: makeBooleanData(false),
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
