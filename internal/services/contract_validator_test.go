package services

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"testing"

	set "github.com/deckarep/golang-set/v2"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/indexer/types"
)

// Helper functions for creating XDR test data

// createScSpecFunctionEntry creates a function spec entry
func createScSpecFunctionEntry(name string, inputs []xdr.ScSpecFunctionInputV0, outputs []xdr.ScSpecTypeDef) xdr.ScSpecEntry {
	funcName := xdr.ScSymbol(name)
	funcV0 := &xdr.ScSpecFunctionV0{
		Name:    funcName,
		Inputs:  inputs,
		Outputs: outputs,
	}
	return xdr.ScSpecEntry{
		Kind:       xdr.ScSpecEntryKindScSpecEntryFunctionV0,
		FunctionV0: funcV0,
	}
}

// createFunctionInput creates a function input parameter
func createFunctionInput(name string, typeDef xdr.ScSpecTypeDef) xdr.ScSpecFunctionInputV0 {
	return xdr.ScSpecFunctionInputV0{
		Name: name,
		Type: typeDef,
	}
}

// createScSpecTypeDef creates a type definition for a given XDR type
func createScSpecTypeDef(scType xdr.ScSpecType) xdr.ScSpecTypeDef {
	return xdr.ScSpecTypeDef{
		Type: scType,
	}
}

// createSEP41ContractSpec creates a complete valid SEP-41 contract spec
func createSEP41ContractSpec() []xdr.ScSpecEntry {
	addressType := createScSpecTypeDef(xdr.ScSpecTypeScSpecTypeAddress)
	i128Type := createScSpecTypeDef(xdr.ScSpecTypeScSpecTypeI128)
	u32Type := createScSpecTypeDef(xdr.ScSpecTypeScSpecTypeU32)
	stringType := createScSpecTypeDef(xdr.ScSpecTypeScSpecTypeString)

	return []xdr.ScSpecEntry{
		// balance: (id: Address) -> (i128)
		createScSpecFunctionEntry("balance",
			[]xdr.ScSpecFunctionInputV0{createFunctionInput("id", addressType)},
			[]xdr.ScSpecTypeDef{i128Type},
		),
		// allowance: (from: Address, spender: Address) -> (i128)
		createScSpecFunctionEntry("allowance",
			[]xdr.ScSpecFunctionInputV0{
				createFunctionInput("from", addressType),
				createFunctionInput("spender", addressType),
			},
			[]xdr.ScSpecTypeDef{i128Type},
		),
		// decimals: () -> (u32)
		createScSpecFunctionEntry("decimals",
			[]xdr.ScSpecFunctionInputV0{},
			[]xdr.ScSpecTypeDef{u32Type},
		),
		// name: () -> (String)
		createScSpecFunctionEntry("name",
			[]xdr.ScSpecFunctionInputV0{},
			[]xdr.ScSpecTypeDef{stringType},
		),
		// symbol: () -> (String)
		createScSpecFunctionEntry("symbol",
			[]xdr.ScSpecFunctionInputV0{},
			[]xdr.ScSpecTypeDef{stringType},
		),
		// approve: (from: Address, spender: Address, amount: i128, expiration_ledger: u32) -> ()
		createScSpecFunctionEntry("approve",
			[]xdr.ScSpecFunctionInputV0{
				createFunctionInput("from", addressType),
				createFunctionInput("spender", addressType),
				createFunctionInput("amount", i128Type),
				createFunctionInput("expiration_ledger", u32Type),
			},
			[]xdr.ScSpecTypeDef{},
		),
		// transfer: (from: Address, to: Address, amount: i128) -> ()
		createScSpecFunctionEntry("transfer",
			[]xdr.ScSpecFunctionInputV0{
				createFunctionInput("from", addressType),
				createFunctionInput("to", addressType),
				createFunctionInput("amount", i128Type),
			},
			[]xdr.ScSpecTypeDef{},
		),
		// transfer_from: (spender: Address, from: Address, to: Address, amount: i128) -> ()
		createScSpecFunctionEntry("transfer_from",
			[]xdr.ScSpecFunctionInputV0{
				createFunctionInput("spender", addressType),
				createFunctionInput("from", addressType),
				createFunctionInput("to", addressType),
				createFunctionInput("amount", i128Type),
			},
			[]xdr.ScSpecTypeDef{},
		),
		// burn: (from: Address, amount: i128) -> ()
		createScSpecFunctionEntry("burn",
			[]xdr.ScSpecFunctionInputV0{
				createFunctionInput("from", addressType),
				createFunctionInput("amount", i128Type),
			},
			[]xdr.ScSpecTypeDef{},
		),
		// burn_from: (spender: Address, from: Address, amount: i128) -> ()
		createScSpecFunctionEntry("burn_from",
			[]xdr.ScSpecFunctionInputV0{
				createFunctionInput("spender", addressType),
				createFunctionInput("from", addressType),
				createFunctionInput("amount", i128Type),
			},
			[]xdr.ScSpecTypeDef{},
		),
	}
}

// createPartialSEP41Spec creates a SEP-41 spec with missing functions
func createPartialSEP41Spec(missingFunctions []string) []xdr.ScSpecEntry {
	fullSpec := createSEP41ContractSpec()
	missingSet := set.NewSet(missingFunctions...)

	var result []xdr.ScSpecEntry
	for _, entry := range fullSpec {
		if entry.FunctionV0 != nil {
			funcName := string(entry.FunctionV0.Name)
			if !missingSet.Contains(funcName) {
				result = append(result, entry)
			}
		}
	}
	return result
}

// createNonSEP41ContractSpec creates a non-SEP-41 contract spec
func createNonSEP41ContractSpec() []xdr.ScSpecEntry {
	u32Type := createScSpecTypeDef(xdr.ScSpecTypeScSpecTypeU32)
	return []xdr.ScSpecEntry{
		createScSpecFunctionEntry("custom_function",
			[]xdr.ScSpecFunctionInputV0{createFunctionInput("value", u32Type)},
			[]xdr.ScSpecTypeDef{u32Type},
		),
	}
}

// createValidWasmWithSpec creates a minimal valid WASM module with contractspecv0 custom section
func createValidWasmWithSpec(spec []xdr.ScSpecEntry) []byte {
	/*
		Minimal valid WASM module structure
		Magic number: 0x00 0x61 0x73 0x6D (wasm).
		The WASM magic number 0x00 0x61 0x73 0x6D is the file signature that identifies a WebAssembly binary module.
		Version: 0x01 0x00 0x00 0x00
	*/
	var buf bytes.Buffer
	buf.Write([]byte{0x00, 0x61, 0x73, 0x6D}) // Magic number
	buf.Write([]byte{0x01, 0x00, 0x00, 0x00}) // Version 1

	// Encode the spec entries to XDR
	var specBuf bytes.Buffer
	for _, entry := range spec {
		_, err := xdr.Marshal(&specBuf, entry)
		if err != nil {
			panic(err)
		}
	}
	specBytes := specBuf.Bytes()

	// Custom section (section ID 0)
	// Format: section_id | size | name_len | name | payload
	sectionName := "contractspecv0"
	nameLen := len(sectionName)
	payloadSize := nameLen + len(specBytes)

	// Section ID
	buf.WriteByte(0) // Custom section

	// Section size (name length byte + name + spec bytes)
	writeLEB128(&buf, uint32(1+payloadSize))

	// Name length
	writeLEB128(&buf, uint32(nameLen))

	// Name
	buf.WriteString(sectionName)

	// Spec bytes
	buf.Write(specBytes)

	return buf.Bytes()
}

// createWasmWithoutSpec creates a valid WASM without contractspecv0 section
func createWasmWithoutSpec() []byte {
	var buf bytes.Buffer
	buf.Write([]byte{0x00, 0x61, 0x73, 0x6D}) // WASM magic
	buf.Write([]byte{0x01, 0x00, 0x00, 0x00}) // Version 1
	return buf.Bytes()
}

// createInvalidWasm creates invalid/corrupted WASM bytes
func createInvalidWasm() []byte {
	return []byte{0x00, 0x00, 0x00, 0x00} // Invalid magic number
}

// writeLEB128 writes an unsigned integer in LEB128 format
func writeLEB128(buf *bytes.Buffer, value uint32) {
	for {
		b := byte(value & 0x7F)
		value >>= 7
		if value != 0 {
			b |= 0x80
		}
		buf.WriteByte(b)
		if value == 0 {
			break
		}
	}
}

// encodeLedgerEntryToBase64 encodes ledger entry data to base64
func encodeLedgerEntryToBase64(data xdr.LedgerEntryData) (string, error) {
	var buf bytes.Buffer
	_, err := xdr.Marshal(&buf, data)
	if err != nil {
		return "", err
	}
	// Convert to base64
	return base64.StdEncoding.EncodeToString(buf.Bytes()), nil
}

// createContractCodeLedgerEntry creates a contract code ledger entry result
func createContractCodeLedgerEntry(wasmHash xdr.Hash, wasmCode []byte) entities.LedgerEntryResult {
	contractCode := xdr.ContractCodeEntry{
		Hash: wasmHash,
		Code: wasmCode,
	}

	ledgerEntryData := xdr.LedgerEntryData{
		Type:         xdr.LedgerEntryTypeContractCode,
		ContractCode: &contractCode,
	}

	dataXDR, err := encodeLedgerEntryToBase64(ledgerEntryData)
	if err != nil {
		panic(err)
	}

	return entities.LedgerEntryResult{
		DataXDR: dataXDR,
	}
}

// createNonContractCodeLedgerEntry creates a non-contract-code ledger entry
func createNonContractCodeLedgerEntry() entities.LedgerEntryResult {
	accountID := xdr.MustAddress("GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N")
	accountEntry := xdr.AccountEntry{
		AccountId: accountID,
		Balance:   1000,
		SeqNum:    1,
	}

	ledgerEntryData := xdr.LedgerEntryData{
		Type:    xdr.LedgerEntryTypeAccount,
		Account: &accountEntry,
	}

	dataXDR, err := encodeLedgerEntryToBase64(ledgerEntryData)
	if err != nil {
		panic(err)
	}

	return entities.LedgerEntryResult{
		DataXDR: dataXDR,
	}
}

// createInvalidBase64LedgerEntry creates a ledger entry with invalid base64
func createInvalidBase64LedgerEntry() entities.LedgerEntryResult {
	return entities.LedgerEntryResult{
		DataXDR: "!!!invalid-base64!!!",
	}
}

// Test functions start here

func TestNewContractSpecValidator(t *testing.T) {
	t.Run("creates validator successfully", func(t *testing.T) {
		mockRPC := NewRPCServiceMock(t)
		validator := NewContractValidator(mockRPC)

		require.NotNil(t, validator)
		assert.IsType(t, &contractValidator{}, validator)
	})

	t.Run("initializes runtime with custom sections", func(t *testing.T) {
		mockRPC := NewRPCServiceMock(t)
		validator := NewContractValidator(mockRPC)

		// Verify we can close the validator (runtime exists)
		err := validator.Close(context.Background())
		assert.NoError(t, err)
	})
}

func TestValidateFromWasmHash(t *testing.T) {
	ctx := context.Background()

	t.Run("returns empty map for empty input", func(t *testing.T) {
		mockRPC := NewRPCServiceMock(t)
		validator := NewContractValidator(mockRPC)
		defer func() {
			_ = validator.Close(ctx)
		}()

		result, err := validator.ValidateFromWasmHash(ctx, []xdr.Hash{})

		require.NoError(t, err)
		assert.Empty(t, result)
	})

	t.Run("validates single SEP-41 contract", func(t *testing.T) {
		mockRPC := NewRPCServiceMock(t)
		validator := NewContractValidator(mockRPC)
		defer func() {
			_ = validator.Close(ctx)
		}()

		wasmHash := xdr.Hash{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}
		sep41Spec := createSEP41ContractSpec()
		wasmCode := createValidWasmWithSpec(sep41Spec)
		ledgerEntry := createContractCodeLedgerEntry(wasmHash, wasmCode)

		mockRPC.On("GetLedgerEntries", mock.Anything).Return(entities.RPCGetLedgerEntriesResult{
			Entries: []entities.LedgerEntryResult{ledgerEntry},
		}, nil)

		result, err := validator.ValidateFromWasmHash(ctx, []xdr.Hash{wasmHash})

		require.NoError(t, err)
		assert.Len(t, result, 1)
		assert.Equal(t, types.ContractTypeSEP41, result[wasmHash])
	})

	t.Run("validates single non-SEP-41 contract", func(t *testing.T) {
		mockRPC := NewRPCServiceMock(t)
		validator := NewContractValidator(mockRPC)
		defer func() {
			_ = validator.Close(ctx)
		}()

		wasmHash := xdr.Hash{2, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}
		nonSep41Spec := createNonSEP41ContractSpec()
		wasmCode := createValidWasmWithSpec(nonSep41Spec)
		ledgerEntry := createContractCodeLedgerEntry(wasmHash, wasmCode)

		mockRPC.On("GetLedgerEntries", mock.Anything).Return(entities.RPCGetLedgerEntriesResult{
			Entries: []entities.LedgerEntryResult{ledgerEntry},
		}, nil)

		result, err := validator.ValidateFromWasmHash(ctx, []xdr.Hash{wasmHash})

		require.NoError(t, err)
		assert.Len(t, result, 1)
		assert.Equal(t, types.ContractTypeUnknown, result[wasmHash])
	})

	t.Run("validates multiple contracts with mixed types", func(t *testing.T) {
		mockRPC := NewRPCServiceMock(t)
		validator := NewContractValidator(mockRPC)
		defer func() {
			_ = validator.Close(ctx)
		}()

		wasmHash1 := xdr.Hash{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}
		wasmHash2 := xdr.Hash{2, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}

		sep41Spec := createSEP41ContractSpec()
		nonSep41Spec := createNonSEP41ContractSpec()

		wasmCode1 := createValidWasmWithSpec(sep41Spec)
		wasmCode2 := createValidWasmWithSpec(nonSep41Spec)

		ledgerEntry1 := createContractCodeLedgerEntry(wasmHash1, wasmCode1)
		ledgerEntry2 := createContractCodeLedgerEntry(wasmHash2, wasmCode2)

		mockRPC.On("GetLedgerEntries", mock.Anything).Return(entities.RPCGetLedgerEntriesResult{
			Entries: []entities.LedgerEntryResult{ledgerEntry1, ledgerEntry2},
		}, nil)

		result, err := validator.ValidateFromWasmHash(ctx, []xdr.Hash{wasmHash1, wasmHash2})

		require.NoError(t, err)
		assert.Len(t, result, 2)
		assert.Equal(t, types.ContractTypeSEP41, result[wasmHash1])
		assert.Equal(t, types.ContractTypeUnknown, result[wasmHash2])
	})

	t.Run("batches requests for more than 10 contracts", func(t *testing.T) {
		mockRPC := NewRPCServiceMock(t)
		validator := NewContractValidator(mockRPC)
		defer func() {
			_ = validator.Close(ctx)
		}()

		// Create 15 wasm hashes
		var wasmHashes []xdr.Hash
		var entries1 []entities.LedgerEntryResult
		var entries2 []entities.LedgerEntryResult

		sep41Spec := createSEP41ContractSpec()
		wasmCode := createValidWasmWithSpec(sep41Spec)

		for i := 0; i < 15; i++ {
			hash := xdr.Hash{}
			binary.BigEndian.PutUint32(hash[:], uint32(i))
			wasmHashes = append(wasmHashes, hash)

			entry := createContractCodeLedgerEntry(hash, wasmCode)
			if i < 10 {
				entries1 = append(entries1, entry)
			} else {
				entries2 = append(entries2, entry)
			}
		}

		// Expect two batched calls
		mockRPC.On("GetLedgerEntries", mock.Anything).Return(entities.RPCGetLedgerEntriesResult{
			Entries: entries1,
		}, nil).Once()

		mockRPC.On("GetLedgerEntries", mock.Anything).Return(entities.RPCGetLedgerEntriesResult{
			Entries: entries2,
		}, nil).Once()

		result, err := validator.ValidateFromWasmHash(ctx, wasmHashes)

		require.NoError(t, err)
		assert.Len(t, result, 15)
		for _, hash := range wasmHashes {
			assert.Equal(t, types.ContractTypeSEP41, result[hash])
		}
	})

	t.Run("handles RPC GetLedgerEntries error", func(t *testing.T) {
		mockRPC := NewRPCServiceMock(t)
		validator := NewContractValidator(mockRPC)
		defer func() {
			_ = validator.Close(ctx)
		}()

		wasmHash := xdr.Hash{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}

		mockRPC.On("GetLedgerEntries", mock.Anything).Return(entities.RPCGetLedgerEntriesResult{}, assert.AnError)

		result, err := validator.ValidateFromWasmHash(ctx, []xdr.Hash{wasmHash})

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "getting ledger entries")
		assert.Nil(t, result)
	})

	t.Run("handles invalid base64 in DataXDR", func(t *testing.T) {
		mockRPC := NewRPCServiceMock(t)
		validator := NewContractValidator(mockRPC)
		defer func() {
			_ = validator.Close(ctx)
		}()

		wasmHash := xdr.Hash{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}
		invalidEntry := createInvalidBase64LedgerEntry()

		mockRPC.On("GetLedgerEntries", mock.Anything).Return(entities.RPCGetLedgerEntriesResult{
			Entries: []entities.LedgerEntryResult{invalidEntry},
		}, nil)

		result, err := validator.ValidateFromWasmHash(ctx, []xdr.Hash{wasmHash})

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshalling ledger entry data")
		assert.Nil(t, result)
	})

	t.Run("skips wrong ledger entry type", func(t *testing.T) {
		mockRPC := NewRPCServiceMock(t)
		validator := NewContractValidator(mockRPC)
		defer func() {
			_ = validator.Close(ctx)
		}()

		wasmHash := xdr.Hash{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}
		accountEntry := createNonContractCodeLedgerEntry()

		mockRPC.On("GetLedgerEntries", mock.Anything).Return(entities.RPCGetLedgerEntriesResult{
			Entries: []entities.LedgerEntryResult{accountEntry},
		}, nil)

		result, err := validator.ValidateFromWasmHash(ctx, []xdr.Hash{wasmHash})

		require.NoError(t, err)
		assert.Empty(t, result)
	})

	t.Run("handles invalid WASM code", func(t *testing.T) {
		mockRPC := NewRPCServiceMock(t)
		validator := NewContractValidator(mockRPC)
		defer func() {
			_ = validator.Close(ctx)
		}()

		wasmHash := xdr.Hash{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}
		invalidWasm := createInvalidWasm()
		ledgerEntry := createContractCodeLedgerEntry(wasmHash, invalidWasm)

		mockRPC.On("GetLedgerEntries", mock.Anything).Return(entities.RPCGetLedgerEntriesResult{
			Entries: []entities.LedgerEntryResult{ledgerEntry},
		}, nil)

		result, err := validator.ValidateFromWasmHash(ctx, []xdr.Hash{wasmHash})

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "extracting contract spec from WASM")
		assert.Nil(t, result)
	})

	t.Run("handles WASM without contractspecv0 section", func(t *testing.T) {
		mockRPC := NewRPCServiceMock(t)
		validator := NewContractValidator(mockRPC)
		defer func() {
			_ = validator.Close(ctx)
		}()

		wasmHash := xdr.Hash{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}
		wasmCode := createWasmWithoutSpec()
		ledgerEntry := createContractCodeLedgerEntry(wasmHash, wasmCode)

		mockRPC.On("GetLedgerEntries", mock.Anything).Return(entities.RPCGetLedgerEntriesResult{
			Entries: []entities.LedgerEntryResult{ledgerEntry},
		}, nil)

		result, err := validator.ValidateFromWasmHash(ctx, []xdr.Hash{wasmHash})

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "contractspecv0 section not found")
		assert.Nil(t, result)
	})

	t.Run("handles context cancellation", func(t *testing.T) {
		mockRPC := NewRPCServiceMock(t)
		validator := NewContractValidator(mockRPC)
		defer func() {
			_ = validator.Close(context.Background())
		}()

		cancelledCtx, cancel := context.WithCancel(ctx)
		cancel() // Cancel immediately

		wasmHash := xdr.Hash{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}

		// The Validate function doesn't explicitly check context before making calls
		// So it will proceed normally
		result, err := validator.ValidateFromWasmHash(cancelledCtx, []xdr.Hash{wasmHash})

		// Test should complete without panic
		// Results may vary depending on when context is checked
		assert.Nil(t, result)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "validation cancelled before start: context canceled")
	})
}

func TestIsContractCodeSEP41(t *testing.T) {
	mockRPC := NewRPCServiceMock(t)
	validator := &contractValidator{rpcService: mockRPC}

	t.Run("returns true for complete SEP-41 contract", func(t *testing.T) {
		spec := createSEP41ContractSpec()
		result := validator.isContractCodeSEP41(spec)
		assert.True(t, result)
	})

	t.Run("returns false when missing balance function", func(t *testing.T) {
		spec := createPartialSEP41Spec([]string{"balance"})
		result := validator.isContractCodeSEP41(spec)
		assert.False(t, result)
	})

	t.Run("returns false when missing allowance function", func(t *testing.T) {
		spec := createPartialSEP41Spec([]string{"allowance"})
		result := validator.isContractCodeSEP41(spec)
		assert.False(t, result)
	})

	t.Run("returns false when missing decimals function", func(t *testing.T) {
		spec := createPartialSEP41Spec([]string{"decimals"})
		result := validator.isContractCodeSEP41(spec)
		assert.False(t, result)
	})

	t.Run("returns false when missing name function", func(t *testing.T) {
		spec := createPartialSEP41Spec([]string{"name"})
		result := validator.isContractCodeSEP41(spec)
		assert.False(t, result)
	})

	t.Run("returns false when missing symbol function", func(t *testing.T) {
		spec := createPartialSEP41Spec([]string{"symbol"})
		result := validator.isContractCodeSEP41(spec)
		assert.False(t, result)
	})

	t.Run("returns false when missing approve function", func(t *testing.T) {
		spec := createPartialSEP41Spec([]string{"approve"})
		result := validator.isContractCodeSEP41(spec)
		assert.False(t, result)
	})

	t.Run("returns false when missing transfer function", func(t *testing.T) {
		spec := createPartialSEP41Spec([]string{"transfer"})
		result := validator.isContractCodeSEP41(spec)
		assert.False(t, result)
	})

	t.Run("returns false when missing transfer_from function", func(t *testing.T) {
		spec := createPartialSEP41Spec([]string{"transfer_from"})
		result := validator.isContractCodeSEP41(spec)
		assert.False(t, result)
	})

	t.Run("returns false when missing burn function", func(t *testing.T) {
		spec := createPartialSEP41Spec([]string{"burn"})
		result := validator.isContractCodeSEP41(spec)
		assert.False(t, result)
	})

	t.Run("returns false when missing burn_from function", func(t *testing.T) {
		spec := createPartialSEP41Spec([]string{"burn_from"})
		result := validator.isContractCodeSEP41(spec)
		assert.False(t, result)
	})

	t.Run("returns false for balance with wrong input name", func(t *testing.T) {
		addressType := createScSpecTypeDef(xdr.ScSpecTypeScSpecTypeAddress)
		i128Type := createScSpecTypeDef(xdr.ScSpecTypeScSpecTypeI128)

		// Create balance function with wrong input name (should be "id", not "address")
		balanceFunc := createScSpecFunctionEntry("balance",
			[]xdr.ScSpecFunctionInputV0{createFunctionInput("address", addressType)},
			[]xdr.ScSpecTypeDef{i128Type},
		)

		// Create all other functions correctly
		spec := createPartialSEP41Spec([]string{"balance"})
		spec = append(spec, balanceFunc)

		result := validator.isContractCodeSEP41(spec)
		assert.False(t, result)
	})

	t.Run("returns false for balance with wrong input type", func(t *testing.T) {
		u32Type := createScSpecTypeDef(xdr.ScSpecTypeScSpecTypeU32)
		i128Type := createScSpecTypeDef(xdr.ScSpecTypeScSpecTypeI128)

		// Create balance function with wrong input type (should be Address, not u32)
		balanceFunc := createScSpecFunctionEntry("balance",
			[]xdr.ScSpecFunctionInputV0{createFunctionInput("id", u32Type)},
			[]xdr.ScSpecTypeDef{i128Type},
		)

		spec := createPartialSEP41Spec([]string{"balance"})
		spec = append(spec, balanceFunc)

		result := validator.isContractCodeSEP41(spec)
		assert.False(t, result)
	})

	t.Run("returns false for balance with wrong output type", func(t *testing.T) {
		addressType := createScSpecTypeDef(xdr.ScSpecTypeScSpecTypeAddress)
		u32Type := createScSpecTypeDef(xdr.ScSpecTypeScSpecTypeU32)

		// Create balance function with wrong output type (should be i128, not u32)
		balanceFunc := createScSpecFunctionEntry("balance",
			[]xdr.ScSpecFunctionInputV0{createFunctionInput("id", addressType)},
			[]xdr.ScSpecTypeDef{u32Type},
		)

		spec := createPartialSEP41Spec([]string{"balance"})
		spec = append(spec, balanceFunc)

		result := validator.isContractCodeSEP41(spec)
		assert.False(t, result)
	})

	t.Run("returns false for empty contract spec", func(t *testing.T) {
		spec := []xdr.ScSpecEntry{}
		result := validator.isContractCodeSEP41(spec)
		assert.False(t, result)
	})

	t.Run("returns true with extra non-SEP-41 functions", func(t *testing.T) {
		spec := createSEP41ContractSpec()

		// Add extra custom functions but all SEP-41 functions should be present
		u32Type := createScSpecTypeDef(xdr.ScSpecTypeScSpecTypeU32)
		customFunc := createScSpecFunctionEntry("custom_function",
			[]xdr.ScSpecFunctionInputV0{createFunctionInput("value", u32Type)},
			[]xdr.ScSpecTypeDef{u32Type},
		)

		spec = append(spec, customFunc)

		result := validator.isContractCodeSEP41(spec)
		assert.True(t, result)
	})

	t.Run("skips non-function spec entries", func(t *testing.T) {
		spec := createSEP41ContractSpec()

		// Add a non-function entry (UDT struct)
		udtEntry := xdr.ScSpecEntry{
			Kind: xdr.ScSpecEntryKindScSpecEntryUdtStructV0,
			UdtStructV0: &xdr.ScSpecUdtStructV0{
				Name:   "CustomStruct",
				Doc:    "",
				Lib:    "",
				Fields: []xdr.ScSpecUdtStructFieldV0{},
			},
		}

		spec = append(spec, udtEntry)

		result := validator.isContractCodeSEP41(spec)
		assert.True(t, result)
	})
}

func TestValidateFunctionInputsAndOutputs(t *testing.T) {
	mockRPC := NewRPCServiceMock(t)
	validator := &contractValidator{rpcService: mockRPC}

	t.Run("returns true for exact match", func(t *testing.T) {
		inputs := map[string]any{
			"from": "Address",
			"to":   "Address",
		}
		outputs := set.NewSet("i128")

		expectedInputs := map[string]any{
			"from": "Address",
			"to":   "Address",
		}
		expectedOutputs := set.NewSet("i128")

		result := validator.validateFunctionInputsAndOutputs(inputs, outputs, expectedInputs, expectedOutputs)
		assert.True(t, result)
	})

	t.Run("returns false for too few inputs", func(t *testing.T) {
		inputs := map[string]any{
			"from": "Address",
		}
		outputs := set.NewSet("i128")

		expectedInputs := map[string]any{
			"from": "Address",
			"to":   "Address",
		}
		expectedOutputs := set.NewSet("i128")

		result := validator.validateFunctionInputsAndOutputs(inputs, outputs, expectedInputs, expectedOutputs)
		assert.False(t, result)
	})

	t.Run("returns false for too many inputs", func(t *testing.T) {
		inputs := map[string]any{
			"from":   "Address",
			"to":     "Address",
			"amount": "i128",
		}
		outputs := set.NewSet("i128")

		expectedInputs := map[string]any{
			"from": "Address",
			"to":   "Address",
		}
		expectedOutputs := set.NewSet("i128")

		result := validator.validateFunctionInputsAndOutputs(inputs, outputs, expectedInputs, expectedOutputs)
		assert.False(t, result)
	})

	t.Run("returns false for wrong input parameter name", func(t *testing.T) {
		inputs := map[string]any{
			"sender":   "Address",
			"receiver": "Address",
		}
		outputs := set.NewSet("i128")

		expectedInputs := map[string]any{
			"from": "Address",
			"to":   "Address",
		}
		expectedOutputs := set.NewSet("i128")

		result := validator.validateFunctionInputsAndOutputs(inputs, outputs, expectedInputs, expectedOutputs)
		assert.False(t, result)
	})

	t.Run("returns false for wrong input parameter type", func(t *testing.T) {
		inputs := map[string]any{
			"from": "Address",
			"to":   "u32", // Wrong type
		}
		outputs := set.NewSet("i128")

		expectedInputs := map[string]any{
			"from": "Address",
			"to":   "Address",
		}
		expectedOutputs := set.NewSet("i128")

		result := validator.validateFunctionInputsAndOutputs(inputs, outputs, expectedInputs, expectedOutputs)
		assert.False(t, result)
	})

	t.Run("returns false for too few outputs", func(t *testing.T) {
		inputs := map[string]any{
			"from": "Address",
			"to":   "Address",
		}
		outputs := set.NewSet[string]()

		expectedInputs := map[string]any{
			"from": "Address",
			"to":   "Address",
		}
		expectedOutputs := set.NewSet("i128")

		result := validator.validateFunctionInputsAndOutputs(inputs, outputs, expectedInputs, expectedOutputs)
		assert.False(t, result)
	})

	t.Run("returns false for too many outputs", func(t *testing.T) {
		inputs := map[string]any{
			"from": "Address",
			"to":   "Address",
		}
		outputs := set.NewSet("i128", "u32")

		expectedInputs := map[string]any{
			"from": "Address",
			"to":   "Address",
		}
		expectedOutputs := set.NewSet("i128")

		result := validator.validateFunctionInputsAndOutputs(inputs, outputs, expectedInputs, expectedOutputs)
		assert.False(t, result)
	})

	t.Run("returns false for wrong output type", func(t *testing.T) {
		inputs := map[string]any{
			"from": "Address",
			"to":   "Address",
		}
		outputs := set.NewSet("u32") // Wrong type

		expectedInputs := map[string]any{
			"from": "Address",
			"to":   "Address",
		}
		expectedOutputs := set.NewSet("i128")

		result := validator.validateFunctionInputsAndOutputs(inputs, outputs, expectedInputs, expectedOutputs)
		assert.False(t, result)
	})

	t.Run("returns true for empty inputs and outputs", func(t *testing.T) {
		inputs := map[string]any{}
		outputs := set.NewSet[string]()

		expectedInputs := map[string]any{}
		expectedOutputs := set.NewSet[string]()

		result := validator.validateFunctionInputsAndOutputs(inputs, outputs, expectedInputs, expectedOutputs)
		assert.True(t, result)
	})

	t.Run("returns true for matching with one of the many input types in a set", func(t *testing.T) {
		inputs := map[string]any{
			"from": "MuxedAddress",
		}
		outputs := set.NewSet("i128")

		expectedInputs := map[string]any{
			"from": set.NewSet("Address", "MuxedAddress"),
		}
		expectedOutputs := set.NewSet("i128")

		result := validator.validateFunctionInputsAndOutputs(inputs, outputs, expectedInputs, expectedOutputs)
		assert.True(t, result)

		inputs = map[string]any{
			"from": "Address",
		}
		result = validator.validateFunctionInputsAndOutputs(inputs, outputs, expectedInputs, expectedOutputs)
		assert.True(t, result)
	})
}

func TestExtractContractSpecFromWasmCode(t *testing.T) {
	ctx := context.Background()
	mockRPC := NewRPCServiceMock(t)
	validator := NewContractValidator(mockRPC)
	defer func() {
		_ = validator.Close(ctx)
	}()

	v := validator.(*contractValidator)

	t.Run("extracts spec from valid WASM", func(t *testing.T) {
		spec := createSEP41ContractSpec()
		wasmCode := createValidWasmWithSpec(spec)

		result, err := v.extractContractSpecFromWasmCode(context.Background(), wasmCode)

		require.NoError(t, err)
		assert.Len(t, result, len(spec))
		// Verify we got function entries back
		for _, entry := range result {
			assert.Equal(t, xdr.ScSpecEntryKindScSpecEntryFunctionV0, entry.Kind)
		}
	})

	t.Run("returns error for WASM without contractspecv0 section", func(t *testing.T) {
		wasmCode := createWasmWithoutSpec()

		result, err := v.extractContractSpecFromWasmCode(context.Background(), wasmCode)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "contractspecv0 section not found")
		assert.Nil(t, result)
	})

	t.Run("returns error for invalid WASM bytes", func(t *testing.T) {
		wasmCode := createInvalidWasm()

		result, err := v.extractContractSpecFromWasmCode(context.Background(), wasmCode)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "compiling WASM module")
		assert.Nil(t, result)
	})

	t.Run("returns error for empty WASM bytes", func(t *testing.T) {
		wasmCode := []byte{}

		result, err := v.extractContractSpecFromWasmCode(context.Background(), wasmCode)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "compiling WASM module")
		assert.Nil(t, result)
	})

	t.Run("returns error for corrupted XDR in contractspecv0", func(t *testing.T) {
		// Create a valid WASM structure but with corrupted XDR data in contractspecv0 section
		var buf bytes.Buffer
		buf.Write([]byte{0x00, 0x61, 0x73, 0x6D}) // WASM magic
		buf.Write([]byte{0x01, 0x00, 0x00, 0x00}) // Version 1

		// Custom section with corrupted XDR data
		sectionName := "contractspecv0"
		corruptedXDR := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF} // Invalid XDR

		buf.WriteByte(0) // Custom section
		writeLEB128(&buf, uint32(1+len(sectionName)+len(corruptedXDR)))
		writeLEB128(&buf, uint32(len(sectionName)))
		buf.WriteString(sectionName)
		buf.Write(corruptedXDR)

		wasmCode := buf.Bytes()

		result, err := v.extractContractSpecFromWasmCode(context.Background(), wasmCode)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshaling spec entry")
		assert.Nil(t, result)
	})

	t.Run("handles multiple custom sections correctly", func(t *testing.T) {
		// Create WASM with multiple custom sections, including contractspecv0
		var buf bytes.Buffer
		buf.Write([]byte{0x00, 0x61, 0x73, 0x6D}) // WASM magic
		buf.Write([]byte{0x01, 0x00, 0x00, 0x00}) // Version 1

		// First custom section (not contractspecv0)
		otherSectionName := "othersection"
		otherData := []byte{0x01, 0x02, 0x03}
		buf.WriteByte(0)
		writeLEB128(&buf, uint32(1+len(otherSectionName)+len(otherData)))
		writeLEB128(&buf, uint32(len(otherSectionName)))
		buf.WriteString(otherSectionName)
		buf.Write(otherData)

		// Second custom section (contractspecv0)
		spec := createNonSEP41ContractSpec()
		var specBuf bytes.Buffer
		for _, entry := range spec {
			_, err := xdr.Marshal(&specBuf, entry)
			require.NoError(t, err)
		}
		specBytes := specBuf.Bytes()

		sectionName := "contractspecv0"
		buf.WriteByte(0)
		writeLEB128(&buf, uint32(1+len(sectionName)+len(specBytes)))
		writeLEB128(&buf, uint32(len(sectionName)))
		buf.WriteString(sectionName)
		buf.Write(specBytes)

		wasmCode := buf.Bytes()

		result, err := v.extractContractSpecFromWasmCode(context.Background(), wasmCode)

		require.NoError(t, err)
		assert.Len(t, result, len(spec))
		assert.Equal(t, xdr.ScSpecEntryKindScSpecEntryFunctionV0, result[0].Kind)
	})
}

func TestGetContractCodeLedgerKey(t *testing.T) {
	ctx := context.Background()
	mockRPC := NewRPCServiceMock(t)
	validator := NewContractValidator(mockRPC)
	defer func() {
		_ = validator.Close(ctx)
	}()

	v := validator.(*contractValidator)

	t.Run("creates ledger key for valid hash", func(t *testing.T) {
		wasmHash := xdr.Hash{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}

		result, err := v.getContractCodeLedgerKey(wasmHash)

		require.NoError(t, err)
		assert.NotEmpty(t, result)

		// Verify it's valid base64
		var ledgerKey xdr.LedgerKey
		err = xdr.SafeUnmarshalBase64(result, &ledgerKey)
		require.NoError(t, err)
		assert.Equal(t, xdr.LedgerEntryTypeContractCode, ledgerKey.Type)
	})

	t.Run("creates ledger key for zero hash", func(t *testing.T) {
		wasmHash := xdr.Hash{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}

		result, err := v.getContractCodeLedgerKey(wasmHash)

		require.NoError(t, err)
		assert.NotEmpty(t, result)

		// Verify it's valid base64
		var ledgerKey xdr.LedgerKey
		err = xdr.SafeUnmarshalBase64(result, &ledgerKey)
		require.NoError(t, err)
		assert.Equal(t, xdr.LedgerEntryTypeContractCode, ledgerKey.Type)
	})

	t.Run("returns base64 encoded string", func(t *testing.T) {
		wasmHash := xdr.Hash{255, 254, 253, 252, 251, 250, 249, 248, 247, 246, 245, 244, 243, 242, 241, 240, 239, 238, 237, 236, 235, 234, 233, 232, 231, 230, 229, 228, 227, 226, 225, 224}

		result, err := v.getContractCodeLedgerKey(wasmHash)

		require.NoError(t, err)
		assert.NotEmpty(t, result)

		// Verify we can decode it back
		var ledgerKey xdr.LedgerKey
		err = xdr.SafeUnmarshalBase64(result, &ledgerKey)
		require.NoError(t, err)

		// Verify the hash matches
		contractCodeKey := ledgerKey.MustContractCode()
		assert.Equal(t, wasmHash, contractCodeKey.Hash)
	})
}
