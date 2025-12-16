package services

import (
	"bytes"
	"context"
	"testing"

	set "github.com/deckarep/golang-set/v2"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

// createValidWasmCodeWithSpec creates a minimal valid WASM module with contractspecv0 custom section
func createValidWasmCodeWithSpec(spec []xdr.ScSpecEntry) []byte {
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

// Test functions start here

func TestNewContractSpecValidator(t *testing.T) {
	t.Run("creates validator successfully", func(t *testing.T) {
		validator := NewContractValidator()

		require.NotNil(t, validator)
		assert.IsType(t, &contractValidator{}, validator)
	})

	t.Run("initializes runtime with custom sections", func(t *testing.T) {
		validator := NewContractValidator()

		// Verify we can close the validator (runtime exists)
		err := validator.Close(context.Background())
		assert.NoError(t, err)
	})
}

func TestValidateFromContractCode(t *testing.T) {
	ctx := context.Background()

	t.Run("returns error for empty input", func(t *testing.T) {
		validator := NewContractValidator()
		defer func() {
			_ = validator.Close(ctx)
		}()

		_, err := validator.ValidateFromContractCode(ctx, []byte{})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "extracting contract spec from WASM: compiling WASM module: invalid magic number")
	})

	t.Run("validates single SEP-41 contract", func(t *testing.T) {
		validator := NewContractValidator()
		defer func() {
			_ = validator.Close(ctx)
		}()

		sep41Spec := createSEP41ContractSpec()
		wasmCode := createValidWasmCodeWithSpec(sep41Spec)

		result, err := validator.ValidateFromContractCode(ctx, wasmCode)

		require.NoError(t, err)
		assert.Equal(t, types.ContractTypeSEP41, result)
	})

	t.Run("validates single non-SEP-41 contract", func(t *testing.T) {
		validator := NewContractValidator()
		defer func() {
			_ = validator.Close(ctx)
		}()

		nonSep41Spec := createNonSEP41ContractSpec()
		wasmCode := createValidWasmCodeWithSpec(nonSep41Spec)

		result, err := validator.ValidateFromContractCode(ctx, wasmCode)

		require.NoError(t, err)
		assert.Equal(t, types.ContractTypeUnknown, result)
	})

	t.Run("handles invalid WASM code", func(t *testing.T) {
		validator := NewContractValidator()
		defer func() {
			_ = validator.Close(ctx)
		}()

		invalidWasm := createInvalidWasm()

		result, err := validator.ValidateFromContractCode(ctx, invalidWasm)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "extracting contract spec from WASM")
		assert.Equal(t, types.ContractTypeUnknown, result)
	})

	t.Run("handles WASM without contractspecv0 section", func(t *testing.T) {
		validator := NewContractValidator()
		defer func() {
			_ = validator.Close(ctx)
		}()

		wasmCode := createWasmWithoutSpec()

		result, err := validator.ValidateFromContractCode(ctx, wasmCode)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "contractspecv0 section not found")
		assert.Equal(t, types.ContractTypeUnknown, result)
	})
}

func TestIsContractCodeSEP41(t *testing.T) {
	validator := NewContractValidator().(*contractValidator)

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
	validator := NewContractValidator().(*contractValidator)

	t.Run("returns true for exact match", func(t *testing.T) {
		inputs := map[string]any{
			"from": "Address",
			"to":   "Address",
		}
		outputs := set.NewSet("i128")

		expectedInputs := map[string]string{
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

		expectedInputs := map[string]string{
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

		expectedInputs := map[string]string{
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

		expectedInputs := map[string]string{
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

		expectedInputs := map[string]string{
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

		expectedInputs := map[string]string{
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

		expectedInputs := map[string]string{
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

		expectedInputs := map[string]string{
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

		expectedInputs := map[string]string{}
		expectedOutputs := set.NewSet[string]()

		result := validator.validateFunctionInputsAndOutputs(inputs, outputs, expectedInputs, expectedOutputs)
		assert.True(t, result)
	})
}

func TestExtractContractSpecFromWasmCode(t *testing.T) {
	ctx := context.Background()
	validator := NewContractValidator()
	defer func() {
		_ = validator.Close(ctx)
	}()

	v := validator.(*contractValidator)

	t.Run("extracts spec from valid WASM", func(t *testing.T) {
		spec := createSEP41ContractSpec()
		wasmCode := createValidWasmCodeWithSpec(spec)

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

	t.Run("handles empty contractspecv0 section", func(t *testing.T) {
		// Create WASM with valid structure but zero spec entries
		var buf bytes.Buffer
		buf.Write([]byte{0x00, 0x61, 0x73, 0x6D}) // WASM magic
		buf.Write([]byte{0x01, 0x00, 0x00, 0x00}) // Version 1

		// Custom section with empty contractspecv0
		sectionName := "contractspecv0"
		buf.WriteByte(0)                              // Custom section
		writeLEB128(&buf, uint32(1+len(sectionName))) // Size (just name length byte + name, no payload)
		writeLEB128(&buf, uint32(len(sectionName)))
		buf.WriteString(sectionName)
		// No spec bytes - empty section

		wasmCode := buf.Bytes()

		result, err := v.extractContractSpecFromWasmCode(context.Background(), wasmCode)

		require.Error(t, err)
		assert.Empty(t, result, "compiling WASM module: failed to read custom section name[contractspecv0]: EOF")
	})

	t.Run("returns error for truncated XDR in contractspecv0", func(t *testing.T) {
		// Create WASM with contractspecv0 containing incomplete/truncated XDR
		var buf bytes.Buffer
		buf.Write([]byte{0x00, 0x61, 0x73, 0x6D}) // WASM magic
		buf.Write([]byte{0x01, 0x00, 0x00, 0x00}) // Version 1

		// Create partial XDR data (incomplete spec entry)
		// This is just the beginning of a valid ScSpecEntry but truncated
		truncatedXDR := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00} // Partial data

		sectionName := "contractspecv0"
		buf.WriteByte(0) // Custom section
		writeLEB128(&buf, uint32(1+len(sectionName)+len(truncatedXDR)))
		writeLEB128(&buf, uint32(len(sectionName)))
		buf.WriteString(sectionName)
		buf.Write(truncatedXDR)

		wasmCode := buf.Bytes()

		result, err := v.extractContractSpecFromWasmCode(context.Background(), wasmCode)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshaling spec entry")
		assert.Nil(t, result)
	})
}
