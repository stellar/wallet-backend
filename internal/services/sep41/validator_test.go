package sep41

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/services"
)

func TestIsContractCodeSEP41(t *testing.T) {
	t.Run("returns true for complete SEP-41 contract", func(t *testing.T) {
		spec := createSEP41ContractSpec()
		result := NewValidator().Validate(spec)
		assert.True(t, result)
	})

	t.Run("returns false when missing balance function", func(t *testing.T) {
		spec := createPartialSEP41Spec([]string{"balance"})
		result := NewValidator().Validate(spec)
		assert.False(t, result)
	})

	t.Run("returns false when missing allowance function", func(t *testing.T) {
		spec := createPartialSEP41Spec([]string{"allowance"})
		result := NewValidator().Validate(spec)
		assert.False(t, result)
	})

	t.Run("returns false when missing decimals function", func(t *testing.T) {
		spec := createPartialSEP41Spec([]string{"decimals"})
		result := NewValidator().Validate(spec)
		assert.False(t, result)
	})

	t.Run("returns false when missing name function", func(t *testing.T) {
		spec := createPartialSEP41Spec([]string{"name"})
		result := NewValidator().Validate(spec)
		assert.False(t, result)
	})

	t.Run("returns false when missing symbol function", func(t *testing.T) {
		spec := createPartialSEP41Spec([]string{"symbol"})
		result := NewValidator().Validate(spec)
		assert.False(t, result)
	})

	t.Run("returns false when missing approve function", func(t *testing.T) {
		spec := createPartialSEP41Spec([]string{"approve"})
		result := NewValidator().Validate(spec)
		assert.False(t, result)
	})

	t.Run("returns false when missing transfer function", func(t *testing.T) {
		spec := createPartialSEP41Spec([]string{"transfer"})
		result := NewValidator().Validate(spec)
		assert.False(t, result)
	})

	t.Run("returns false when missing transfer_from function", func(t *testing.T) {
		spec := createPartialSEP41Spec([]string{"transfer_from"})
		result := NewValidator().Validate(spec)
		assert.False(t, result)
	})

	t.Run("returns false when missing burn function", func(t *testing.T) {
		spec := createPartialSEP41Spec([]string{"burn"})
		result := NewValidator().Validate(spec)
		assert.False(t, result)
	})

	t.Run("returns false when missing burn_from function", func(t *testing.T) {
		spec := createPartialSEP41Spec([]string{"burn_from"})
		result := NewValidator().Validate(spec)
		assert.False(t, result)
	})

	t.Run("returns false for balance with wrong input name", func(t *testing.T) {
		addressType := createScSpecTypeDef(xdr.ScSpecTypeScSpecTypeAddress)
		i128Type := createScSpecTypeDef(xdr.ScSpecTypeScSpecTypeI128)

		balanceFunc := createScSpecFunctionEntry("balance",
			[]xdr.ScSpecFunctionInputV0{createFunctionInput("address", addressType)},
			[]xdr.ScSpecTypeDef{i128Type},
		)

		spec := createPartialSEP41Spec([]string{"balance"})
		spec = append(spec, balanceFunc)

		result := NewValidator().Validate(spec)
		assert.False(t, result)
	})

	t.Run("returns false for balance with wrong input type", func(t *testing.T) {
		u32Type := createScSpecTypeDef(xdr.ScSpecTypeScSpecTypeU32)
		i128Type := createScSpecTypeDef(xdr.ScSpecTypeScSpecTypeI128)

		balanceFunc := createScSpecFunctionEntry("balance",
			[]xdr.ScSpecFunctionInputV0{createFunctionInput("id", u32Type)},
			[]xdr.ScSpecTypeDef{i128Type},
		)

		spec := createPartialSEP41Spec([]string{"balance"})
		spec = append(spec, balanceFunc)

		result := NewValidator().Validate(spec)
		assert.False(t, result)
	})

	t.Run("returns false for balance with wrong output type", func(t *testing.T) {
		addressType := createScSpecTypeDef(xdr.ScSpecTypeScSpecTypeAddress)
		u32Type := createScSpecTypeDef(xdr.ScSpecTypeScSpecTypeU32)

		balanceFunc := createScSpecFunctionEntry("balance",
			[]xdr.ScSpecFunctionInputV0{createFunctionInput("id", addressType)},
			[]xdr.ScSpecTypeDef{u32Type},
		)

		spec := createPartialSEP41Spec([]string{"balance"})
		spec = append(spec, balanceFunc)

		result := NewValidator().Validate(spec)
		assert.False(t, result)
	})

	t.Run("returns false for transfer with reordered inputs", func(t *testing.T) {
		addressType := createScSpecTypeDef(xdr.ScSpecTypeScSpecTypeAddress)
		i128Type := createScSpecTypeDef(xdr.ScSpecTypeScSpecTypeI128)

		transferFunc := createScSpecFunctionEntry("transfer",
			[]xdr.ScSpecFunctionInputV0{
				createFunctionInput("to", addressType),
				createFunctionInput("from", addressType),
				createFunctionInput("amount", i128Type),
			},
			[]xdr.ScSpecTypeDef{},
		)

		spec := createPartialSEP41Spec([]string{"transfer"})
		spec = append(spec, transferFunc)

		result := NewValidator().Validate(spec)
		assert.False(t, result)
	})

	t.Run("returns false for balance with duplicate outputs", func(t *testing.T) {
		addressType := createScSpecTypeDef(xdr.ScSpecTypeScSpecTypeAddress)
		i128Type := createScSpecTypeDef(xdr.ScSpecTypeScSpecTypeI128)

		balanceFunc := createScSpecFunctionEntry("balance",
			[]xdr.ScSpecFunctionInputV0{createFunctionInput("id", addressType)},
			[]xdr.ScSpecTypeDef{i128Type, i128Type},
		)

		spec := createPartialSEP41Spec([]string{"balance"})
		spec = append(spec, balanceFunc)

		result := NewValidator().Validate(spec)
		assert.False(t, result)
	})

	t.Run("returns false for empty contract spec", func(t *testing.T) {
		spec := []xdr.ScSpecEntry{}
		result := NewValidator().Validate(spec)
		assert.False(t, result)
	})

	t.Run("returns true with extra non-SEP-41 functions", func(t *testing.T) {
		spec := createSEP41ContractSpec()

		u32Type := createScSpecTypeDef(xdr.ScSpecTypeScSpecTypeU32)
		customFunc := createScSpecFunctionEntry("custom_function",
			[]xdr.ScSpecFunctionInputV0{createFunctionInput("value", u32Type)},
			[]xdr.ScSpecTypeDef{u32Type},
		)

		spec = append(spec, customFunc)

		result := NewValidator().Validate(spec)
		assert.True(t, result)
	})

	t.Run("skips non-function spec entries", func(t *testing.T) {
		spec := createSEP41ContractSpec()

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

		result := NewValidator().Validate(spec)
		assert.True(t, result)
	})
}

func TestValidateFunctionInputsAndOutputs(t *testing.T) {
	t.Run("returns true for exact match", func(t *testing.T) {
		inputs := []contractFunctionInputSpec{
			{name: "from", typeName: "Address"},
			{name: "to", typeName: "Address"},
		}
		outputs := []string{"i128"}

		expectedInputs := []contractFunctionInputSpec{
			{name: "from", typeName: "Address"},
			{name: "to", typeName: "Address"},
		}
		expectedOutputs := []string{"i128"}

		result := validateFunctionInputsAndOutputs(inputs, outputs, expectedInputs, expectedOutputs)
		assert.True(t, result)
	})

	t.Run("returns false for too few inputs", func(t *testing.T) {
		inputs := []contractFunctionInputSpec{
			{name: "from", typeName: "Address"},
		}
		outputs := []string{"i128"}

		expectedInputs := []contractFunctionInputSpec{
			{name: "from", typeName: "Address"},
			{name: "to", typeName: "Address"},
		}
		expectedOutputs := []string{"i128"}

		result := validateFunctionInputsAndOutputs(inputs, outputs, expectedInputs, expectedOutputs)
		assert.False(t, result)
	})

	t.Run("returns false for too many inputs", func(t *testing.T) {
		inputs := []contractFunctionInputSpec{
			{name: "from", typeName: "Address"},
			{name: "to", typeName: "Address"},
			{name: "amount", typeName: "i128"},
		}
		outputs := []string{"i128"}

		expectedInputs := []contractFunctionInputSpec{
			{name: "from", typeName: "Address"},
			{name: "to", typeName: "Address"},
		}
		expectedOutputs := []string{"i128"}

		result := validateFunctionInputsAndOutputs(inputs, outputs, expectedInputs, expectedOutputs)
		assert.False(t, result)
	})

	t.Run("returns false for wrong input parameter name", func(t *testing.T) {
		inputs := []contractFunctionInputSpec{
			{name: "sender", typeName: "Address"},
			{name: "receiver", typeName: "Address"},
		}
		outputs := []string{"i128"}

		expectedInputs := []contractFunctionInputSpec{
			{name: "from", typeName: "Address"},
			{name: "to", typeName: "Address"},
		}
		expectedOutputs := []string{"i128"}

		result := validateFunctionInputsAndOutputs(inputs, outputs, expectedInputs, expectedOutputs)
		assert.False(t, result)
	})

	t.Run("returns false for wrong input parameter type", func(t *testing.T) {
		inputs := []contractFunctionInputSpec{
			{name: "from", typeName: "Address"},
			{name: "to", typeName: "u32"},
		}
		outputs := []string{"i128"}

		expectedInputs := []contractFunctionInputSpec{
			{name: "from", typeName: "Address"},
			{name: "to", typeName: "Address"},
		}
		expectedOutputs := []string{"i128"}

		result := validateFunctionInputsAndOutputs(inputs, outputs, expectedInputs, expectedOutputs)
		assert.False(t, result)
	})

	t.Run("returns false for reordered inputs", func(t *testing.T) {
		inputs := []contractFunctionInputSpec{
			{name: "to", typeName: "Address"},
			{name: "from", typeName: "Address"},
		}
		outputs := []string{"i128"}

		expectedInputs := []contractFunctionInputSpec{
			{name: "from", typeName: "Address"},
			{name: "to", typeName: "Address"},
		}
		expectedOutputs := []string{"i128"}

		result := validateFunctionInputsAndOutputs(inputs, outputs, expectedInputs, expectedOutputs)
		assert.False(t, result)
	})

	t.Run("returns false for too few outputs", func(t *testing.T) {
		inputs := []contractFunctionInputSpec{
			{name: "from", typeName: "Address"},
			{name: "to", typeName: "Address"},
		}
		outputs := []string{}

		expectedInputs := []contractFunctionInputSpec{
			{name: "from", typeName: "Address"},
			{name: "to", typeName: "Address"},
		}
		expectedOutputs := []string{"i128"}

		result := validateFunctionInputsAndOutputs(inputs, outputs, expectedInputs, expectedOutputs)
		assert.False(t, result)
	})

	t.Run("returns false for too many outputs", func(t *testing.T) {
		inputs := []contractFunctionInputSpec{
			{name: "from", typeName: "Address"},
			{name: "to", typeName: "Address"},
		}
		outputs := []string{"i128", "u32"}

		expectedInputs := []contractFunctionInputSpec{
			{name: "from", typeName: "Address"},
			{name: "to", typeName: "Address"},
		}
		expectedOutputs := []string{"i128"}

		result := validateFunctionInputsAndOutputs(inputs, outputs, expectedInputs, expectedOutputs)
		assert.False(t, result)
	})

	t.Run("returns false for wrong output type", func(t *testing.T) {
		inputs := []contractFunctionInputSpec{
			{name: "from", typeName: "Address"},
			{name: "to", typeName: "Address"},
		}
		outputs := []string{"u32"}

		expectedInputs := []contractFunctionInputSpec{
			{name: "from", typeName: "Address"},
			{name: "to", typeName: "Address"},
		}
		expectedOutputs := []string{"i128"}

		result := validateFunctionInputsAndOutputs(inputs, outputs, expectedInputs, expectedOutputs)
		assert.False(t, result)
	})

	t.Run("returns false for duplicate outputs", func(t *testing.T) {
		inputs := []contractFunctionInputSpec{
			{name: "from", typeName: "Address"},
			{name: "to", typeName: "Address"},
		}
		outputs := []string{"i128", "i128"}

		expectedInputs := []contractFunctionInputSpec{
			{name: "from", typeName: "Address"},
			{name: "to", typeName: "Address"},
		}
		expectedOutputs := []string{"i128"}

		result := validateFunctionInputsAndOutputs(inputs, outputs, expectedInputs, expectedOutputs)
		assert.False(t, result)
	})

	t.Run("returns true for empty inputs and outputs", func(t *testing.T) {
		inputs := []contractFunctionInputSpec{}
		outputs := []string{}

		expectedInputs := []contractFunctionInputSpec{}
		expectedOutputs := []string{}

		result := validateFunctionInputsAndOutputs(inputs, outputs, expectedInputs, expectedOutputs)
		assert.True(t, result)
	})
}

// wasmTestdataDir returns the absolute path to the shared WASM testdata directory.
func wasmTestdataDir() string {
	_, filename, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(filename), "..", "..", "integrationtests", "infrastructure", "testdata")
}

func loadTestWasm(t *testing.T, filename string) []byte {
	t.Helper()
	wasmBytes, err := os.ReadFile(filepath.Join(wasmTestdataDir(), filename))
	require.NoError(t, err, "reading test WASM file %s", filename)
	return wasmBytes
}

func TestValidator_RealWasm(t *testing.T) {
	ctx := context.Background()
	extractor := services.NewWasmSpecExtractor()
	defer func() { require.NoError(t, extractor.Close(ctx)) }()

	validator := NewValidator()

	t.Run("token contract validates as SEP-41", func(t *testing.T) {
		wasmBytes := loadTestWasm(t, "soroban_token_contract.wasm")
		specs, err := extractor.ExtractSpec(ctx, wasmBytes)
		require.NoError(t, err)

		assert.True(t, validator.Validate(specs), "token contract should validate as SEP-41")
		assert.Equal(t, "SEP41", validator.ProtocolID())
	})

	t.Run("increment contract does not validate as SEP-41", func(t *testing.T) {
		wasmBytes := loadTestWasm(t, "soroban_increment_contract.wasm")
		specs, err := extractor.ExtractSpec(ctx, wasmBytes)
		require.NoError(t, err)

		assert.False(t, validator.Validate(specs), "increment contract should not validate as SEP-41")
	})
}
