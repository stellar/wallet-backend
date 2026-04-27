package sep41

import (
	"context"
	"encoding/hex"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
	indexerTypes "github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/services"
)

func TestIsContractCodeSEP41(t *testing.T) {
	t.Run("returns true for complete SEP-41 contract", func(t *testing.T) {
		spec := createSEP41ContractSpec()
		result := matchSEP41Spec(spec)
		assert.True(t, result)
	})

	t.Run("returns false when missing balance function", func(t *testing.T) {
		spec := createPartialSEP41Spec([]string{"balance"})
		result := matchSEP41Spec(spec)
		assert.False(t, result)
	})

	t.Run("returns false when missing allowance function", func(t *testing.T) {
		spec := createPartialSEP41Spec([]string{"allowance"})
		result := matchSEP41Spec(spec)
		assert.False(t, result)
	})

	t.Run("returns false when missing decimals function", func(t *testing.T) {
		spec := createPartialSEP41Spec([]string{"decimals"})
		result := matchSEP41Spec(spec)
		assert.False(t, result)
	})

	t.Run("returns false when missing name function", func(t *testing.T) {
		spec := createPartialSEP41Spec([]string{"name"})
		result := matchSEP41Spec(spec)
		assert.False(t, result)
	})

	t.Run("returns false when missing symbol function", func(t *testing.T) {
		spec := createPartialSEP41Spec([]string{"symbol"})
		result := matchSEP41Spec(spec)
		assert.False(t, result)
	})

	t.Run("returns false when missing approve function", func(t *testing.T) {
		spec := createPartialSEP41Spec([]string{"approve"})
		result := matchSEP41Spec(spec)
		assert.False(t, result)
	})

	t.Run("returns false when missing transfer function", func(t *testing.T) {
		spec := createPartialSEP41Spec([]string{"transfer"})
		result := matchSEP41Spec(spec)
		assert.False(t, result)
	})

	t.Run("returns false when missing transfer_from function", func(t *testing.T) {
		spec := createPartialSEP41Spec([]string{"transfer_from"})
		result := matchSEP41Spec(spec)
		assert.False(t, result)
	})

	t.Run("returns false when missing burn function", func(t *testing.T) {
		spec := createPartialSEP41Spec([]string{"burn"})
		result := matchSEP41Spec(spec)
		assert.False(t, result)
	})

	t.Run("returns false when missing burn_from function", func(t *testing.T) {
		spec := createPartialSEP41Spec([]string{"burn_from"})
		result := matchSEP41Spec(spec)
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

		result := matchSEP41Spec(spec)
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

		result := matchSEP41Spec(spec)
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

		result := matchSEP41Spec(spec)
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

		result := matchSEP41Spec(spec)
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

		result := matchSEP41Spec(spec)
		assert.False(t, result)
	})

	t.Run("returns false for empty contract spec", func(t *testing.T) {
		spec := []xdr.ScSpecEntry{}
		result := matchSEP41Spec(spec)
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

		result := matchSEP41Spec(spec)
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

		result := matchSEP41Spec(spec)
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

		assert.True(t, matchSEP41Spec(specs), "token contract should validate as SEP-41")
		assert.Equal(t, "SEP41", validator.ProtocolID())
	})

	t.Run("increment contract does not validate as SEP-41", func(t *testing.T) {
		wasmBytes := loadTestWasm(t, "soroban_increment_contract.wasm")
		specs, err := extractor.ExtractSpec(ctx, wasmBytes)
		require.NoError(t, err)

		assert.False(t, matchSEP41Spec(specs), "increment contract should not validate as SEP-41")
	})
}

// TestValidator_NoMatch covers the case where the signature check rejects the
// candidate spec. No contract_tokens writes happen and no matches are
// returned.
func TestValidator_NoMatch(t *testing.T) {
	v := NewValidator()
	hash := indexerTypes.HashBytea("aabb")
	out, err := v.Validate(context.Background(), nil, services.ValidationInput{
		Candidates: []services.WasmCandidate{
			{Hash: hash, SpecEntries: []xdr.ScSpecEntry{{Kind: xdr.ScSpecEntryKindScSpecEntryFunctionV0}}},
		},
	})
	require.NoError(t, err)
	assert.Empty(t, out.MatchedWasms)
}

// TestValidator_KnownContractEnrichmentRunsWithoutCandidate verifies that the
// validator enriches contracts whose wasm hash was classified as SEP-41 in a
// prior batch (KnownProtocolID = "SEP41") even when the wasm itself is not
// in this batch's Candidates.
func TestValidator_KnownContractEnrichmentRunsWithoutCandidate(t *testing.T) {
	contractsMock := data.NewContractModelMock(t)
	models := &data.Models{Contract: contractsMock}

	contractsMock.On("BatchInsert", mock.Anything, mock.Anything, mock.MatchedBy(func(cs []*data.Contract) bool {
		return len(cs) == 1 && cs[0].Type == ProtocolID
	})).Return(nil).Once()

	v := NewValidator()

	contractAddr := "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA"
	rawAddr, err := strkey.Decode(strkey.VersionByteContract, contractAddr)
	require.NoError(t, err)
	contractID := indexerTypes.HashBytea(hex.EncodeToString(rawAddr))

	out, err := v.Validate(context.Background(), nil, services.ValidationInput{
		Contracts: []services.ContractCandidate{
			{ContractID: contractID, WasmHash: indexerTypes.HashBytea("aabb"), KnownProtocolID: ProtocolID},
		},
		Models: models,
	})
	require.NoError(t, err)
	assert.Empty(t, out.MatchedWasms, "no in-batch candidates → no match returned")
}
