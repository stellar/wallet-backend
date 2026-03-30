package services

import (
	"testing"

	set "github.com/deckarep/golang-set/v2"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
)

// Shared XDR test helpers for creating contract spec entries.

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

func createFunctionInput(name string, typeDef xdr.ScSpecTypeDef) xdr.ScSpecFunctionInputV0 {
	return xdr.ScSpecFunctionInputV0{
		Name: name,
		Type: typeDef,
	}
}

func createScSpecTypeDef(scType xdr.ScSpecType) xdr.ScSpecTypeDef {
	return xdr.ScSpecTypeDef{
		Type: scType,
	}
}

func createSEP41ContractSpec() []xdr.ScSpecEntry {
	addressType := createScSpecTypeDef(xdr.ScSpecTypeScSpecTypeAddress)
	i128Type := createScSpecTypeDef(xdr.ScSpecTypeScSpecTypeI128)
	u32Type := createScSpecTypeDef(xdr.ScSpecTypeScSpecTypeU32)
	stringType := createScSpecTypeDef(xdr.ScSpecTypeScSpecTypeString)

	return []xdr.ScSpecEntry{
		createScSpecFunctionEntry("balance",
			[]xdr.ScSpecFunctionInputV0{createFunctionInput("id", addressType)},
			[]xdr.ScSpecTypeDef{i128Type},
		),
		createScSpecFunctionEntry("allowance",
			[]xdr.ScSpecFunctionInputV0{
				createFunctionInput("from", addressType),
				createFunctionInput("spender", addressType),
			},
			[]xdr.ScSpecTypeDef{i128Type},
		),
		createScSpecFunctionEntry("decimals",
			[]xdr.ScSpecFunctionInputV0{},
			[]xdr.ScSpecTypeDef{u32Type},
		),
		createScSpecFunctionEntry("name",
			[]xdr.ScSpecFunctionInputV0{},
			[]xdr.ScSpecTypeDef{stringType},
		),
		createScSpecFunctionEntry("symbol",
			[]xdr.ScSpecFunctionInputV0{},
			[]xdr.ScSpecTypeDef{stringType},
		),
		createScSpecFunctionEntry("approve",
			[]xdr.ScSpecFunctionInputV0{
				createFunctionInput("from", addressType),
				createFunctionInput("spender", addressType),
				createFunctionInput("amount", i128Type),
				createFunctionInput("expiration_ledger", u32Type),
			},
			[]xdr.ScSpecTypeDef{},
		),
		createScSpecFunctionEntry("transfer",
			[]xdr.ScSpecFunctionInputV0{
				createFunctionInput("from", addressType),
				createFunctionInput("to", addressType),
				createFunctionInput("amount", i128Type),
			},
			[]xdr.ScSpecTypeDef{},
		),
		createScSpecFunctionEntry("transfer_from",
			[]xdr.ScSpecFunctionInputV0{
				createFunctionInput("spender", addressType),
				createFunctionInput("from", addressType),
				createFunctionInput("to", addressType),
				createFunctionInput("amount", i128Type),
			},
			[]xdr.ScSpecTypeDef{},
		),
		createScSpecFunctionEntry("burn",
			[]xdr.ScSpecFunctionInputV0{
				createFunctionInput("from", addressType),
				createFunctionInput("amount", i128Type),
			},
			[]xdr.ScSpecTypeDef{},
		),
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

func createPartialSEP41Spec(missingFunctions []string) []xdr.ScSpecEntry {
	fullSpec := createSEP41ContractSpec()
	missingSet := make(map[string]bool, len(missingFunctions))
	for _, f := range missingFunctions {
		missingSet[f] = true
	}

	var result []xdr.ScSpecEntry
	for _, entry := range fullSpec {
		if entry.FunctionV0 != nil {
			funcName := string(entry.FunctionV0.Name)
			if !missingSet[funcName] {
				result = append(result, entry)
			}
		}
	}
	return result
}

func TestIsContractCodeSEP41(t *testing.T) {
	t.Run("returns true for complete SEP-41 contract", func(t *testing.T) {
		spec := createSEP41ContractSpec()
		result := NewSEP41ProtocolValidator().Validate(spec)
		assert.True(t, result)
	})

	t.Run("returns false when missing balance function", func(t *testing.T) {
		spec := createPartialSEP41Spec([]string{"balance"})
		result := NewSEP41ProtocolValidator().Validate(spec)
		assert.False(t, result)
	})

	t.Run("returns false when missing allowance function", func(t *testing.T) {
		spec := createPartialSEP41Spec([]string{"allowance"})
		result := NewSEP41ProtocolValidator().Validate(spec)
		assert.False(t, result)
	})

	t.Run("returns false when missing decimals function", func(t *testing.T) {
		spec := createPartialSEP41Spec([]string{"decimals"})
		result := NewSEP41ProtocolValidator().Validate(spec)
		assert.False(t, result)
	})

	t.Run("returns false when missing name function", func(t *testing.T) {
		spec := createPartialSEP41Spec([]string{"name"})
		result := NewSEP41ProtocolValidator().Validate(spec)
		assert.False(t, result)
	})

	t.Run("returns false when missing symbol function", func(t *testing.T) {
		spec := createPartialSEP41Spec([]string{"symbol"})
		result := NewSEP41ProtocolValidator().Validate(spec)
		assert.False(t, result)
	})

	t.Run("returns false when missing approve function", func(t *testing.T) {
		spec := createPartialSEP41Spec([]string{"approve"})
		result := NewSEP41ProtocolValidator().Validate(spec)
		assert.False(t, result)
	})

	t.Run("returns false when missing transfer function", func(t *testing.T) {
		spec := createPartialSEP41Spec([]string{"transfer"})
		result := NewSEP41ProtocolValidator().Validate(spec)
		assert.False(t, result)
	})

	t.Run("returns false when missing transfer_from function", func(t *testing.T) {
		spec := createPartialSEP41Spec([]string{"transfer_from"})
		result := NewSEP41ProtocolValidator().Validate(spec)
		assert.False(t, result)
	})

	t.Run("returns false when missing burn function", func(t *testing.T) {
		spec := createPartialSEP41Spec([]string{"burn"})
		result := NewSEP41ProtocolValidator().Validate(spec)
		assert.False(t, result)
	})

	t.Run("returns false when missing burn_from function", func(t *testing.T) {
		spec := createPartialSEP41Spec([]string{"burn_from"})
		result := NewSEP41ProtocolValidator().Validate(spec)
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

		result := NewSEP41ProtocolValidator().Validate(spec)
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

		result := NewSEP41ProtocolValidator().Validate(spec)
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

		result := NewSEP41ProtocolValidator().Validate(spec)
		assert.False(t, result)
	})

	t.Run("returns false for empty contract spec", func(t *testing.T) {
		spec := []xdr.ScSpecEntry{}
		result := NewSEP41ProtocolValidator().Validate(spec)
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

		result := NewSEP41ProtocolValidator().Validate(spec)
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

		result := NewSEP41ProtocolValidator().Validate(spec)
		assert.True(t, result)
	})
}

func TestValidateFunctionInputsAndOutputs(t *testing.T) {
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

		result := validateFunctionInputsAndOutputs(inputs, outputs, expectedInputs, expectedOutputs)
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

		result := validateFunctionInputsAndOutputs(inputs, outputs, expectedInputs, expectedOutputs)
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

		result := validateFunctionInputsAndOutputs(inputs, outputs, expectedInputs, expectedOutputs)
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

		result := validateFunctionInputsAndOutputs(inputs, outputs, expectedInputs, expectedOutputs)
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

		result := validateFunctionInputsAndOutputs(inputs, outputs, expectedInputs, expectedOutputs)
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

		result := validateFunctionInputsAndOutputs(inputs, outputs, expectedInputs, expectedOutputs)
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

		result := validateFunctionInputsAndOutputs(inputs, outputs, expectedInputs, expectedOutputs)
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

		result := validateFunctionInputsAndOutputs(inputs, outputs, expectedInputs, expectedOutputs)
		assert.False(t, result)
	})

	t.Run("returns true for empty inputs and outputs", func(t *testing.T) {
		inputs := map[string]any{}
		outputs := set.NewSet[string]()

		expectedInputs := map[string]string{}
		expectedOutputs := set.NewSet[string]()

		result := validateFunctionInputsAndOutputs(inputs, outputs, expectedInputs, expectedOutputs)
		assert.True(t, result)
	})
}
