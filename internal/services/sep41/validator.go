package sep41

import (
	set "github.com/deckarep/golang-set/v2"
	"github.com/stellar/go-stellar-sdk/xdr"
)

const ProtocolID = "SEP41"

// scSpecTypeNames maps XDR ScSpecType values to human-readable type names used during validation.
var scSpecTypeNames = map[xdr.ScSpecType]string{
	xdr.ScSpecTypeScSpecTypeBool:         "bool",
	xdr.ScSpecTypeScSpecTypeU32:          "u32",
	xdr.ScSpecTypeScSpecTypeI32:          "i32",
	xdr.ScSpecTypeScSpecTypeU64:          "u64",
	xdr.ScSpecTypeScSpecTypeI64:          "i64",
	xdr.ScSpecTypeScSpecTypeU128:         "u128",
	xdr.ScSpecTypeScSpecTypeI128:         "i128",
	xdr.ScSpecTypeScSpecTypeU256:         "u256",
	xdr.ScSpecTypeScSpecTypeI256:         "i256",
	xdr.ScSpecTypeScSpecTypeAddress:      "Address",
	xdr.ScSpecTypeScSpecTypeMuxedAddress: "MuxedAddress",
	xdr.ScSpecTypeScSpecTypeString:       "String",
	xdr.ScSpecTypeScSpecTypeBytes:        "Bytes",
	xdr.ScSpecTypeScSpecTypeSymbol:       "Symbol",
	xdr.ScSpecTypeScSpecTypeVec:          "Vec",
	xdr.ScSpecTypeScSpecTypeMap:          "Map",
	xdr.ScSpecTypeScSpecTypeOption:       "Option",
	xdr.ScSpecTypeScSpecTypeResult:       "Result",
	xdr.ScSpecTypeScSpecTypeTuple:        "Tuple",
	xdr.ScSpecTypeScSpecTypeBytesN:       "BytesN",
	xdr.ScSpecTypeScSpecTypeUdt:          "UDT",
	xdr.ScSpecTypeScSpecTypeVoid:         "void",
}

// functionSpec defines the expected signature for a SEP-41 token function.
type functionSpec struct {
	name            string
	expectedInputs  []contractFunctionInputSpec
	expectedOutputs []string
}

type contractFunctionInputSpec struct {
	name     string
	typeName string
}

// sep41RequiredFunctions defines all required functions for SEP-41 token standard compliance.
// A contract must implement all of these functions with the exact signatures specified.
var requiredFunctions = []functionSpec{
	{
		name:            "balance",
		expectedInputs:  []contractFunctionInputSpec{{name: "id", typeName: "Address"}},
		expectedOutputs: []string{"i128"},
	},
	{
		name:            "allowance",
		expectedInputs:  []contractFunctionInputSpec{{name: "from", typeName: "Address"}, {name: "spender", typeName: "Address"}},
		expectedOutputs: []string{"i128"},
	},
	{
		name:            "decimals",
		expectedInputs:  []contractFunctionInputSpec{},
		expectedOutputs: []string{"u32"},
	},
	{
		name:            "name",
		expectedInputs:  []contractFunctionInputSpec{},
		expectedOutputs: []string{"String"},
	},
	{
		name:            "symbol",
		expectedInputs:  []contractFunctionInputSpec{},
		expectedOutputs: []string{"String"},
	},
	{
		name: "approve",
		expectedInputs: []contractFunctionInputSpec{
			{name: "from", typeName: "Address"},
			{name: "spender", typeName: "Address"},
			{name: "amount", typeName: "i128"},
			{name: "expiration_ledger", typeName: "u32"},
		},
		expectedOutputs: []string{},
	},
	{
		name: "transfer",
		expectedInputs: []contractFunctionInputSpec{
			{name: "from", typeName: "Address"},
			{name: "to", typeName: "Address"},
			{name: "amount", typeName: "i128"},
		},
	},
	// CAP-67 variant: (from: Address, to_muxed: MuxedAddress, amount: i128) -> ()
	{
		name: "transfer",
		expectedInputs: []contractFunctionInputSpec{
			{name: "from", typeName: "Address"},
			{name: "to_muxed", typeName: "MuxedAddress"},
			{name: "amount", typeName: "i128"},
		},
	},
	{
		name: "transfer_from",
		expectedInputs: []contractFunctionInputSpec{
			{name: "spender", typeName: "Address"},
			{name: "from", typeName: "Address"},
			{name: "to", typeName: "Address"},
			{name: "amount", typeName: "i128"},
		},
		expectedOutputs: []string{},
	},
	{
		name: "burn",
		expectedInputs: []contractFunctionInputSpec{
			{name: "from", typeName: "Address"},
			{name: "amount", typeName: "i128"},
		},
		expectedOutputs: []string{},
	},
	{
		name: "burn_from",
		expectedInputs: []contractFunctionInputSpec{
			{name: "spender", typeName: "Address"},
			{name: "from", typeName: "Address"},
			{name: "amount", typeName: "i128"},
		},
		expectedOutputs: []string{},
	},
}

// Validator validates whether a WASM implements the SEP-41 token standard.
type Validator struct{}

// NewValidator constructs a SEP-41 ProtocolValidator.
func NewValidator() *Validator { return &Validator{} }

func (v *Validator) ProtocolID() string { return ProtocolID }

// Validate checks whether a contract spec implements the SEP-41 token standard.
// For a contract to be SEP-41 compliant, it must implement all required functions with exact signatures:
//   - balance: (id: Address) -> (i128)
//   - allowance: (from: Address, spender: Address) -> (i128)
//   - decimals: () -> (u32)
//   - name: () -> (String)
//   - symbol: () -> (String)
//   - approve: (from: Address, spender: Address, amount: i128, expiration_ledger: u32) -> ()
//   - transfer: (from: Address, to: Address, amount: i128) -> () or (from: Address, to_muxed: MuxedAddress, amount: i128) -> ()
//   - transfer_from: (spender: Address, from: Address, to: Address, amount: i128) -> ()
//   - burn: (from: Address, amount: i128) -> ()
//   - burn_from: (spender: Address, from: Address, amount: i128) -> ()
func (v *Validator) Validate(contractSpec []xdr.ScSpecEntry) bool {
	requiredSpecs := make(map[string][]functionSpec, len(requiredFunctions))
	for _, spec := range requiredFunctions {
		requiredSpecs[spec.name] = append(requiredSpecs[spec.name], spec)
	}

	foundFunctions := set.NewSet[string]()

	for _, spec := range contractSpec {
		if spec.Kind != xdr.ScSpecEntryKindScSpecEntryFunctionV0 || spec.FunctionV0 == nil {
			continue
		}

		function := spec.FunctionV0
		funcName := string(function.Name)

		expectedSpecs, isRequired := requiredSpecs[funcName]
		if !isRequired {
			continue
		}

		// Extract actual inputs from the contract function, preserving order.
		actualInputs := make([]contractFunctionInputSpec, 0, len(function.Inputs))
		for _, input := range function.Inputs {
			actualInputs = append(actualInputs, contractFunctionInputSpec{
				name:     input.Name,
				typeName: getTypeName(input.Type.Type),
			})
		}

		// Extract actual outputs from the contract function, preserving order.
		actualOutputs := make([]string, 0, len(function.Outputs))
		for _, output := range function.Outputs {
			actualOutputs = append(actualOutputs, getTypeName(output.Type))
		}

		for _, expectedSpec := range expectedSpecs {
			// Validate the function signature matches SEP-41 requirements
			if validateFunctionInputsAndOutputs(actualInputs, actualOutputs, expectedSpec.expectedInputs, expectedSpec.expectedOutputs) {
				foundFunctions.Add(funcName)
				break
			}
		}
	}

	return foundFunctions.Cardinality() == len(requiredSpecs)
}

// validateFunctionInputsAndOutputs checks if a function's signature matches the expected SEP-41 specification.
// It compares ordered input/output slices with exact arity and exact position-by-position matches.
func validateFunctionInputsAndOutputs(
	inputs []contractFunctionInputSpec,
	outputs []string,
	expectedInputs []contractFunctionInputSpec,
	expectedOutputs []string,
) bool {
	if len(inputs) != len(expectedInputs) {
		return false
	}

	for i := range expectedInputs {
		if inputs[i] != expectedInputs[i] {
			return false
		}
	}

	if len(outputs) != len(expectedOutputs) {
		return false
	}

	for i := range expectedOutputs {
		if outputs[i] != expectedOutputs[i] {
			return false
		}
	}
	return true
}

// getTypeName converts an XDR ScSpecType to its human-readable string representation.
func getTypeName(scType xdr.ScSpecType) string {
	if name, ok := scSpecTypeNames[scType]; ok {
		return name
	}
	return "unknown"
}
