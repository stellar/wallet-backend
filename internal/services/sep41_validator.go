package services

import (
	set "github.com/deckarep/golang-set/v2"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// Map of XDR ScSpecType to human-readable type names
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

// sep41FunctionSpec defines the expected signature for a SEP-41 token function.
type sep41FunctionSpec struct {
	name            string
	expectedInputs  []sep41FunctionInputSpec
	expectedOutputs []string
}

type sep41FunctionInputSpec struct {
	name     string
	typeName string
}

// sep41RequiredFunctions defines all required functions for SEP-41 token standard compliance.
// A contract must implement all of these functions with the exact signatures specified.
var sep41RequiredFunctions = []sep41FunctionSpec{
	{
		name:            "balance",
		expectedInputs:  []sep41FunctionInputSpec{{name: "id", typeName: "Address"}},
		expectedOutputs: []string{"i128"},
	},
	{
		name:            "allowance",
		expectedInputs:  []sep41FunctionInputSpec{{name: "from", typeName: "Address"}, {name: "spender", typeName: "Address"}},
		expectedOutputs: []string{"i128"},
	},
	{
		name:            "decimals",
		expectedInputs:  []sep41FunctionInputSpec{},
		expectedOutputs: []string{"u32"},
	},
	{
		name:            "name",
		expectedInputs:  []sep41FunctionInputSpec{},
		expectedOutputs: []string{"String"},
	},
	{
		name:            "symbol",
		expectedInputs:  []sep41FunctionInputSpec{},
		expectedOutputs: []string{"String"},
	},
	{
		name: "approve",
		expectedInputs: []sep41FunctionInputSpec{
			{name: "from", typeName: "Address"},
			{name: "spender", typeName: "Address"},
			{name: "amount", typeName: "i128"},
			{name: "expiration_ledger", typeName: "u32"},
		},
		expectedOutputs: []string{},
	},
	{
		name: "transfer",
		expectedInputs: []sep41FunctionInputSpec{
			{name: "from", typeName: "Address"},
			{name: "to", typeName: "Address"},
			{name: "amount", typeName: "i128"},
		},
	},
	// transfer: (from: Address, to_muxed: MuxedAddress, amount: i128) -> () -> CAP-67
	{
		name: "transfer",
		expectedInputs: []sep41FunctionInputSpec{
			{name: "from", typeName: "Address"},
			{name: "to_muxed", typeName: "MuxedAddress"},
			{name: "amount", typeName: "i128"},
		},
	},
	{
		name: "transfer_from",
		expectedInputs: []sep41FunctionInputSpec{
			{name: "spender", typeName: "Address"},
			{name: "from", typeName: "Address"},
			{name: "to", typeName: "Address"},
			{name: "amount", typeName: "i128"},
		},
		expectedOutputs: []string{},
	},
	{
		name: "burn",
		expectedInputs: []sep41FunctionInputSpec{
			{name: "from", typeName: "Address"},
			{name: "amount", typeName: "i128"},
		},
		expectedOutputs: []string{},
	},
	{
		name: "burn_from",
		expectedInputs: []sep41FunctionInputSpec{
			{name: "spender", typeName: "Address"},
			{name: "from", typeName: "Address"},
			{name: "amount", typeName: "i128"},
		},
		expectedOutputs: []string{},
	},
}

// SEP41ProtocolValidator validates whether a WASM implements the SEP-41 token standard.
type SEP41ProtocolValidator struct{}

func NewSEP41ProtocolValidator() *SEP41ProtocolValidator { return &SEP41ProtocolValidator{} }

func (v *SEP41ProtocolValidator) ProtocolID() string { return "SEP41" }

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
func (v *SEP41ProtocolValidator) Validate(contractSpec []xdr.ScSpecEntry) bool {
	// Build a map of required function names to their specs for quick lookup
	requiredSpecs := make(map[string][]sep41FunctionSpec, len(sep41RequiredFunctions))
	for _, spec := range sep41RequiredFunctions {
		requiredSpecs[spec.name] = append(requiredSpecs[spec.name], spec)
	}

	// Track which required functions we've found and validated
	foundFunctions := set.NewSet[string]()

	// Iterate through the contract spec to find and validate SEP-41 functions
	for _, spec := range contractSpec {
		if spec.Kind != xdr.ScSpecEntryKindScSpecEntryFunctionV0 || spec.FunctionV0 == nil {
			continue
		}

		function := spec.FunctionV0
		funcName := string(function.Name)

		// Check if this is a SEP-41 required function
		expectedSpecs, isRequired := requiredSpecs[funcName]
		if !isRequired {
			continue
		}

		// Extract actual inputs from the contract function, preserving order.
		actualInputs := make([]sep41FunctionInputSpec, 0, len(function.Inputs))
		for _, input := range function.Inputs {
			actualInputs = append(actualInputs, sep41FunctionInputSpec{
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

	// All required functions must be present
	return foundFunctions.Cardinality() == len(requiredSpecs)
}

// validateFunctionInputsAndOutputs checks if a function's signature matches the expected SEP-41 specification.
// It compares ordered input/output slices with exact arity and exact position-by-position matches.
func validateFunctionInputsAndOutputs(
	inputs []sep41FunctionInputSpec,
	outputs []string,
	expectedInputs []sep41FunctionInputSpec,
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
// Returns "unknown" for unmapped types.
func getTypeName(scType xdr.ScSpecType) string {
	if name, ok := scSpecTypeNames[scType]; ok {
		return name
	}
	return "unknown"
}
