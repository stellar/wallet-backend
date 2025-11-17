package services

import (
	"bytes"
	"context"
	"fmt"

	set "github.com/deckarep/golang-set/v2"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"
	"github.com/tetratelabs/wazero"

	"github.com/stellar/wallet-backend/internal/indexer/types"
)

const (
	contractSpecV0SectionName = "contractspecv0"
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
	expectedInputs  map[string]any
	expectedOutputs []string
}

// sep41RequiredFunctions defines all required functions for SEP-41 token standard compliance.
// A contract must implement all of these functions with the exact signatures specified.
var sep41RequiredFunctions = []sep41FunctionSpec{
	{
		name:            "balance",
		expectedInputs:  map[string]any{"id": "Address"},
		expectedOutputs: []string{"i128"},
	},
	{
		name:            "allowance",
		expectedInputs:  map[string]any{"from": "Address", "spender": "Address"},
		expectedOutputs: []string{"i128"},
	},
	{
		name:            "decimals",
		expectedInputs:  map[string]any{},
		expectedOutputs: []string{"u32"},
	},
	{
		name:            "name",
		expectedInputs:  map[string]any{},
		expectedOutputs: []string{"String"},
	},
	{
		name:            "symbol",
		expectedInputs:  map[string]any{},
		expectedOutputs: []string{"String"},
	},
	{
		name: "approve",
		expectedInputs: map[string]any{
			"from":              "Address",
			"spender":           "Address",
			"amount":            "i128",
			"expiration_ledger": "u32",
		},
		expectedOutputs: []string{},
	},
	{
		name: "transfer",
		expectedInputs: map[string]any{
			"from":   set.NewSet("Address", "MuxedAddress"), // Support the new MuxedAddress type change introduced in CAP-67
			"to":     "Address",
			"amount": "i128",
		},
		expectedOutputs: []string{},
	},
	{
		name: "transfer_from",
		expectedInputs: map[string]any{
			"spender": "Address",
			"from":    "Address",
			"to":      "Address",
			"amount":  "i128",
		},
		expectedOutputs: []string{},
	},
	{
		name: "burn",
		expectedInputs: map[string]any{
			"from":   "Address",
			"amount": "i128",
		},
		expectedOutputs: []string{},
	},
	{
		name: "burn_from",
		expectedInputs: map[string]any{
			"spender": "Address",
			"from":    "Address",
			"amount":  "i128",
		},
		expectedOutputs: []string{},
	},
}

type ContractValidator interface {
	ValidateFromContractCode(_ context.Context, _ []byte) (types.ContractType, error)
	Close(_ context.Context) error
}

type contractValidator struct {
	runtime wazero.Runtime
}

// NewContractValidator creates a new ContractValidator with a configured wazero runtime.
// The runtime is initialized with custom sections enabled to extract contract specifications from WASM bytecode.
func NewContractValidator() ContractValidator {
	// Create wazero runtime with custom sections enabled
	config := wazero.NewRuntimeConfig().WithCustomSections(true)
	runtime := wazero.NewRuntimeWithConfig(context.Background(), config)

	return &contractValidator{
		runtime: runtime,
	}
}

// ValidateFromContractCode validates a contract code against the SEP-41 token standard.
func (v *contractValidator) ValidateFromContractCode(ctx context.Context, contractCode []byte) (types.ContractType, error) {
	contractSpec, err := v.extractContractSpecFromWasmCode(ctx, contractCode)
	if err != nil {
		return types.ContractTypeUnknown, fmt.Errorf("extracting contract spec from WASM: %w", err)
	}
	isSep41 := v.isContractCodeSEP41(contractSpec)
	if isSep41 {
		return types.ContractTypeSEP41, nil
	}
	return types.ContractTypeUnknown, nil
}

// Close shuts down the wazero runtime and releases associated resources.
// Should be called when the validator is no longer needed to prevent resource leaks.
func (v *contractValidator) Close(ctx context.Context) error {
	if err := v.runtime.Close(ctx); err != nil {
		return fmt.Errorf("closing contract spec validator: %w", err)
	}
	return nil
}

// isContractCodeSEP41 validates whether a contract spec implements the SEP-41 token standard.
// For a contract to be SEP-41 compliant, it must implement all required functions with exact signatures:
//   - balance: (id: Address) -> (i128)
//   - allowance: (from: Address, spender: Address) -> (i128)
//   - decimals: () -> (u32)
//   - name: () -> (String)
//   - symbol: () -> (String)
//   - approve: (from: Address, spender: Address, amount: i128, expiration_ledger: u32) -> ()
//   - transfer: (from: Address, to: Address, amount: i128) -> ()
//   - transfer_from: (spender: Address, from: Address, to: Address, amount: i128) -> ()
//   - burn: (from: Address, amount: i128) -> ()
//   - burn_from: (spender: Address, from: Address, amount: i128) -> ()
func (v *contractValidator) isContractCodeSEP41(contractSpec []xdr.ScSpecEntry) bool {
	// Build a map of required function names to their specs for quick lookup
	requiredFuncsMap := make(map[string]sep41FunctionSpec, len(sep41RequiredFunctions))
	for _, spec := range sep41RequiredFunctions {
		requiredFuncsMap[spec.name] = spec
	}

	// Track which required functions we've found and validated
	foundFunctions := make(map[string]bool, len(sep41RequiredFunctions))

	// Iterate through the contract spec to find and validate SEP-41 functions
	for _, spec := range contractSpec {
		if spec.Kind != xdr.ScSpecEntryKindScSpecEntryFunctionV0 || spec.FunctionV0 == nil {
			continue
		}

		function := spec.FunctionV0
		funcName := string(function.Name)

		// Check if this is a SEP-41 required function
		expectedSpec, isRequired := requiredFuncsMap[funcName]
		if !isRequired {
			continue
		}

		// Extract actual inputs from the contract function
		actualInputs := make(map[string]any, len(function.Inputs))
		for _, input := range function.Inputs {
			actualInputs[input.Name] = getTypeName(input.Type.Type)
		}

		// Extract actual outputs from the contract function
		actualOutputs := set.NewSet[string]()
		for _, output := range function.Outputs {
			actualOutputs.Add(getTypeName(output.Type))
		}

		// Convert expected outputs to set for comparison
		expectedOutputs := set.NewSet(expectedSpec.expectedOutputs...)

		// Validate the function signature matches SEP-41 requirements
		if !v.validateFunctionInputsAndOutputs(actualInputs, actualOutputs, expectedSpec.expectedInputs, expectedOutputs) {
			// If a required function exists but has wrong signature, fail immediately
			return false
		}

		foundFunctions[funcName] = true
	}

	// All required functions must be present
	return len(foundFunctions) == len(sep41RequiredFunctions)
}

// validateFunctionInputsAndOutputs checks if a function's signature matches the expected SEP-41 specification.
// It compares input parameter names/types and output types, supporting both exact matches and sets of valid types
// (e.g., for CAP-67 where "from" parameter accepts both Address and MuxedAddress).
func (v *contractValidator) validateFunctionInputsAndOutputs(inputs map[string]any, outputs set.Set[string], expectedInputs map[string]any, expectedOutputs set.Set[string]) bool {
	if len(inputs) != len(expectedInputs) {
		return false
	}

	for expectedInput, expectedInputType := range expectedInputs {
		switch inputType := expectedInputType.(type) {
		// This handles the case where new input types are introduced in the future CAPs.
		// We need to support both old and new input types.
		case set.Set[string]:
			if !inputType.Contains(inputs[expectedInput].(string)) {
				return false
			}
		default:
			if inputs[expectedInput] != inputType {
				return false
			}
		}
	}

	if expectedOutputs.Cardinality() != outputs.Cardinality() {
		return false
	}

	if !expectedOutputs.Equal(outputs) {
		return false
	}
	return true
}

// extractContractSpecFromWasmCode parses the contract specification from WASM bytecode.
// It compiles the WASM module, extracts the "contractspecv0" custom section, and unmarshals
// the XDR-encoded contract specification entries.
func (v *contractValidator) extractContractSpecFromWasmCode(ctx context.Context, wasmCode []byte) ([]xdr.ScSpecEntry, error) {
	// Compile WASM module (validates structure and won't panic)
	compiledModule, err := v.runtime.CompileModule(ctx, wasmCode)
	if err != nil {
		return nil, fmt.Errorf("compiling WASM module: %w", err)
	}
	defer func() {
		if closeErr := compiledModule.Close(ctx); closeErr != nil {
			log.Warnf("Failed to close compiled module: %v", closeErr)
		}
	}()

	// Extract all custom sections
	customSections := compiledModule.CustomSections()

	// Find contractspecv0 section
	var specBytes []byte
	for _, section := range customSections {
		if section.Name() == contractSpecV0SectionName {
			specBytes = section.Data()
			break
		}
	}

	if specBytes == nil {
		return nil, fmt.Errorf("contractspecv0 section not found")
	}

	// Parse XDR stream of ScSpecEntry
	var specs []xdr.ScSpecEntry
	reader := bytes.NewReader(specBytes)

	for reader.Len() > 0 {
		var spec xdr.ScSpecEntry
		_, err := xdr.Unmarshal(reader, &spec)
		if err != nil {
			return nil, fmt.Errorf("unmarshaling spec entry: %w", err)
		}
		specs = append(specs, spec)
	}

	return specs, nil
}

// getTypeName converts an XDR ScSpecType to its human-readable string representation.
// Returns "unknown" for unmapped types.
func getTypeName(scType xdr.ScSpecType) string {
	if name, ok := scSpecTypeNames[scType]; ok {
		return name
	}
	return "unknown"
}
