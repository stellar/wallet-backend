// Package wasmspec matches a contract's WASM interface (its parsed
// xdr.ScSpecEntry list) against a table of required function signatures. It
// is used by per-protocol validators (e.g. SEP-41, Blend) that identify a
// contract by exact function-signature matching rather than by wasm hash.
package wasmspec

import (
	set "github.com/deckarep/golang-set/v2"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// InputSpec defines the expected name and type of a single function
// parameter.
type InputSpec struct {
	Name     string
	TypeName string
}

// FunctionSpec defines the expected signature for a contract function: exact
// input parameter names/types (ordered) and exact output types (ordered).
type FunctionSpec struct {
	Name            string
	ExpectedInputs  []InputSpec
	ExpectedOutputs []string
}

// scSpecTypeNames maps XDR ScSpecType values to human-readable type names
// used during signature matching.
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

// TypeName converts an XDR ScSpecType to its human-readable string
// representation. Returns "unknown" for any type not in the known set.
func TypeName(scType xdr.ScSpecType) string {
	if name, ok := scSpecTypeNames[scType]; ok {
		return name
	}
	return "unknown"
}

// Match reports whether contractSpec implements every function listed in
// required with an exact-match signature. A contract satisfies a
// FunctionSpec when it declares a function of the same name whose ordered
// input names/types and ordered output types are identical to one of the
// FunctionSpecs sharing that name (multiple FunctionSpecs may share a name to
// express accepted variants). Extra functions in contractSpec that are not
// listed in required are ignored.
func Match(contractSpec []xdr.ScSpecEntry, required []FunctionSpec) bool {
	requiredSpecs := make(map[string][]FunctionSpec, len(required))
	for _, spec := range required {
		requiredSpecs[spec.Name] = append(requiredSpecs[spec.Name], spec)
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

		actualInputs := make([]InputSpec, 0, len(function.Inputs))
		for _, input := range function.Inputs {
			actualInputs = append(actualInputs, InputSpec{
				Name:     input.Name,
				TypeName: TypeName(input.Type.Type),
			})
		}

		actualOutputs := make([]string, 0, len(function.Outputs))
		for _, output := range function.Outputs {
			actualOutputs = append(actualOutputs, TypeName(output.Type))
		}

		for _, expectedSpec := range expectedSpecs {
			if validateFunctionInputsAndOutputs(actualInputs, actualOutputs, expectedSpec.ExpectedInputs, expectedSpec.ExpectedOutputs) {
				foundFunctions.Add(funcName)
				break
			}
		}
	}

	return foundFunctions.Cardinality() == len(requiredSpecs)
}

// validateFunctionInputsAndOutputs checks if a function's signature matches
// the expected specification. It compares ordered input/output slices with
// exact arity and exact position-by-position matches.
func validateFunctionInputsAndOutputs(
	inputs []InputSpec,
	outputs []string,
	expectedInputs []InputSpec,
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
