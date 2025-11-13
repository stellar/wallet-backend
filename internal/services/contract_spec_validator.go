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
	getLedgerEntriesBatchSize = 10
)

type ContractSpecValidator interface {
	Validate(_ context.Context, _ []xdr.Hash) (map[xdr.Hash]types.ContractType, error)
	Close(_ context.Context) error
}

type contractSpecValidator struct {
	rpcService RPCService
	runtime    wazero.Runtime
}

func NewContractSpecValidator(rpcService RPCService) ContractSpecValidator {
	// Create wazero runtime with custom sections enabled
	config := wazero.NewRuntimeConfig().WithCustomSections(true)
	runtime := wazero.NewRuntimeWithConfig(context.Background(), config)

	return &contractSpecValidator{
		rpcService: rpcService,
		runtime:    runtime,
	}
}

func (v *contractSpecValidator) Validate(ctx context.Context, wasmHashes []xdr.Hash) (map[xdr.Hash]types.ContractType, error) {
	ledgerKeys := make([]string, 0)
	for _, wasmHash := range wasmHashes {
		ledgerKey, err := v.getContractCodeLedgerKey(wasmHash)
		if err != nil {
			return nil, fmt.Errorf("getting contract code ledger key: %w", err)
		}
		ledgerKeys = append(ledgerKeys, ledgerKey)
	}

	contractTypesByWasmHash := make(map[xdr.Hash]types.ContractType)
	for i := 0; i < len(ledgerKeys); i += getLedgerEntriesBatchSize {
		end := min(i+getLedgerEntriesBatchSize, len(ledgerKeys))
		batch := ledgerKeys[i:end]

		entries, err := v.rpcService.GetLedgerEntries(batch)
		if err != nil {
			return nil, fmt.Errorf("getting ledger entries: %w", err)
		}
		for _, entry := range entries.Entries {
			var ledgerEntryData xdr.LedgerEntryData
			err := xdr.SafeUnmarshalBase64(entry.DataXDR, &ledgerEntryData)
			if err != nil {
				return nil, fmt.Errorf("unmarshalling ledger entry data: %w", err)
			}

			if ledgerEntryData.Type != xdr.LedgerEntryTypeContractCode {
				continue
			}

			contractCodeEntry := ledgerEntryData.MustContractCode()
			wasmCode := contractCodeEntry.Code
			wasmHash := contractCodeEntry.Hash
			contractSpec, err := v.extractContractSpecFromWasmCode(wasmCode)
			if err != nil {
				return nil, fmt.Errorf("extracting contract spec from WASM: %w", err)
			}
			isSep41 := v.isContractCodeSEP41(contractSpec)
			if isSep41 {
				contractTypesByWasmHash[wasmHash] = types.ContractTypeSEP41
			} else {
				contractTypesByWasmHash[wasmHash] = types.ContractTypeUnknown
			}
		}
	}
	return contractTypesByWasmHash, nil
}

func (v *contractSpecValidator) Close(ctx context.Context) error {
	if err := v.runtime.Close(ctx); err != nil {
		return fmt.Errorf("closing contract spec validator: %w", err)
	}
	return nil
}

func (v *contractSpecValidator) isContractCodeSEP41(contractSpec []xdr.ScSpecEntry) bool {
	/*
		For a contract to be SEP-41, it must atleast have the following functions and inputs/outputs:
		- balance: (id: Address) -> (i128)
		- allowance: (from: Address, spender: Address) -> (i128)
		- decimals: () -> (u32)
		- name: () -> (String)
		- symbol: () -> (String)
		- approve: (from: Address, spender: Address, amount: i128, expiration_ledger: u32) -> ()
		- transfer: (from: Address, to: Address, amount: i128) -> ()
		- transfer_from: (spender: Address, from: Address, to: Address, amount: i128) -> ()
		- burn: (from: Address, amount: i128) -> ()
		- burn_from: (spender: Address, from: Address, amount: i128) -> ()
	*/
	requiredFunctions := map[string]bool{
		"balance":       false,
		"allowance":     false,
		"decimals":      false,
		"name":          false,
		"symbol":        false,
		"approve":       false,
		"transfer":      false,
		"transfer_from": false,
		"burn":          false,
		"burn_from":     false,
	}
	for _, spec := range contractSpec {
		if spec.Kind == xdr.ScSpecEntryKindScSpecEntryFunctionV0 && spec.FunctionV0 != nil {
			function := spec.FunctionV0

			funcName := string(function.Name)
			inputs := make(map[string]string, 0)
			for _, input := range function.Inputs {
				inputs[input.Name] = getTypeName(input.Type.Type)
			}

			outputs := set.NewSet[string]()
			for _, output := range function.Outputs {
				outputs.Add(getTypeName(output.Type))
			}

			switch funcName {
			case "balance":
				expectedInputs := map[string]string{
					"id": "Address",
				}
				expectedOutputs := set.NewSet(
					"i128",
				)

				if !v.validateFunctionInputsAndOutputs(inputs, outputs, expectedInputs, expectedOutputs) {
					return false
				}
				requiredFunctions["balance"] = true
			case "allowance":
				expectedInputs := map[string]string{
					"from":    "Address",
					"spender": "Address",
				}
				expectedOutputs := set.NewSet(
					"i128",
				)

				if !v.validateFunctionInputsAndOutputs(inputs, outputs, expectedInputs, expectedOutputs) {
					return false
				}
				requiredFunctions["allowance"] = true
			case "decimals":
				expectedInputs := map[string]string{}
				expectedOutputs := set.NewSet(
					"u32",
				)

				if !v.validateFunctionInputsAndOutputs(inputs, outputs, expectedInputs, expectedOutputs) {
					return false
				}
				requiredFunctions["decimals"] = true
			case "name":
				expectedInputs := map[string]string{}
				expectedOutputs := set.NewSet(
					"String",
				)

				if !v.validateFunctionInputsAndOutputs(inputs, outputs, expectedInputs, expectedOutputs) {
					return false
				}
				requiredFunctions["name"] = true
			case "symbol":
				expectedInputs := map[string]string{}
				expectedOutputs := set.NewSet(
					"String",
				)

				if !v.validateFunctionInputsAndOutputs(inputs, outputs, expectedInputs, expectedOutputs) {
					return false
				}
				requiredFunctions["symbol"] = true
			case "approve":
				expectedInputs := map[string]string{
					"from":              "Address",
					"spender":           "Address",
					"amount":            "i128",
					"expiration_ledger": "u32",
				}
				expectedOutputs := set.NewSet[string]()

				if !v.validateFunctionInputsAndOutputs(inputs, outputs, expectedInputs, expectedOutputs) {
					return false
				}
				requiredFunctions["approve"] = true
			case "transfer":
				expectedInputs := map[string]string{
					"from":   "Address",
					"to":     "Address",
					"amount": "i128",
				}
				expectedOutputs := set.NewSet[string]()

				if !v.validateFunctionInputsAndOutputs(inputs, outputs, expectedInputs, expectedOutputs) {
					return false
				}
				requiredFunctions["transfer"] = true
			case "transfer_from":
				expectedInputs := map[string]string{
					"spender": "Address",
					"from":    "Address",
					"to":      "Address",
					"amount":  "i128",
				}
				expectedOutputs := set.NewSet[string]()

				if !v.validateFunctionInputsAndOutputs(inputs, outputs, expectedInputs, expectedOutputs) {
					return false
				}
				requiredFunctions["transfer_from"] = true
			case "burn":
				expectedInputs := map[string]string{
					"from":   "Address",
					"amount": "i128",
				}
				expectedOutputs := set.NewSet[string]()

				if !v.validateFunctionInputsAndOutputs(inputs, outputs, expectedInputs, expectedOutputs) {
					return false
				}
				requiredFunctions["burn"] = true
			case "burn_from":
				expectedInputs := map[string]string{
					"spender": "Address",
					"from":    "Address",
					"amount":  "i128",
				}
				expectedOutputs := set.NewSet[string]()

				if !v.validateFunctionInputsAndOutputs(inputs, outputs, expectedInputs, expectedOutputs) {
					return false
				}
				requiredFunctions["burn_from"] = true
			default:
				continue
			}
		}
	}
	for _, requiredFunction := range requiredFunctions {
		if !requiredFunction {
			return false
		}
	}
	return true
}

func (v *contractSpecValidator) validateFunctionInputsAndOutputs(inputs map[string]string, outputs set.Set[string], expectedInputs map[string]string, expectedOutputs set.Set[string]) bool {
	if len(inputs) != len(expectedInputs) {
		return false
	}

	for expectedInput, expectedInputType := range expectedInputs {
		if inputs[expectedInput] != expectedInputType {
			return false
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

func (v *contractSpecValidator) extractContractSpecFromWasmCode(wasmCode []byte) ([]xdr.ScSpecEntry, error) {
	// Compile WASM module (validates structure and won't panic)
	compiledModule, err := v.runtime.CompileModule(context.Background(), wasmCode)
	if err != nil {
		return nil, fmt.Errorf("compiling WASM module: %w", err)
	}
	defer func() {
		if err := compiledModule.Close(context.Background()); err != nil {
			log.Warnf("Failed to close compiled module: %v", err)
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

func (v *contractSpecValidator) getContractCodeLedgerKey(wasmHash xdr.Hash) (string, error) {
	// Create a LedgerKey for ContractCode
	var ledgerKey xdr.LedgerKey
	err := ledgerKey.SetContractCode(wasmHash)
	if err != nil {
		return "", fmt.Errorf("creating contract code ledger key: %w", err)
	}

	// Encode to base64 for RPC call
	keyBase64, err := ledgerKey.MarshalBinaryBase64()
	if err != nil {
		return "", fmt.Errorf("encoding ledger key to base64: %w", err)
	}

	return keyBase64, nil
}

// Helper function to get type name as string
func getTypeName(scType xdr.ScSpecType) string {
	switch scType {
	case xdr.ScSpecTypeScSpecTypeBool:
		return "bool"
	case xdr.ScSpecTypeScSpecTypeU32:
		return "u32"
	case xdr.ScSpecTypeScSpecTypeI32:
		return "i32"
	case xdr.ScSpecTypeScSpecTypeU64:
		return "u64"
	case xdr.ScSpecTypeScSpecTypeI64:
		return "i64"
	case xdr.ScSpecTypeScSpecTypeU128:
		return "u128"
	case xdr.ScSpecTypeScSpecTypeI128:
		return "i128"
	case xdr.ScSpecTypeScSpecTypeU256:
		return "u256"
	case xdr.ScSpecTypeScSpecTypeI256:
		return "i256"
	case xdr.ScSpecTypeScSpecTypeAddress:
		return "Address"
	case xdr.ScSpecTypeScSpecTypeString:
		return "String"
	case xdr.ScSpecTypeScSpecTypeBytes:
		return "Bytes"
	case xdr.ScSpecTypeScSpecTypeSymbol:
		return "Symbol"
	case xdr.ScSpecTypeScSpecTypeVec:
		return "Vec"
	case xdr.ScSpecTypeScSpecTypeMap:
		return "Map"
	case xdr.ScSpecTypeScSpecTypeOption:
		return "Option"
	case xdr.ScSpecTypeScSpecTypeResult:
		return "Result"
	case xdr.ScSpecTypeScSpecTypeTuple:
		return "Tuple"
	case xdr.ScSpecTypeScSpecTypeBytesN:
		return "BytesN"
	case xdr.ScSpecTypeScSpecTypeUdt:
		return "UDT"
	case xdr.ScSpecTypeScSpecTypeVoid:
		return "void"
	default:
		return "unknown"
	}
}
