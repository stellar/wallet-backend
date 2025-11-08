package services

import (
	"bytes"
	"context"
	"fmt"

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
}

type contractSpecValidator struct {
	rpcService RPCService
}

func NewContractSpecValidator(rpcService RPCService) ContractSpecValidator {
	return &contractSpecValidator{
		rpcService: rpcService,
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

		log.Ctx(ctx).Infof("Validating WASM batch %d-%d of %d", i+1, end, len(ledgerKeys))

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
			isSep41 := v.isContractCodeSEP41(wasmCode)
			if isSep41 {
				contractTypesByWasmHash[wasmHash] = types.ContractTypeSEP41
			} else {
				contractTypesByWasmHash[wasmHash] = types.ContractTypeCustom
			}
		}
	}
	return contractTypesByWasmHash, nil
}

func (v *contractSpecValidator) isContractCodeSEP41(wasmCode []byte) bool {
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

	contractSpec, err := v.extractContractSpecFromWasmCode(wasmCode)
	if err != nil {
		// Log warning but don't fail - mark as Custom
		log.Warnf("Failed to extract contract spec from WASM: %v", err)
		return false
	}

	for _, spec := range contractSpec {
		if spec.Kind == xdr.ScSpecEntryKindScSpecEntryFunctionV0 && spec.FunctionV0 != nil {
			funcName := string(spec.FunctionV0.Name)
			if _, exists := requiredFunctions[funcName]; exists {
				requiredFunctions[funcName] = true
			}
		}
	}

	for _, exists := range requiredFunctions {
		if !exists {
			return false
		}
	}
	return true
}

func (v *contractSpecValidator) extractContractSpecFromWasmCode(wasmCode []byte) ([]xdr.ScSpecEntry, error) {
	// Create wazero runtime with custom sections enabled
	ctx := context.Background()
	config := wazero.NewRuntimeConfig().WithCustomSections(true)
	runtime := wazero.NewRuntimeWithConfig(ctx, config)
	defer func() {
		if err := runtime.Close(ctx); err != nil {
			log.Warnf("Failed to close wazero runtime: %v", err)
		}
	}()

	// Compile WASM module (validates structure and won't panic)
	compiledModule, err := runtime.CompileModule(ctx, wasmCode)
	if err != nil {
		return nil, fmt.Errorf("compiling WASM module: %w", err)
	}
	defer func() {
		if err := compiledModule.Close(ctx); err != nil {
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
