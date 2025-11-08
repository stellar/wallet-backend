package services

import (
	"bytes"
	"context"
	"fmt"

	"github.com/akupila/go-wasm"
	"github.com/stellar/go/xdr"

	"github.com/stellar/wallet-backend/internal/indexer/types"
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

	entries, err := v.rpcService.GetLedgerEntries(ledgerKeys)
	if err != nil {
		return nil, fmt.Errorf("getting ledger entries: %w", err)
	}

	contractTypesByWasmHash := make(map[xdr.Hash]types.ContractType)
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
		isSep41, err := v.isContractCodeSEP41(wasmCode)
		if err != nil {
			return nil, fmt.Errorf("checking if contract code is SEP41: %w", err)
		}
		if isSep41 {
			contractTypesByWasmHash[wasmHash] = types.ContractTypeSEP41
		}
	}
	return contractTypesByWasmHash, nil
}

func (v *contractSpecValidator) isContractCodeSEP41(wasmCode []byte) (bool, error) {
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
		return false, fmt.Errorf("extracting contract spec from WASM: %w", err)
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
			return false, nil
		}
	}
	return true, nil
}

func (v *contractSpecValidator) extractContractSpecFromWasmCode(wasmCode []byte) ([]xdr.ScSpecEntry, error) {
	// Parse WASM module
	module, err := wasm.Parse(bytes.NewReader(wasmCode))
	if err != nil {
		return nil, fmt.Errorf("parsing WASM: %w", err)
	}

	// Find contractspecv0 custom section
	var specBytes []byte
	for _, s := range module.Sections {
		switch section := s.(type) {
		case *wasm.SectionCustom:
			if section.SectionName == "contractspecv0" {
				specBytes = section.Payload
				break
			}
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
