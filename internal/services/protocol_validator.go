package services

import (
	"bytes"
	"context"
	"fmt"

	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/tetratelabs/wazero"
)

// ProtocolValidator is the contract between protocol-setup and individual protocol validators.
// Each validator knows how to identify whether a WASM contract implements a specific protocol.
type ProtocolValidator interface {
	ProtocolID() string
	Validate(specEntries []xdr.ScSpecEntry) bool
}

// WasmSpecExtractor extracts ScSpecEntry from WASM bytecode.
type WasmSpecExtractor interface {
	ExtractSpec(ctx context.Context, wasmCode []byte) ([]xdr.ScSpecEntry, error)
	Close(ctx context.Context) error
}

// wasmSpecExtractor is the concrete implementation using wazero.
type wasmSpecExtractor struct {
	runtime wazero.Runtime
}

// NewWasmSpecExtractor creates a new WasmSpecExtractor with a configured wazero runtime.
func NewWasmSpecExtractor() WasmSpecExtractor {
	config := wazero.NewRuntimeConfig().WithCustomSections(true)
	runtime := wazero.NewRuntimeWithConfig(context.Background(), config)
	return &wasmSpecExtractor{runtime: runtime}
}

// ExtractSpec compiles the WASM module and extracts the contractspecv0 custom section.
func (e *wasmSpecExtractor) ExtractSpec(ctx context.Context, wasmCode []byte) ([]xdr.ScSpecEntry, error) {
	compiledModule, err := e.runtime.CompileModule(ctx, wasmCode)
	if err != nil {
		return nil, fmt.Errorf("compiling WASM module: %w", err)
	}
	defer func() {
		if closeErr := compiledModule.Close(ctx); closeErr != nil {
			log.Warnf("Failed to close compiled module: %v", closeErr)
		}
	}()

	customSections := compiledModule.CustomSections()

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

// Close shuts down the wazero runtime and releases associated resources.
func (e *wasmSpecExtractor) Close(ctx context.Context) error {
	if err := e.runtime.Close(ctx); err != nil {
		return fmt.Errorf("closing wasm spec extractor: %w", err)
	}
	return nil
}
