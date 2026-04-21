package services

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/tetratelabs/wazero"
)

const (
	contractSpecV0SectionName = "contractspecv0"

	// maxWasmBytes caps the size of a WASM blob accepted for spec extraction.
	// Stellar-core's on-chain contract-code limit is smaller (currently ~64 KiB),
	// but bytecode fetched via RPC is attacker-controlled, and future protocol
	// upgrades may raise the on-chain ceiling. 2 MiB leaves headroom without
	// letting a crafted blob exhaust memory during wazero compilation.
	maxWasmBytes = 2 * 1024 * 1024

	// wasmCompileTimeout bounds the time wazero may spend compiling and
	// decoding a single WASM module. Pathologically crafted modules can make
	// wazero's validator run for a long time; this timeout keeps
	// protocol-setup responsive even when RPC returns hostile bytecode.
	wasmCompileTimeout = 10 * time.Second

	// maxSpecEntries caps the number of ScSpecEntry items decoded from a
	// single contractspecv0 section. Well-behaved contracts have far fewer;
	// the cap bounds the cost of the Unmarshal loop on malformed sections.
	maxSpecEntries = 10_000
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
// The caller must supply a context with a deadline appropriate for the workload;
// ExtractSpec additionally applies wasmCompileTimeout as an upper bound so a single
// hostile blob cannot stall the pipeline.
func (e *wasmSpecExtractor) ExtractSpec(ctx context.Context, wasmCode []byte) ([]xdr.ScSpecEntry, error) {
	if len(wasmCode) > maxWasmBytes {
		return nil, fmt.Errorf("wasm module too large: %d bytes (max %d)", len(wasmCode), maxWasmBytes)
	}

	compileCtx, cancel := context.WithTimeout(ctx, wasmCompileTimeout)
	defer cancel()

	compiledModule, err := e.runtime.CompileModule(compileCtx, wasmCode)
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
		if len(specs) >= maxSpecEntries {
			return nil, fmt.Errorf("contractspecv0 section has more than %d entries", maxSpecEntries)
		}
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
