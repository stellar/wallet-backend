package services

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/tetratelabs/wazero"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer/types"
)

// ProtocolValidator is the per-protocol seam the data-migration framework
// calls during classification. The framework hands the validator a batch of
// candidate WASMs (with parsed spec entries) plus the contracts referencing
// those WASMs, and the validator returns the wasm hashes it claims. A
// validator is free to do anything else it needs inside the supplied dbTx —
// fetch on-chain metadata, write protocol-specific side tables, anything —
// and the framework treats those side effects as invisible. Only the returned
// matches drive generic persistence into protocol_wasms.protocol_id.
//
// First-match-wins ordering across protocols is enforced by the dispatcher:
// by the time Validate is called for a given protocol, candidates already
// claimed by a higher-priority protocol have been filtered out.
type ProtocolValidator interface {
	ProtocolID() string
	Validate(ctx context.Context, dbTx pgx.Tx, input ValidationInput) (ValidationResult, error)
}

// ValidationInput is the batch handed to a protocol's Validate call.
type ValidationInput struct {
	// Candidates are WASMs awaiting classification in this batch. Each entry
	// carries the raw bytecode and pre-extracted ScSpecEntry items.
	Candidates []WasmCandidate
	// Contracts references both in-batch candidates and previously-classified
	// WASMs. KnownProtocolID is set for hashes already classified by an
	// earlier ledger or protocol-setup run; the empty string means the wasm
	// has not been classified yet (or was classified to no match).
	Contracts []ContractCandidate
	// RPC is available for validators that need live network reads (e.g.
	// fetching token metadata via simulation). May be nil; validators must
	// degrade gracefully.
	RPC RPCService
	// Models provides the full data layer. Validators should write only to
	// protocol-specific tables here; the framework persists the generic
	// protocol_wasms / protocol_contracts mapping based on the returned
	// matches.
	Models *data.Models
}

// WasmCandidate represents one WASM awaiting classification. Bytecode and
// SpecEntries are pre-populated by the dispatcher.
type WasmCandidate struct {
	Hash        types.HashBytea
	Bytecode    []byte
	SpecEntries []xdr.ScSpecEntry
}

// ContractCandidate represents one contract instance whose wasm hash was seen
// in this batch (either via a fresh upload or a deploy/upgrade against an
// older wasm). KnownProtocolID is the empty string until the wasm has been
// classified.
type ContractCandidate struct {
	ContractID      types.HashBytea
	WasmHash        types.HashBytea
	KnownProtocolID string
}

// ValidationResult is what a protocol returns from Validate. MatchedWasms
// drives the framework's stamp of protocol_wasms.protocol_id. Contract claims
// are an internal detail of the validator's own enrichment path — the
// framework persists protocol_contracts unconditionally as a contract→wasm
// map and the wasm's protocol_id (set via this match) is what downstream
// JOINs use to resolve the protocol.
type ValidationResult struct {
	MatchedWasms []types.HashBytea
}

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

	// wasmCloseTimeout bounds the time wazero may spend releasing a compiled
	// module. It is applied to a background-derived ctx so caller
	// cancellation during spec decoding cannot abort resource release mid-
	// flight and leak the underlying compiler state.
	wasmCloseTimeout = 5 * time.Second
)

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
	// Short-circuit on caller cancellation. wazero.CompileModule may return
	// before observing ctx on small, well-formed inputs, so we check here to
	// guarantee the caller's cancellation is honored regardless.
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("extracting wasm spec: %w", err)
	}

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
		// Detach from the caller's ctx: if the caller cancels while we are
		// decoding spec entries, Close should still run to release wazero's
		// compiled state rather than returning early with a ctx error.
		closeCtx, cancelClose := context.WithTimeout(context.Background(), wasmCloseTimeout)
		defer cancelClose()
		if closeErr := compiledModule.Close(closeCtx); closeErr != nil {
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
