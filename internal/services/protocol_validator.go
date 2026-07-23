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
// calls during classification. Classification is split into three phases so
// the framework can prefetch RPC-sourced enrichment before opening any
// database transaction, then apply pure DB writes inside it:
//
//  1. Match: pure signature check, no RPC, no DB.
//  2. Prefetch: resolves on-chain enrichment (e.g. SEP-41 token
//     metadata) via RPC. Called before any transaction opens.
//  3. Apply: persists the protocol's side effects (e.g.
//     contract_tokens rows) inside the caller's dbTx, using only what Prefetch
//     already resolved. No RPC handle reaches this phase.
//
// First-match-wins ordering across protocols is enforced by the dispatcher:
// by the time Match is called for a given protocol, candidates already
// claimed by a higher-priority protocol have been filtered out.
type ProtocolValidator interface {
	ProtocolID() string

	// Match runs the protocol's signature check against each candidate's
	// pre-extracted spec entries and returns the hashes it claims. Pure — no
	// RPC, no DB access — safe to call before any transaction or network
	// round-trip.
	Match(candidates []WasmCandidate) map[types.HashBytea]struct{}

	// Prefetch resolves RPC-sourced enrichment for contracts whose wasm is
	// newly matched (per matched) or already classified by an earlier run
	// (per each ContractCandidate.KnownProtocolID). candidates is the same
	// filtered slice handed to Match, in case a validator needs to
	// re-derive per-candidate detail Match's uniform return type can't
	// carry (e.g. a protocol-specific contract role). Called before any
	// database transaction opens; rpc may be nil (e.g. offline
	// protocol-migrate paths without RPC configured), in which case
	// implementations must degrade to an empty/zero-value plan rather than
	// erroring. Per-contract fetch failures are absorbed here (logged,
	// omitted from the returned plan) — mirroring today's best-effort
	// semantics, Prefetch should not fail the batch.
	//
	// The returned plan is opaque to the framework; it is passed back
	// verbatim to Apply.
	Prefetch(ctx context.Context, rpc RPCService, candidates []WasmCandidate, matched map[types.HashBytea]struct{}, contracts []ContractCandidate) (plan any, err error)

	// Apply persists this batch's classification side effects (e.g.
	// contract_tokens rows) inside dbTx, using only the plan Prefetch already
	// resolved. The dispatch boundary passes Apply no RPC handle — Prefetch is
	// the sole RPC entry point — so implementations must not perform any network
	// round-trip here (including via an RPC client the validator retains on its
	// own receiver) while dbTx's row locks are held; a validator whose Prefetch
	// could not reach RPC already reflects that in its plan (e.g. an empty
	// enrichment map), not as an Apply-time error.
	Apply(ctx context.Context, dbTx pgx.Tx, matched map[types.HashBytea]struct{}, contracts []ContractCandidate, plan any, models *data.Models) error
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
