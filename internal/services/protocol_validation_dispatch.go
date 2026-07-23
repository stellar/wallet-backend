package services

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer/types"
)

// ClassificationPlan is the RPC-free, ready-to-apply output of
// PrepareClassification: the final hash → protocol_id map for a batch (prior
// classifications overlaid with this batch's new matches) plus, for each
// validator that had anything to act on, the RPC-sourced enrichment it
// resolved via Prefetch. Building a plan opens no database transaction and,
// once built, requires no further RPC calls — ApplyClassificationPlan
// consumes it purely against dbTx. A plan is safe to reuse verbatim across
// retry attempts of the same ledger.
type ClassificationPlan struct {
	// Matches is the full hash -> protocol_id map: the knownByHash overlay
	// passed to PrepareClassification, overlaid with this batch's new
	// first-match-wins matches.
	Matches map[types.HashBytea]string

	perValidator []validatorPlan
}

// validatorPlan pairs one validator with the matched-hash set and RPC-sourced
// enrichment PrepareClassification resolved for it, ready for
// ApplyClassificationPlan to persist.
type validatorPlan struct {
	validator ProtocolValidator
	matched   map[types.HashBytea]struct{}
	contracts []ContractCandidate
	prefetch  any
}

// PrepareClassification runs pure signature matching for every candidate WASM
// against every validator in priority order (first-match-wins), then calls
// each relevant validator's Prefetch to resolve RPC-sourced enrichment.
// Priority is the order of the validators slice, which production callers
// build from GetAllValidatorIDs — lexicographic protocol-ID order. When two
// protocols' signatures overlap (a SEP-41-shaped interface embedded in a
// richer protocol, for example), the alphabetically earlier protocol ID
// claims the wasm, so a new protocol whose signature overlaps an existing
// one must account for its ID's sort rank. No
// database transaction is opened and no RPC call happens after this function
// returns — ApplyClassificationPlan finishes the job purely against dbTx.
//
// Spec extraction is performed once per candidate WASM. A candidate whose
// spec cannot be extracted (e.g. a hostile blob that fails wazero validation)
// is logged, counted, and dropped from the candidate set — no signature-based
// validator can act on a spec-less candidate. The caller still persists the
// underlying wasm with protocol_id = NULL because it is absent from
// plan.Matches.
//
// An error from a validator's Prefetch aborts the whole batch (the plan is
// discarded, matching DispatchClassification's previous fail-fast contract
// for validator-level errors); in practice today's validators never return a
// hard Prefetch error — per-contract RPC failures are absorbed internally
// and reflected as missing entries in the plan instead.
func PrepareClassification(
	ctx context.Context,
	extractor WasmSpecExtractor,
	validators []ProtocolValidator,
	bytecodesByHash map[types.HashBytea][]byte,
	contracts []data.ProtocolContracts,
	rpc RPCService,
	knownByHash map[types.HashBytea]string,
	failureCounter *prometheus.CounterVec,
) (*ClassificationPlan, error) {
	if extractor == nil {
		return nil, fmt.Errorf("dispatch: spec extractor required")
	}

	plan := &ClassificationPlan{Matches: make(map[types.HashBytea]string, len(knownByHash))}
	for hash, pid := range knownByHash {
		plan.Matches[hash] = pid
	}
	if len(bytecodesByHash) == 0 && len(contracts) == 0 {
		return plan, nil
	}

	candidates := make([]WasmCandidate, 0, len(bytecodesByHash))
	for hash, bytecode := range bytecodesByHash {
		specs, err := extractor.ExtractSpec(ctx, bytecode)
		if err != nil {
			log.Ctx(ctx).Warnf("classification prepare: spec extraction failed for wasm %s: %v", hash, err)
			if failureCounter != nil {
				failureCounter.WithLabelValues("unknown", "spec_extraction_error").Inc()
			}
			continue
		}
		candidates = append(candidates, WasmCandidate{Hash: hash, Bytecode: bytecode, SpecEntries: specs})
	}

	annotated := annotateContracts(contracts, knownByHash)

	for _, v := range validators {
		filtered := filterCandidates(candidates, plan.Matches)
		if len(filtered) == 0 && len(annotated) == 0 {
			break
		}

		matched := v.Match(filtered)
		for hash := range matched {
			if _, alreadyClaimed := plan.Matches[hash]; alreadyClaimed {
				continue
			}
			plan.Matches[hash] = v.ProtocolID()
		}

		prefetch, err := v.Prefetch(ctx, rpc, filtered, matched, annotated)
		if err != nil {
			if failureCounter != nil {
				// Reuses the "validate_error" label from before the
				// Match/Prefetch/Apply split so existing dashboards/alerts
				// on this counter keep working unchanged.
				failureCounter.WithLabelValues(v.ProtocolID(), "validate_error").Inc()
			}
			return nil, fmt.Errorf("validator %s: prefetch: %w", v.ProtocolID(), err)
		}
		plan.perValidator = append(plan.perValidator, validatorPlan{
			validator: v,
			matched:   matched,
			contracts: annotated,
			prefetch:  prefetch,
		})
	}
	return plan, nil
}

// ApplyClassificationPlan persists each validator's classification side
// effects (e.g. contract_tokens) inside dbTx, using only the enrichment data
// PrepareClassification already resolved via RPC. The dispatch boundary hands
// Apply no RPC handle — Prefetch is the sole RPC entry point and Apply's
// signature carries no RPCService — so the enforced contract is that a
// validator performs no network round-trip while dbTx (and the row locks it
// holds) is open. This is a boundary convention rather than a compiler-proven
// impossibility: a validator that retains its own RPC client on its receiver
// must still honor it and never call out during Apply.
func ApplyClassificationPlan(ctx context.Context, dbTx pgx.Tx, models *data.Models, plan *ClassificationPlan, failureCounter *prometheus.CounterVec) error {
	if plan == nil {
		return nil
	}
	for _, vp := range plan.perValidator {
		if err := vp.validator.Apply(ctx, dbTx, vp.matched, vp.contracts, vp.prefetch, models); err != nil {
			if failureCounter != nil {
				failureCounter.WithLabelValues(vp.validator.ProtocolID(), "validate_error").Inc()
			}
			return fmt.Errorf("validator %s: apply: %w", vp.validator.ProtocolID(), err)
		}
	}
	return nil
}

// filterCandidates returns the subset of candidates whose hash is not yet
// claimed in matches.
func filterCandidates(candidates []WasmCandidate, matches map[types.HashBytea]string) []WasmCandidate {
	if len(matches) == 0 {
		return candidates
	}
	out := candidates[:0:0]
	for _, c := range candidates {
		if _, claimed := matches[c.Hash]; claimed {
			continue
		}
		out = append(out, c)
	}
	return out
}

// annotateContracts builds ContractCandidate entries from the raw contract
// slice, stamping KnownProtocolID from the supplied lookup table for hashes
// already classified outside this batch.
func annotateContracts(contracts []data.ProtocolContracts, known map[types.HashBytea]string) []ContractCandidate {
	out := make([]ContractCandidate, 0, len(contracts))
	for _, c := range contracts {
		out = append(out, ContractCandidate{
			ContractID:      c.ContractID,
			WasmHash:        c.WasmHash,
			KnownProtocolID: known[c.WasmHash],
		})
	}
	return out
}
