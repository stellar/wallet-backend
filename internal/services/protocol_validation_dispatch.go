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

// DispatchClassification runs each validator against the supplied batch in
// the supplied order, enforcing first-match-wins. The caller is responsible
// for persisting protocol_wasms / protocol_contracts based on the returned
// matches; the dispatcher only wires the per-protocol seam.
//
// Spec extraction is performed once per candidate WASM. A candidate whose
// spec cannot be extracted (e.g. a hostile blob that fails wazero validation)
// is logged and skipped — it is still included in the returned set with no
// match so the caller can persist it with protocol_id = NULL.
//
// A validator that returns an error (or panics) aborts classification for the
// entire batch: the error propagates to the caller so its surrounding
// RunInTransaction rolls back. Validators may write to the shared dbTx during
// Validate (e.g. contract_tokens enrichment), so a failure cannot be treated
// as a best-effort no-match — that would commit the failed validator's partial
// writes. Any failure must roll back the whole transaction.
func DispatchClassification(
	ctx context.Context,
	dbTx pgx.Tx,
	extractor WasmSpecExtractor,
	validators []ProtocolValidator,
	bytecodesByHash map[types.HashBytea][]byte,
	contracts []data.ProtocolContracts,
	rpc RPCService,
	models *data.Models,
	knownByHash map[types.HashBytea]string,
	failureCounter *prometheus.CounterVec,
) (map[types.HashBytea]string, error) {
	if len(bytecodesByHash) == 0 && len(contracts) == 0 {
		return nil, nil
	}

	candidates := make([]WasmCandidate, 0, len(bytecodesByHash))
	for hash, bytecode := range bytecodesByHash {
		if extractor == nil {
			candidates = append(candidates, WasmCandidate{Hash: hash, Bytecode: bytecode})
			continue
		}
		specs, err := extractor.ExtractSpec(ctx, bytecode)
		if err != nil {
			log.Ctx(ctx).Warnf("validation dispatch: spec extraction failed for wasm %s: %v", hash, err)
			if failureCounter != nil {
				failureCounter.WithLabelValues("unknown", "spec_extraction_error").Inc()
			}
			candidates = append(candidates, WasmCandidate{Hash: hash, Bytecode: bytecode})
			continue
		}
		candidates = append(candidates, WasmCandidate{Hash: hash, Bytecode: bytecode, SpecEntries: specs})
	}

	annotated := annotateContracts(contracts, knownByHash)

	matches := make(map[types.HashBytea]string)
	for _, v := range validators {
		filtered := filterCandidates(candidates, matches)
		if len(filtered) == 0 && len(annotated) == 0 {
			break
		}
		input := ValidationInput{
			Candidates: filtered,
			Contracts:  annotated,
			RPC:        rpc,
			Models:     models,
		}
		result, err := v.Validate(ctx, dbTx, input)
		if err != nil {
			if failureCounter != nil {
				failureCounter.WithLabelValues(v.ProtocolID(), "validate_error").Inc()
			}
			return nil, fmt.Errorf("validator %s: %w", v.ProtocolID(), err)
		}
		for _, hash := range result.MatchedWasms {
			if _, alreadyClaimed := matches[hash]; alreadyClaimed {
				continue
			}
			matches[hash] = v.ProtocolID()
		}
	}
	return matches, nil
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
