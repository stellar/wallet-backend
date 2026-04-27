package services

import (
	"context"
	"fmt"
	"runtime/debug"

	"github.com/jackc/pgx/v5"
	"github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer/types"
)

// RawWasm is the input the dispatcher needs to assemble a WasmCandidate. It
// keeps the on-the-wire bytecode that the caller already has in hand (live
// ingest from the ledger meta, protocol-setup from the RPC fetch) without
// forcing the caller to think about wazero.
type RawWasm struct {
	Hash     types.HashBytea
	Bytecode []byte
}

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
// Each call to Validate is wrapped in a panic recovery so a buggy validator
// cannot bring down the entire ingest path. Errors from Validate are logged
// and treated as a no-match for that protocol — classification is
// best-effort across protocols, and the framework's invariant is "matches I
// see are sound" rather than "I always see all possible matches."
func DispatchClassification(
	ctx context.Context,
	dbTx pgx.Tx,
	extractor WasmSpecExtractor,
	validators []ProtocolValidator,
	rawWasms []RawWasm,
	contracts []data.ProtocolContracts,
	rpc RPCService,
	models *data.Models,
	knownByHash map[types.HashBytea]string,
) (map[types.HashBytea]string, error) {
	if len(rawWasms) == 0 && len(contracts) == 0 {
		return nil, nil
	}

	candidates := make([]WasmCandidate, 0, len(rawWasms))
	for _, w := range rawWasms {
		if extractor == nil {
			candidates = append(candidates, WasmCandidate{Hash: w.Hash, Bytecode: w.Bytecode})
			continue
		}
		specs, err := extractor.ExtractSpec(ctx, w.Bytecode)
		if err != nil {
			log.Ctx(ctx).Warnf("validation dispatch: spec extraction failed for wasm %s: %v", w.Hash, err)
			candidates = append(candidates, WasmCandidate{Hash: w.Hash, Bytecode: w.Bytecode})
			continue
		}
		candidates = append(candidates, WasmCandidate{Hash: w.Hash, Bytecode: w.Bytecode, SpecEntries: specs})
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
		result, err := safeValidate(ctx, v, dbTx, input)
		if err != nil {
			log.Ctx(ctx).Warnf("validation dispatch: protocol %s Validate returned error, treating as no-match: %v", v.ProtocolID(), err)
			continue
		}
		for _, hash := range result.MatchedWasms {
			if _, alreadyClaimed := matches[hash]; alreadyClaimed {
				continue
			}
			matches[hash] = v.ProtocolID()
		}
		annotated = updateAnnotations(annotated, matches)
	}
	return matches, nil
}

// safeValidate wraps a Validate call so a panic in a validator does not bring
// down the dispatch loop.
func safeValidate(ctx context.Context, v ProtocolValidator, dbTx pgx.Tx, input ValidationInput) (result ValidationResult, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in %s Validate: %v\n%s", v.ProtocolID(), r, debug.Stack())
		}
	}()
	result, validateErr := v.Validate(ctx, dbTx, input)
	if validateErr != nil {
		return result, fmt.Errorf("validator %s: %w", v.ProtocolID(), validateErr)
	}
	return result, nil
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

// updateAnnotations refreshes KnownProtocolID on the contract slice given the
// matches accumulated so far this batch. New annotations only — never
// overwrites an existing non-empty KnownProtocolID, since first-match-wins
// already prevented re-classification of an earlier-stamped wasm.
func updateAnnotations(contracts []ContractCandidate, matches map[types.HashBytea]string) []ContractCandidate {
	for i := range contracts {
		if contracts[i].KnownProtocolID != "" {
			continue
		}
		if pid, ok := matches[contracts[i].WasmHash]; ok {
			contracts[i].KnownProtocolID = pid
		}
	}
	return contracts
}

// ResolveKnownProtocols queries protocol_wasms for the supplied hashes and
// returns hash → protocol_id for any rows where protocol_id IS NOT NULL.
// Hashes already represented in `excludeHashes` (typically the in-batch
// candidates) are skipped.
//
// Lives on the framework side so each validator doesn't reimplement the SQL.
// Used by both protocol-setup (to annotate previously-classified wasms when
// contracts are revisited) and live ingest (to resolve hashes uploaded in
// earlier ledgers).
func ResolveKnownProtocols(
	ctx context.Context,
	dbTx pgx.Tx,
	contracts []data.ProtocolContracts,
	excludeHashes map[types.HashBytea]struct{},
) (map[types.HashBytea]string, error) {
	if len(contracts) == 0 {
		return nil, nil
	}
	seen := make(map[types.HashBytea]struct{}, len(contracts))
	hashes := make([][]byte, 0, len(contracts))
	for _, c := range contracts {
		if _, dup := seen[c.WasmHash]; dup {
			continue
		}
		if _, skip := excludeHashes[c.WasmHash]; skip {
			continue
		}
		seen[c.WasmHash] = struct{}{}
		val, err := c.WasmHash.Value()
		if err != nil {
			log.Ctx(ctx).Debugf("resolve known protocols: skipping invalid hash %q: %v", c.WasmHash, err)
			continue
		}
		hashes = append(hashes, val.([]byte))
	}
	if len(hashes) == 0 {
		return nil, nil
	}
	rows, err := dbTx.Query(ctx,
		`SELECT wasm_hash, protocol_id FROM protocol_wasms
		 WHERE wasm_hash = ANY($1) AND protocol_id IS NOT NULL`, hashes)
	if err != nil {
		return nil, fmt.Errorf("looking up known protocol_wasms classifications: %w", err)
	}
	defer rows.Close()

	out := make(map[types.HashBytea]string)
	for rows.Next() {
		var h types.HashBytea
		var pid string
		if scanErr := rows.Scan(&h, &pid); scanErr != nil {
			return nil, fmt.Errorf("scanning protocol_wasms row: %w", scanErr)
		}
		out[h] = pid
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating protocol_wasms rows: %w", err)
	}
	return out, nil
}
