package sep41

import (
	"context"
	"encoding/hex"
	"fmt"

	"github.com/alitto/pond/v2"
	"github.com/jackc/pgx/v5"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/services"
	"github.com/stellar/wallet-backend/internal/services/wasmspec"
)

const ProtocolID = "SEP41"

// contractTokenType is the lower-case value written to contract_tokens.type
// for SEP-41 rows. It is intentionally distinct from ProtocolID, which is the
// upper-case identifier stored in protocols.protocol_id and
// protocol_wasms.protocol_id. Pre-existing rows in contract_tokens use
// lower-case discriminators ("sac", "sep41", "unknown"), so writes from the
// SEP-41 validator must match that convention.
const contractTokenType = "sep41"

// Validator is SEP-41's implementation of services.ProtocolValidator. It
// satisfies the framework's per-protocol seam: signature-check candidate
// WASMs against the SEP-41 token interface and, for any contract whose wasm
// is now (or was already) classified as SEP-41, fetch on-chain
// name/symbol/decimals via RPC (Prefetch) and write them to contract_tokens
// (Apply).
//
// Contract write strategy: contract_tokens rows are inserted with
// deterministic IDs and ON CONFLICT DO NOTHING; metadata is then upserted via
// BatchUpdateMetadata. The flow is idempotent across retries within the same
// transaction and across re-runs (e.g. ledger transaction rolling back and
// replaying).
type Validator struct {
	fetcher *metadataFetcher
	pool    pond.Pool
}

var _ services.ProtocolValidator = (*Validator)(nil)

// NewValidator constructs a SEP-41 validator with no metadata fetcher
// configured. Suitable for tests that exercise only the signature-check path
// or that want to wire a custom fetcher in later.
func NewValidator() *Validator {
	return &Validator{}
}

// newValidator constructs a SEP-41 validator from generic ProtocolDeps. If
// deps.ContractMetadataService is nil (e.g. offline migration paths), metadata
// enrichment becomes a no-op — the validator still claims matched wasms and
// inserts contract_tokens rows with default values.
func newValidator(deps services.ProtocolDeps) *Validator {
	v := &Validator{}
	if deps.ContractMetadataService != nil {
		// Owned worker pool — capped here to keep the public RPC endpoint
		// happy under bursts.
		v.pool = pond.NewPool(0)
		v.fetcher = newMetadataFetcher(deps.ContractMetadataService, v.pool)
	}
	return v
}

func (v *Validator) ProtocolID() string { return ProtocolID }

// Match runs the SEP-41 signature check over each candidate WASM's
// pre-extracted spec entries. Pure — no RPC, no DB access.
func (v *Validator) Match(candidates []services.WasmCandidate) map[types.HashBytea]struct{} {
	return v.matchCandidates(candidates)
}

// sep41Prefetch is the RPC-sourced enrichment Prefetch resolves: token
// metadata keyed by C-address, ready for Apply to write without any further
// network access.
type sep41Prefetch struct {
	metaByAddr map[string]*data.Contract
}

// Prefetch resolves on-chain name/symbol/decimals for every contract whose
// wasm is now (or was already) classified as SEP-41. See the
// services.ProtocolValidator godoc for the framework-level contract.
func (v *Validator) Prefetch(ctx context.Context, _ services.RPCService, _ []services.WasmCandidate, matched map[types.HashBytea]struct{}, contracts []services.ContractCandidate) (any, error) {
	contractsForUs := v.collectClaimedContracts(contracts, matched)
	if len(contractsForUs) == 0 || v.fetcher == nil {
		return sep41Prefetch{}, nil
	}

	addrs := v.decodeClaimedAddrs(ctx, contractsForUs)
	if len(addrs) == 0 {
		return sep41Prefetch{}, nil
	}

	metaByAddr, err := v.fetcher.FetchMetadata(ctx, addrs)
	if err != nil {
		// Fetch-level errors are best-effort — Apply still inserts default
		// rows for contractsForUs; metadata is filled on a later
		// classification pass once RPC is reachable.
		log.Ctx(ctx).Warnf("sep41 prefetch: metadata fetch returned error, continuing with defaults: %v", err)
		return sep41Prefetch{}, nil
	}
	return sep41Prefetch{metaByAddr: metaByAddr}, nil
}

// Apply persists this batch's contract_tokens rows inside dbTx: default rows
// for every claimed contract (idempotent, ON CONFLICT DO NOTHING) followed by
// a metadata upsert for whatever Prefetch resolved via RPC. No RPC handle is
// available here.
func (v *Validator) Apply(ctx context.Context, dbTx pgx.Tx, matched map[types.HashBytea]struct{}, contracts []services.ContractCandidate, plan any, models *data.Models) error {
	contractsForUs := v.collectClaimedContracts(contracts, matched)
	if len(contractsForUs) == 0 {
		return nil
	}
	prefetch, _ := plan.(sep41Prefetch)
	if err := v.applyContractTokens(ctx, dbTx, models, contractsForUs, prefetch); err != nil {
		// A DB-level error inside the caller's tx still propagates, but we
		// wrap it so callers can distinguish enrichment failures from
		// matching failures when surfacing logs.
		return fmt.Errorf("sep41 enrichment: %w", err)
	}
	return nil
}

// Close releases any resources owned by this validator (worker pool).
func (v *Validator) Close() {
	if v == nil {
		return
	}
	if v.pool != nil {
		v.pool.StopAndWait()
	}
}

// matchCandidates runs the SEP-41 signature check against each candidate's
// pre-extracted spec entries.
func (v *Validator) matchCandidates(candidates []services.WasmCandidate) map[types.HashBytea]struct{} {
	matched := map[types.HashBytea]struct{}{}
	for _, cand := range candidates {
		if len(cand.SpecEntries) == 0 {
			continue
		}
		if matchSEP41Spec(cand.SpecEntries) {
			matched[cand.Hash] = struct{}{}
		}
	}
	return matched
}

// collectClaimedContracts returns the contracts whose wasm hash is matched in
// this batch or already classified as SEP-41 from a prior run.
func (v *Validator) collectClaimedContracts(contracts []services.ContractCandidate, matched map[types.HashBytea]struct{}) []services.ContractCandidate {
	out := make([]services.ContractCandidate, 0, len(contracts))
	for _, ct := range contracts {
		if _, ok := matched[ct.WasmHash]; ok {
			out = append(out, ct)
			continue
		}
		if ct.KnownProtocolID == ProtocolID {
			out = append(out, ct)
		}
	}
	return out
}

// decodeClaimedAddrs decodes each claimed contract's hex ID into its
// C-address form, deduplicating and skipping any that fail to decode. Shared
// by Prefetch (which needs the address list to fetch metadata) and Apply
// (which needs it again to insert default rows), so no per-contract decode
// state has to be smuggled across the RPC boundary.
func (v *Validator) decodeClaimedAddrs(ctx context.Context, contracts []services.ContractCandidate) []string {
	seen := make(map[string]struct{}, len(contracts))
	addrs := make([]string, 0, len(contracts))
	for _, ct := range contracts {
		addr, ok := decodeContractAddr(ct.ContractID)
		if !ok {
			log.Ctx(ctx).Debugf("sep41: skipping contract with undecodable ID %q", ct.ContractID)
			continue
		}
		if _, dup := seen[addr]; dup {
			continue
		}
		seen[addr] = struct{}{}
		addrs = append(addrs, addr)
	}
	return addrs
}

// applyContractTokens writes contract_tokens rows inside dbTx: default rows
// for every claimed contract, then a metadata upsert for whatever Prefetch
// resolved via RPC (prefetch.metaByAddr may be empty when RPC was
// unavailable or every fetch failed, in which case the defaults stand).
func (v *Validator) applyContractTokens(ctx context.Context, dbTx pgx.Tx, models *data.Models, contracts []services.ContractCandidate, prefetch sep41Prefetch) error {
	if models == nil || models.Contract == nil {
		return nil
	}
	addrs := v.decodeClaimedAddrs(ctx, contracts)
	if len(addrs) == 0 {
		return nil
	}

	// Insert default rows for contracts we haven't seen yet so subsequent
	// BatchUpdateMetadata calls have rows to land on. Existing rows are
	// idempotent (ON CONFLICT DO NOTHING).
	defaults := make([]*data.Contract, 0, len(addrs))
	for _, addr := range addrs {
		defaults = append(defaults, &data.Contract{
			ID:         data.DeterministicContractID(addr),
			ContractID: addr,
			Type:       contractTokenType,
		})
	}
	if err := models.Contract.BatchInsert(ctx, dbTx, defaults); err != nil {
		return fmt.Errorf("inserting default contract_tokens: %w", err)
	}

	if len(prefetch.metaByAddr) == 0 {
		return nil
	}

	updates := make([]*data.Contract, 0, len(prefetch.metaByAddr))
	for _, contract := range prefetch.metaByAddr {
		// Force the row's type to the lower-case SEP-41 discriminator so the
		// source of truth stays in protocols/protocol_wasms — defensive
		// against future fetcher refactors that might omit the type field.
		contract.Type = contractTokenType
		updates = append(updates, contract)
	}
	if err := models.Contract.BatchUpdateMetadata(ctx, dbTx, updates); err != nil {
		return fmt.Errorf("updating contract_tokens metadata: %w", err)
	}
	log.Ctx(ctx).Debugf("sep41 validator: enriched %d contract_tokens rows", len(updates))
	return nil
}

// decodeContractAddr converts a hex-encoded HashBytea (32 bytes) into the
// strkey C-address representation. Returns false on any decode error.
func decodeContractAddr(hash types.HashBytea) (string, bool) {
	raw, err := hex.DecodeString(string(hash))
	if err != nil {
		return "", false
	}
	addr, err := strkey.Encode(strkey.VersionByteContract, raw)
	if err != nil {
		return "", false
	}
	return addr, true
}

// requiredFunctions defines all required functions for SEP-41 token standard
// compliance. A contract must implement all of these functions with the exact
// signatures specified.
var requiredFunctions = []wasmspec.FunctionSpec{
	{
		Name:            "balance",
		ExpectedInputs:  []wasmspec.InputSpec{{Name: "id", TypeName: "Address"}},
		ExpectedOutputs: []string{"i128"},
	},
	{
		Name:            "allowance",
		ExpectedInputs:  []wasmspec.InputSpec{{Name: "from", TypeName: "Address"}, {Name: "spender", TypeName: "Address"}},
		ExpectedOutputs: []string{"i128"},
	},
	{
		Name:            "decimals",
		ExpectedInputs:  []wasmspec.InputSpec{},
		ExpectedOutputs: []string{"u32"},
	},
	{
		Name:            "name",
		ExpectedInputs:  []wasmspec.InputSpec{},
		ExpectedOutputs: []string{"String"},
	},
	{
		Name:            "symbol",
		ExpectedInputs:  []wasmspec.InputSpec{},
		ExpectedOutputs: []string{"String"},
	},
	{
		Name: "approve",
		ExpectedInputs: []wasmspec.InputSpec{
			{Name: "from", TypeName: "Address"},
			{Name: "spender", TypeName: "Address"},
			{Name: "amount", TypeName: "i128"},
			{Name: "expiration_ledger", TypeName: "u32"},
		},
		ExpectedOutputs: []string{},
	},
	{
		Name: "transfer",
		ExpectedInputs: []wasmspec.InputSpec{
			{Name: "from", TypeName: "Address"},
			{Name: "to", TypeName: "Address"},
			{Name: "amount", TypeName: "i128"},
		},
	},
	// CAP-67 variant: (from: Address, to_muxed: MuxedAddress, amount: i128) -> ()
	{
		Name: "transfer",
		ExpectedInputs: []wasmspec.InputSpec{
			{Name: "from", TypeName: "Address"},
			{Name: "to_muxed", TypeName: "MuxedAddress"},
			{Name: "amount", TypeName: "i128"},
		},
	},
	{
		Name: "transfer_from",
		ExpectedInputs: []wasmspec.InputSpec{
			{Name: "spender", TypeName: "Address"},
			{Name: "from", TypeName: "Address"},
			{Name: "to", TypeName: "Address"},
			{Name: "amount", TypeName: "i128"},
		},
		ExpectedOutputs: []string{},
	},
	{
		Name: "burn",
		ExpectedInputs: []wasmspec.InputSpec{
			{Name: "from", TypeName: "Address"},
			{Name: "amount", TypeName: "i128"},
		},
		ExpectedOutputs: []string{},
	},
	{
		Name: "burn_from",
		ExpectedInputs: []wasmspec.InputSpec{
			{Name: "spender", TypeName: "Address"},
			{Name: "from", TypeName: "Address"},
			{Name: "amount", TypeName: "i128"},
		},
		ExpectedOutputs: []string{},
	},
}

// matchSEP41Spec is the pure SEP-41 signature check. Returns true iff the
// supplied contract spec implements every required SEP-41 function with an
// exact-match signature. Suitable for unit testing in isolation; production
// callers reach this via Validator.Validate.
func matchSEP41Spec(contractSpec []xdr.ScSpecEntry) bool {
	return wasmspec.Match(contractSpec, requiredFunctions)
}
