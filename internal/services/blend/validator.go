// Package blend implements the BLEND v2 lending protocol's on-chain
// interface classification: matching candidate WASM signatures against the
// Blend Pool and Backstop contract interfaces and, for pools, seeding
// blend_pools with PoolConfig fetched via RPC at classification time.
package blend

import (
	"context"
	"encoding/hex"
	"fmt"
	"sort"

	"github.com/jackc/pgx/v5"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	blenddata "github.com/stellar/wallet-backend/internal/data/blend"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/services"
	"github.com/stellar/wallet-backend/internal/services/wasmspec"
)

// ProtocolID is the identifier stored in protocols.protocol_id and
// protocol_wasms.protocol_id for every Blend v2 contract, whether it
// implements the Pool or the Backstop interface. The two interfaces are
// distinguished internally (see role below) but share one protocol_id
// because downstream JOINs only need "is this a Blend contract".
const ProtocolID = "BLEND"

// role distinguishes which Blend v2 contract interface a wasm implements.
// Deliberately 1-indexed so the zero value (an uninitialized role, e.g. from
// a map miss handled incorrectly) never aliases a real role.
type role int

const (
	rolePool role = iota + 1
	roleBackstop
)

// blndTokenAddress returns the C-address of the BLND Stellar Asset Contract
// for the given network passphrase. Blend v2 (pool, backstop, and the BLND
// SAC) is deployed byte-identically on pubnet and testnet. On the standalone
// network (passphrase "Standalone Network ; February 2017", used by the
// integration test suite), BLND is the classic asset "BLND:<master
// G-address>" issued by the network's master account (keypair.Root of the
// passphrase), so its SAC address is a deterministic function of the
// passphrase and is pinned here as a constant. Any other passphrase
// (futurenet, a custom network, etc.) has no known BLND SAC, so callers get
// the empty string and must degrade gracefully — mirrored on the
// ContractMetadataService nil-degradation pattern used throughout this
// package.
func blndTokenAddress(networkPassphrase string) string {
	switch networkPassphrase {
	case network.PublicNetworkPassphrase:
		return "CD25MNVTZDL4Y3XBCPCJXGXATV5WUHHOWMYFF4YBEGU5FCPGMYTVG5JY"
	case network.TestNetworkPassphrase:
		return "CB22KRA3YZVCNCQI64JQ5WE7UY2VAV7WFLK6A2JN3HEX56T2EDAFO7QF"
	case "Standalone Network ; February 2017":
		return "CDYLJJT2VBKY55ZK57MTMKAVRCRPQMYB4YJ7JFFARMSJZ73I5CMCITSU"
	default:
		return ""
	}
}

// canonicalBackstopAddress returns the C-address of the one canonical Blend v2
// backstop contract for the given network passphrase. Blend v2 deploys a single
// backstop per network (the emitter drips BLND to exactly one backstop), so the
// address is a fixed per-network constant, mirroring blndTokenAddress.
//
// The backstop-derived tables (blend_backstop_positions, blend_backstop_pools,
// the backstop-source rows of blend_emissions, blend_backstop_claimed, and
// blend_pools.in_reward_zone) key their rows on pool and/or user with no
// backstop contract id anywhere in the key. Because Blend contracts are
// classified by WASM-interface match, any contract deployed from the backstop
// WASM would otherwise be tracked and could write pool/user-keyed rows that
// silently overwrite the real backstop's. Folding backstop-shaped state only
// from this address closes that impostor-collision hole. Any other passphrase
// (futurenet, a custom standalone network) has no known backstop and returns
// the empty string — the same nil-degradation contract as blndTokenAddress.
//
// Operational caveat: the emitter can swap the active backstop via a 31-day
// queued swap. If Blend ever executes one, these pinned addresses must be
// updated and the backstop-derived tables migrated to the new backstop's
// state, since backstop-shaped folds from any other contract are dropped.
func canonicalBackstopAddress(networkPassphrase string) string {
	switch networkPassphrase {
	case network.PublicNetworkPassphrase:
		return "CAQQR5SWBXKIGZKPBZDH3KM5GQ5GUTPKB7JAFCINLZBC5WXPJKRG3IM7"
	case network.TestNetworkPassphrase:
		return "CBDVWXT433PRVTUNM56C3JREF3HIZHRBA64NB2C3B2UNCKIS65ZYCLZA"
	default:
		return ""
	}
}

// poolRequiredFunctions defines the Blend v2 Pool contract's required
// functions. A contract must implement all of these with the exact
// signatures specified to be classified as a Blend pool.
var poolRequiredFunctions = []wasmspec.FunctionSpec{
	{
		Name:            "get_config",
		ExpectedInputs:  []wasmspec.InputSpec{},
		ExpectedOutputs: []string{"UDT"}, // PoolConfig
	},
	{
		Name:            "get_reserve",
		ExpectedInputs:  []wasmspec.InputSpec{{Name: "asset", TypeName: "Address"}},
		ExpectedOutputs: []string{"UDT"}, // Reserve
	},
	{
		Name:            "get_positions",
		ExpectedInputs:  []wasmspec.InputSpec{{Name: "address", TypeName: "Address"}},
		ExpectedOutputs: []string{"UDT"}, // Positions
	},
	{
		Name: "submit",
		ExpectedInputs: []wasmspec.InputSpec{
			{Name: "from", TypeName: "Address"},
			{Name: "spender", TypeName: "Address"},
			{Name: "to", TypeName: "Address"},
			{Name: "requests", TypeName: "Vec"}, // Vec<Request>
		},
		ExpectedOutputs: []string{"UDT"}, // Positions
	},
	{
		Name: "flash_loan",
		ExpectedInputs: []wasmspec.InputSpec{
			{Name: "from", TypeName: "Address"},
			{Name: "flash_loan", TypeName: "UDT"}, // FlashLoan
			{Name: "requests", TypeName: "Vec"},   // Vec<Request>
		},
		ExpectedOutputs: []string{"UDT"}, // Positions
	},
	{
		Name:            "bad_debt",
		ExpectedInputs:  []wasmspec.InputSpec{{Name: "user", TypeName: "Address"}},
		ExpectedOutputs: []string{},
	},
}

// backstopRequiredFunctions defines the Blend v2 Backstop contract's
// required functions. A contract must implement all of these with the exact
// signatures specified to be classified as the Blend backstop.
var backstopRequiredFunctions = []wasmspec.FunctionSpec{
	{
		Name: "deposit",
		ExpectedInputs: []wasmspec.InputSpec{
			{Name: "from", TypeName: "Address"},
			{Name: "pool_address", TypeName: "Address"},
			{Name: "amount", TypeName: "i128"},
		},
		ExpectedOutputs: []string{"i128"},
	},
	{
		Name: "queue_withdrawal",
		ExpectedInputs: []wasmspec.InputSpec{
			{Name: "from", TypeName: "Address"},
			{Name: "pool_address", TypeName: "Address"},
			{Name: "amount", TypeName: "i128"},
		},
		ExpectedOutputs: []string{"UDT"}, // Q4W
	},
	{
		Name: "dequeue_withdrawal",
		ExpectedInputs: []wasmspec.InputSpec{
			{Name: "from", TypeName: "Address"},
			{Name: "pool_address", TypeName: "Address"},
			{Name: "amount", TypeName: "i128"},
		},
		ExpectedOutputs: []string{},
	},
	{
		Name: "withdraw",
		ExpectedInputs: []wasmspec.InputSpec{
			{Name: "from", TypeName: "Address"},
			{Name: "pool_address", TypeName: "Address"},
			{Name: "amount", TypeName: "i128"},
		},
		ExpectedOutputs: []string{"i128"},
	},
	{
		Name: "claim",
		ExpectedInputs: []wasmspec.InputSpec{
			{Name: "from", TypeName: "Address"},
			{Name: "pool_addresses", TypeName: "Vec"}, // Vec<Address>
			{Name: "min_lp_tokens_out", TypeName: "i128"},
		},
		ExpectedOutputs: []string{"i128"},
	},
	{
		Name: "draw",
		ExpectedInputs: []wasmspec.InputSpec{
			{Name: "pool_address", TypeName: "Address"},
			{Name: "amount", TypeName: "i128"},
			{Name: "to", TypeName: "Address"},
		},
		ExpectedOutputs: []string{},
	},
	{
		Name: "donate",
		ExpectedInputs: []wasmspec.InputSpec{
			{Name: "from", TypeName: "Address"},
			{Name: "pool_address", TypeName: "Address"},
			{Name: "amount", TypeName: "i128"},
		},
		ExpectedOutputs: []string{},
	},
}

// matchPoolSpec is the pure Blend Pool signature check. Suitable for unit
// testing in isolation; production callers reach this via Validator.Validate.
func matchPoolSpec(contractSpec []xdr.ScSpecEntry) bool {
	return wasmspec.Match(contractSpec, poolRequiredFunctions)
}

// matchBackstopSpec is the pure Blend Backstop signature check. Suitable for
// unit testing in isolation; production callers reach this via
// Validator.Validate.
func matchBackstopSpec(contractSpec []xdr.ScSpecEntry) bool {
	return wasmspec.Match(contractSpec, backstopRequiredFunctions)
}

// Validator is Blend's implementation of services.ProtocolValidator. It
// signature-checks candidate WASMs against both the Pool and Backstop
// interfaces and, for any contract now (or already) classified as a Blend
// pool, fetches PoolConfig via RPC and seeds blend_pools inside the supplied
// dbTx. Backstop contracts get no such enrichment here — the backstop is a
// per-network singleton with no equivalent "config" getter, and its stateful
// tables are populated by the ledger-driven processor instead.
type Validator struct {
	// metadata is nil when no RPC is configured (e.g. the datastore-backed
	// protocol-migrate path). Pool enrichment then becomes a no-op — pool
	// signature matching is unaffected, and the processor backfills
	// blend_pools from ledger events regardless.
	metadata services.ContractMetadataService
}

var _ services.ProtocolValidator = (*Validator)(nil)

// NewValidator constructs a Blend validator with no metadata service
// configured. Suitable for tests that exercise only the signature-check path
// or that want to wire a custom metadata service in later.
func NewValidator() *Validator {
	return &Validator{}
}

// newValidator constructs a Blend validator from generic ProtocolDeps.
func newValidator(deps services.ProtocolDeps) *Validator {
	return &Validator{metadata: deps.ContractMetadataService}
}

// ProtocolID returns the identifier this validator classifies contracts as.
func (v *Validator) ProtocolID() string { return ProtocolID }

// Validate runs the Blend Pool and Backstop signature checks over each
// candidate WASM and, for every contract whose wasm is now (or was already)
// classified as a Blend pool, enriches blend_pools with PoolConfig fetched
// via RPC. See the services.ProtocolValidator godoc for the framework-level
// contract.
func (v *Validator) Validate(ctx context.Context, dbTx pgx.Tx, input services.ValidationInput) (services.ValidationResult, error) {
	matched := v.matchCandidates(input.Candidates)
	if len(matched) == 0 && !v.hasKnownClaims(input.Contracts) {
		return services.ValidationResult{}, nil
	}

	if input.Models != nil && input.Models.Blend.Pools != nil {
		poolContracts := v.collectPoolContracts(input.Contracts, matched)
		if len(poolContracts) > 0 {
			if err := v.enrichPools(ctx, dbTx, input.Models.Blend.Pools, poolContracts); err != nil {
				return services.ValidationResult{}, fmt.Errorf("blend enrichment: %w", err)
			}
		}
	}

	out := services.ValidationResult{MatchedWasms: make([]types.HashBytea, 0, len(matched))}
	for h := range matched {
		out.MatchedWasms = append(out.MatchedWasms, h)
	}
	sort.Slice(out.MatchedWasms, func(i, j int) bool {
		return out.MatchedWasms[i] < out.MatchedWasms[j]
	})
	return out, nil
}

// matchCandidates runs the Blend Pool and Backstop signature checks against
// each candidate's pre-extracted spec entries, recording which interface (if
// any) it matched.
func (v *Validator) matchCandidates(candidates []services.WasmCandidate) map[types.HashBytea]role {
	matched := map[types.HashBytea]role{}
	for _, cand := range candidates {
		if len(cand.SpecEntries) == 0 {
			continue
		}
		switch {
		case matchPoolSpec(cand.SpecEntries):
			matched[cand.Hash] = rolePool
		case matchBackstopSpec(cand.SpecEntries):
			matched[cand.Hash] = roleBackstop
		}
	}
	return matched
}

// hasKnownClaims reports whether any contract in the batch references a wasm
// already classified as Blend by an earlier ledger or protocol-setup run.
func (v *Validator) hasKnownClaims(contracts []services.ContractCandidate) bool {
	for _, ct := range contracts {
		if ct.KnownProtocolID == ProtocolID {
			return true
		}
	}
	return false
}

// collectPoolContracts returns the contracts that should be attempted for
// pool enrichment: those whose wasm was matched as a pool in this batch, plus
// those whose wasm was already classified as Blend by a prior run.
//
// The prior-run case exists because Blend pools are deployed via a factory
// that reuses one canonical pool wasm across every pool instance — a new
// pool's wasm hash is typically already classified by the time its contract
// instance shows up here, so it never appears in this batch's Candidates.
// The framework's classification table only records protocol_id ("BLEND"),
// not pool-vs-backstop, so a prior-run contract's role can't be recovered
// here; it is included optimistically and left to enrichPools/fetchPoolConfig
// to discover that a backstop contract has no get_config and skip it.
func (v *Validator) collectPoolContracts(contracts []services.ContractCandidate, matched map[types.HashBytea]role) []services.ContractCandidate {
	out := make([]services.ContractCandidate, 0, len(contracts))
	for _, ct := range contracts {
		if r, ok := matched[ct.WasmHash]; ok {
			if r == rolePool {
				out = append(out, ct)
			}
			continue
		}
		if ct.KnownProtocolID == ProtocolID {
			out = append(out, ct)
		}
	}
	return out
}

// enrichPools fetches PoolConfig via RPC for each candidate pool contract and
// upserts the resulting rows into blend_pools inside dbTx. Best-effort:
// per-contract fetch/decode failures are logged and skipped, never
// propagated — a backstop contract routed here (see collectPoolContracts)
// simply fails get_config and is silently excluded from the batch.
func (v *Validator) enrichPools(ctx context.Context, dbTx pgx.Tx, poolModel blenddata.PoolModelInterface, contracts []services.ContractCandidate) error {
	if v.metadata == nil {
		// No RPC available — blend_pools stays unenriched at classification
		// time. The ledger-driven processor fills PoolConfig fields from
		// subsequent events; there is no other fallback here.
		return nil
	}

	seen := make(map[string]struct{}, len(contracts))
	rows := make([]blenddata.Pool, 0, len(contracts))
	for _, ct := range contracts {
		addr, ok := decodeContractAddr(ct.ContractID)
		if !ok {
			log.Ctx(ctx).Debugf("blend: skipping pool contract with undecodable id %q", ct.ContractID)
			continue
		}
		if _, dup := seen[addr]; dup {
			continue
		}
		seen[addr] = struct{}{}

		row, err := v.fetchPoolConfig(ctx, addr)
		if err != nil {
			log.Ctx(ctx).Debugf("blend: get_config unavailable for %s, skipping pool enrichment: %v", addr, err)
			continue
		}
		rows = append(rows, row)
	}
	if len(rows) == 0 {
		return nil
	}

	if err := poolModel.BatchUpsert(ctx, dbTx, rows); err != nil {
		return fmt.Errorf("upserting blend pools: %w", err)
	}
	log.Ctx(ctx).Debugf("blend validator: enriched %d blend_pools rows", len(rows))
	return nil
}

// fetchPoolConfig calls get_config on the pool at poolAddr via RPC simulation
// and decodes the returned PoolConfig map into a blend_pools row. Name is
// left nil — there is no name()/get_name() getter on the pool interface; the
// processor fills it in from instance storage. Individual fields that fail
// to decode (unexpected shape, missing key) are left nil rather than failing
// the whole row, consistent with BatchUpsert's COALESCE semantics.
func (v *Validator) fetchPoolConfig(ctx context.Context, poolAddr string) (blenddata.Pool, error) {
	val, err := v.metadata.FetchSingleField(ctx, poolAddr, "get_config")
	if err != nil {
		return blenddata.Pool{}, fmt.Errorf("fetching get_config: %w", err)
	}
	m, ok := val.GetMap()
	if !ok || m == nil {
		return blenddata.Pool{}, fmt.Errorf("get_config: expected map, got %v", val.Type)
	}

	// LastModifiedLedger is deliberately left at its zero value: Validate has
	// no ledger sequence available (ValidationInput carries none). blend_pools
	// overwrites this column unconditionally on every upsert, so it must not
	// regress a real ledger number written later by the ledger-driven
	// processor — 0 is a safe placeholder that no legitimate ledger number
	// exceeds. The true value awaits the processor's first ledger-driven
	// update; this call is a best-effort initial seed only.
	row := blenddata.Pool{PoolContractID: types.AddressBytea(poolAddr)}

	if fv, ok := mapGet(m, "oracle"); ok {
		if addr, ok := addrString(fv); ok {
			row.OracleContractID = types.AddressBytea(addr)
		}
	}
	if fv, ok := mapGet(m, "bstop_rate"); ok {
		if u, ok := u32Val(fv); ok {
			rate := int32(u)
			row.BackstopRate = &rate
		}
	}
	if fv, ok := mapGet(m, "status"); ok {
		if u, ok := u32Val(fv); ok {
			status := int32(u)
			row.Status = &status
		}
	}
	if fv, ok := mapGet(m, "max_positions"); ok {
		if u, ok := u32Val(fv); ok {
			maxPositions := int32(u)
			row.MaxPositions = &maxPositions
		}
	}
	if fv, ok := mapGet(m, "min_collateral"); ok {
		if s, ok := i128String(fv); ok {
			row.MinCollateral = &s
		}
	}
	return row, nil
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
