package sep41

import (
	"context"
	"encoding/hex"
	"fmt"
	"sort"

	"github.com/alitto/pond/v2"
	set "github.com/deckarep/golang-set/v2"
	"github.com/jackc/pgx/v5"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/services"
)

const ProtocolID = "SEP41"

// Validator is SEP-41's implementation of services.ProtocolValidator. It
// satisfies the framework's per-protocol seam: signature-check candidate
// WASMs against the SEP-41 token interface and, for any contract whose wasm
// is now (or was already) classified as SEP-41, fetch on-chain
// name/symbol/decimals via RPC and write them to contract_tokens inside the
// supplied dbTx.
//
// Contract write strategy: contract_tokens rows are inserted with
// deterministic IDs and ON CONFLICT DO NOTHING; metadata is then upserted via
// BatchUpdateMetadata. The flow is idempotent across retries within the same
// transaction and across re-runs (e.g. ledger transaction rolling back and
// replaying).
type Validator struct {
	fetcher  *metadataFetcher
	pool     pond.Pool
	ownsPool bool
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
		v.ownsPool = true
		v.fetcher = newMetadataFetcher(deps.ContractMetadataService, v.pool)
	}
	return v
}

func (v *Validator) ProtocolID() string { return ProtocolID }

// Validate runs the SEP-41 signature check over each candidate WASM and, for
// every contract whose wasm is now (or was already) classified as SEP-41,
// enriches contract_tokens with metadata fetched via RPC. See the
// services.ProtocolValidator godoc for the framework-level contract.
func (v *Validator) Validate(ctx context.Context, dbTx pgx.Tx, input services.ValidationInput) (services.ValidationResult, error) {
	matched := v.matchCandidates(input.Candidates)
	if len(matched) == 0 && !v.hasKnownClaims(input.Contracts) {
		return services.ValidationResult{}, nil
	}

	contractsForUs := v.collectClaimedContracts(input.Contracts, matched)
	if len(contractsForUs) > 0 {
		if err := v.enrichContractTokens(ctx, dbTx, input.Models, contractsForUs); err != nil {
			// Metadata enrichment is best-effort. A DB-level error inside the
			// caller's tx still propagates, but we wrap it so callers can
			// distinguish enrichment failures from validation failures when
			// surfacing logs.
			return services.ValidationResult{}, fmt.Errorf("sep41 enrichment: %w", err)
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

// Close releases any resources owned by this validator (worker pool).
func (v *Validator) Close() {
	if v == nil {
		return
	}
	if v.ownsPool && v.pool != nil {
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

// hasKnownClaims reports whether any contract in the batch references a wasm
// already classified as SEP-41 by an earlier ledger or protocol-setup run.
func (v *Validator) hasKnownClaims(contracts []services.ContractCandidate) bool {
	for _, ct := range contracts {
		if ct.KnownProtocolID == ProtocolID {
			return true
		}
	}
	return false
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

// enrichContractTokens decodes contract IDs to C-addresses, fetches metadata
// via RPC, and writes contract_tokens rows inside dbTx.
func (v *Validator) enrichContractTokens(ctx context.Context, dbTx pgx.Tx, models *data.Models, contracts []services.ContractCandidate) error {
	if models == nil || models.Contract == nil {
		return nil
	}
	addrs := make([]string, 0, len(contracts))
	addrToHash := make(map[string]types.HashBytea, len(contracts))
	for _, ct := range contracts {
		addr, ok := decodeContractAddr(ct.ContractID)
		if !ok {
			log.Ctx(ctx).Debugf("sep41: skipping contract with undecodable ID %q", ct.ContractID)
			continue
		}
		if _, dup := addrToHash[addr]; dup {
			continue
		}
		addrs = append(addrs, addr)
		addrToHash[addr] = ct.WasmHash
	}
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
			Type:       ProtocolID,
		})
	}
	if err := models.Contract.BatchInsert(ctx, dbTx, defaults); err != nil {
		return fmt.Errorf("inserting default contract_tokens: %w", err)
	}

	if v.fetcher == nil {
		// No RPC available — leave defaults in place and let the SEP-41
		// processor fill metadata in PersistCurrentState if the contract
		// becomes state-active later.
		return nil
	}

	metaByAddr, err := v.fetcher.FetchMetadata(ctx, addrs)
	if err != nil {
		// Treat fetch-level errors as best-effort; the rows already exist
		// with defaults.
		log.Ctx(ctx).Warnf("sep41 enrich: metadata fetch returned error, continuing with defaults: %v", err)
		return nil
	}
	if len(metaByAddr) == 0 {
		return nil
	}

	updates := make([]*data.Contract, 0, len(metaByAddr))
	for _, contract := range metaByAddr {
		// Force the row's type to SEP41 so the source of truth stays in
		// protocols/protocol_wasms — defensive against future fetcher refactors
		// that might omit the type field.
		contract.Type = ProtocolID
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

// scSpecTypeNames maps XDR ScSpecType values to human-readable type names used
// during signature validation.
var scSpecTypeNames = map[xdr.ScSpecType]string{
	xdr.ScSpecTypeScSpecTypeBool:         "bool",
	xdr.ScSpecTypeScSpecTypeU32:          "u32",
	xdr.ScSpecTypeScSpecTypeI32:          "i32",
	xdr.ScSpecTypeScSpecTypeU64:          "u64",
	xdr.ScSpecTypeScSpecTypeI64:          "i64",
	xdr.ScSpecTypeScSpecTypeU128:         "u128",
	xdr.ScSpecTypeScSpecTypeI128:         "i128",
	xdr.ScSpecTypeScSpecTypeU256:         "u256",
	xdr.ScSpecTypeScSpecTypeI256:         "i256",
	xdr.ScSpecTypeScSpecTypeAddress:      "Address",
	xdr.ScSpecTypeScSpecTypeMuxedAddress: "MuxedAddress",
	xdr.ScSpecTypeScSpecTypeString:       "String",
	xdr.ScSpecTypeScSpecTypeBytes:        "Bytes",
	xdr.ScSpecTypeScSpecTypeSymbol:       "Symbol",
	xdr.ScSpecTypeScSpecTypeVec:          "Vec",
	xdr.ScSpecTypeScSpecTypeMap:          "Map",
	xdr.ScSpecTypeScSpecTypeOption:       "Option",
	xdr.ScSpecTypeScSpecTypeResult:       "Result",
	xdr.ScSpecTypeScSpecTypeTuple:        "Tuple",
	xdr.ScSpecTypeScSpecTypeBytesN:       "BytesN",
	xdr.ScSpecTypeScSpecTypeUdt:          "UDT",
	xdr.ScSpecTypeScSpecTypeVoid:         "void",
}

// functionSpec defines the expected signature for a SEP-41 token function.
type functionSpec struct {
	name            string
	expectedInputs  []contractFunctionInputSpec
	expectedOutputs []string
}

type contractFunctionInputSpec struct {
	name     string
	typeName string
}

// requiredFunctions defines all required functions for SEP-41 token standard
// compliance. A contract must implement all of these functions with the exact
// signatures specified.
var requiredFunctions = []functionSpec{
	{
		name:            "balance",
		expectedInputs:  []contractFunctionInputSpec{{name: "id", typeName: "Address"}},
		expectedOutputs: []string{"i128"},
	},
	{
		name:            "allowance",
		expectedInputs:  []contractFunctionInputSpec{{name: "from", typeName: "Address"}, {name: "spender", typeName: "Address"}},
		expectedOutputs: []string{"i128"},
	},
	{
		name:            "decimals",
		expectedInputs:  []contractFunctionInputSpec{},
		expectedOutputs: []string{"u32"},
	},
	{
		name:            "name",
		expectedInputs:  []contractFunctionInputSpec{},
		expectedOutputs: []string{"String"},
	},
	{
		name:            "symbol",
		expectedInputs:  []contractFunctionInputSpec{},
		expectedOutputs: []string{"String"},
	},
	{
		name: "approve",
		expectedInputs: []contractFunctionInputSpec{
			{name: "from", typeName: "Address"},
			{name: "spender", typeName: "Address"},
			{name: "amount", typeName: "i128"},
			{name: "expiration_ledger", typeName: "u32"},
		},
		expectedOutputs: []string{},
	},
	{
		name: "transfer",
		expectedInputs: []contractFunctionInputSpec{
			{name: "from", typeName: "Address"},
			{name: "to", typeName: "Address"},
			{name: "amount", typeName: "i128"},
		},
	},
	// CAP-67 variant: (from: Address, to_muxed: MuxedAddress, amount: i128) -> ()
	{
		name: "transfer",
		expectedInputs: []contractFunctionInputSpec{
			{name: "from", typeName: "Address"},
			{name: "to_muxed", typeName: "MuxedAddress"},
			{name: "amount", typeName: "i128"},
		},
	},
	{
		name: "transfer_from",
		expectedInputs: []contractFunctionInputSpec{
			{name: "spender", typeName: "Address"},
			{name: "from", typeName: "Address"},
			{name: "to", typeName: "Address"},
			{name: "amount", typeName: "i128"},
		},
		expectedOutputs: []string{},
	},
	{
		name: "burn",
		expectedInputs: []contractFunctionInputSpec{
			{name: "from", typeName: "Address"},
			{name: "amount", typeName: "i128"},
		},
		expectedOutputs: []string{},
	},
	{
		name: "burn_from",
		expectedInputs: []contractFunctionInputSpec{
			{name: "spender", typeName: "Address"},
			{name: "from", typeName: "Address"},
			{name: "amount", typeName: "i128"},
		},
		expectedOutputs: []string{},
	},
}

// matchSEP41Spec is the pure SEP-41 signature check. Returns true iff the
// supplied contract spec implements every required SEP-41 function with an
// exact-match signature. Suitable for unit testing in isolation; production
// callers reach this via Validator.Validate.
func matchSEP41Spec(contractSpec []xdr.ScSpecEntry) bool {
	requiredSpecs := make(map[string][]functionSpec, len(requiredFunctions))
	for _, spec := range requiredFunctions {
		requiredSpecs[spec.name] = append(requiredSpecs[spec.name], spec)
	}

	foundFunctions := set.NewSet[string]()

	for _, spec := range contractSpec {
		if spec.Kind != xdr.ScSpecEntryKindScSpecEntryFunctionV0 || spec.FunctionV0 == nil {
			continue
		}

		function := spec.FunctionV0
		funcName := string(function.Name)

		expectedSpecs, isRequired := requiredSpecs[funcName]
		if !isRequired {
			continue
		}

		actualInputs := make([]contractFunctionInputSpec, 0, len(function.Inputs))
		for _, input := range function.Inputs {
			actualInputs = append(actualInputs, contractFunctionInputSpec{
				name:     input.Name,
				typeName: getTypeName(input.Type.Type),
			})
		}

		actualOutputs := make([]string, 0, len(function.Outputs))
		for _, output := range function.Outputs {
			actualOutputs = append(actualOutputs, getTypeName(output.Type))
		}

		for _, expectedSpec := range expectedSpecs {
			if validateFunctionInputsAndOutputs(actualInputs, actualOutputs, expectedSpec.expectedInputs, expectedSpec.expectedOutputs) {
				foundFunctions.Add(funcName)
				break
			}
		}
	}

	return foundFunctions.Cardinality() == len(requiredSpecs)
}

// validateFunctionInputsAndOutputs checks if a function's signature matches
// the expected SEP-41 specification. It compares ordered input/output slices
// with exact arity and exact position-by-position matches.
func validateFunctionInputsAndOutputs(
	inputs []contractFunctionInputSpec,
	outputs []string,
	expectedInputs []contractFunctionInputSpec,
	expectedOutputs []string,
) bool {
	if len(inputs) != len(expectedInputs) {
		return false
	}

	for i := range expectedInputs {
		if inputs[i] != expectedInputs[i] {
			return false
		}
	}

	if len(outputs) != len(expectedOutputs) {
		return false
	}

	for i := range expectedOutputs {
		if outputs[i] != expectedOutputs[i] {
			return false
		}
	}
	return true
}

// getTypeName converts an XDR ScSpecType to its human-readable string
// representation.
func getTypeName(scType xdr.ScSpecType) string {
	if name, ok := scSpecTypeNames[scType]; ok {
		return name
	}
	return "unknown"
}
