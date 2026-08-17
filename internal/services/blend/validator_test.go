package blend

import (
	"context"
	"encoding/hex"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
	blenddata "github.com/stellar/wallet-backend/internal/data/blend"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	indexerTypes "github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/services"
)

// spec-building fixtures ----------------------------------------------------

func createScSpecFunctionEntry(name string, inputs []xdr.ScSpecFunctionInputV0, outputs []xdr.ScSpecTypeDef) xdr.ScSpecEntry {
	funcName := xdr.ScSymbol(name)
	funcV0 := &xdr.ScSpecFunctionV0{
		Name:    funcName,
		Inputs:  inputs,
		Outputs: outputs,
	}
	return xdr.ScSpecEntry{
		Kind:       xdr.ScSpecEntryKindScSpecEntryFunctionV0,
		FunctionV0: funcV0,
	}
}

func createFunctionInput(name string, typeDef xdr.ScSpecTypeDef) xdr.ScSpecFunctionInputV0 {
	return xdr.ScSpecFunctionInputV0{Name: name, Type: typeDef}
}

func createScSpecTypeDef(scType xdr.ScSpecType) xdr.ScSpecTypeDef {
	return xdr.ScSpecTypeDef{Type: scType}
}

var (
	addressType = createScSpecTypeDef(xdr.ScSpecTypeScSpecTypeAddress)
	i128Type    = createScSpecTypeDef(xdr.ScSpecTypeScSpecTypeI128)
	u32Type     = createScSpecTypeDef(xdr.ScSpecTypeScSpecTypeU32)
	stringType  = createScSpecTypeDef(xdr.ScSpecTypeScSpecTypeString)
	vecType     = createScSpecTypeDef(xdr.ScSpecTypeScSpecTypeVec)
	udtType     = createScSpecTypeDef(xdr.ScSpecTypeScSpecTypeUdt)
)

// fullPoolSpec returns the 6 required Blend v2 Pool functions with their
// exact on-chain signatures (verified against the deployed pubnet/testnet
// pool wasm).
func fullPoolSpec() []xdr.ScSpecEntry {
	return []xdr.ScSpecEntry{
		createScSpecFunctionEntry("get_config", nil, []xdr.ScSpecTypeDef{udtType}),
		createScSpecFunctionEntry("get_reserve",
			[]xdr.ScSpecFunctionInputV0{createFunctionInput("asset", addressType)},
			[]xdr.ScSpecTypeDef{udtType},
		),
		createScSpecFunctionEntry("get_positions",
			[]xdr.ScSpecFunctionInputV0{createFunctionInput("address", addressType)},
			[]xdr.ScSpecTypeDef{udtType},
		),
		createScSpecFunctionEntry("submit",
			[]xdr.ScSpecFunctionInputV0{
				createFunctionInput("from", addressType),
				createFunctionInput("spender", addressType),
				createFunctionInput("to", addressType),
				createFunctionInput("requests", vecType),
			},
			[]xdr.ScSpecTypeDef{udtType},
		),
		createScSpecFunctionEntry("flash_loan",
			[]xdr.ScSpecFunctionInputV0{
				createFunctionInput("from", addressType),
				createFunctionInput("flash_loan", udtType),
				createFunctionInput("requests", vecType),
			},
			[]xdr.ScSpecTypeDef{udtType},
		),
		createScSpecFunctionEntry("bad_debt",
			[]xdr.ScSpecFunctionInputV0{createFunctionInput("user", addressType)},
			nil,
		),
	}
}

// fullBackstopSpec returns the 7 required Blend v2 Backstop functions with
// their exact on-chain signatures (verified against the deployed
// pubnet/testnet backstop wasm).
func fullBackstopSpec() []xdr.ScSpecEntry {
	return []xdr.ScSpecEntry{
		createScSpecFunctionEntry("deposit",
			[]xdr.ScSpecFunctionInputV0{
				createFunctionInput("from", addressType),
				createFunctionInput("pool_address", addressType),
				createFunctionInput("amount", i128Type),
			},
			[]xdr.ScSpecTypeDef{i128Type},
		),
		createScSpecFunctionEntry("queue_withdrawal",
			[]xdr.ScSpecFunctionInputV0{
				createFunctionInput("from", addressType),
				createFunctionInput("pool_address", addressType),
				createFunctionInput("amount", i128Type),
			},
			[]xdr.ScSpecTypeDef{udtType},
		),
		createScSpecFunctionEntry("dequeue_withdrawal",
			[]xdr.ScSpecFunctionInputV0{
				createFunctionInput("from", addressType),
				createFunctionInput("pool_address", addressType),
				createFunctionInput("amount", i128Type),
			},
			nil,
		),
		createScSpecFunctionEntry("withdraw",
			[]xdr.ScSpecFunctionInputV0{
				createFunctionInput("from", addressType),
				createFunctionInput("pool_address", addressType),
				createFunctionInput("amount", i128Type),
			},
			[]xdr.ScSpecTypeDef{i128Type},
		),
		createScSpecFunctionEntry("claim",
			[]xdr.ScSpecFunctionInputV0{
				createFunctionInput("from", addressType),
				createFunctionInput("pool_addresses", vecType),
				createFunctionInput("min_lp_tokens_out", i128Type),
			},
			[]xdr.ScSpecTypeDef{i128Type},
		),
		createScSpecFunctionEntry("draw",
			[]xdr.ScSpecFunctionInputV0{
				createFunctionInput("pool_address", addressType),
				createFunctionInput("amount", i128Type),
				createFunctionInput("to", addressType),
			},
			nil,
		),
		createScSpecFunctionEntry("donate",
			[]xdr.ScSpecFunctionInputV0{
				createFunctionInput("from", addressType),
				createFunctionInput("pool_address", addressType),
				createFunctionInput("amount", i128Type),
			},
			nil,
		),
	}
}

// sep41LikeSpec returns a SEP-41 token interface spec — used to prove Blend's
// matchers don't false-positive on an unrelated protocol.
func sep41LikeSpec() []xdr.ScSpecEntry {
	return []xdr.ScSpecEntry{
		createScSpecFunctionEntry("balance",
			[]xdr.ScSpecFunctionInputV0{createFunctionInput("id", addressType)},
			[]xdr.ScSpecTypeDef{i128Type},
		),
		createScSpecFunctionEntry("allowance",
			[]xdr.ScSpecFunctionInputV0{
				createFunctionInput("from", addressType),
				createFunctionInput("spender", addressType),
			},
			[]xdr.ScSpecTypeDef{i128Type},
		),
		createScSpecFunctionEntry("decimals", nil, []xdr.ScSpecTypeDef{u32Type}),
		createScSpecFunctionEntry("name", nil, []xdr.ScSpecTypeDef{stringType}),
		createScSpecFunctionEntry("symbol", nil, []xdr.ScSpecTypeDef{stringType}),
		createScSpecFunctionEntry("transfer",
			[]xdr.ScSpecFunctionInputV0{
				createFunctionInput("from", addressType),
				createFunctionInput("to", addressType),
				createFunctionInput("amount", i128Type),
			},
			nil,
		),
	}
}

// removeFunction returns a copy of spec with the named function entry
// dropped, simulating a contract that's missing one required function.
func removeFunction(spec []xdr.ScSpecEntry, name string) []xdr.ScSpecEntry {
	out := make([]xdr.ScSpecEntry, 0, len(spec))
	for _, entry := range spec {
		if entry.FunctionV0 != nil && string(entry.FunctionV0.Name) == name {
			continue
		}
		out = append(out, entry)
	}
	return out
}

// contractIDFromAddr converts testPoolAddr to the hex HashBytea form
// ContractCandidate.ContractID uses, mirroring how the framework decodes
// contract IDs.
func contractIDFromAddr(t *testing.T) indexerTypes.HashBytea {
	t.Helper()
	raw, err := strkey.Decode(strkey.VersionByteContract, testPoolAddr)
	require.NoError(t, err)
	return indexerTypes.HashBytea(hex.EncodeToString(raw))
}

// signature-matching tests ---------------------------------------------------

func TestMatchPoolSpec(t *testing.T) {
	t.Run("matches the full pool interface", func(t *testing.T) {
		assert.True(t, matchPoolSpec(fullPoolSpec()))
	})

	t.Run("does not match the backstop interface", func(t *testing.T) {
		assert.False(t, matchPoolSpec(fullBackstopSpec()))
	})

	t.Run("does not match a SEP-41 token interface", func(t *testing.T) {
		assert.False(t, matchPoolSpec(sep41LikeSpec()))
	})

	t.Run("does not match when flash_loan is missing", func(t *testing.T) {
		assert.False(t, matchPoolSpec(removeFunction(fullPoolSpec(), "flash_loan")))
	})

	t.Run("does not match when get_config is missing", func(t *testing.T) {
		assert.False(t, matchPoolSpec(removeFunction(fullPoolSpec(), "get_config")))
	})

	t.Run("still matches with extra unrelated functions present", func(t *testing.T) {
		spec := fullPoolSpec()
		spec = append(spec, createScSpecFunctionEntry("propose_admin",
			[]xdr.ScSpecFunctionInputV0{createFunctionInput("new_admin", addressType)}, nil))
		assert.True(t, matchPoolSpec(spec))
	})
}

func TestMatchBackstopSpec(t *testing.T) {
	t.Run("matches the full backstop interface", func(t *testing.T) {
		assert.True(t, matchBackstopSpec(fullBackstopSpec()))
	})

	t.Run("does not match the pool interface", func(t *testing.T) {
		assert.False(t, matchBackstopSpec(fullPoolSpec()))
	})

	t.Run("does not match a SEP-41 token interface", func(t *testing.T) {
		assert.False(t, matchBackstopSpec(sep41LikeSpec()))
	})

	t.Run("does not match when donate is missing", func(t *testing.T) {
		assert.False(t, matchBackstopSpec(removeFunction(fullBackstopSpec(), "donate")))
	})
}

// real-wasm tests -------------------------------------------------------------

func wasmTestdataDir() string {
	_, filename, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(filename), "testdata")
}

func loadTestWasm(t *testing.T, filename string) []byte {
	t.Helper()
	wasmBytes, err := os.ReadFile(filepath.Join(wasmTestdataDir(), filename))
	require.NoError(t, err, "reading test wasm file %s", filename)
	return wasmBytes
}

// TestValidator_RealWasm exercises the signature matchers against the actual
// Blend v2 pool and backstop WASMs fetched from mainnet (pubnet and testnet
// are byte-identical deployments), guarding against the synthetic spec
// fixtures above silently drifting from the real on-chain interface.
func TestValidator_RealWasm(t *testing.T) {
	ctx := context.Background()
	extractor := services.NewWasmSpecExtractor()
	defer func() { require.NoError(t, extractor.Close(ctx)) }()

	t.Run("pool wasm validates as pool, not backstop", func(t *testing.T) {
		wasmBytes := loadTestWasm(t, "blend_pool_v2.wasm")
		specs, err := extractor.ExtractSpec(ctx, wasmBytes)
		require.NoError(t, err)
		assert.True(t, matchPoolSpec(specs), "pool contract should validate as a Blend pool")
		assert.False(t, matchBackstopSpec(specs), "pool contract should not validate as the Blend backstop")
	})

	t.Run("backstop wasm validates as backstop, not pool", func(t *testing.T) {
		wasmBytes := loadTestWasm(t, "blend_backstop_v2.wasm")
		specs, err := extractor.ExtractSpec(ctx, wasmBytes)
		require.NoError(t, err)
		assert.True(t, matchBackstopSpec(specs), "backstop contract should validate as the Blend backstop")
		assert.False(t, matchPoolSpec(specs), "backstop contract should not validate as a Blend pool")
	})
}

// blndTokenAddress tests ------------------------------------------------------

func TestBlndTokenAddress(t *testing.T) {
	assert.Equal(t, "CD25MNVTZDL4Y3XBCPCJXGXATV5WUHHOWMYFF4YBEGU5FCPGMYTVG5JY", blndTokenAddress(network.PublicNetworkPassphrase))
	assert.Equal(t, "CB22KRA3YZVCNCQI64JQ5WE7UY2VAV7WFLK6A2JN3HEX56T2EDAFO7QF", blndTokenAddress(network.TestNetworkPassphrase))
	assert.Empty(t, blndTokenAddress(network.FutureNetworkPassphrase))
	assert.Empty(t, blndTokenAddress("some custom standalone network"))
}

// Validator construction tests ------------------------------------------------

func TestNewValidator(t *testing.T) {
	v := NewValidator()
	require.NotNil(t, v)
	assert.Nil(t, v.metadata)
	assert.Equal(t, ProtocolID, v.ProtocolID())
}

func TestNewValidatorFromDeps(t *testing.T) {
	metadata := services.NewContractMetadataServiceMock(t)
	v := newValidator(services.ProtocolDeps{ContractMetadataService: metadata})
	require.NotNil(t, v)
	assert.Same(t, metadata, v.metadata)
}

// Match tests --------------------------------------------------------------------

func TestValidator_Match_ClaimsPoolAndBackstopHashes(t *testing.T) {
	v := NewValidator()

	poolHash := indexerTypes.HashBytea("bb")
	backstopHash := indexerTypes.HashBytea("aa")

	matched := v.Match([]services.WasmCandidate{
		{Hash: poolHash, SpecEntries: fullPoolSpec()},
		{Hash: backstopHash, SpecEntries: fullBackstopSpec()},
	})
	assert.Equal(t, map[indexerTypes.HashBytea]struct{}{poolHash: {}, backstopHash: {}}, matched,
		"both Blend interfaces are claimed under the single BLEND protocol id")
}

func TestValidator_Match_NoMatch(t *testing.T) {
	v := NewValidator()
	matched := v.Match([]services.WasmCandidate{
		{Hash: indexerTypes.HashBytea("aabb"), SpecEntries: sep41LikeSpec()},
	})
	assert.Empty(t, matched)
}

// TestValidator_PrefetchApply_NilModelsSkipsEnrichment covers a matched pool
// whose classification carries no enrichment: with no metadata service the plan
// is empty, so Apply reaches no model and nil Models is harmless.
func TestValidator_PrefetchApply_NilModelsSkipsEnrichment(t *testing.T) {
	ctx := context.Background()
	v := NewValidator()
	poolHash := indexerTypes.HashBytea("cc")
	candidates := []services.WasmCandidate{{Hash: poolHash, SpecEntries: fullPoolSpec()}}
	contracts := []services.ContractCandidate{{ContractID: indexerTypes.HashBytea("11"), WasmHash: poolHash}}

	matched := v.Match(candidates)
	require.Equal(t, map[indexerTypes.HashBytea]struct{}{poolHash: {}}, matched)

	assert.NotPanics(t, func() {
		plan, err := v.Prefetch(ctx, nil, candidates, matched, contracts)
		require.NoError(t, err)
		assert.Equal(t, blendPlan{}, plan)
		require.NoError(t, v.Apply(ctx, nil, matched, contracts, plan, nil))
	})
}

// collectPoolContracts tests ---------------------------------------------------

func TestValidator_CollectPoolContracts(t *testing.T) {
	v := NewValidator()
	poolHash := indexerTypes.HashBytea("bb")
	backstopHash := indexerTypes.HashBytea("aa")
	unrelatedHash := indexerTypes.HashBytea("cc")

	matched := map[indexerTypes.HashBytea]role{
		poolHash:     rolePool,
		backstopHash: roleBackstop,
	}

	contracts := []services.ContractCandidate{
		{ContractID: indexerTypes.HashBytea("p1"), WasmHash: poolHash},
		{ContractID: indexerTypes.HashBytea("b1"), WasmHash: backstopHash},
		{ContractID: indexerTypes.HashBytea("p2-known"), WasmHash: indexerTypes.HashBytea("dd"), KnownProtocolID: ProtocolID},
		{ContractID: indexerTypes.HashBytea("unrelated"), WasmHash: unrelatedHash, KnownProtocolID: "SEP41"},
		{ContractID: indexerTypes.HashBytea("unclassified"), WasmHash: indexerTypes.HashBytea("ee")},
	}

	got := v.collectPoolContracts(contracts, matched)

	gotIDs := make([]indexerTypes.HashBytea, 0, len(got))
	for _, ct := range got {
		gotIDs = append(gotIDs, ct.ContractID)
	}
	assert.ElementsMatch(t, []indexerTypes.HashBytea{"p1", "p2-known"}, gotIDs,
		"a freshly-matched pool and a previously-classified Blend contract (role unknown) are both candidates; "+
			"a freshly-matched backstop, an unrelated protocol, and an unclassified contract are excluded")
}

// Prefetch / fetchPoolConfig tests ------------------------------------------------

const testPoolAddr = "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"

// testPoolWasmHash is the wasm hash the pool contracts below are deployed from;
// pairing it with a fullPoolSpec candidate is what makes Prefetch re-derive the
// pool role and attempt enrichment (see collectPoolContracts).
const testPoolWasmHash = indexerTypes.HashBytea("bb")

// prefetchPools runs Prefetch over one freshly-matched pool wasm and the
// contract instances deployed from it, returning the blend_pools rows the
// resulting plan carries.
func prefetchPools(t *testing.T, v *Validator, contractIDs ...indexerTypes.HashBytea) []blenddata.Pool {
	t.Helper()
	candidates := []services.WasmCandidate{{Hash: testPoolWasmHash, SpecEntries: fullPoolSpec()}}
	contracts := make([]services.ContractCandidate, 0, len(contractIDs))
	for _, id := range contractIDs {
		contracts = append(contracts, services.ContractCandidate{ContractID: id, WasmHash: testPoolWasmHash})
	}

	plan, err := v.Prefetch(context.Background(), nil, candidates, v.Match(candidates), contracts)
	require.NoError(t, err)
	p, ok := plan.(blendPlan)
	require.True(t, ok, "Prefetch must return a blendPlan")
	return p.pools
}

func poolConfigScVal(t *testing.T, oracle string, bstopRate, status, maxPositions uint32, minCollateral int64) xdr.ScVal {
	t.Helper()
	m := mapScVal(
		xdr.ScMapEntry{Key: symScVal("bstop_rate"), Val: u32ScVal(bstopRate)},
		xdr.ScMapEntry{Key: symScVal("max_positions"), Val: u32ScVal(maxPositions)},
		xdr.ScMapEntry{Key: symScVal("min_collateral"), Val: i128ScVal(minCollateral)},
		xdr.ScMapEntry{Key: symScVal("oracle"), Val: contractAddrScVal(t, oracle)},
		xdr.ScMapEntry{Key: symScVal("status"), Val: u32ScVal(status)},
	)
	return xdr.ScVal{Type: xdr.ScValTypeScvMap, Map: &m}
}

func TestValidator_Prefetch(t *testing.T) {
	const oracleAddr = "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA"

	t.Run("nil metadata service plans nothing", func(t *testing.T) {
		assert.Empty(t, prefetchPools(t, NewValidator(), contractIDFromAddr(t)))
	})

	t.Run("decodes get_config into one planned row", func(t *testing.T) {
		metadata := services.NewContractMetadataServiceMock(t)
		metadata.On("FetchSingleField", mock.Anything, testPoolAddr, "get_config", mock.Anything).
			Return(poolConfigScVal(t, oracleAddr, 2500, 1, 4, 1_000_000), nil).Once()

		rows := prefetchPools(t, &Validator{metadata: metadata}, contractIDFromAddr(t))
		require.Len(t, rows, 1)

		row := rows[0]
		assert.Equal(t, testPoolAddr, string(row.PoolContractID))
		assert.Equal(t, oracleAddr, string(row.OracleContractID))
		require.NotNil(t, row.BackstopRate)
		assert.Equal(t, int32(2500), *row.BackstopRate)
		require.NotNil(t, row.Status)
		assert.Equal(t, int32(1), *row.Status)
		require.NotNil(t, row.MaxPositions)
		assert.Equal(t, int32(4), *row.MaxPositions)
		require.NotNil(t, row.MinCollateral)
		assert.Equal(t, "1000000", *row.MinCollateral)
		assert.Nil(t, row.Name, "no name getter exists on the pool interface")
	})

	t.Run("skips a contract whose get_config call fails, without erroring", func(t *testing.T) {
		metadata := services.NewContractMetadataServiceMock(t)
		metadata.On("FetchSingleField", mock.Anything, testPoolAddr, "get_config", mock.Anything).
			Return(xdr.ScVal{}, assert.AnError).Once()

		assert.Empty(t, prefetchPools(t, &Validator{metadata: metadata}, contractIDFromAddr(t)),
			"a contract that decoded nothing must not reach the plan")
	})

	t.Run("skips a contract whose get_config returns a non-map value (e.g. a backstop contract)", func(t *testing.T) {
		metadata := services.NewContractMetadataServiceMock(t)
		metadata.On("FetchSingleField", mock.Anything, testPoolAddr, "get_config", mock.Anything).
			Return(u32ScVal(1), nil).Once()

		assert.Empty(t, prefetchPools(t, &Validator{metadata: metadata}, contractIDFromAddr(t)))
	})

	t.Run("dedupes repeated contract IDs, fetching get_config only once", func(t *testing.T) {
		metadata := services.NewContractMetadataServiceMock(t)
		metadata.On("FetchSingleField", mock.Anything, testPoolAddr, "get_config", mock.Anything).
			Return(poolConfigScVal(t, oracleAddr, 100, 0, 1, 1), nil).Once()

		id := contractIDFromAddr(t)
		assert.Len(t, prefetchPools(t, &Validator{metadata: metadata}, id, id), 1)
	})
}

// TestValidator_Apply_EmptyPlanWritesNothing covers Apply's guard paths: an
// empty plan, an absent pool model, and a plan belonging to another protocol's
// validator all return without touching the (nil) transaction.
func TestValidator_Apply_EmptyPlanWritesNothing(t *testing.T) {
	ctx := context.Background()
	v := NewValidator()

	plan, err := v.Prefetch(ctx, nil, nil, nil, nil)
	require.NoError(t, err)

	require.NoError(t, v.Apply(ctx, nil, nil, nil, plan, &data.Models{}))
	require.NoError(t, v.Apply(ctx, nil, nil, nil, plan, nil))
	require.NoError(t, v.Apply(ctx, nil, nil, nil, "not a blend plan", &data.Models{}))
}

// TestValidator_Apply_UpsertsPlannedPools exercises the real upsert path:
// a plan carrying one pool row lands in blend_pools inside the transaction.
func TestValidator_Apply_UpsertsPlannedPools(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	pool, err := db.OpenDBConnectionPool(context.Background(), dbt.DSN)
	require.NoError(t, err)
	defer pool.Close()

	ctx := context.Background()
	m := metrics.NewMetrics(prometheus.NewRegistry())
	models, err := data.NewModels(pool, m.DB)
	require.NoError(t, err)

	rate := int32(2500)
	plan := blendPlan{pools: []blenddata.Pool{{
		PoolContractID: indexerTypes.AddressBytea(testPoolAddr),
		BackstopRate:   &rate,
	}}}

	v := NewValidator()
	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(ctx) }()

	require.NoError(t, v.Apply(ctx, tx, nil, nil, plan, models))

	var gotRate int32
	require.NoError(t, tx.QueryRow(ctx,
		`SELECT backstop_rate FROM blend_pools WHERE pool_contract_id = $1`,
		indexerTypes.AddressBytea(testPoolAddr),
	).Scan(&gotRate))
	assert.Equal(t, rate, gotRate)
}

func TestDecodeContractAddr(t *testing.T) {
	t.Run("round-trips a valid contract id", func(t *testing.T) {
		addr, ok := decodeContractAddr(contractIDFromAddr(t))
		require.True(t, ok)
		assert.Equal(t, testPoolAddr, addr)
	})

	t.Run("returns false for invalid hex", func(t *testing.T) {
		_, ok := decodeContractAddr(indexerTypes.HashBytea("not-hex"))
		assert.False(t, ok)
	})
}
