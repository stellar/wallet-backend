package blend

import (
	"context"
	"encoding/hex"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	blenddata "github.com/stellar/wallet-backend/internal/data/blend"
	indexerTypes "github.com/stellar/wallet-backend/internal/indexer/types"
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
	assert.Equal(t, "CDYLJJT2VBKY55ZK57MTMKAVRCRPQMYB4YJ7JFFARMSJZ73I5CMCITSU", blndTokenAddress("Standalone Network ; February 2017"))
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

// Validate tests ---------------------------------------------------------------

func TestValidator_Validate_ReturnsSortedUnionOfPoolAndBackstopHashes(t *testing.T) {
	v := NewValidator()

	poolHash := indexerTypes.HashBytea("bb")
	backstopHash := indexerTypes.HashBytea("aa")

	out, err := v.Validate(context.Background(), nil, services.ValidationInput{
		Candidates: []services.WasmCandidate{
			{Hash: poolHash, SpecEntries: fullPoolSpec()},
			{Hash: backstopHash, SpecEntries: fullBackstopSpec()},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, []indexerTypes.HashBytea{backstopHash, poolHash}, out.MatchedWasms, "must be sorted ascending")
}

func TestValidator_Validate_NoMatch(t *testing.T) {
	v := NewValidator()
	out, err := v.Validate(context.Background(), nil, services.ValidationInput{
		Candidates: []services.WasmCandidate{
			{Hash: indexerTypes.HashBytea("aabb"), SpecEntries: sep41LikeSpec()},
		},
	})
	require.NoError(t, err)
	assert.Empty(t, out.MatchedWasms)
}

func TestValidator_Validate_NilModelsSkipsEnrichmentWithoutPanicking(t *testing.T) {
	v := NewValidator()
	poolHash := indexerTypes.HashBytea("cc")
	assert.NotPanics(t, func() {
		out, err := v.Validate(context.Background(), nil, services.ValidationInput{
			Candidates: []services.WasmCandidate{{Hash: poolHash, SpecEntries: fullPoolSpec()}},
			Contracts: []services.ContractCandidate{
				{ContractID: indexerTypes.HashBytea("11"), WasmHash: poolHash},
			},
			Models: nil,
		})
		require.NoError(t, err)
		assert.Equal(t, []indexerTypes.HashBytea{poolHash}, out.MatchedWasms)
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

// enrichPools / fetchPoolConfig tests --------------------------------------------

// stubPoolModel is a minimal hand-written stub satisfying
// blenddata.PoolModelInterface — internal/data/blend.Models.Pools is a
// concrete *PoolModel (not an interface), so it can't be swapped for a mock
// via data.Models; this stub lets enrichPools be tested directly instead.
type stubPoolModel struct {
	rows      []blenddata.Pool
	batchErr  error
	callCount int
}

func (s *stubPoolModel) BatchUpsert(_ context.Context, _ pgx.Tx, rows []blenddata.Pool) error {
	s.callCount++
	s.rows = rows
	return s.batchErr
}

func (s *stubPoolModel) SetRewardZone(_ context.Context, _ pgx.Tx, _ []indexerTypes.AddressBytea, _ int32) error {
	return nil
}

var _ blenddata.PoolModelInterface = (*stubPoolModel)(nil)

const testPoolAddr = "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"

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

func TestValidator_EnrichPools(t *testing.T) {
	const oracleAddr = "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA"

	t.Run("nil metadata service is a no-op", func(t *testing.T) {
		v := NewValidator()
		stub := &stubPoolModel{}
		contracts := []services.ContractCandidate{{ContractID: contractIDFromAddr(t)}}
		err := v.enrichPools(context.Background(), nil, stub, contracts)
		require.NoError(t, err)
		assert.Zero(t, stub.callCount)
	})

	t.Run("decodes get_config and upserts one row", func(t *testing.T) {
		metadata := services.NewContractMetadataServiceMock(t)
		metadata.On("FetchSingleField", mock.Anything, testPoolAddr, "get_config", mock.Anything).
			Return(poolConfigScVal(t, oracleAddr, 2500, 1, 4, 1_000_000), nil).Once()

		v := &Validator{metadata: metadata}
		stub := &stubPoolModel{}
		contracts := []services.ContractCandidate{{ContractID: contractIDFromAddr(t)}}

		err := v.enrichPools(context.Background(), nil, stub, contracts)
		require.NoError(t, err)
		require.Equal(t, 1, stub.callCount)
		require.Len(t, stub.rows, 1)

		row := stub.rows[0]
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

		v := &Validator{metadata: metadata}
		stub := &stubPoolModel{}
		contracts := []services.ContractCandidate{{ContractID: contractIDFromAddr(t)}}

		err := v.enrichPools(context.Background(), nil, stub, contracts)
		require.NoError(t, err)
		assert.Zero(t, stub.callCount, "BatchUpsert should not be called when nothing decoded successfully")
	})

	t.Run("skips a contract whose get_config returns a non-map value (e.g. a backstop contract)", func(t *testing.T) {
		metadata := services.NewContractMetadataServiceMock(t)
		metadata.On("FetchSingleField", mock.Anything, testPoolAddr, "get_config", mock.Anything).
			Return(u32ScVal(1), nil).Once()

		v := &Validator{metadata: metadata}
		stub := &stubPoolModel{}
		contracts := []services.ContractCandidate{{ContractID: contractIDFromAddr(t)}}

		err := v.enrichPools(context.Background(), nil, stub, contracts)
		require.NoError(t, err)
		assert.Zero(t, stub.callCount)
	})

	t.Run("dedupes repeated contract IDs, fetching get_config only once", func(t *testing.T) {
		metadata := services.NewContractMetadataServiceMock(t)
		metadata.On("FetchSingleField", mock.Anything, testPoolAddr, "get_config", mock.Anything).
			Return(poolConfigScVal(t, oracleAddr, 100, 0, 1, 1), nil).Once()

		v := &Validator{metadata: metadata}
		stub := &stubPoolModel{}
		id := contractIDFromAddr(t)
		contracts := []services.ContractCandidate{{ContractID: id}, {ContractID: id}}

		err := v.enrichPools(context.Background(), nil, stub, contracts)
		require.NoError(t, err)
		require.Len(t, stub.rows, 1)
	})

	t.Run("propagates a BatchUpsert error", func(t *testing.T) {
		metadata := services.NewContractMetadataServiceMock(t)
		metadata.On("FetchSingleField", mock.Anything, testPoolAddr, "get_config", mock.Anything).
			Return(poolConfigScVal(t, oracleAddr, 100, 0, 1, 1), nil).Once()

		v := &Validator{metadata: metadata}
		stub := &stubPoolModel{batchErr: assert.AnError}
		contracts := []services.ContractCandidate{{ContractID: contractIDFromAddr(t)}}

		err := v.enrichPools(context.Background(), nil, stub, contracts)
		assert.Error(t, err)
	})
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
