package blend

import (
	"crypto/rand"
	"testing"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// test fixtures --------------------------------------------------------------
//
// symScVal, u32ScVal, i128ScVal, mapScVal, contractAddrScVal and
// accountAddrScVal are declared in scval_test.go and reused here.

// randomContractAddr returns a fresh, validly-checksummed C-address with no
// relationship to any real contract — DecodeEntry only ever round-trips the
// strkey, it never resolves it.
func randomContractAddr(t *testing.T) string {
	t.Helper()
	var raw [32]byte
	_, err := rand.Read(raw[:])
	require.NoError(t, err)
	addr, err := strkey.Encode(strkey.VersionByteContract, raw[:])
	require.NoError(t, err)
	return addr
}

// randomAccountAddr returns a fresh G-address.
func randomAccountAddr(t *testing.T) string {
	t.Helper()
	return keypair.MustRandom().Address()
}

func symEntry(sym string, val xdr.ScVal) xdr.ScMapEntry {
	return xdr.ScMapEntry{Key: symScVal(sym), Val: val}
}

func u32Entry(k uint32, val xdr.ScVal) xdr.ScMapEntry {
	return xdr.ScMapEntry{Key: u32ScVal(k), Val: val}
}

func stringScVal(s string) xdr.ScVal {
	str := xdr.ScString(s)
	return xdr.ScVal{Type: xdr.ScValTypeScvString, Str: &str}
}

func boolScVal(b bool) xdr.ScVal {
	return xdr.ScVal{Type: xdr.ScValTypeScvBool, B: &b}
}

func u64ScVal(n uint64) xdr.ScVal {
	v := xdr.Uint64(n)
	return xdr.ScVal{Type: xdr.ScValTypeScvU64, U64: &v}
}

func voidScVal() xdr.ScVal {
	return xdr.ScVal{Type: xdr.ScValTypeScvVoid}
}

func vecScVal(elems ...xdr.ScVal) xdr.ScVal {
	vec := xdr.ScVec(elems)
	vecPtr := &vec
	return xdr.ScVal{Type: xdr.ScValTypeScvVec, Vec: &vecPtr}
}

// mapValScVal wraps m as a Map-typed ScVal, for use as a nested map value
// (e.g. Config inside instance storage, or a struct-shaped key argument like
// PoolUserKey) — as opposed to mapScVal (scval_test.go), which returns the
// bare *xdr.ScMap for use with mapGet.
func mapValScVal(m *xdr.ScMap) xdr.ScVal {
	return xdr.ScVal{Type: xdr.ScValTypeScvMap, Map: &m}
}

func instanceKeyScVal() xdr.ScVal {
	return xdr.ScVal{Type: xdr.ScValTypeScvLedgerKeyContractInstance}
}

func instanceValScVal(storage *xdr.ScMap) xdr.ScVal {
	return xdr.ScVal{
		Type:     xdr.ScValTypeScvContractInstance,
		Instance: &xdr.ScContractInstance{Storage: storage},
	}
}

// contractDataLedgerEntry builds a minimal xdr.LedgerEntry wrapping a
// ContractDataEntry with the given key/value. The owning contract address is
// irrelevant to DecodeEntry (per DecodedEntry's godoc, the caller already
// knows it) so it's a fixed placeholder.
func contractDataLedgerEntry(key, val xdr.ScVal) *xdr.LedgerEntry {
	var cid xdr.ContractId
	cd := xdr.ContractDataEntry{
		Contract:   xdr.ScAddress{Type: xdr.ScAddressTypeScAddressTypeContract, ContractId: &cid},
		Key:        key,
		Durability: xdr.ContractDataDurabilityPersistent,
		Val:        val,
	}
	return &xdr.LedgerEntry{
		Data: xdr.LedgerEntryData{
			Type:         xdr.LedgerEntryTypeContractData,
			ContractData: &cd,
		},
	}
}

// createdChange builds an ingest.Change simulating a freshly created (or
// updated) ContractData entry: Post is set, Pre is nil.
func createdChange(key, val xdr.ScVal) ingest.Change {
	return ingest.Change{
		Type: xdr.LedgerEntryTypeContractData,
		Post: contractDataLedgerEntry(key, val),
	}
}

// removedChange builds an ingest.Change simulating a deleted ContractData
// entry: Pre carries the entry's last known state, Post is nil.
func removedChange(key, val xdr.ScVal) ingest.Change {
	return ingest.Change{
		Type: xdr.LedgerEntryTypeContractData,
		Pre:  contractDataLedgerEntry(key, val),
	}
}

// tests -----------------------------------------------------------------

func TestDecodeEntry_PoolInstance(t *testing.T) {
	oracleAddr := randomContractAddr(t)

	storage := mapScVal(
		symEntry("Name", stringScVal("Fixed Pool v2")),
		symEntry("Config", mapValScVal(mapScVal(
			symEntry("oracle", contractAddrScVal(t, oracleAddr)),
			symEntry("bstop_rate", u32ScVal(2500)),
			symEntry("status", u32ScVal(0)),
			symEntry("max_positions", u32ScVal(4)),
			symEntry("min_collateral", i128ScVal(1_000_000)),
		))),
	)

	got, err := DecodeEntry(createdChange(instanceKeyScVal(), instanceValScVal(storage)))
	require.NoError(t, err)

	assert.Equal(t, KindPoolInstance, got.Kind)
	assert.False(t, got.Removed)
	require.NotNil(t, got.PoolInstance)
	require.NotNil(t, got.PoolInstance.Name)
	assert.Equal(t, "Fixed Pool v2", *got.PoolInstance.Name)
	assert.Equal(t, oracleAddr, got.PoolInstance.Oracle)
	assert.Equal(t, uint32(2500), got.PoolInstance.BstopRate)
	assert.Equal(t, uint32(0), got.PoolInstance.Status)
	assert.Equal(t, uint32(4), got.PoolInstance.MaxPositions)
	assert.Equal(t, "1000000", got.PoolInstance.MinCollateral)
}

func TestDecodeEntry_BackstopInstance(t *testing.T) {
	// Backstop instance storage has no "Config" key.
	storage := mapScVal(
		symEntry("Emitter", contractAddrScVal(t, randomContractAddr(t))),
		symEntry("BToken", contractAddrScVal(t, randomContractAddr(t))),
		symEntry("PoolFact", contractAddrScVal(t, randomContractAddr(t))),
		symEntry("BLNDTkn", contractAddrScVal(t, randomContractAddr(t))),
		symEntry("USDCTkn", contractAddrScVal(t, randomContractAddr(t))),
	)

	got, err := DecodeEntry(createdChange(instanceKeyScVal(), instanceValScVal(storage)))
	require.NoError(t, err)
	assert.Equal(t, KindIgnored, got.Kind)
	assert.False(t, got.Removed)
}

func TestDecodeEntry_Positions(t *testing.T) {
	userAddr := randomAccountAddr(t)
	key := vecScVal(symScVal("Positions"), accountAddrScVal(t, userAddr))
	val := mapValScVal(mapScVal(
		symEntry("collateral", mapValScVal(mapScVal(u32Entry(0, i128ScVal(500))))),
		symEntry("liabilities", mapValScVal(mapScVal())),
		symEntry("supply", mapValScVal(mapScVal(u32Entry(1, i128ScVal(200))))),
	))

	got, err := DecodeEntry(createdChange(key, val))
	require.NoError(t, err)

	assert.Equal(t, KindPositions, got.Kind)
	assert.Equal(t, userAddr, got.User)
	require.NotNil(t, got.Positions)
	assert.Equal(t, map[uint32]string{0: "500"}, got.Positions.Collateral)
	assert.Equal(t, map[uint32]string{}, got.Positions.Liabilities)
	assert.Equal(t, map[uint32]string{1: "200"}, got.Positions.Supply)
}

func TestDecodeEntry_ResConfig(t *testing.T) {
	assetAddr := randomContractAddr(t)
	key := vecScVal(symScVal("ResConfig"), contractAddrScVal(t, assetAddr))
	val := mapValScVal(mapScVal(
		symEntry("c_factor", u32ScVal(9000)),
		symEntry("decimals", u32ScVal(7)),
		symEntry("enabled", boolScVal(true)),
		symEntry("index", u32ScVal(2)),
		symEntry("l_factor", u32ScVal(9500)),
		symEntry("max_util", u32ScVal(9500)),
		symEntry("r_base", u32ScVal(100)),
		symEntry("r_one", u32ScVal(200)),
		symEntry("r_three", u32ScVal(400)),
		symEntry("r_two", u32ScVal(300)),
		symEntry("reactivity", u32ScVal(1000)),
		symEntry("supply_cap", i128ScVal(1_000_000_000)),
		symEntry("util", u32ScVal(8000)),
	))

	got, err := DecodeEntry(createdChange(key, val))
	require.NoError(t, err)

	assert.Equal(t, KindResConfig, got.Kind)
	assert.Equal(t, assetAddr, got.Asset)
	assert.Equal(t, &ReserveConfigData{
		CFactor:    9000,
		Decimals:   7,
		Enabled:    true,
		Index:      2,
		LFactor:    9500,
		MaxUtil:    9500,
		RBase:      100,
		ROne:       200,
		RThree:     400,
		RTwo:       300,
		Reactivity: 1000,
		SupplyCap:  "1000000000",
		Util:       8000,
	}, got.ResConfig)
}

func TestDecodeEntry_ResData(t *testing.T) {
	assetAddr := randomContractAddr(t)
	key := vecScVal(symScVal("ResData"), contractAddrScVal(t, assetAddr))
	val := mapValScVal(mapScVal(
		symEntry("b_rate", i128ScVal(1_050_000_000_000)),
		symEntry("b_supply", i128ScVal(100_000_000_000)),
		symEntry("backstop_credit", i128ScVal(500)),
		symEntry("d_rate", i128ScVal(1_020_000_000_000)),
		symEntry("d_supply", i128ScVal(40_000_000_000)),
		symEntry("ir_mod", i128ScVal(1_000_000_000_000)),
		symEntry("last_time", u64ScVal(1_700_000_000)),
	))

	got, err := DecodeEntry(createdChange(key, val))
	require.NoError(t, err)

	assert.Equal(t, KindResData, got.Kind)
	assert.Equal(t, assetAddr, got.Asset)
	assert.Equal(t, &ReserveDataData{
		BRate:          "1050000000000",
		BSupply:        "100000000000",
		BackstopCredit: "500",
		DRate:          "1020000000000",
		DSupply:        "40000000000",
		IRMod:          "1000000000000",
		LastTime:       1_700_000_000,
	}, got.ResData)
}

func TestDecodeEntry_EmisData(t *testing.T) {
	key := vecScVal(symScVal("EmisData"), u32ScVal(5))
	val := mapValScVal(mapScVal(
		symEntry("eps", u64ScVal(1000)),
		symEntry("expiration", u64ScVal(1_800_000_000)),
		symEntry("index", i128ScVal(42)),
		symEntry("last_time", u64ScVal(1_700_000_000)),
	))

	got, err := DecodeEntry(createdChange(key, val))
	require.NoError(t, err)

	assert.Equal(t, KindEmisData, got.Kind)
	assert.Equal(t, uint32(5), got.TokenID)
	assert.Equal(t, &EmissionData{Eps: 1000, Expiration: 1_800_000_000, Index: "42", LastTime: 1_700_000_000}, got.EmisData)
}

func TestDecodeEntry_UserEmis(t *testing.T) {
	userAddr := randomAccountAddr(t)
	key := vecScVal(symScVal("UserEmis"), mapValScVal(mapScVal(
		symEntry("reserve_id", u32ScVal(3)),
		symEntry("user", accountAddrScVal(t, userAddr)),
	)))
	val := mapValScVal(mapScVal(
		symEntry("accrued", i128ScVal(777)),
		symEntry("index", i128ScVal(88)),
	))

	got, err := DecodeEntry(createdChange(key, val))
	require.NoError(t, err)

	assert.Equal(t, KindUserEmis, got.Kind)
	assert.Equal(t, uint32(3), got.TokenID)
	assert.Equal(t, userAddr, got.User)
	assert.Equal(t, &UserEmissionData{Accrued: "777", Index: "88"}, got.UserEmis)
}

func TestDecodeEntry_BackstopUserBalance(t *testing.T) {
	poolAddr := randomContractAddr(t)
	userAddr := randomAccountAddr(t)
	key := vecScVal(symScVal("UserBalance"), mapValScVal(mapScVal(
		symEntry("pool", contractAddrScVal(t, poolAddr)),
		symEntry("user", accountAddrScVal(t, userAddr)),
	)))

	t.Run("with queued withdrawals", func(t *testing.T) {
		val := mapValScVal(mapScVal(
			symEntry("q4w", vecScVal(
				mapValScVal(mapScVal(symEntry("amount", i128ScVal(100)), symEntry("exp", u64ScVal(1000)))),
				mapValScVal(mapScVal(symEntry("amount", i128ScVal(200)), symEntry("exp", u64ScVal(2000)))),
			)),
			symEntry("shares", i128ScVal(5000)),
		))

		got, err := DecodeEntry(createdChange(key, val))
		require.NoError(t, err)

		assert.Equal(t, KindBackstopUserBalance, got.Kind)
		assert.Equal(t, poolAddr, got.Pool)
		assert.Equal(t, userAddr, got.User)
		require.NotNil(t, got.BackstopUserBalance)
		assert.Equal(t, "5000", got.BackstopUserBalance.Shares)
		require.Len(t, got.BackstopUserBalance.Q4W, 2)
		assert.Equal(t, Q4WData{Amount: "100", Exp: 1000}, got.BackstopUserBalance.Q4W[0])
		assert.Equal(t, Q4WData{Amount: "200", Exp: 2000}, got.BackstopUserBalance.Q4W[1])
	})

	t.Run("with no queued withdrawals", func(t *testing.T) {
		val := mapValScVal(mapScVal(
			symEntry("q4w", vecScVal()),
			symEntry("shares", i128ScVal(0)),
		))

		got, err := DecodeEntry(createdChange(key, val))
		require.NoError(t, err)

		require.NotNil(t, got.BackstopUserBalance)
		assert.Empty(t, got.BackstopUserBalance.Q4W)
	})
}

func TestDecodeEntry_BackstopPoolBalance(t *testing.T) {
	poolAddr := randomContractAddr(t)
	key := vecScVal(symScVal("PoolBalance"), contractAddrScVal(t, poolAddr))
	val := mapValScVal(mapScVal(
		symEntry("q4w", i128ScVal(50)),
		symEntry("shares", i128ScVal(1000)),
		symEntry("tokens", i128ScVal(2000)),
	))

	got, err := DecodeEntry(createdChange(key, val))
	require.NoError(t, err)

	assert.Equal(t, KindBackstopPoolBalance, got.Kind)
	assert.Equal(t, poolAddr, got.Pool)
	assert.Equal(t, &BackstopPoolBalanceData{Q4W: "50", Shares: "1000", Tokens: "2000"}, got.BackstopPoolBalance)
}

func TestDecodeEntry_BackstopBEmisData(t *testing.T) {
	poolAddr := randomContractAddr(t)
	key := vecScVal(symScVal("BEmisData"), contractAddrScVal(t, poolAddr))

	t.Run("Some", func(t *testing.T) {
		val := mapValScVal(mapScVal(
			symEntry("eps", u64ScVal(10)),
			symEntry("expiration", u64ScVal(9999)),
			symEntry("index", i128ScVal(1)),
			symEntry("last_time", u64ScVal(500)),
		))

		got, err := DecodeEntry(createdChange(key, val))
		require.NoError(t, err)

		assert.Equal(t, KindBackstopBEmisData, got.Kind)
		assert.Equal(t, poolAddr, got.Pool)
		assert.Equal(t, &EmissionData{Eps: 10, Expiration: 9999, Index: "1", LastTime: 500}, got.EmisData)
	})

	t.Run("None (Option::None on-chain)", func(t *testing.T) {
		got, err := DecodeEntry(createdChange(key, voidScVal()))
		require.NoError(t, err)

		assert.Equal(t, KindBackstopBEmisData, got.Kind)
		assert.Equal(t, poolAddr, got.Pool)
		assert.False(t, got.Removed)
		assert.Nil(t, got.EmisData)
	})
}

func TestDecodeEntry_BackstopUEmisData(t *testing.T) {
	poolAddr := randomContractAddr(t)
	userAddr := randomAccountAddr(t)
	key := vecScVal(symScVal("UEmisData"), mapValScVal(mapScVal(
		symEntry("pool", contractAddrScVal(t, poolAddr)),
		symEntry("user", accountAddrScVal(t, userAddr)),
	)))

	t.Run("Some", func(t *testing.T) {
		val := mapValScVal(mapScVal(
			symEntry("accrued", i128ScVal(10)),
			symEntry("index", i128ScVal(20)),
		))

		got, err := DecodeEntry(createdChange(key, val))
		require.NoError(t, err)

		assert.Equal(t, KindBackstopUEmisData, got.Kind)
		assert.Equal(t, poolAddr, got.Pool)
		assert.Equal(t, userAddr, got.User)
		assert.Equal(t, &UserEmissionData{Accrued: "10", Index: "20"}, got.UserEmis)
	})

	t.Run("None (Option::None on-chain)", func(t *testing.T) {
		got, err := DecodeEntry(createdChange(key, voidScVal()))
		require.NoError(t, err)

		assert.Equal(t, KindBackstopUEmisData, got.Kind)
		assert.Nil(t, got.UserEmis)
	})
}

func TestDecodeEntry_Auction(t *testing.T) {
	user := randomAccountAddr(t)
	assetA := randomContractAddr(t)
	assetB := randomContractAddr(t)

	auctionKey := func(auctType uint32) xdr.ScVal {
		return vecScVal(symScVal("Auction"), mapValScVal(mapScVal(
			symEntry("auct_type", u32ScVal(auctType)),
			symEntry("user", accountAddrScVal(t, user)),
		)))
	}
	val := mapValScVal(mapScVal(
		symEntry("bid", mapValScVal(mapScVal(addrMapEntry(t, assetA, i128ScVal(1000))))),
		symEntry("block", u32ScVal(12345)),
		symEntry("lot", mapValScVal(mapScVal(addrMapEntry(t, assetB, i128ScVal(2000))))),
	))

	names := map[uint32]string{0: "UserLiquidation", 1: "BadDebtAuction", 2: "InterestAuction"}
	for auctType, name := range names {
		t.Run(name, func(t *testing.T) {
			got, err := DecodeEntry(createdChange(auctionKey(auctType), val))
			require.NoError(t, err)

			assert.Equal(t, KindAuction, got.Kind)
			assert.False(t, got.Removed)
			assert.Equal(t, user, got.User)
			assert.Equal(t, int32(auctType), got.AuctionType)
			require.NotNil(t, got.Auction)
			assert.Equal(t, map[string]string{assetA: "1000"}, got.Auction.Bid)
			assert.Equal(t, map[string]string{assetB: "2000"}, got.Auction.Lot)
			assert.Equal(t, uint32(12345), got.Auction.Block)
		})
	}

	t.Run("removal reports identity with no payload", func(t *testing.T) {
		got, err := DecodeEntry(removedChange(auctionKey(1), mapValScVal(mapScVal())))
		require.NoError(t, err)

		assert.Equal(t, KindAuction, got.Kind)
		assert.True(t, got.Removed)
		assert.Equal(t, user, got.User)
		assert.Equal(t, int32(1), got.AuctionType)
		assert.Nil(t, got.Auction)
	})

	t.Run("malformed inner key missing auct_type", func(t *testing.T) {
		key := vecScVal(symScVal("Auction"), mapValScVal(mapScVal(
			symEntry("user", accountAddrScVal(t, user)),
		)))
		_, err := DecodeEntry(createdChange(key, val))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "auct_type")
	})

	t.Run("malformed payload: bid is not a map", func(t *testing.T) {
		badVal := mapValScVal(mapScVal(
			symEntry("bid", u32ScVal(1)),
			symEntry("block", u32ScVal(1)),
			symEntry("lot", mapValScVal(mapScVal())),
		))
		_, err := DecodeEntry(createdChange(auctionKey(0), badVal))
		require.Error(t, err)
	})
}

func TestDecodeEntry_RewardZone(t *testing.T) {
	poolA := randomContractAddr(t)
	poolB := randomContractAddr(t)

	t.Run("decodes the ordered pool list", func(t *testing.T) {
		val := vecScVal(contractAddrScVal(t, poolA), contractAddrScVal(t, poolB))
		got, err := DecodeEntry(createdChange(symScVal("RZ"), val))
		require.NoError(t, err)

		assert.Equal(t, KindRewardZone, got.Kind)
		assert.False(t, got.Removed)
		assert.Equal(t, []string{poolA, poolB}, got.RewardZone)
	})

	t.Run("empty vec decodes to an empty (non-nil) reward zone", func(t *testing.T) {
		got, err := DecodeEntry(createdChange(symScVal("RZ"), vecScVal()))
		require.NoError(t, err)

		assert.Equal(t, KindRewardZone, got.Kind)
		assert.NotNil(t, got.RewardZone)
		assert.Empty(t, got.RewardZone)
	})

	t.Run("removal reports no payload", func(t *testing.T) {
		got, err := DecodeEntry(removedChange(symScVal("RZ"), vecScVal(contractAddrScVal(t, poolA))))
		require.NoError(t, err)

		assert.Equal(t, KindRewardZone, got.Kind)
		assert.True(t, got.Removed)
		assert.Nil(t, got.RewardZone)
	})

	t.Run("malformed: value is not a vec", func(t *testing.T) {
		_, err := DecodeEntry(createdChange(symScVal("RZ"), u32ScVal(1)))
		require.Error(t, err)
	})

	t.Run("malformed: element is not an address", func(t *testing.T) {
		val := vecScVal(contractAddrScVal(t, poolA), u32ScVal(1))
		_, err := DecodeEntry(createdChange(symScVal("RZ"), val))
		require.Error(t, err)
	})
}

func TestDecodeEntry_PoolInstanceAdmin(t *testing.T) {
	baseConfig := func() *xdr.ScMap {
		return mapScVal(
			symEntry("oracle", contractAddrScVal(t, randomContractAddr(t))),
			symEntry("bstop_rate", u32ScVal(1)),
			symEntry("status", u32ScVal(0)),
			symEntry("max_positions", u32ScVal(1)),
			symEntry("min_collateral", i128ScVal(1)),
		)
	}

	t.Run("Admin present", func(t *testing.T) {
		adminAddr := randomAccountAddr(t)
		storage := mapScVal(
			symEntry("Admin", accountAddrScVal(t, adminAddr)),
			symEntry("Config", mapValScVal(baseConfig())),
		)

		got, err := DecodeEntry(createdChange(instanceKeyScVal(), instanceValScVal(storage)))
		require.NoError(t, err)

		assert.Equal(t, KindPoolInstance, got.Kind)
		require.NotNil(t, got.PoolInstance)
		require.NotNil(t, got.PoolInstance.Admin)
		assert.Equal(t, adminAddr, *got.PoolInstance.Admin)
	})

	t.Run("Admin absent leaves the field nil, decode still succeeds", func(t *testing.T) {
		storage := mapScVal(
			symEntry("Config", mapValScVal(baseConfig())),
		)

		got, err := DecodeEntry(createdChange(instanceKeyScVal(), instanceValScVal(storage)))
		require.NoError(t, err)

		assert.Equal(t, KindPoolInstance, got.Kind)
		require.NotNil(t, got.PoolInstance)
		assert.Nil(t, got.PoolInstance.Admin)
	})

	t.Run("backstop instance (no Config) stays ignored even with an Admin-like key", func(t *testing.T) {
		storage := mapScVal(
			symEntry("Admin", accountAddrScVal(t, randomAccountAddr(t))),
			symEntry("Emitter", contractAddrScVal(t, randomContractAddr(t))),
		)

		got, err := DecodeEntry(createdChange(instanceKeyScVal(), instanceValScVal(storage)))
		require.NoError(t, err)
		assert.Equal(t, KindIgnored, got.Kind)
	})
}

func TestDecodeEntry_Removal(t *testing.T) {
	t.Run("ResConfig removal reports identity with no payload", func(t *testing.T) {
		assetAddr := randomContractAddr(t)
		key := vecScVal(symScVal("ResConfig"), contractAddrScVal(t, assetAddr))
		// Deliberately incomplete: on a removal the payload must never be
		// decoded, so this would error out if it were.
		val := mapValScVal(mapScVal(symEntry("c_factor", u32ScVal(1))))

		got, err := DecodeEntry(removedChange(key, val))
		require.NoError(t, err)

		assert.Equal(t, KindResConfig, got.Kind)
		assert.True(t, got.Removed)
		assert.Equal(t, assetAddr, got.Asset)
		assert.Nil(t, got.ResConfig)
	})

	t.Run("Positions removal reports identity with no payload", func(t *testing.T) {
		userAddr := randomAccountAddr(t)
		key := vecScVal(symScVal("Positions"), accountAddrScVal(t, userAddr))

		got, err := DecodeEntry(removedChange(key, mapValScVal(mapScVal())))
		require.NoError(t, err)

		assert.Equal(t, KindPositions, got.Kind)
		assert.True(t, got.Removed)
		assert.Equal(t, userAddr, got.User)
		assert.Nil(t, got.Positions)
	})

	t.Run("backstop UserBalance removal reports pool and user with no payload", func(t *testing.T) {
		poolAddr := randomContractAddr(t)
		userAddr := randomAccountAddr(t)
		key := vecScVal(symScVal("UserBalance"), mapValScVal(mapScVal(
			symEntry("pool", contractAddrScVal(t, poolAddr)),
			symEntry("user", accountAddrScVal(t, userAddr)),
		)))

		got, err := DecodeEntry(removedChange(key, mapValScVal(mapScVal())))
		require.NoError(t, err)

		assert.Equal(t, KindBackstopUserBalance, got.Kind)
		assert.True(t, got.Removed)
		assert.Equal(t, poolAddr, got.Pool)
		assert.Equal(t, userAddr, got.User)
		assert.Nil(t, got.BackstopUserBalance)
	})

	t.Run("pool instance removal still classifies as KindPoolInstance from Pre storage", func(t *testing.T) {
		storage := mapScVal(
			symEntry("Config", mapValScVal(mapScVal(
				symEntry("oracle", contractAddrScVal(t, randomContractAddr(t))),
				symEntry("bstop_rate", u32ScVal(1)),
				symEntry("status", u32ScVal(0)),
				symEntry("max_positions", u32ScVal(1)),
				symEntry("min_collateral", i128ScVal(1)),
			))),
		)

		got, err := DecodeEntry(removedChange(instanceKeyScVal(), instanceValScVal(storage)))
		require.NoError(t, err)

		assert.Equal(t, KindPoolInstance, got.Kind)
		assert.True(t, got.Removed)
		assert.Nil(t, got.PoolInstance)
	})
}

func TestDecodeEntry_UnknownKeys(t *testing.T) {
	t.Run("unrecognized vec key symbol is ignored, not an error", func(t *testing.T) {
		key := vecScVal(symScVal("SomeFutureKey"), contractAddrScVal(t, randomContractAddr(t)))
		got, err := DecodeEntry(createdChange(key, u32ScVal(1)))
		require.NoError(t, err)
		assert.Equal(t, KindIgnored, got.Kind)
	})

	t.Run("known-but-unmodeled vec keys are ignored", func(t *testing.T) {
		for _, sym := range []string{"ResInit", "RzEmis", "PoolUSDC"} {
			key := vecScVal(symScVal(sym), contractAddrScVal(t, randomContractAddr(t)))
			got, err := DecodeEntry(createdChange(key, u32ScVal(1)))
			require.NoError(t, err)
			assert.Equal(t, KindIgnored, got.Kind, "symbol %s should be ignored", sym)
		}
	})

	t.Run("bare symbol keys are ignored", func(t *testing.T) {
		for _, sym := range []string{"ResList", "LastDist", "PoolEmis", "PropAdmin", "DropList", "BackfillEmis", "Backfill"} {
			got, err := DecodeEntry(createdChange(symScVal(sym), u32ScVal(1)))
			require.NoError(t, err)
			assert.Equal(t, KindIgnored, got.Kind, "bare symbol %s should be ignored", sym)
		}
	})

	t.Run("unrecognized bare symbol key is ignored", func(t *testing.T) {
		got, err := DecodeEntry(createdChange(symScVal("DropListSomeFutureKey"), u32ScVal(1)))
		require.NoError(t, err)
		assert.Equal(t, KindIgnored, got.Kind)
	})
}

func TestDecodeEntry_Malformed(t *testing.T) {
	t.Run("ResConfig value is not a map", func(t *testing.T) {
		key := vecScVal(symScVal("ResConfig"), contractAddrScVal(t, randomContractAddr(t)))
		_, err := DecodeEntry(createdChange(key, u32ScVal(5)))
		require.Error(t, err)
	})

	t.Run("ResConfig map missing a required field", func(t *testing.T) {
		key := vecScVal(symScVal("ResConfig"), contractAddrScVal(t, randomContractAddr(t)))
		val := mapValScVal(mapScVal(
			symEntry("decimals", u32ScVal(7)),
			// c_factor deliberately omitted.
		))
		_, err := DecodeEntry(createdChange(key, val))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "c_factor")
	})

	t.Run("ResConfig key arg is not an address", func(t *testing.T) {
		key := vecScVal(symScVal("ResConfig"), u32ScVal(1))
		_, err := DecodeEntry(createdChange(key, mapValScVal(mapScVal())))
		require.Error(t, err)
	})

	t.Run("pool instance Config is not a map", func(t *testing.T) {
		storage := mapScVal(symEntry("Config", u32ScVal(1)))
		_, err := DecodeEntry(createdChange(instanceKeyScVal(), instanceValScVal(storage)))
		require.Error(t, err)
	})

	t.Run("instance value is not ScContractInstance", func(t *testing.T) {
		_, err := DecodeEntry(createdChange(instanceKeyScVal(), u32ScVal(1)))
		require.Error(t, err)
	})

	t.Run("change is not a ContractData entry", func(t *testing.T) {
		change := ingest.Change{
			Type: xdr.LedgerEntryTypeAccount,
			Post: &xdr.LedgerEntry{
				Data: xdr.LedgerEntryData{
					Type:    xdr.LedgerEntryTypeAccount,
					Account: &xdr.AccountEntry{},
				},
			},
		}
		_, err := DecodeEntry(change)
		require.Error(t, err)
	})

	t.Run("change has neither Pre nor Post", func(t *testing.T) {
		_, err := DecodeEntry(ingest.Change{})
		require.Error(t, err)
	})
}
