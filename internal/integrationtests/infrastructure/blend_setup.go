// Package infrastructure provides Soroban transaction helpers for integration tests.
//
// This file deploys a full Blend v2 protocol stack (Comet AMM backstop LP token, mock SEP-40
// oracle, emitter, pool factory, backstop, and one lending pool with two reserves) on the
// standalone network, using the typed contract wrappers in blend_contracts.go and the ScVal
// builders in blend_operations.go.
package infrastructure

import (
	"context"
	"crypto/rand"
	"math/big"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/txnbuild"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/services"
)

// blndPinnedSACAddress is the C-address blndTokenAddress (internal/services/blend/validator.go)
// pins for the standalone network's networkPassphrase. BLND is deployed here as classic asset
// "BLND:<master G-address>", so its SAC address is a deterministic function of the master
// keypair (which is itself keypair.Root(networkPassphrase)) and the network passphrase alone.
// SetupBlendStack asserts the SAC it deploys matches this constant so a drift in either pin goes
// noticed immediately rather than silently degrading blend/validator's price/emissions lookups.
const blndPinnedSACAddress = "CDYLJJT2VBKY55ZK57MTMKAVRCRPQMYB4YJ7JFFARMSJZ73I5CMCITSU"

// backstopPinnedAddress is the C-address canonicalBackstopAddress
// (internal/services/blend/validator.go) pins for the standalone network's
// networkPassphrase. The backstop is deployed by the master keypair (itself
// keypair.Root(networkPassphrase)) with a fixed salt (0xB5), so its address is
// a deterministic function of the passphrase alone. SetupBlendStack asserts
// the backstop it deploys matches this constant so a drift in either pin goes
// noticed immediately — a mismatch would make the processor drop every
// backstop-shaped entry and event as a non-canonical impostor.
const backstopPinnedAddress = "CARICDGXKY6NZVNAHW5UHWUTOUB4QP4RL2B6PUN4BTPQZ6LC4RGPARED"

// BlendStack holds the contract addresses and actor keypairs of a Blend v2 deployment on the
// standalone network, produced by SetupBlendStack.
type BlendStack struct {
	// StartLedger is the RPC latest ledger captured before the first Blend transaction was
	// submitted, so later tests (e.g. a windowed protocol-migrate run) know where Blend history
	// begins.
	StartLedger uint32

	Whale, Supplier, Borrower, Filler *keypair.Full

	BLNDTokenID, USDCTokenID, XLMTokenID string

	CometFactoryID, CometID, OracleID, EmitterID string

	PoolFactoryID, BackstopID, PoolID string

	PoolName string
}

// blendReserveConfigs returns the ReserveConfig values SetupBlendStack queues for the USDC (index
// 0) and XLM (index 1) reserves. Every field except index/decimals/enabled/supply_cap is mirrored
// verbatim from blend-capital/blend-utils (pinned commit b05242df30b6b6caf9d317646f754541824a5a8b)
// src/v2/testing-scripts/mock-example.ts's "Testnet Pool" reserve setup:
//   - USDC values from testnetPoolUsdcReserveMetaData (lines 188-202): c_factor=950_0000,
//     l_factor=950_0000, util=700_0000, max_util=950_0000, r_base=5000, r_one=30_0000,
//     r_two=100_0000, r_three=1_000_0000, reactivity=20.
//   - XLM values from testnetPoolXlmReserveMetaData (lines 116-130): c_factor=900_0000,
//     l_factor=900_0000, util=500_0000, max_util=950_0000, r_base=5000, r_one=30_0000,
//     r_two=200_0000, r_three=1_000_0000, reactivity=50.
//
// supply_cap mirrors that file's I128MAX (blend-sdk's max positive i128, 2^127-1) for both
// reserves, since a real supply cap is irrelevant to a test pool and blend-utils itself uses the
// unbounded sentinel.
func blendReserveConfigs() (usdc, xlm BlendReserveConfig) {
	i128Max := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 127), big.NewInt(1))

	usdc = BlendReserveConfig{
		CFactor:    9_500_000,
		Decimals:   7,
		Enabled:    true,
		Index:      0,
		LFactor:    9_500_000,
		MaxUtil:    9_500_000,
		RBase:      5000,
		ROne:       300_000,
		RThree:     10_000_000,
		RTwo:       1_000_000,
		Reactivity: 20,
		SupplyCap:  i128Max,
		Util:       7_000_000,
	}
	xlm = BlendReserveConfig{
		CFactor:    9_000_000,
		Decimals:   7,
		Enabled:    true,
		Index:      1,
		LFactor:    9_000_000,
		MaxUtil:    9_500_000,
		RBase:      5000,
		ROne:       300_000,
		RThree:     10_000_000,
		RTwo:       2_000_000,
		Reactivity: 50,
		SupplyCap:  i128Max,
		Util:       5_000_000,
	}
	return usdc, xlm
}

// nativeAssetContractAddress computes the C-address of the native (XLM) Stellar Asset Contract,
// which deployNativeAssetSAC (account_setup.go) deploys during setupContracts but does not store
// on SharedContainers. Mirrors createUSDCTrustlines' ContractID-then-strkey-encode pattern for
// the classic credit assets.
func nativeAssetContractAddress(t *testing.T) string {
	t.Helper()

	contractID, err := xdr.Asset{Type: xdr.AssetTypeAssetTypeNative}.ContractID(networkPassphrase)
	require.NoError(t, err, "computing native asset contract ID")
	return strkey.MustEncode(strkey.VersionByteContract, contractID[:])
}

// SetupBlendStack deploys a full Blend v2 protocol stack on the standalone network: a Comet AMM
// pool holding the BLND/USDC backstop LP token, a mock SEP-40 oracle, the emitter, the pool
// factory, the backstop, and one lending pool ("TestPool") with USDC (index 0) and XLM (index 1)
// reserves, funded and activated so it is ready to accept Supply/Borrow/etc. requests.
//
// It reuses the USDC SAC and native XLM SAC already deployed by setupContracts (via
// s.usdcContractAddress and nativeAssetContractAddress) and deploys a new classic asset, BLND,
// issued by the master account, asserting its SAC address matches the pin in
// internal/services/blend/validator.go.
//
// Every wrapper call below that drives the shared master account (s.masterKeyPair) through
// executeSorobanOperationAs — i.e. every blend_contracts.go wrapper with an admin/caller/
// controller parameter set to master — fetches its sequence fresh from RPC and does not update
// s.masterAccount's locally tracked counter (see executeSorobanOperationAs's doc comment). Any
// subsequent call that goes through executeSorobanOperation or executeClassicOperation (upload,
// deploy, classic ops), which DO rely on that local counter, would then submit with a stale
// sequence and fail. SetupBlendStack calls s.SyncMasterSequence at each such transition; look for
// the "re-sync" comments below.
func (s *SharedContainers) SetupBlendStack(ctx context.Context, t *testing.T, rpcService services.RPCService) *BlendStack {
	t.Helper()

	s.SyncMasterSequence(ctx, t)

	// Capture the RPC latest ledger BEFORE any Blend transaction, so later tests know where
	// Blend-specific history begins.
	health, err := rpcService.GetHealth()
	require.NoError(t, err, "getting RPC health for Blend stack start ledger")
	startLedger := health.LatestLedger

	stack := &BlendStack{
		StartLedger: startLedger,
		Whale:       keypair.MustRandom(),
		Supplier:    keypair.MustRandom(),
		Borrower:    keypair.MustRandom(),
		Filler:      keypair.MustRandom(),
		PoolName:    "TestPool",
	}

	s.CreateAndFundAccounts(ctx, t, []*keypair.Full{stack.Whale, stack.Supplier, stack.Borrower, stack.Filler})
	log.Ctx(ctx).Info("✅ Funded Blend actor accounts (whale, supplier, borrower, filler)")

	// ---------------------------------------------------------------------------------------
	// Assets: BLND (new classic asset), USDC (reused), XLM (reused).
	// ---------------------------------------------------------------------------------------
	blndAsset := txnbuild.CreditAsset{Code: "BLND", Issuer: s.masterKeyPair.Address()}
	blndXDRAsset, err := blndAsset.ToXDR()
	require.NoError(t, err, "converting BLND asset to XDR")
	blndContractID, err := blndXDRAsset.ContractID(networkPassphrase)
	require.NoError(t, err, "computing BLND SAC contract ID")
	stack.BLNDTokenID = strkey.MustEncode(strkey.VersionByteContract, blndContractID[:])
	require.Equal(t, blndPinnedSACAddress, stack.BLNDTokenID, "BLND SAC address must match the pin in internal/services/blend/validator.go")

	s.deployCreditAssetSAC(ctx, t, "BLND", s.masterKeyPair.Address())
	log.Ctx(ctx).Infof("✅ Deployed BLND SAC at %s", stack.BLNDTokenID)

	stack.USDCTokenID = s.usdcContractAddress
	stack.XLMTokenID = nativeAssetContractAddress(t)

	// Trustlines + mints: whale gets 500,100 BLND + 12,501 USDC; supplier/borrower/filler get
	// 5,000 USDC each plus an empty BLND trustline — BLND is a classic-asset SAC, so a G-account
	// cannot receive claimed BLND emissions (pool.claim pays out via transfer_from) without a
	// classic trustline. One batched transaction, mirroring createUSDCTrustlines.
	usdcAsset := txnbuild.CreditAsset{Code: "USDC", Issuer: s.masterKeyPair.Address()}
	fundOps := []txnbuild.Operation{
		&txnbuild.ChangeTrust{Line: txnbuild.ChangeTrustAssetWrapper{Asset: blndAsset}, Limit: DefaultTrustlineLimit, SourceAccount: stack.Whale.Address()},
		&txnbuild.Payment{Destination: stack.Whale.Address(), Amount: "500100", Asset: blndAsset, SourceAccount: s.masterKeyPair.Address()},
		&txnbuild.ChangeTrust{Line: txnbuild.ChangeTrustAssetWrapper{Asset: usdcAsset}, Limit: DefaultTrustlineLimit, SourceAccount: stack.Whale.Address()},
		&txnbuild.Payment{Destination: stack.Whale.Address(), Amount: "12501", Asset: usdcAsset, SourceAccount: s.masterKeyPair.Address()},
	}
	for _, actor := range []*keypair.Full{stack.Supplier, stack.Borrower, stack.Filler} {
		fundOps = append(fundOps,
			&txnbuild.ChangeTrust{Line: txnbuild.ChangeTrustAssetWrapper{Asset: blndAsset}, Limit: DefaultTrustlineLimit, SourceAccount: actor.Address()},
			&txnbuild.ChangeTrust{Line: txnbuild.ChangeTrustAssetWrapper{Asset: usdcAsset}, Limit: DefaultTrustlineLimit, SourceAccount: actor.Address()},
			&txnbuild.Payment{Destination: actor.Address(), Amount: "5000", Asset: usdcAsset, SourceAccount: s.masterKeyPair.Address()},
		)
	}
	_, err = executeClassicOperation(ctx, t, s, fundOps, []*keypair.Full{s.masterKeyPair, stack.Whale, stack.Supplier, stack.Borrower, stack.Filler})
	require.NoError(t, err, "funding Blend actors with BLND/USDC")
	log.Ctx(ctx).Info("✅ Funded Blend actors with BLND/USDC trustlines and balances")

	dir := blendTestdataDir(t)

	// ---------------------------------------------------------------------------------------
	// Comet AMM: backstop LP token pool.
	// ---------------------------------------------------------------------------------------
	cometWasmBytes, err := os.ReadFile(filepath.Join(dir, "comet.wasm"))
	require.NoError(t, err, "reading comet.wasm")
	cometWasmHash := s.uploadContractWasm(ctx, t, cometWasmBytes)

	cometFactoryWasmBytes, err := os.ReadFile(filepath.Join(dir, "comet_factory.wasm"))
	require.NoError(t, err, "reading comet_factory.wasm")
	cometFactoryWasmHash := s.uploadContractWasm(ctx, t, cometFactoryWasmBytes)

	stack.CometFactoryID = s.deployContractWithConstructor(ctx, t, cometFactoryWasmHash, []xdr.ScVal{})
	log.Ctx(ctx).Infof("✅ Deployed Comet factory at %s", stack.CometFactoryID)

	s.CometFactoryInit(ctx, t, stack.CometFactoryID, s.masterKeyPair, cometWasmHash)

	var cometSalt [32]byte
	_, err = rand.Read(cometSalt[:])
	require.NoError(t, err, "generating comet pool salt")
	stack.CometID = s.CometFactoryNewPool(
		ctx, t, stack.CometFactoryID, s.masterKeyPair, cometSalt,
		[]string{stack.BLNDTokenID, stack.USDCTokenID},
		[]*big.Int{big.NewInt(8_000_000), big.NewInt(2_000_000)},        // weights: 0.8e7 BLND / 0.2e7 USDC
		[]*big.Int{big.NewInt(10_000_000_000), big.NewInt(250_000_000)}, // balances: 1000e7 BLND / 25e7 USDC
		big.NewInt(30_000), // swap fee: 0.003e7
	)
	log.Ctx(ctx).Infof("✅ Deployed Comet pool at %s", stack.CometID)

	s.CometJoinPool(
		ctx, t, stack.CometID, stack.Whale,
		big.NewInt(500_000_000_000), // poolAmountOut: 50,000e7
		[]*big.Int{big.NewInt(5_001_000_000_000), big.NewInt(125_010_000_000)}, // maxAmountsIn: 500,100e7 BLND / 12,501e7 USDC
	)
	log.Ctx(ctx).Info("✅ Whale joined Comet pool")

	// ---------------------------------------------------------------------------------------
	// Mock SEP-40 oracle.
	// ---------------------------------------------------------------------------------------
	s.SyncMasterSequence(ctx, t) // re-sync before the next executeSorobanOperation-driven (master) call.

	oracleWasmBytes, err := os.ReadFile(filepath.Join(dir, "oracle.wasm"))
	require.NoError(t, err, "reading oracle.wasm")
	oracleWasmHash := s.uploadContractWasm(ctx, t, oracleWasmBytes)
	stack.OracleID = s.deployContractWithConstructor(ctx, t, oracleWasmHash, []xdr.ScVal{})
	log.Ctx(ctx).Infof("✅ Deployed mock oracle at %s", stack.OracleID)

	s.OracleSetData(ctx, t, stack.OracleID, s.masterKeyPair, []string{stack.USDCTokenID, stack.XLMTokenID, stack.BLNDTokenID}, 7, 300)
	s.OracleSetPriceStable(ctx, t, stack.OracleID, s.masterKeyPair, []*big.Int{
		big.NewInt(10_000_000), // USDC $1.00
		big.NewInt(1_000_000),  // XLM $0.10
		big.NewInt(500_000),    // BLND $0.05
	})
	log.Ctx(ctx).Info("✅ Set oracle data and stable prices")

	// ---------------------------------------------------------------------------------------
	// Emitter (initialized with the backstop's precomputed address).
	// ---------------------------------------------------------------------------------------
	s.SyncMasterSequence(ctx, t) // re-sync: OracleSetData/OracleSetPriceStable drove master through executeSorobanOperationAs.

	emitterWasmBytes, err := os.ReadFile(filepath.Join(dir, "emitter.wasm"))
	require.NoError(t, err, "reading emitter.wasm")
	emitterWasmHash := s.uploadContractWasm(ctx, t, emitterWasmBytes)
	stack.EmitterID = s.deployContractWithConstructor(ctx, t, emitterWasmHash, []xdr.ScVal{})
	log.Ctx(ctx).Infof("✅ Deployed emitter at %s", stack.EmitterID)

	var backstopSalt [32]byte
	backstopSalt[0] = 0xB5
	stack.BackstopID, err = PrecomputeContractAddress(s.masterKeyPair.Address(), backstopSalt)
	require.NoError(t, err, "precomputing backstop address")
	require.Equal(t, backstopPinnedAddress, stack.BackstopID,
		"backstop address must match the canonical-backstop pin in internal/services/blend/validator.go")

	s.EmitterInitialize(ctx, t, stack.EmitterID, s.masterKeyPair, stack.BLNDTokenID, stack.BackstopID, stack.CometID)
	log.Ctx(ctx).Infof("✅ Initialized emitter (backstop precomputed at %s)", stack.BackstopID)

	// ---------------------------------------------------------------------------------------
	// Pool factory (constructed with PoolInitMeta, referencing the precomputed backstop).
	// ---------------------------------------------------------------------------------------
	s.SyncMasterSequence(ctx, t) // re-sync: EmitterInitialize drove master through executeSorobanOperationAs.

	poolFactoryWasmBytes, err := os.ReadFile(filepath.Join(dir, "pool_factory.wasm"))
	require.NoError(t, err, "reading pool_factory.wasm")
	poolFactoryWasmHash := s.uploadContractWasm(ctx, t, poolFactoryWasmBytes)

	poolWasmBytes, err := os.ReadFile(blendPoolWasmPath(t))
	require.NoError(t, err, "reading blend_pool_v2.wasm")
	poolWasmHash := s.uploadContractWasm(ctx, t, poolWasmBytes)

	poolInitMeta := scMap(t,
		scMapEntry{key: "backstop", val: scAddr(t, stack.BackstopID)},
		scMapEntry{key: "blnd_id", val: scAddr(t, stack.BLNDTokenID)},
		scMapEntry{key: "pool_hash", val: scBytes32(poolWasmHash)},
	)
	stack.PoolFactoryID = s.deployContractWithConstructor(ctx, t, poolFactoryWasmHash, []xdr.ScVal{poolInitMeta})
	log.Ctx(ctx).Infof("✅ Deployed pool factory at %s", stack.PoolFactoryID)

	// ---------------------------------------------------------------------------------------
	// Backstop (deployed at the precomputed address, verified to match).
	// ---------------------------------------------------------------------------------------
	backstopWasmBytes, err := os.ReadFile(blendBackstopWasmPath(t))
	require.NoError(t, err, "reading blend_backstop_v2.wasm")
	backstopWasmHash := s.uploadContractWasm(ctx, t, backstopWasmBytes)

	backstopCtorArgs := []xdr.ScVal{
		scAddr(t, stack.CometID),       // backstop_token
		scAddr(t, stack.EmitterID),     // emitter
		scAddr(t, stack.BLNDTokenID),   // blnd_token
		scAddr(t, stack.USDCTokenID),   // usdc_token
		scAddr(t, stack.PoolFactoryID), // pool_factory
		scVec(),                        // drop_list: empty Vec<(Address, i128)>
	}
	deployedBackstopID := s.deployContractWithSalt(ctx, t, backstopWasmHash, backstopCtorArgs, backstopSalt)
	require.Equal(t, stack.BackstopID, deployedBackstopID, "backstop must deploy at its precomputed address")
	log.Ctx(ctx).Infof("✅ Deployed backstop at %s", stack.BackstopID)

	// ---------------------------------------------------------------------------------------
	// Pool ("TestPool"), reserves (USDC index 0, XLM index 1), and emissions config.
	// ---------------------------------------------------------------------------------------
	var poolSalt [32]byte
	_, err = rand.Read(poolSalt[:])
	require.NoError(t, err, "generating pool salt")
	stack.PoolID = s.PoolFactoryDeploy(ctx, t, stack.PoolFactoryID, s.masterKeyPair, stack.PoolName, poolSalt, stack.OracleID, 1_000_000, 4, big.NewInt(0))
	log.Ctx(ctx).Infof("✅ Deployed pool %q at %s", stack.PoolName, stack.PoolID)

	usdcReserveConfig, xlmReserveConfig := blendReserveConfigs()

	s.PoolQueueSetReserve(ctx, t, stack.PoolID, s.masterKeyPair, stack.USDCTokenID, usdcReserveConfig)
	s.PoolSetReserve(ctx, t, stack.PoolID, s.masterKeyPair, stack.USDCTokenID)
	s.PoolQueueSetReserve(ctx, t, stack.PoolID, s.masterKeyPair, stack.XLMTokenID, xlmReserveConfig)
	s.PoolSetReserve(ctx, t, stack.PoolID, s.masterKeyPair, stack.XLMTokenID)
	log.Ctx(ctx).Info("✅ Set USDC (index 0) and XLM (index 1) reserves")

	s.PoolSetEmissionsConfig(ctx, t, stack.PoolID, s.masterKeyPair, []BlendEmissionMetadata{
		{ResIndex: 0, ResType: 1, Share: 5_000_000}, // USDC bToken supply
		{ResIndex: 1, ResType: 0, Share: 5_000_000}, // XLM dToken liabilities
	})
	log.Ctx(ctx).Info("✅ Set pool emissions config")

	// ---------------------------------------------------------------------------------------
	// Backstop funding + pool activation.
	// ---------------------------------------------------------------------------------------
	s.BackstopDeposit(ctx, t, stack.BackstopID, stack.Whale, stack.PoolID, big.NewInt(500_000_000_000)) // 50,000e7
	s.PoolSetStatus(ctx, t, stack.PoolID, s.masterKeyPair, 0)
	s.BackstopAddReward(ctx, t, stack.BackstopID, s.masterKeyPair, stack.PoolID)
	log.Ctx(ctx).Info("✅ Backstop funded and pool activated (status 0)")

	// The emitter must be able to mint BLND (1 BLND/sec on distribute).
	s.SACSetAdmin(ctx, t, stack.BLNDTokenID, s.masterKeyPair, stack.EmitterID)
	log.Ctx(ctx).Infof("✅ Transferred BLND SAC admin to emitter %s", stack.EmitterID)

	s.SyncMasterSequence(ctx, t)

	return stack
}

// blendTestdataDir resolves the directory holding the Blend auxiliary contract WASM files
// (comet, comet_factory, oracle, emitter, pool_factory), which live alongside this source file.
func blendTestdataDir(t *testing.T) string {
	t.Helper()

	_, filename, _, ok := runtime.Caller(0)
	require.True(t, ok, "failed to get caller information")
	return filepath.Join(filepath.Dir(filename), "testdata", "blend")
}

// blendPoolWasmPath resolves the path to the Blend v2 pool contract WASM, which lives under
// internal/services/blend/testdata rather than this package's own testdata directory (see
// testdata/blend/README.md).
func blendPoolWasmPath(t *testing.T) string {
	t.Helper()

	_, filename, _, ok := runtime.Caller(0)
	require.True(t, ok, "failed to get caller information")
	return filepath.Join(filepath.Dir(filename), "..", "..", "services", "blend", "testdata", "blend_pool_v2.wasm")
}

// blendBackstopWasmPath resolves the path to the Blend v2 backstop contract WASM; see
// blendPoolWasmPath.
func blendBackstopWasmPath(t *testing.T) string {
	t.Helper()

	_, filename, _, ok := runtime.Caller(0)
	require.True(t, ok, "failed to get caller information")
	return filepath.Join(filepath.Dir(filename), "..", "..", "services", "blend", "testdata", "blend_backstop_v2.wasm")
}
