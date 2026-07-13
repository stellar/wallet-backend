// Package infrastructure provides Soroban transaction helpers for integration tests.
//
// This file adds thin, typed wrappers over executeSorobanOperationAs for each Blend v2 contract
// call the integration tests need to drive. Each wrapper builds an InvokeHostFunction from a
// contract address, function name, and ScVal args (using the builders in blend_operations.go),
// executes it, and returns either the transaction hash or a decoded return value.
//
// All wrappers take an explicit contract-address string rather than a stack struct — assembling
// wrappers into a full deployed-stack abstraction is deferred to a later task.
package infrastructure

import (
	"context"
	"encoding/base64"
	"fmt"
	"math/big"
	"testing"

	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/txnbuild"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/require"
)

// scAddressVec builds a Vec<Address> ScVal from the given addresses, in order.
func scAddressVec(t *testing.T, addrs []string) xdr.ScVal {
	t.Helper()

	vals := make([]xdr.ScVal, 0, len(addrs))
	for _, a := range addrs {
		vals = append(vals, scAddr(t, a))
	}
	return scVec(vals...)
}

// scU32Vec builds a Vec<u32> ScVal from the given values, in order.
func scU32Vec(vals []uint32) xdr.ScVal {
	scVals := make([]xdr.ScVal, 0, len(vals))
	for _, v := range vals {
		scVals = append(scVals, scU32(v))
	}
	return scVec(scVals...)
}

// scI128Vec builds a Vec<i128> ScVal from the given values, in order.
func scI128Vec(t *testing.T, vals []*big.Int) xdr.ScVal {
	t.Helper()

	scVals := make([]xdr.ScVal, 0, len(vals))
	for _, v := range vals {
		scVals = append(scVals, scI128(t, v))
	}
	return scVec(scVals...)
}

// scVoid builds an ScvVoid ScVal, used to encode Option::None.
func scVoid() xdr.ScVal {
	return xdr.ScVal{Type: xdr.ScValTypeScvVoid}
}

// buildInvokeOp builds an InvokeHostFunction operation invoking functionName on the contract at
// contractAddress with args, sourced from sourceAddress.
func buildInvokeOp(t *testing.T, contractAddress, functionName string, args []xdr.ScVal, sourceAddress string) *txnbuild.InvokeHostFunction {
	t.Helper()

	contractScAddress, err := parseAddressToScAddress(contractAddress)
	require.NoError(t, err, "parsing contract address %s", contractAddress)

	return &txnbuild.InvokeHostFunction{
		HostFunction: xdr.HostFunction{
			Type: xdr.HostFunctionTypeHostFunctionTypeInvokeContract,
			InvokeContract: &xdr.InvokeContractArgs{
				ContractAddress: contractScAddress,
				FunctionName:    xdr.ScSymbol(functionName),
				Args:            xdr.ScVec(args),
			},
		},
		SourceAccount: sourceAddress,
	}
}

// invokeResultAddress fetches the confirmed transaction identified by hash and decodes its Soroban
// return value as an Address, returning the address in G-/C- string form. Used by contract
// factories (comet new_c_pool, pool factory deploy) whose invoke return value is the address of
// the contract they just deployed.
//
// Handles both TransactionMetaV3 (SorobanTransactionMeta.ReturnValue, a plain ScVal) and
// TransactionMetaV4 (SorobanTransactionMetaV2.ReturnValue, a *ScVal) — mirroring
// ExtractClaimableBalanceIDsFromMeta's meta-version handling, extended to also cover V3 since the
// return value (unlike claimable balance creation, which that helper looks for) is populated by
// both meta versions.
func (s *SharedContainers) invokeResultAddress(ctx context.Context, t *testing.T, hash string) (string, error) {
	t.Helper()

	rpcURL, err := s.RPCContainer.GetConnectionString(ctx)
	if err != nil {
		return "", fmt.Errorf("getting RPC connection string: %w", err)
	}

	txResult, err := getTransactionFromRPC(s.httpClient, rpcURL, hash)
	if err != nil {
		return "", fmt.Errorf("fetching transaction %s: %w", hash, err)
	}

	metaBytes, err := base64.StdEncoding.DecodeString(txResult.ResultMetaXDR)
	if err != nil {
		return "", fmt.Errorf("decoding result meta XDR: %w", err)
	}

	var txMeta xdr.TransactionMeta
	if err := xdr.SafeUnmarshal(metaBytes, &txMeta); err != nil {
		return "", fmt.Errorf("unmarshalling transaction meta: %w", err)
	}

	var returnValue xdr.ScVal
	switch txMeta.V {
	case 3:
		v3 := txMeta.MustV3()
		if v3.SorobanMeta == nil {
			return "", fmt.Errorf("transaction %s meta v3 has no soroban meta", hash)
		}
		returnValue = v3.SorobanMeta.ReturnValue
	case 4:
		v4 := txMeta.MustV4()
		if v4.SorobanMeta == nil || v4.SorobanMeta.ReturnValue == nil {
			return "", fmt.Errorf("transaction %s meta v4 has no soroban return value", hash)
		}
		returnValue = *v4.SorobanMeta.ReturnValue
	default:
		return "", fmt.Errorf("transaction %s has unsupported meta version %d", hash, txMeta.V)
	}

	if returnValue.Type != xdr.ScValTypeScvAddress || returnValue.Address == nil {
		return "", fmt.Errorf("transaction %s return value is %v, not an address", hash, returnValue.Type)
	}

	return scAddressToString(*returnValue.Address)
}

// ---------------------------------------------------------------------------------------------
// Pool (blend_pool_v2)
// ---------------------------------------------------------------------------------------------

// PoolSubmit invokes submit(from, spender, to, requests) on the pool, with from=spender=to=user.
func (s *SharedContainers) PoolSubmit(ctx context.Context, t *testing.T, poolID string, user *keypair.Full, requests []BlendRequest) string {
	t.Helper()

	userAddr := scAddr(t, user.Address())
	args := []xdr.ScVal{userAddr, userAddr, userAddr, scRequestVec(t, requests)}
	op := buildInvokeOp(t, poolID, "submit", args, user.Address())

	hash, err := s.executeSorobanOperationAs(ctx, t, op, user, nil, DefaultConfirmationRetries)
	require.NoError(t, err, "submitting pool requests")
	return hash
}

// PoolClaim invokes claim(from, reserve_token_ids, to) on the pool, with from=to=user.
func (s *SharedContainers) PoolClaim(ctx context.Context, t *testing.T, poolID string, user *keypair.Full, reserveTokenIDs []uint32) string {
	t.Helper()

	userAddr := scAddr(t, user.Address())
	args := []xdr.ScVal{userAddr, scU32Vec(reserveTokenIDs), userAddr}
	op := buildInvokeOp(t, poolID, "claim", args, user.Address())

	hash, err := s.executeSorobanOperationAs(ctx, t, op, user, nil, DefaultConfirmationRetries)
	require.NoError(t, err, "claiming pool emissions")
	return hash
}

// PoolNewAuction invokes new_auction(auction_type, user, bid, lot, percent) on the pool, signed by
// caller. The auctioned user is passed as a plain address argument, not a signer.
func (s *SharedContainers) PoolNewAuction(ctx context.Context, t *testing.T, poolID string, caller *keypair.Full, auctionType uint32, user string, bid, lot []string, percent uint32) string {
	t.Helper()

	args := []xdr.ScVal{scU32(auctionType), scAddr(t, user), scAddressVec(t, bid), scAddressVec(t, lot), scU32(percent)}
	op := buildInvokeOp(t, poolID, "new_auction", args, caller.Address())

	hash, err := s.executeSorobanOperationAs(ctx, t, op, caller, nil, DefaultConfirmationRetries)
	require.NoError(t, err, "creating pool auction")
	return hash
}

// PoolSetStatus invokes set_status(pool_status) on the pool, signed by admin.
func (s *SharedContainers) PoolSetStatus(ctx context.Context, t *testing.T, poolID string, admin *keypair.Full, status uint32) string {
	t.Helper()

	args := []xdr.ScVal{scU32(status)}
	op := buildInvokeOp(t, poolID, "set_status", args, admin.Address())

	hash, err := s.executeSorobanOperationAs(ctx, t, op, admin, nil, DefaultConfirmationRetries)
	require.NoError(t, err, "setting pool status")
	return hash
}

// PoolQueueSetReserve invokes queue_set_reserve(asset, metadata) on the pool, signed by admin.
func (s *SharedContainers) PoolQueueSetReserve(ctx context.Context, t *testing.T, poolID string, admin *keypair.Full, asset string, cfg BlendReserveConfig) string {
	t.Helper()

	args := []xdr.ScVal{scAddr(t, asset), scReserveConfig(t, cfg)}
	op := buildInvokeOp(t, poolID, "queue_set_reserve", args, admin.Address())

	hash, err := s.executeSorobanOperationAs(ctx, t, op, admin, nil, DefaultConfirmationRetries)
	require.NoError(t, err, "queueing pool reserve config")
	return hash
}

// PoolSetReserve invokes set_reserve(asset) -> u32 on the pool, signed by admin, executing a
// previously queued reserve config change.
func (s *SharedContainers) PoolSetReserve(ctx context.Context, t *testing.T, poolID string, admin *keypair.Full, asset string) string {
	t.Helper()

	args := []xdr.ScVal{scAddr(t, asset)}
	op := buildInvokeOp(t, poolID, "set_reserve", args, admin.Address())

	hash, err := s.executeSorobanOperationAs(ctx, t, op, admin, nil, DefaultConfirmationRetries)
	require.NoError(t, err, "setting pool reserve")
	return hash
}

// PoolSetEmissionsConfig invokes set_emissions_config(res_emission_metadata) on the pool, signed by
// admin.
func (s *SharedContainers) PoolSetEmissionsConfig(ctx context.Context, t *testing.T, poolID string, admin *keypair.Full, metas []BlendEmissionMetadata) string {
	t.Helper()

	args := []xdr.ScVal{scEmissionMetadataVec(t, metas)}
	op := buildInvokeOp(t, poolID, "set_emissions_config", args, admin.Address())

	hash, err := s.executeSorobanOperationAs(ctx, t, op, admin, nil, DefaultConfirmationRetries)
	require.NoError(t, err, "setting pool emissions config")
	return hash
}

// PoolGulpEmissions invokes gulp_emissions() -> i128 on the pool, signed by caller. This is a
// permissionless call that pulls newly distributed emissions into the pool's local accounting.
func (s *SharedContainers) PoolGulpEmissions(ctx context.Context, t *testing.T, poolID string, caller *keypair.Full) string {
	t.Helper()

	op := buildInvokeOp(t, poolID, "gulp_emissions", nil, caller.Address())

	hash, err := s.executeSorobanOperationAs(ctx, t, op, caller, nil, DefaultConfirmationRetries)
	require.NoError(t, err, "gulping pool emissions")
	return hash
}

// ---------------------------------------------------------------------------------------------
// Backstop (blend_backstop_v2)
// ---------------------------------------------------------------------------------------------

// BackstopDeposit invokes deposit(from, pool_address, amount) -> i128 on the backstop, signed by
// user.
func (s *SharedContainers) BackstopDeposit(ctx context.Context, t *testing.T, backstopID string, user *keypair.Full, poolID string, amount *big.Int) string {
	t.Helper()

	args := []xdr.ScVal{scAddr(t, user.Address()), scAddr(t, poolID), scI128(t, amount)}
	op := buildInvokeOp(t, backstopID, "deposit", args, user.Address())

	hash, err := s.executeSorobanOperationAs(ctx, t, op, user, nil, DefaultConfirmationRetries)
	require.NoError(t, err, "depositing into backstop")
	return hash
}

// BackstopQueueWithdrawal invokes queue_withdrawal(from, pool_address, amount) -> Q4W on the
// backstop, signed by user.
func (s *SharedContainers) BackstopQueueWithdrawal(ctx context.Context, t *testing.T, backstopID string, user *keypair.Full, poolID string, amount *big.Int) string {
	t.Helper()

	args := []xdr.ScVal{scAddr(t, user.Address()), scAddr(t, poolID), scI128(t, amount)}
	op := buildInvokeOp(t, backstopID, "queue_withdrawal", args, user.Address())

	hash, err := s.executeSorobanOperationAs(ctx, t, op, user, nil, DefaultConfirmationRetries)
	require.NoError(t, err, "queueing backstop withdrawal")
	return hash
}

// BackstopDequeueWithdrawal invokes dequeue_withdrawal(from, pool_address, amount) on the
// backstop, signed by user.
func (s *SharedContainers) BackstopDequeueWithdrawal(ctx context.Context, t *testing.T, backstopID string, user *keypair.Full, poolID string, amount *big.Int) string {
	t.Helper()

	args := []xdr.ScVal{scAddr(t, user.Address()), scAddr(t, poolID), scI128(t, amount)}
	op := buildInvokeOp(t, backstopID, "dequeue_withdrawal", args, user.Address())

	hash, err := s.executeSorobanOperationAs(ctx, t, op, user, nil, DefaultConfirmationRetries)
	require.NoError(t, err, "dequeueing backstop withdrawal")
	return hash
}

// BackstopClaim invokes claim(from, pool_addresses, min_lp_tokens_out) -> i128 on the backstop,
// signed by user.
func (s *SharedContainers) BackstopClaim(ctx context.Context, t *testing.T, backstopID string, user *keypair.Full, poolIDs []string, minLPTokensOut *big.Int) string {
	t.Helper()

	args := []xdr.ScVal{scAddr(t, user.Address()), scAddressVec(t, poolIDs), scI128(t, minLPTokensOut)}
	op := buildInvokeOp(t, backstopID, "claim", args, user.Address())

	hash, err := s.executeSorobanOperationAs(ctx, t, op, user, nil, DefaultConfirmationRetries)
	require.NoError(t, err, "claiming backstop emissions")
	return hash
}

// BackstopDistribute invokes distribute() -> i128 on the backstop, signed by caller. This is a
// permissionless call that distributes queued BLND emissions out to backstop pools.
func (s *SharedContainers) BackstopDistribute(ctx context.Context, t *testing.T, backstopID string, caller *keypair.Full) string {
	t.Helper()

	op := buildInvokeOp(t, backstopID, "distribute", nil, caller.Address())

	hash, err := s.executeSorobanOperationAs(ctx, t, op, caller, nil, DefaultConfirmationRetries)
	require.NoError(t, err, "distributing backstop emissions")
	return hash
}

// BackstopAddReward invokes add_reward(to_add, to_remove) on the backstop, signed by caller, with
// to_remove always encoded as Option::None (ScvVoid).
func (s *SharedContainers) BackstopAddReward(ctx context.Context, t *testing.T, backstopID string, caller *keypair.Full, toAdd string) string {
	t.Helper()

	args := []xdr.ScVal{scAddr(t, toAdd), scVoid()}
	op := buildInvokeOp(t, backstopID, "add_reward", args, caller.Address())

	hash, err := s.executeSorobanOperationAs(ctx, t, op, caller, nil, DefaultConfirmationRetries)
	require.NoError(t, err, "adding backstop reward")
	return hash
}

// ---------------------------------------------------------------------------------------------
// Emitter
// ---------------------------------------------------------------------------------------------

// EmitterDistribute invokes distribute() -> i128 on the emitter, signed by caller. This is a
// permissionless call that mints and distributes BLND emissions since the last call.
func (s *SharedContainers) EmitterDistribute(ctx context.Context, t *testing.T, emitterID string, caller *keypair.Full) string {
	t.Helper()

	op := buildInvokeOp(t, emitterID, "distribute", nil, caller.Address())

	hash, err := s.executeSorobanOperationAs(ctx, t, op, caller, nil, DefaultConfirmationRetries)
	require.NoError(t, err, "distributing emitter emissions")
	return hash
}

// EmitterInitialize invokes initialize(blnd_token, backstop, backstop_token) on the emitter,
// signed by caller.
func (s *SharedContainers) EmitterInitialize(ctx context.Context, t *testing.T, emitterID string, caller *keypair.Full, blndToken, backstop, backstopToken string) string {
	t.Helper()

	args := []xdr.ScVal{scAddr(t, blndToken), scAddr(t, backstop), scAddr(t, backstopToken)}
	op := buildInvokeOp(t, emitterID, "initialize", args, caller.Address())

	hash, err := s.executeSorobanOperationAs(ctx, t, op, caller, nil, DefaultConfirmationRetries)
	require.NoError(t, err, "initializing emitter")
	return hash
}

// ---------------------------------------------------------------------------------------------
// Mock oracle (SEP-40)
// ---------------------------------------------------------------------------------------------

// OracleSetData invokes set_data(admin, base, assets, decimals, resolution) on the mock oracle,
// signed by admin. base is hardcoded to the SEP-40 Asset::Other("USD") variant.
func (s *SharedContainers) OracleSetData(ctx context.Context, t *testing.T, oracleID string, admin *keypair.Full, assetAddrs []string, decimals, resolution uint32) string {
	t.Helper()

	args := []xdr.ScVal{
		scAddr(t, admin.Address()),
		scSep40OtherAsset("USD"),
		scSep40StellarAssetVec(t, assetAddrs),
		scU32(decimals),
		scU32(resolution),
	}
	op := buildInvokeOp(t, oracleID, "set_data", args, admin.Address())

	hash, err := s.executeSorobanOperationAs(ctx, t, op, admin, nil, DefaultConfirmationRetries)
	require.NoError(t, err, "setting oracle data")
	return hash
}

// OracleSetPriceStable invokes set_price_stable(prices) on the mock oracle, signed by admin.
func (s *SharedContainers) OracleSetPriceStable(ctx context.Context, t *testing.T, oracleID string, admin *keypair.Full, prices []*big.Int) string {
	t.Helper()

	args := []xdr.ScVal{scI128Vec(t, prices)}
	op := buildInvokeOp(t, oracleID, "set_price_stable", args, admin.Address())

	hash, err := s.executeSorobanOperationAs(ctx, t, op, admin, nil, DefaultConfirmationRetries)
	require.NoError(t, err, "setting oracle stable prices")
	return hash
}

// ---------------------------------------------------------------------------------------------
// Comet factory / Comet
// ---------------------------------------------------------------------------------------------

// CometFactoryInit invokes init(pool_wasm_hash) on the Comet factory, signed by caller.
func (s *SharedContainers) CometFactoryInit(ctx context.Context, t *testing.T, factoryID string, caller *keypair.Full, wasmHash xdr.Hash) string {
	t.Helper()

	args := []xdr.ScVal{scBytes32(wasmHash)}
	op := buildInvokeOp(t, factoryID, "init", args, caller.Address())

	hash, err := s.executeSorobanOperationAs(ctx, t, op, caller, nil, DefaultConfirmationRetries)
	require.NoError(t, err, "initializing comet factory")
	return hash
}

// CometFactoryNewPool invokes new_c_pool(salt, controller, tokens, weights, balances, swap_fee) ->
// Address on the Comet factory, signed by controller, and returns the newly deployed Comet pool's
// address decoded from the invoke's return value.
func (s *SharedContainers) CometFactoryNewPool(ctx context.Context, t *testing.T, factoryID string, controller *keypair.Full, salt [32]byte, tokens []string, weights, balances []*big.Int, swapFee *big.Int) string {
	t.Helper()

	args := []xdr.ScVal{
		scBytes32(salt),
		scAddr(t, controller.Address()),
		scAddressVec(t, tokens),
		scI128Vec(t, weights),
		scI128Vec(t, balances),
		scI128(t, swapFee),
	}
	op := buildInvokeOp(t, factoryID, "new_c_pool", args, controller.Address())

	hash, err := s.executeSorobanOperationAs(ctx, t, op, controller, nil, DefaultConfirmationRetries)
	require.NoError(t, err, "deploying comet pool")

	poolAddr, err := s.invokeResultAddress(ctx, t, hash)
	require.NoError(t, err, "decoding comet pool address")
	return poolAddr
}

// CometJoinPool invokes join_pool(pool_amount_out, max_amounts_in, user) on the Comet pool, signed
// by user.
func (s *SharedContainers) CometJoinPool(ctx context.Context, t *testing.T, cometID string, user *keypair.Full, poolAmountOut *big.Int, maxAmountsIn []*big.Int) string {
	t.Helper()

	args := []xdr.ScVal{scI128(t, poolAmountOut), scI128Vec(t, maxAmountsIn), scAddr(t, user.Address())}
	op := buildInvokeOp(t, cometID, "join_pool", args, user.Address())

	hash, err := s.executeSorobanOperationAs(ctx, t, op, user, nil, DefaultConfirmationRetries)
	require.NoError(t, err, "joining comet pool")
	return hash
}

// ---------------------------------------------------------------------------------------------
// Pool factory
// ---------------------------------------------------------------------------------------------

// PoolFactoryDeploy invokes deploy(admin, name, salt, oracle, backstop_take_rate, max_positions,
// min_collateral) -> Address on the pool factory, signed by admin, and returns the newly deployed
// pool's address decoded from the invoke's return value.
func (s *SharedContainers) PoolFactoryDeploy(ctx context.Context, t *testing.T, factoryID string, admin *keypair.Full, name string, salt [32]byte, oracle string, backstopTakeRate, maxPositions uint32, minCollateral *big.Int) string {
	t.Helper()

	args := []xdr.ScVal{
		scAddr(t, admin.Address()),
		scString(t, name),
		scBytes32(salt),
		scAddr(t, oracle),
		scU32(backstopTakeRate),
		scU32(maxPositions),
		scI128(t, minCollateral),
	}
	op := buildInvokeOp(t, factoryID, "deploy", args, admin.Address())

	hash, err := s.executeSorobanOperationAs(ctx, t, op, admin, nil, DefaultConfirmationRetries)
	require.NoError(t, err, "deploying pool")

	poolAddr, err := s.invokeResultAddress(ctx, t, hash)
	require.NoError(t, err, "decoding deployed pool address")
	return poolAddr
}

// ---------------------------------------------------------------------------------------------
// Stellar Asset Contract (SAC) admin
// ---------------------------------------------------------------------------------------------

// SACSetAdmin invokes set_admin(new_admin) on a Stellar Asset Contract, signed by currentAdmin.
//
// Note: since currentAdmin is often the shared master account acting as an arbitrary
// executeSorobanOperationAs source (rather than through executeSorobanOperation), the master
// account's locally tracked sequence counter drifts from the ledger's view. Call
// SyncMasterSequence before resuming master-account operations through executeSorobanOperation.
func (s *SharedContainers) SACSetAdmin(ctx context.Context, t *testing.T, sacID string, currentAdmin *keypair.Full, newAdmin string) string {
	t.Helper()

	args := []xdr.ScVal{scAddr(t, newAdmin)}
	op := buildInvokeOp(t, sacID, "set_admin", args, currentAdmin.Address())

	hash, err := s.executeSorobanOperationAs(ctx, t, op, currentAdmin, nil, DefaultConfirmationRetries)
	require.NoError(t, err, "setting SAC admin")
	return hash
}
