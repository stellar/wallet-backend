// Package resolvers — blend_positions.go assembles Account.blendPositions: a
// single-pass, fully-materialized batch read across every Blend v2 table an
// account touches, then pure valuation via internal/services/blend's
// reactive-rate-curve and position-valuation math (rates.go). No dataloaders:
// the resolver already fans out from one account into a fixed, small set of
// batch queries (never per-row), so there is no N+1 for a dataloader to
// avoid — see internal/services/blend/rates.go for the math this leans on.
package resolvers

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/stellar/wallet-backend/internal/data"
	blenddata "github.com/stellar/wallet-backend/internal/data/blend"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	graphql1 "github.com/stellar/wallet-backend/internal/serve/graphql/generated"
	blendrates "github.com/stellar/wallet-backend/internal/services/blend"
)

// blndDecimals is BLND's on-chain decimal scale. BLND is a classic Stellar
// asset issued via a SAC, and SACs for classic assets are always 7-decimal —
// a fixed protocol constant, not a per-deployment configurable.
const blndDecimals int32 = 7

// backstopLPDecimals is the Comet BLND-USDC LP share token's decimal scale.
// Backstop shares are always 7-decimal (rates.go's ClaimableEmissions doc:
// "the backstop's own fixed SCALAR_7 — its LP-share balances are always
// 7-decimal").
const backstopLPDecimals int32 = 7

// scalar7 is Blend v2's SCALAR_7 (1e7) fixed-point base. Used here only to
// convert blend_reserves.ir_mod — stored as TEXT because, unlike the Curve
// fields, ir_mod isn't guaranteed to stay within int32 range — into the
// *big.Rat blendrates.BorrowAPR expects.
const scalar7 = 10_000_000

// backstopClaimScalar is the backstop's claim-conversion divisor: SCALAR_7 *
// SCALAR_7 (backstop LP shares are always 7-decimal), per rates.go's
// ClaimableEmissions doc.
var backstopClaimScalar = big.NewInt(int64(scalar7) * int64(scalar7))

// reserveClaimScalar is the per-reserve b/dToken claim-conversion divisor
// (rates.go's ClaimableEmissions doc): 10^decimals * 1e7.
func reserveClaimScalar(decimals int32) *big.Int {
	scale := pow10(decimals)
	return scale.Mul(scale, big.NewInt(scalar7))
}

// pow10 returns 10^n as a *big.Int.
func pow10(n int32) *big.Int {
	return new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(n)), nil)
}

// parseBigInt parses a plain base-10 integer string — the raw on-chain
// integer columns blend_reserves/blend_positions/blend_backstop_pools/
// blend_emissions store (b_rate, d_rate, supply_b_tokens, shares, accrued,
// emission_index, price, ...) — none of which carry a decimal point, unlike
// blend_positions.net_supplied/net_borrowed (see blendrates.
// ParseDecimalToBigInt for those).
func parseBigInt(s string) (*big.Int, error) {
	n, ok := new(big.Int).SetString(s, 10)
	if !ok {
		return nil, fmt.Errorf("parsing blend integer %q", s)
	}
	return n, nil
}

// poolTokenKey joins a pool/source address with a small integer id (a
// reserve index or a reserve/backstop token id) into a lookup-map key.
func poolTokenKey(addr string, id int32) string {
	return fmt.Sprintf("%s|%d", addr, id)
}

// usdValueOrNil is amount's USD value at decimals against price, or nil when
// that value can't be computed. A genuinely zero amount is always 0 — never
// nil — regardless of whether a price is available, since there is nothing
// to price. A nonzero amount with no price row is uncomputable (nil), per
// this schema's Float-nullability convention.
func usdValueOrNil(amount *big.Int, decimals int32, price *blenddata.OraclePrice) *float64 {
	if amount.Sign() == 0 {
		zero := 0.0
		return &zero
	}
	if price == nil {
		return nil
	}
	priceInt, err := parseBigInt(price.Price)
	if err != nil {
		return nil
	}
	v := blendrates.USDValue(amount, decimals, priceInt, price.PriceDecimals)
	f, _ := v.Float64()
	return &f
}

// claimableStream computes claimable (uncollected) BLND for one emission
// stream, per rates.go's ClaimableEmissions. hasUser reports whether the
// account has a blend_emissions row for this stream. When false but the
// stream is configured and the account holds a balance, the contract's
// "user had tokens before emissions began" branch applies (distributor.rs's
// update_user_emissions): the holder is owed balance*index/scalar the first
// time their emissions are touched — the full index counts, from a zero
// starting point, with nothing pre-accrued.
//
// configIndex is the stream's own current emission_index (from
// blend_reserve_emissions or blend_backstop_pools.emis_index), used as the
// projection target even past expiration — expiry freezes further real-time
// growth but doesn't erase the last recorded index. A nil configIndex means
// no config row exists at all (the stream was never configured); in that
// case no further growth beyond the user's own Accrued is possible, so the
// user's own index is used as both sides of the delta.
func claimableStream(userEmission blenddata.Emission, hasUser bool, configIndex *string, tokenBalance, scalar *big.Int) (*big.Int, error) {
	if !hasUser {
		if configIndex == nil || tokenBalance.Sign() <= 0 {
			return big.NewInt(0), nil
		}
		emisIndex, err := parseBigInt(*configIndex)
		if err != nil {
			return nil, fmt.Errorf("parsing blend emission config index: %w", err)
		}
		return blendrates.ClaimableEmissions(big.NewInt(0), big.NewInt(0), emisIndex, tokenBalance, scalar), nil
	}
	accrued, err := parseBigInt(userEmission.Accrued)
	if err != nil {
		return nil, fmt.Errorf("parsing blend emission accrued: %w", err)
	}
	userIndex, err := parseBigInt(userEmission.EmissionIndex)
	if err != nil {
		return nil, fmt.Errorf("parsing blend emission index: %w", err)
	}

	emisIndex := userIndex
	if configIndex != nil {
		emisIndex, err = parseBigInt(*configIndex)
		if err != nil {
			return nil, fmt.Errorf("parsing blend emission config index: %w", err)
		}
	}

	return blendrates.ClaimableEmissions(accrued, userIndex, emisIndex, tokenBalance, scalar), nil
}

// isFreshPrice reports whether a stored oracle price is still usable as of
// now. The Blend pool contract itself refuses prices older than
// blendrates.MaxPriceAge (pool.rs::load_price), so a staler row cannot
// honestly value anything — treating it as missing makes every USD/APY
// field go null exactly like a never-priced asset, instead of quietly
// pricing off a dead oracle's last snapshot forever.
func isFreshPrice(op blenddata.OraclePrice, now int64) bool {
	return op.PriceTimestamp >= now-int64(blendrates.MaxPriceAge/time.Second)
}

// freshPriceMap indexes price rows by "oracle|asset", dropping stale rows
// per isFreshPrice.
func freshPriceMap(rows []blenddata.OraclePrice, now int64) map[string]blenddata.OraclePrice {
	m := make(map[string]blenddata.OraclePrice, len(rows))
	for _, op := range rows {
		if !isFreshPrice(op, now) {
			continue
		}
		m[string(op.OracleContractID)+"|"+string(op.AssetContractID)] = op
	}
	return m
}

// freshPrices filters price rows to the fresh ones per isFreshPrice.
func freshPrices(rows []blenddata.OraclePrice, now int64) []blenddata.OraclePrice {
	out := make([]blenddata.OraclePrice, 0, len(rows))
	for _, op := range rows {
		if isFreshPrice(op, now) {
			out = append(out, op)
		}
	}
	return out
}

// findBackstopPrices picks the (LP, BLND) price pair out of
// OraclePrices.GetBackstopLPPrices' result set. Rows share an
// oracle_contract_id in groups of (one self-priced LP row, its sibling BLND
// row) — see GetBackstopLPPrices' doc. Blend v2 has a single protocol-wide
// backstop LP token, so in practice there is at most one complete group;
// returns (nil, nil) if none is found.
func findBackstopPrices(rows []blenddata.OraclePrice) (lp, blnd *blenddata.OraclePrice) {
	selfPriced := map[string]blenddata.OraclePrice{}
	siblings := map[string][]blenddata.OraclePrice{}
	for _, row := range rows {
		if string(row.OracleContractID) == string(row.AssetContractID) {
			selfPriced[string(row.OracleContractID)] = row
		} else {
			siblings[string(row.OracleContractID)] = append(siblings[string(row.OracleContractID)], row)
		}
	}
	for oracle, self := range selfPriced {
		sibs := siblings[oracle]
		if len(sibs) == 0 {
			continue
		}
		sib := sibs[0]
		return &self, &sib
	}
	return nil, nil
}

// blendAssembly holds every batch-fetched Blend v2 row and derived lookup
// index needed to assemble one account's Account.blendPositions in a single
// pass.
type blendAssembly struct {
	poolByID                   map[string]blenddata.Pool
	reserveByPoolIndex         map[string]blenddata.Reserve
	reserveEmissionByPoolToken map[string]blenddata.ReserveEmission
	userEmissionByPoolToken    map[string]blenddata.Emission
	backstopPoolByID           map[string]blenddata.BackstopPool
	priceByOracleAsset         map[string]blenddata.OraclePrice
	metaByContractID           map[string]data.Contract
	lpPrice                    *blenddata.OraclePrice
	blndPrice                  *blenddata.OraclePrice
	claimedByPool              map[string]string
	now                        int64
}

func (d *blendAssembly) poolBackstopRate(poolAddr string) int32 {
	if pool, ok := d.poolByID[poolAddr]; ok && pool.BackstopRate != nil {
		return *pool.BackstopRate
	}
	return 0
}

func (d *blendAssembly) priceLookup(oracle, asset types.AddressBytea) *blenddata.OraclePrice {
	if oracle == "" {
		return nil
	}
	if price, ok := d.priceByOracleAsset[string(oracle)+"|"+string(asset)]; ok {
		return &price
	}
	return nil
}

func (d *blendAssembly) reserveEmissionConfigIndex(poolAddr string, tokenID int32) *string {
	if re, ok := d.reserveEmissionByPoolToken[poolTokenKey(poolAddr, tokenID)]; ok {
		idx := re.EmissionIndex
		return &idx
	}
	return nil
}

// emissionsAPRFor computes the emissions APR for one (pool, tokenID) reserve
// emission stream against the pool-wide underlying USD size of that side
// (sideRaw token units at sideRate). Returns a concrete 0 — not nil — when
// there is no emission config for this stream or it has expired (both are
// well-defined "no emissions" results, not missing data). Returns nil only
// when emissions are active and non-expired but the USD size can't be priced.
func (d *blendAssembly) emissionsAPRFor(poolAddr string, tokenID int32, sideRaw, sideRate *big.Int, decimals int32, price *blenddata.OraclePrice) *float64 {
	re, ok := d.reserveEmissionByPoolToken[poolTokenKey(poolAddr, tokenID)]
	if !ok || d.now >= re.Expiration {
		zero := 0.0
		return &zero
	}
	if price == nil || d.blndPrice == nil {
		return nil
	}
	priceInt, err := parseBigInt(price.Price)
	if err != nil {
		return nil
	}
	blndPriceInt, err := parseBigInt(d.blndPrice.Price)
	if err != nil {
		return nil
	}

	sideUnderlying := blendrates.Underlying(sideRaw, sideRate)
	sideUSD := blendrates.USDValue(sideUnderlying, decimals, priceInt, price.PriceDecimals)
	blndUSD := new(big.Rat).SetFrac(blndPriceInt, pow10(d.blndPrice.PriceDecimals))

	apr := blendrates.EmissionsAPR(re.Eps, re.Expiration, d.now, blndUSD, sideUSD)
	f, _ := apr.Float64()
	return &f
}

// reserveRates holds one reserve's current-rate-curve outputs: utilization,
// supply/borrow APY, and the b/dRate projected forward to "now". It is
// computed once per reserve by computeReserveRates and shared by both the
// per-account position assembly (buildReservePosition) and the pool-wide
// catalog assembly (blend_pools.go's buildPoolReserve) — the two differ only
// in which raw b/dToken amount they apply these rates to (a user's holdings
// vs. the reserve's pool-wide BSupply/DSupply, both of which this struct
// carries so callers never need to re-parse the reserve row).
type reserveRates struct {
	BRate, DRate     *big.Int // current (unprojected) on-chain rates
	BSupply, DSupply *big.Int // pool-wide token supply, current on-chain
	CurrentUtil      *big.Rat
	SupplyApy        float64
	BorrowApy        float64
	PB, PD           *big.Int // b/dRate projected forward to "now"
}

// computeReserveRates parses reserve's rate/curve columns and derives its
// current utilization, supply/borrow APY, and rates projected to "now" — the
// shared reactive-rate-curve computation underlying every Blend v2 GraphQL
// view of a reserve. bstopRate comes from the owning pool (poolAddr) via
// poolBackstopRate.
func (d *blendAssembly) computeReserveRates(poolAddr string, reserve blenddata.Reserve) (*reserveRates, error) {
	bRate, err := parseBigInt(reserve.BRate)
	if err != nil {
		return nil, fmt.Errorf("parsing blend reserve b_rate: %w", err)
	}
	dRate, err := parseBigInt(reserve.DRate)
	if err != nil {
		return nil, fmt.Errorf("parsing blend reserve d_rate: %w", err)
	}
	bSupply, err := parseBigInt(reserve.BSupply)
	if err != nil {
		return nil, fmt.Errorf("parsing blend reserve b_supply: %w", err)
	}
	dSupply, err := parseBigInt(reserve.DSupply)
	if err != nil {
		return nil, fmt.Errorf("parsing blend reserve d_supply: %w", err)
	}
	irModRaw, err := parseBigInt(reserve.IRMod)
	if err != nil {
		return nil, fmt.Errorf("parsing blend reserve ir_mod: %w", err)
	}
	irMod := new(big.Rat).SetFrac(irModRaw, big.NewInt(scalar7))

	curve := blendrates.Curve{Util: reserve.Util, RBase: reserve.RBase, ROne: reserve.ROne, RTwo: reserve.RTwo, RThree: reserve.RThree}
	currentUtil := blendrates.Utilization(bSupply, bRate, dSupply, dRate)
	borrowAPR := blendrates.BorrowAPR(currentUtil, curve, irMod)

	bstopRate := d.poolBackstopRate(poolAddr)
	supplyAPR := blendrates.SupplyAPR(borrowAPR, currentUtil, bstopRate)
	supplyApy := blendrates.ToAPY(supplyAPR, blendrates.SupplyAPYCompoundingPeriods)
	borrowApy := blendrates.ToAPY(borrowAPR, blendrates.BorrowAPYCompoundingPeriods)

	pB, pD := blendrates.ProjectRates(bRate, dRate, borrowAPR, currentUtil, bstopRate, reserve.LastTime, d.now)

	return &reserveRates{
		BRate: bRate, DRate: dRate,
		BSupply: bSupply, DSupply: dSupply,
		CurrentUtil: currentUtil,
		SupplyApy:   supplyApy,
		BorrowApy:   borrowApy,
		PB:          pB,
		PD:          pD,
	}, nil
}

// buildReservePosition assembles one blend_positions row into a
// BlendReservePosition. Returns (nil, nil) when the row's reserve config
// can't be found — defensive only; every position is written against an
// already-known reserve, so this shouldn't happen in practice.
//
// A row whose token columns are all zero (a fully-exited position) still
// produces an entry whenever net_supplied/net_borrowed is nonzero: the
// position's underlying token amounts are legitimately 0, but
// interestEarned/interestPaid still reports the cost-basis-vs-0 gap, i.e.
// earnings realized up to the exit.
func (d *blendAssembly) buildReservePosition(p blenddata.Position) (*graphql1.BlendReservePosition, error) {
	poolAddr := string(p.PoolContractID)
	reserve, ok := d.reserveByPoolIndex[poolTokenKey(poolAddr, p.ReserveIndex)]
	if !ok {
		return nil, nil
	}

	supplyB, err := parseBigInt(p.SupplyBTokens)
	if err != nil {
		return nil, fmt.Errorf("parsing blend position supply_b_tokens: %w", err)
	}
	collateralB, err := parseBigInt(p.CollateralBTokens)
	if err != nil {
		return nil, fmt.Errorf("parsing blend position collateral_b_tokens: %w", err)
	}
	liabilityD, err := parseBigInt(p.LiabilityDTokens)
	if err != nil {
		return nil, fmt.Errorf("parsing blend position liability_d_tokens: %w", err)
	}
	netSupplied, err := blendrates.ParseDecimalToBigInt(p.NetSupplied)
	if err != nil {
		return nil, fmt.Errorf("parsing blend position net_supplied: %w", err)
	}
	netBorrowed, err := blendrates.ParseDecimalToBigInt(p.NetBorrowed)
	if err != nil {
		return nil, fmt.Errorf("parsing blend position net_borrowed: %w", err)
	}

	rr, err := d.computeReserveRates(poolAddr, reserve)
	if err != nil {
		return nil, err
	}

	suppliedTokens := blendrates.Underlying(supplyB, rr.PB)
	collateralTokens := blendrates.Underlying(collateralB, rr.PB)
	borrowedTokens := blendrates.Underlying(liabilityD, rr.PD)

	oracle := types.AddressBytea("")
	if pool, ok := d.poolByID[poolAddr]; ok {
		oracle = pool.OracleContractID
	}
	price := d.priceLookup(oracle, reserve.AssetContractID)

	totalSupplySide := new(big.Int).Add(suppliedTokens, collateralTokens)
	suppliedUsd := usdValueOrNil(totalSupplySide, reserve.Decimals, price)
	borrowedUsd := usdValueOrNil(borrowedTokens, reserve.Decimals, price)

	// emissionsApr reports whichever token side (bToken/dToken) this position
	// currently holds a nonzero balance on. Blend tracks one BLND-per-second
	// stream per token id (dToken=idx*2, bToken=idx*2+1), and
	// BlendReservePosition exposes a single emissionsApr number, so this
	// resolves that ambiguity by reporting the side actually relevant to the
	// account's exposure: bToken emissions when it supplies/collateralizes,
	// otherwise dToken emissions when it has debt. 0 when the position holds
	// neither side (a fully-exited, zeroed row).
	bTokenID := reserve.ReserveIndex*2 + 1
	dTokenID := reserve.ReserveIndex * 2
	bSideBalance := new(big.Int).Add(supplyB, collateralB)

	var emissionsApr *float64
	switch {
	case bSideBalance.Sign() > 0:
		emissionsApr = d.emissionsAPRFor(poolAddr, bTokenID, rr.BSupply, rr.BRate, reserve.Decimals, price)
	case liabilityD.Sign() > 0:
		emissionsApr = d.emissionsAPRFor(poolAddr, dTokenID, rr.DSupply, rr.DRate, reserve.Decimals, price)
	default:
		zero := 0.0
		emissionsApr = &zero
	}

	// emissionsEarnedBlnd, unlike emissionsApr, always sums BOTH streams:
	// a position can carry claimable history on a side it no longer holds
	// (e.g. fully repaid debt with unclaimed dToken-stream emissions).
	claimScalar := reserveClaimScalar(reserve.Decimals)
	bUserEmission, bHasUser := d.userEmissionByPoolToken[poolTokenKey(poolAddr, bTokenID)]
	bClaimable, err := claimableStream(bUserEmission, bHasUser, d.reserveEmissionConfigIndex(poolAddr, bTokenID), bSideBalance, claimScalar)
	if err != nil {
		return nil, err
	}
	dUserEmission, dHasUser := d.userEmissionByPoolToken[poolTokenKey(poolAddr, dTokenID)]
	dClaimable, err := claimableStream(dUserEmission, dHasUser, d.reserveEmissionConfigIndex(poolAddr, dTokenID), liabilityD, claimScalar)
	if err != nil {
		return nil, err
	}
	claimableBLND := new(big.Int).Add(bClaimable, dClaimable)
	emissionsEarnedUsd := usdValueOrNil(claimableBLND, blndDecimals, d.blndPrice)

	interestEarned := blendrates.EarnedInterest(supplyB, collateralB, rr.PB, netSupplied)
	// interestPaid mirrors EarnedInterest's shape for the debt side: current
	// underlying liability minus net borrowed principal. rates.go has no
	// dedicated helper for this side (EarnedInterest is specifically the
	// supply+collateral combination), so it's derived directly here.
	interestPaid := new(big.Int).Sub(blendrates.Underlying(liabilityD, rr.PD), netBorrowed)

	meta := d.metaByContractID[string(reserve.AssetContractID)]
	decimals := reserve.Decimals

	return &graphql1.BlendReservePosition{
		AssetContractID:     string(reserve.AssetContractID),
		TokenName:           meta.Name,
		TokenSymbol:         meta.Symbol,
		TokenDecimals:       &decimals,
		SuppliedTokens:      suppliedTokens.String(),
		CollateralTokens:    collateralTokens.String(),
		BorrowedTokens:      borrowedTokens.String(),
		SuppliedUsd:         suppliedUsd,
		BorrowedUsd:         borrowedUsd,
		SupplyApy:           &rr.SupplyApy,
		BorrowApy:           &rr.BorrowApy,
		EmissionsApr:        emissionsApr,
		InterestEarned:      interestEarned.String(),
		InterestPaid:        interestPaid.String(),
		EmissionsEarnedBlnd: claimableBLND.String(),
		EmissionsEarnedUsd:  emissionsEarnedUsd,
	}, nil
}

// buildPoolPosition rolls up one pool's reserve positions into a
// BlendPoolPosition. suppliedUsd/borrowedUsd/usdValue/netApy become nil (not
// a silently-understated sum) as soon as any contributing reserve's own
// suppliedUsd/borrowedUsd is nil, since a missing price on one reserve makes
// the pool-wide total genuinely uncomputable, not just smaller.
func (d *blendAssembly) buildPoolPosition(poolAddr string, positions []blenddata.Position) (*graphql1.BlendPoolPosition, error) {
	reservePositions := make([]*graphql1.BlendReservePosition, 0, len(positions))

	suppliedUsdSum, borrowedUsdSum := 0.0, 0.0
	supplyApyNumerator, borrowApyNumerator := 0.0, 0.0
	suppliedKnown, borrowedKnown := true, true

	for _, p := range positions {
		rp, err := d.buildReservePosition(p)
		if err != nil {
			return nil, err
		}
		if rp == nil {
			continue
		}
		reservePositions = append(reservePositions, rp)

		if rp.SuppliedUsd == nil {
			suppliedKnown = false
		} else if suppliedKnown {
			suppliedUsdSum += *rp.SuppliedUsd
			if rp.SupplyApy != nil {
				supplyApyNumerator += *rp.SuppliedUsd * *rp.SupplyApy
			}
		}
		if rp.BorrowedUsd == nil {
			borrowedKnown = false
		} else if borrowedKnown {
			borrowedUsdSum += *rp.BorrowedUsd
			if rp.BorrowApy != nil {
				borrowApyNumerator += *rp.BorrowedUsd * *rp.BorrowApy
			}
		}
	}

	var suppliedUsd, borrowedUsd, usdValue, netApy *float64
	if suppliedKnown {
		v := suppliedUsdSum
		suppliedUsd = &v
	}
	if borrowedKnown {
		v := borrowedUsdSum
		borrowedUsd = &v
	}
	if suppliedKnown && borrowedKnown {
		v := suppliedUsdSum - borrowedUsdSum
		usdValue = &v
		if v != 0 {
			apy := (supplyApyNumerator - borrowApyNumerator) / v
			netApy = &apy
		}
	}

	claimed, ok := d.claimedByPool[poolAddr]
	if !ok {
		claimed = "0"
	}

	return &graphql1.BlendPoolPosition{
		PoolAddress: poolAddr,
		PoolName:    d.poolByID[poolAddr].Name,
		UsdValue:    usdValue,
		SuppliedUsd: suppliedUsd,
		BorrowedUsd: borrowedUsd,
		NetApy:      netApy,
		ClaimedBlnd: claimed,
		Reserves:    reservePositions,
	}, nil
}

// buildBackstopPosition assembles one blend_backstop_positions row into a
// BlendBackstopPosition. bp.Shares holds only the ACTIVE share balance —
// queued-for-withdrawal shares live in bp.Q4W — but queued shares keep
// earning pool interest and remain slashable first-loss capital until
// withdrawn, so lpTokens/usdValue value active+queued together, with each
// Q4W entry also valued individually through the same shares→LP→USD chain.
// Emissions are the one queued-share exception: they accrue on active
// shares only, so claimableStream gets the active balance.
func (d *blendAssembly) buildBackstopPosition(bp blenddata.BackstopPosition) (*graphql1.BlendBackstopPosition, error) {
	poolAddr := string(bp.PoolContractID)

	shares, err := parseBigInt(bp.Shares)
	if err != nil {
		return nil, fmt.Errorf("parsing blend backstop position shares: %w", err)
	}

	poolShares, poolTokens := big.NewInt(0), big.NewInt(0)
	var configIndex *string
	if backstopPool, ok := d.backstopPoolByID[poolAddr]; ok {
		poolShares, err = parseBigInt(backstopPool.Shares)
		if err != nil {
			return nil, fmt.Errorf("parsing blend backstop pool shares: %w", err)
		}
		poolTokens, err = parseBigInt(backstopPool.Tokens)
		if err != nil {
			return nil, fmt.Errorf("parsing blend backstop pool tokens: %w", err)
		}
		configIndex = backstopPool.EmisIndex
	}

	totalShares := new(big.Int).Set(shares)
	q4w := make([]*graphql1.BlendQ4w, 0, len(bp.Q4W))
	for _, w := range bp.Q4W {
		queuedShares, parseErr := parseBigInt(w.Amount)
		if parseErr != nil {
			return nil, fmt.Errorf("parsing blend backstop Q4W amount: %w", parseErr)
		}
		totalShares.Add(totalShares, queuedShares)
		queuedLP := blendrates.BackstopLPTokens(queuedShares, poolShares, poolTokens)
		q4w = append(q4w, &graphql1.BlendQ4w{
			Amount:     w.Amount,
			Expiration: w.Expiration,
			LpTokens:   queuedLP.String(),
			UsdValue:   usdValueOrNil(queuedLP, backstopLPDecimals, d.lpPrice),
		})
	}

	lpTokens := blendrates.BackstopLPTokens(totalShares, poolShares, poolTokens)
	usdValue := usdValueOrNil(lpTokens, backstopLPDecimals, d.lpPrice)

	userEmission, hasUser := d.userEmissionByPoolToken[poolTokenKey(poolAddr, blenddata.BackstopEmissionTokenID)]
	claimable, err := claimableStream(userEmission, hasUser, configIndex, shares, backstopClaimScalar)
	if err != nil {
		return nil, err
	}
	emissionsEarnedUsd := usdValueOrNil(claimable, blndDecimals, d.blndPrice)

	return &graphql1.BlendBackstopPosition{
		PoolAddress:         poolAddr,
		PoolName:            d.poolByID[poolAddr].Name,
		Shares:              bp.Shares,
		LpTokens:            lpTokens.String(),
		UsdValue:            usdValue,
		Q4w:                 q4w,
		EmissionsEarnedBlnd: claimable.String(),
		EmissionsEarnedUsd:  emissionsEarnedUsd,
	}, nil
}

// getBlendPositions is the main implementation for Account.blendPositions: a
// batch read of every Blend v2 table the account appears in, followed by
// pure valuation assembly. An account with no Blend v2 activity at all gets
// empty lists and "0" aggregates, never an error.
func (r *Resolver) getBlendPositions(ctx context.Context, address string) (*graphql1.BlendAccountPositions, error) {
	positions, err := r.models.Blend.Positions.GetByAccount(ctx, address)
	if err != nil {
		return nil, fmt.Errorf("getting blend positions for account %s: %w", address, err)
	}
	backstopPositions, err := r.models.Blend.BackstopPositions.GetByAccount(ctx, address)
	if err != nil {
		return nil, fmt.Errorf("getting blend backstop positions for account %s: %w", address, err)
	}
	emissions, err := r.models.Blend.Emissions.GetByAccount(ctx, address)
	if err != nil {
		return nil, fmt.Errorf("getting blend emissions for account %s: %w", address, err)
	}
	claimTotals, err := r.models.StateChanges.GetLendingClaimTotals(ctx, address)
	if err != nil {
		return nil, fmt.Errorf("getting blend lending claim totals for account %s: %w", address, err)
	}

	poolIDSet := map[string]struct{}{}
	for _, p := range positions {
		poolIDSet[string(p.PoolContractID)] = struct{}{}
	}
	for _, bp := range backstopPositions {
		poolIDSet[string(bp.PoolContractID)] = struct{}{}
	}
	poolIDs := make([]string, 0, len(poolIDSet))
	for id := range poolIDSet {
		poolIDs = append(poolIDs, id)
	}

	pools, err := r.models.Blend.Pools.GetByIDs(ctx, poolIDs)
	if err != nil {
		return nil, fmt.Errorf("getting blend pools: %w", err)
	}
	reserves, err := r.models.Blend.Reserves.GetByPools(ctx, poolIDs)
	if err != nil {
		return nil, fmt.Errorf("getting blend reserves: %w", err)
	}
	reserveEmissions, err := r.models.Blend.ReserveEmissions.GetByPools(ctx, poolIDs)
	if err != nil {
		return nil, fmt.Errorf("getting blend reserve emissions: %w", err)
	}
	backstopPools, err := r.models.Blend.BackstopPools.GetByIDs(ctx, poolIDs)
	if err != nil {
		return nil, fmt.Errorf("getting blend backstop pools: %w", err)
	}

	poolByID := make(map[string]blenddata.Pool, len(pools))
	oracleIDSet := map[string]struct{}{}
	for _, p := range pools {
		poolByID[string(p.PoolContractID)] = p
		if p.OracleContractID != "" {
			oracleIDSet[string(p.OracleContractID)] = struct{}{}
		}
	}
	oracleIDs := make([]string, 0, len(oracleIDSet))
	for id := range oracleIDSet {
		oracleIDs = append(oracleIDs, id)
	}

	oraclePrices, err := r.models.Blend.OraclePrices.GetByOracles(ctx, oracleIDs)
	if err != nil {
		return nil, fmt.Errorf("getting blend oracle prices: %w", err)
	}
	backstopLPPrices, err := r.models.Blend.OraclePrices.GetBackstopLPPrices(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting blend backstop LP prices: %w", err)
	}
	now := time.Now().Unix()
	lpPrice, blndPrice := findBackstopPrices(freshPrices(backstopLPPrices, now))
	priceByOracleAsset := freshPriceMap(oraclePrices, now)

	reserveByPoolIndex := make(map[string]blenddata.Reserve, len(reserves))
	assetIDSet := map[string]struct{}{}
	for _, res := range reserves {
		reserveByPoolIndex[poolTokenKey(string(res.PoolContractID), res.ReserveIndex)] = res
		assetIDSet[string(res.AssetContractID)] = struct{}{}
	}
	assetIDs := make([]string, 0, len(assetIDSet))
	for id := range assetIDSet {
		assetIDs = append(assetIDs, id)
	}
	tokenMeta, err := r.models.Contract.GetTokenMetadataByContractIDs(ctx, assetIDs)
	if err != nil {
		return nil, fmt.Errorf("getting blend reserve asset token metadata: %w", err)
	}
	metaByContractID := make(map[string]data.Contract, len(tokenMeta))
	for _, c := range tokenMeta {
		metaByContractID[c.ContractID] = c
	}

	reserveEmissionByPoolToken := make(map[string]blenddata.ReserveEmission, len(reserveEmissions))
	for _, re := range reserveEmissions {
		reserveEmissionByPoolToken[poolTokenKey(string(re.PoolContractID), re.ReserveTokenID)] = re
	}

	userEmissionByPoolToken := make(map[string]blenddata.Emission, len(emissions))
	for _, e := range emissions {
		userEmissionByPoolToken[poolTokenKey(string(e.SourceContractID), e.TokenID)] = e
	}

	backstopPoolByID := make(map[string]blenddata.BackstopPool, len(backstopPools))
	for _, bp := range backstopPools {
		backstopPoolByID[string(bp.PoolContractID)] = bp
	}

	claimedByPool := map[string]string{}
	backstopClaimed := "0"
	for _, t := range claimTotals {
		if t.PoolID == nil {
			if t.Source != nil && *t.Source == "backstop" {
				backstopClaimed = t.Total
			}
			continue
		}
		if t.Source == nil || *t.Source != "pool" {
			continue
		}
		claimedByPool[*t.PoolID] = t.Total
	}

	assembly := &blendAssembly{
		poolByID:                   poolByID,
		reserveByPoolIndex:         reserveByPoolIndex,
		reserveEmissionByPoolToken: reserveEmissionByPoolToken,
		userEmissionByPoolToken:    userEmissionByPoolToken,
		backstopPoolByID:           backstopPoolByID,
		priceByOracleAsset:         priceByOracleAsset,
		metaByContractID:           metaByContractID,
		lpPrice:                    lpPrice,
		blndPrice:                  blndPrice,
		claimedByPool:              claimedByPool,
		now:                        now,
	}

	// Group reserve positions by pool, preserving GetByAccount's
	// (pool_contract_id, reserve_index) order for both pool-of-first-
	// appearance order and each pool's reserve order.
	positionsByPool := map[string][]blenddata.Position{}
	poolOrder := make([]string, 0, len(poolIDSet))
	seenPool := map[string]bool{}
	for _, p := range positions {
		poolAddr := string(p.PoolContractID)
		positionsByPool[poolAddr] = append(positionsByPool[poolAddr], p)
		if !seenPool[poolAddr] {
			seenPool[poolAddr] = true
			poolOrder = append(poolOrder, poolAddr)
		}
	}

	poolPositions := make([]*graphql1.BlendPoolPosition, 0, len(poolOrder))
	for _, poolAddr := range poolOrder {
		pp, err := assembly.buildPoolPosition(poolAddr, positionsByPool[poolAddr])
		if err != nil {
			return nil, err
		}
		poolPositions = append(poolPositions, pp)
	}

	backstopOut := make([]*graphql1.BlendBackstopPosition, 0, len(backstopPositions))
	for _, bp := range backstopPositions {
		bpOut, err := assembly.buildBackstopPosition(bp)
		if err != nil {
			return nil, err
		}
		backstopOut = append(backstopOut, bpOut)
	}

	return &graphql1.BlendAccountPositions{
		Pools:             poolPositions,
		Backstop:          backstopOut,
		BackstopClaimedLp: backstopClaimed,
	}, nil
}
