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
	"sort"
	"time"

	"github.com/stellar/go-stellar-sdk/support/log"
	"golang.org/x/sync/errgroup"

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
// emisIndex is the stream's emission index already projected to "now" by the
// caller (rates.go's ProjectEmissionIndex over the stored
// blend_reserve_emissions / blend_backstop_pools.emis_index value) — the
// stored index alone is only as fresh as the stream's last on-chain touch.
// Projection stops at expiration, so a past-expiration index still counts in
// full: expiry freezes further real-time growth but doesn't erase what
// accrued. A nil emisIndex means no config row exists at all (the stream was
// never configured); in that case no further growth beyond the user's own
// Accrued is possible, so the user's own index is used as both sides of the
// delta.
func claimableStream(userEmission blenddata.Emission, hasUser bool, emisIndex *big.Int, tokenBalance, scalar *big.Int) (*big.Int, error) {
	if !hasUser {
		if emisIndex == nil || tokenBalance.Sign() <= 0 {
			return big.NewInt(0), nil
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

	if emisIndex == nil {
		emisIndex = userIndex
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
// row) — see GetBackstopLPPrices' doc. Each group has exactly one sibling: the
// snapshot writer stores precisely two rows per Comet oracle (the self-priced LP
// row and the BLND row; see snapshotComet), so the sole sibling is unambiguously
// BLND. Blend v2 has a single protocol-wide backstop LP token — the emitter can
// swap it, but only sequentially, never two at once — so more than one complete
// group means a misconfigured Comet/oracle address: that state is logged loudly
// and resolved by always picking the lowest oracle key, keeping the choice
// deterministic instead of flapping with Go's map-iteration order. Returns
// (nil, nil) if no complete group is found.
func findBackstopPrices(ctx context.Context, rows []blenddata.OraclePrice) (lp, blnd *blenddata.OraclePrice) {
	selfPriced := map[string]blenddata.OraclePrice{}
	siblings := map[string][]blenddata.OraclePrice{}
	for _, row := range rows {
		if string(row.OracleContractID) == string(row.AssetContractID) {
			selfPriced[string(row.OracleContractID)] = row
		} else {
			siblings[string(row.OracleContractID)] = append(siblings[string(row.OracleContractID)], row)
		}
	}

	complete := make([]string, 0, len(selfPriced))
	for oracle := range selfPriced {
		if len(siblings[oracle]) > 0 {
			complete = append(complete, oracle)
		}
	}
	if len(complete) == 0 {
		return nil, nil
	}
	sort.Strings(complete)
	if len(complete) > 1 {
		log.Ctx(ctx).Errorf("blend: %d self-priced backstop LP price groups found (expected exactly 1 — misconfigured Comet/oracle address?); using oracle %s", len(complete), complete[0])
	}

	self := selfPriced[complete[0]]
	sibs := siblings[complete[0]]
	if len(sibs) > 1 {
		// Exactly one sibling (the BLND row) is expected per Comet oracle. More
		// than one means an extra asset was stored under the LP's oracle key,
		// making the BLND row ambiguous; log it and keep the lowest asset address
		// (rows are ordered by asset) so the choice stays deterministic.
		log.Ctx(ctx).Errorf("blend: backstop LP oracle %s has %d sibling price rows (expected 1 — the BLND row); using the lowest asset address", complete[0], len(sibs))
	}
	sib := sibs[0]
	return &self, &sib
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

// projectedReserveEmissionIndex returns a reserve emission stream's index
// projected to "now" (rates.go's ProjectEmissionIndex): the stored index plus
// the accrual earned since the stream's last on-chain touch, spread over the
// side's current raw token supply. nil (no error) when the stream was never
// configured.
func (d *blendAssembly) projectedReserveEmissionIndex(poolAddr string, tokenID int32, supply *big.Int, decimals int32) (*big.Int, error) {
	re, ok := d.reserveEmissionByPoolToken[poolTokenKey(poolAddr, tokenID)]
	if !ok {
		return nil, nil
	}
	stored, err := parseBigInt(re.EmissionIndex)
	if err != nil {
		return nil, fmt.Errorf("parsing blend emission config index: %w", err)
	}
	return blendrates.ProjectEmissionIndex(stored, re.Eps, re.LastTime, re.Expiration, d.now, supply, pow10(decimals)), nil
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

	pB, pD := blendrates.ProjectRates(bRate, dRate, bSupply, dSupply, borrowAPR, bstopRate, reserve.LastTime, d.now)

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

	// Blend runs two independent BLND-per-second emission streams per reserve
	// (dToken=idx*2, bToken=idx*2+1), so both sides are reported as their own
	// pool-wide APR rather than collapsed to whichever side this account
	// happens to hold. Each is the stream's pool-wide rate (over BSupply/DSupply
	// at the projected rate), not scaled to the account's balance.
	bTokenID := reserve.ReserveIndex*2 + 1
	dTokenID := reserve.ReserveIndex * 2
	bSideBalance := new(big.Int).Add(supplyB, collateralB)

	emissionsSupplyApr := d.emissionsAPRFor(poolAddr, bTokenID, rr.BSupply, rr.PB, reserve.Decimals, price)
	emissionsBorrowApr := d.emissionsAPRFor(poolAddr, dTokenID, rr.DSupply, rr.PD, reserve.Decimals, price)

	// emissionsEarnedBlnd always sums BOTH streams:
	// a position can carry claimable history on a side it no longer holds
	// (e.g. fully repaid debt with unclaimed dToken-stream emissions).
	claimScalar := reserveClaimScalar(reserve.Decimals)
	bEmisIndex, err := d.projectedReserveEmissionIndex(poolAddr, bTokenID, rr.BSupply, reserve.Decimals)
	if err != nil {
		return nil, err
	}
	bUserEmission, bHasUser := d.userEmissionByPoolToken[poolTokenKey(poolAddr, bTokenID)]
	bClaimable, err := claimableStream(bUserEmission, bHasUser, bEmisIndex, bSideBalance, claimScalar)
	if err != nil {
		return nil, err
	}
	dEmisIndex, err := d.projectedReserveEmissionIndex(poolAddr, dTokenID, rr.DSupply, reserve.Decimals)
	if err != nil {
		return nil, err
	}
	dUserEmission, dHasUser := d.userEmissionByPoolToken[poolTokenKey(poolAddr, dTokenID)]
	dClaimable, err := claimableStream(dUserEmission, dHasUser, dEmisIndex, liabilityD, claimScalar)
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
		EmissionsSupplyApr:  emissionsSupplyApr,
		EmissionsBorrowApr:  emissionsBorrowApr,
		InterestEarned:      interestEarned.String(),
		InterestPaid:        interestPaid.String(),
		EmissionsEarnedBlnd: claimableBLND.String(),
		EmissionsEarnedUsd:  emissionsEarnedUsd,
		PriceUsd:            priceUsdOrNil(price),
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
		// netApy divides by total supplied — NOT by the net position value —
		// matching blend-sdk-js's PositionsEstimate (user_positions_est.ts):
		// netApy = (Σ suppliedUsd·supplyApy − Σ borrowedUsd·borrowApy) /
		// totalSupplied, and exactly 0 for a supply-less position ("the debt
		// will be forgiven as bad debt so the net APY is still 0"). Dividing
		// by net value would diverge from the Blend UI and blow up as a
		// leveraged position's net value approaches 0.
		apy := 0.0
		if suppliedUsdSum != 0 {
			apy = (supplyApyNumerator - borrowApyNumerator) / suppliedUsdSum
		}
		netApy = &apy
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

// projectedBackstopEmissionIndex returns the backstop emission stream's index
// projected to "now" over the pool's UNQUEUED shares (shares − q4w):
// queued-for-withdrawal shares earn no emissions, so the contract distributor
// spreads accrual over active shares only (backstop/src/emissions/
// distributor.rs's update_emission_data). nil when the pool has never had an
// emission config (emis_index NULL). The emis_* columns are written together
// (BatchUpsertEmissions), so a non-nil index implies the other three are set;
// falling back to the stored index when they aren't is defensive only.
func projectedBackstopEmissionIndex(bp blenddata.BackstopPool, poolShares *big.Int, now int64) (*big.Int, error) {
	if bp.EmisIndex == nil {
		return nil, nil
	}
	stored, err := parseBigInt(*bp.EmisIndex)
	if err != nil {
		return nil, fmt.Errorf("parsing blend backstop emission index: %w", err)
	}
	if bp.EmisEps == nil || bp.EmisLastTime == nil || bp.EmisExpiration == nil {
		return stored, nil
	}
	poolQ4W, err := parseBigInt(bp.Q4W)
	if err != nil {
		return nil, fmt.Errorf("parsing blend backstop pool q4w: %w", err)
	}
	unqueued := new(big.Int).Sub(poolShares, poolQ4W)
	return blendrates.ProjectEmissionIndex(stored, *bp.EmisEps, *bp.EmisLastTime, *bp.EmisExpiration, now, unqueued, big.NewInt(scalar7)), nil
}

// blendAuctionTypeEnum maps a blend_auctions.auction_type value to its
// schema enum (0/1/2, per services/blend/entries.go's AuctionType doc). ok is
// false for any unrecognized value, which the caller skips rather than
// surfaces as a malformed enum.
func blendAuctionTypeEnum(auctionType int32) (graphql1.BlendAuctionType, bool) {
	switch auctionType {
	case 0:
		return graphql1.BlendAuctionTypeUserLiquidation, true
	case 1:
		return graphql1.BlendAuctionTypeBadDebt, true
	case 2:
		return graphql1.BlendAuctionTypeInterest, true
	default:
		return "", false
	}
}

// buildAuctionAmounts converts a bid/lot asset-amount map into a
// deterministically (asset address) sorted BlendAuctionAmount list.
func buildAuctionAmounts(amounts map[string]string) []*graphql1.BlendAuctionAmount {
	assets := make([]string, 0, len(amounts))
	for asset := range amounts {
		assets = append(assets, asset)
	}
	sort.Strings(assets)
	out := make([]*graphql1.BlendAuctionAmount, 0, len(assets))
	for _, asset := range assets {
		out = append(out, &graphql1.BlendAuctionAmount{AssetContractID: asset, Amount: amounts[asset]})
	}
	return out
}

// buildActiveAuction assembles one blend_auctions row into a BlendAuction.
// Returns (nil, false) for an unrecognized auction_type, logged by the
// caller and dropped from the list rather than surfaced as a malformed enum
// value.
func (d *blendAssembly) buildActiveAuction(ctx context.Context, a blenddata.Auction) (*graphql1.BlendAuction, bool) {
	enumType, ok := blendAuctionTypeEnum(a.AuctionType)
	if !ok {
		log.Ctx(ctx).Errorf("blend: skipping auction with unrecognized auction_type %d (pool %s, user %s)", a.AuctionType, a.Pool, a.User)
		return nil, false
	}
	return &graphql1.BlendAuction{
		PoolAddress: string(a.Pool),
		PoolName:    d.poolByID[string(a.Pool)].Name,
		AuctionType: enumType,
		Bid:         buildAuctionAmounts(a.Bid),
		Lot:         buildAuctionAmounts(a.Lot),
		StartBlock:  int32(a.StartBlock),
	}, true
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
	var emisIndex *big.Int
	if backstopPool, ok := d.backstopPoolByID[poolAddr]; ok {
		poolShares, err = parseBigInt(backstopPool.Shares)
		if err != nil {
			return nil, fmt.Errorf("parsing blend backstop pool shares: %w", err)
		}
		poolTokens, err = parseBigInt(backstopPool.Tokens)
		if err != nil {
			return nil, fmt.Errorf("parsing blend backstop pool tokens: %w", err)
		}
		emisIndex, err = projectedBackstopEmissionIndex(backstopPool, poolShares, d.now)
		if err != nil {
			return nil, err
		}
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
	claimable, err := claimableStream(userEmission, hasUser, emisIndex, shares, backstopClaimScalar)
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
	// Account-keyed reads: all keyed only by address and mutually independent, so
	// fetch them concurrently. errgroup cancels the shared context on the first
	// error, and Wait returns it.
	var (
		positions          []blenddata.Position
		backstopPositions  []blenddata.BackstopPosition
		emissions          []blenddata.Emission
		poolClaimed        []blenddata.PoolClaimed
		backstopClaimedRow *blenddata.BackstopClaimed
		auctions           []blenddata.Auction
	)
	accountGroup, accountCtx := errgroup.WithContext(ctx)
	accountGroup.Go(func() (err error) {
		positions, err = r.models.Blend.Positions.GetByAccount(accountCtx, address)
		if err != nil {
			return fmt.Errorf("getting blend positions for account %s: %w", address, err)
		}
		return nil
	})
	accountGroup.Go(func() (err error) {
		backstopPositions, err = r.models.Blend.BackstopPositions.GetByAccount(accountCtx, address)
		if err != nil {
			return fmt.Errorf("getting blend backstop positions for account %s: %w", address, err)
		}
		return nil
	})
	accountGroup.Go(func() (err error) {
		emissions, err = r.models.Blend.Emissions.GetByAccount(accountCtx, address)
		if err != nil {
			return fmt.Errorf("getting blend emissions for account %s: %w", address, err)
		}
		return nil
	})
	accountGroup.Go(func() (err error) {
		poolClaimed, err = r.models.Blend.PoolClaimed.GetByAccount(accountCtx, address)
		if err != nil {
			return fmt.Errorf("getting blend pool claimed totals for account %s: %w", address, err)
		}
		return nil
	})
	accountGroup.Go(func() (err error) {
		backstopClaimedRow, err = r.models.Blend.BackstopClaimed.GetByAccount(accountCtx, address)
		if err != nil {
			return fmt.Errorf("getting blend backstop claimed total for account %s: %w", address, err)
		}
		return nil
	})
	accountGroup.Go(func() (err error) {
		auctions, err = r.models.Blend.Auctions.GetByAccount(accountCtx, address)
		if err != nil {
			return fmt.Errorf("getting blend auctions for account %s: %w", address, err)
		}
		return nil
	})
	if err := accountGroup.Wait(); err != nil {
		return nil, err //nolint:wrapcheck // already wrapped inside the errgroup closures
	}

	poolIDSet := map[string]struct{}{}
	for _, p := range positions {
		poolIDSet[string(p.PoolContractID)] = struct{}{}
	}
	for _, bp := range backstopPositions {
		poolIDSet[string(bp.PoolContractID)] = struct{}{}
	}
	for _, a := range auctions {
		poolIDSet[string(a.Pool)] = struct{}{}
	}
	poolIDs := make([]string, 0, len(poolIDSet))
	for id := range poolIDSet {
		poolIDs = append(poolIDs, id)
	}

	// Pool-keyed reads: all keyed by poolIDs and mutually independent.
	var (
		pools            []blenddata.Pool
		reserves         []blenddata.Reserve
		reserveEmissions []blenddata.ReserveEmission
		backstopPools    []blenddata.BackstopPool
	)
	poolGroup, poolCtx := errgroup.WithContext(ctx)
	poolGroup.Go(func() (err error) {
		pools, err = r.models.Blend.Pools.GetByIDs(poolCtx, poolIDs)
		if err != nil {
			return fmt.Errorf("getting blend pools: %w", err)
		}
		return nil
	})
	poolGroup.Go(func() (err error) {
		reserves, err = r.models.Blend.Reserves.GetByPools(poolCtx, poolIDs)
		if err != nil {
			return fmt.Errorf("getting blend reserves: %w", err)
		}
		return nil
	})
	poolGroup.Go(func() (err error) {
		reserveEmissions, err = r.models.Blend.ReserveEmissions.GetByPools(poolCtx, poolIDs)
		if err != nil {
			return fmt.Errorf("getting blend reserve emissions: %w", err)
		}
		return nil
	})
	poolGroup.Go(func() (err error) {
		backstopPools, err = r.models.Blend.BackstopPools.GetByIDs(poolCtx, poolIDs)
		if err != nil {
			return fmt.Errorf("getting blend backstop pools: %w", err)
		}
		return nil
	})
	if err := poolGroup.Wait(); err != nil {
		return nil, err //nolint:wrapcheck // already wrapped inside the errgroup closures
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

	// Oracle prices (by oracleIDs), reserve-asset token metadata (by assetIDs),
	// and the backstop LP price pair (no input) are mutually independent once
	// oracleIDs and assetIDs are known.
	var (
		oraclePrices     []blenddata.OraclePrice
		backstopLPPrices []blenddata.OraclePrice
		tokenMeta        []data.Contract
	)
	priceGroup, priceCtx := errgroup.WithContext(ctx)
	priceGroup.Go(func() (err error) {
		oraclePrices, err = r.models.Blend.OraclePrices.GetByOracles(priceCtx, oracleIDs)
		if err != nil {
			return fmt.Errorf("getting blend oracle prices: %w", err)
		}
		return nil
	})
	priceGroup.Go(func() (err error) {
		backstopLPPrices, err = r.models.Blend.OraclePrices.GetBackstopLPPrices(priceCtx)
		if err != nil {
			return fmt.Errorf("getting blend backstop LP prices: %w", err)
		}
		return nil
	})
	priceGroup.Go(func() (err error) {
		tokenMeta, err = r.models.Contract.GetTokenMetadataByContractIDs(priceCtx, assetIDs)
		if err != nil {
			return fmt.Errorf("getting blend reserve asset token metadata: %w", err)
		}
		return nil
	})
	if err := priceGroup.Wait(); err != nil {
		return nil, err //nolint:wrapcheck // already wrapped inside the errgroup closures
	}
	now := time.Now().Unix()
	lpPrice, blndPrice := findBackstopPrices(ctx, freshPrices(backstopLPPrices, now))
	priceByOracleAsset := freshPriceMap(oraclePrices, now)
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

	// Lifetime claimed totals: pool-source BLND per pool, backstop-source Comet
	// LP account-wide (its on-chain claim event carries no pool address).
	claimedByPool := map[string]string{}
	for _, c := range poolClaimed {
		claimedByPool[string(c.PoolContractID)] = c.ClaimedBlnd
	}
	backstopClaimed := "0"
	if backstopClaimedRow != nil {
		backstopClaimed = backstopClaimedRow.ClaimedLp
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

	// auctions is already ordered by (pool_contract_id, auction_type) per
	// AuctionModel.GetByAccount's ORDER BY, mirroring how blend_pools.go's
	// pool-list ordering is inherited from its own reader rather than
	// re-sorted here.
	activeAuctions := make([]*graphql1.BlendAuction, 0, len(auctions))
	for _, a := range auctions {
		auctionOut, ok := assembly.buildActiveAuction(ctx, a)
		if !ok {
			continue
		}
		activeAuctions = append(activeAuctions, auctionOut)
	}

	return &graphql1.BlendAccountPositions{
		Pools:             poolPositions,
		Backstop:          backstopOut,
		BackstopClaimedLp: backstopClaimed,
		ActiveAuctions:    activeAuctions,
	}, nil
}
