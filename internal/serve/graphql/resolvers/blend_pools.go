// Package resolvers — blend_pools.go assembles Query.blendPools/blendPool: a
// pool-wide catalog view of every Blend v2 pool the indexer has seen (or one
// named pool), independent of any account. It shares blendAssembly and its
// rate-curve/pricing helpers with blend_positions.go — the two views differ
// only in which raw b/dToken amount they apply a reserve's projected rates
// to: an account's holdings there, a reserve's pool-wide bSupply/dSupply
// here.
package resolvers

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/stellar/wallet-backend/internal/data"
	blenddata "github.com/stellar/wallet-backend/internal/data/blend"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	graphql1 "github.com/stellar/wallet-backend/internal/serve/graphql/generated"
	blendrates "github.com/stellar/wallet-backend/internal/services/blend"
	"github.com/stellar/wallet-backend/internal/utils"
)

// addressPtrOrNil converts an AddressBytea into a *string, nil when empty —
// blend_pools.oracle_contract_id is SQL NULL (scanned as "") when no oracle
// is wired for the pool yet.
func addressPtrOrNil(a types.AddressBytea) *string {
	if a == "" {
		return nil
	}
	s := string(a)
	return &s
}

// priceUsdOrNil converts an oracle price row into its per-unit USD value
// (price/10^priceDecimals), or nil when no price row exists for the asset.
func priceUsdOrNil(price *blenddata.OraclePrice) *float64 {
	if price == nil {
		return nil
	}
	priceInt, err := parseBigInt(price.Price)
	if err != nil {
		return nil
	}
	v := new(big.Rat).SetFrac(priceInt, pow10(price.PriceDecimals))
	f, _ := v.Float64()
	return &f
}

// buildPoolReserve assembles one blend_reserves row into a pool-wide
// BlendReserve catalog entry: utilization/APY/rate-curve outputs come from
// the same computeReserveRates as buildReservePosition, applied here to the
// reserve's own pool-wide BSupply/DSupply rather than an account's holdings.
// emissionsSupplyApr/emissionsBorrowApr size their APR against that same
// pool-wide side, matching buildReservePosition's emissionsAPRFor calls
// exactly (both value the sideRaw denominator at the PROJECTED PB/PD rates,
// consistent with every other as-of-now token/USD figure on the type).
func (d *blendAssembly) buildPoolReserve(poolAddr string, reserve blenddata.Reserve) (*graphql1.BlendReserve, error) {
	rr, err := d.computeReserveRates(poolAddr, reserve)
	if err != nil {
		return nil, err
	}

	suppliedTokens := blendrates.Underlying(rr.BSupply, rr.PB)
	borrowedTokens := blendrates.Underlying(rr.DSupply, rr.PD)

	oracle := types.AddressBytea("")
	if pool, ok := d.poolByID[poolAddr]; ok {
		oracle = pool.OracleContractID
	}
	price := d.priceLookup(oracle, reserve.AssetContractID)

	suppliedUsd := usdValueOrNil(suppliedTokens, reserve.Decimals, price)
	borrowedUsd := usdValueOrNil(borrowedTokens, reserve.Decimals, price)

	bTokenID := reserve.ReserveIndex*2 + 1
	dTokenID := reserve.ReserveIndex * 2
	emissionsSupplyApr := d.emissionsAPRFor(poolAddr, bTokenID, rr.BSupply, rr.PB, reserve.Decimals, price)
	emissionsBorrowApr := d.emissionsAPRFor(poolAddr, dTokenID, rr.DSupply, rr.PD, reserve.Decimals, price)

	utilization, _ := rr.CurrentUtil.Float64()
	priceUsd := priceUsdOrNil(price)

	meta := d.metaByContractID[string(reserve.AssetContractID)]
	decimals := reserve.Decimals
	cFactor := reserve.CFactor
	lFactor := reserve.LFactor
	supplyApy := rr.SupplyApy
	borrowApy := rr.BorrowApy

	return &graphql1.BlendReserve{
		AssetContractID:    string(reserve.AssetContractID),
		TokenName:          meta.Name,
		TokenSymbol:        meta.Symbol,
		TokenDecimals:      &decimals,
		Enabled:            reserve.Enabled,
		Utilization:        &utilization,
		SupplyApy:          &supplyApy,
		BorrowApy:          &borrowApy,
		EmissionsSupplyApr: emissionsSupplyApr,
		EmissionsBorrowApr: emissionsBorrowApr,
		SuppliedTokens:     suppliedTokens.String(),
		BorrowedTokens:     borrowedTokens.String(),
		SuppliedUsd:        suppliedUsd,
		BorrowedUsd:        borrowedUsd,
		CFactor:            &cFactor,
		LFactor:            &lFactor,
		PriceUsd:           priceUsd,
	}, nil
}

// backstopUsdForPool is blend_backstop_pools.tokens (the pool's backstop-LP
// token balance, always 7-decimal) priced at the Comet LP rate. A pool with
// no backstop_pools row yet (no backstop deposit ever observed) is treated
// as a genuine 0 tokens, not missing data.
func (d *blendAssembly) backstopUsdForPool(poolAddr string) (*float64, error) {
	tokens := big.NewInt(0)
	if bp, ok := d.backstopPoolByID[poolAddr]; ok {
		var err error
		tokens, err = parseBigInt(bp.Tokens)
		if err != nil {
			return nil, fmt.Errorf("parsing blend backstop pool tokens: %w", err)
		}
	}
	return usdValueOrNil(tokens, backstopLPDecimals, d.lpPrice), nil
}

// blendPoolStatusEnum maps a blend_pools.status value to its schema enum
// (on-chain encoding 0-6, per the BlendPoolStatus doc). status is a pointer
// because the column is null until the pool's config entry has been ingested;
// nil status, and any unrecognized value, both yield (nil, matching the field
// doc) — the caller surfaces null rather than a malformed enum.
func blendPoolStatusEnum(status *int32) *graphql1.BlendPoolStatus {
	if status == nil {
		return nil
	}
	var out graphql1.BlendPoolStatus
	switch *status {
	case 0:
		out = graphql1.BlendPoolStatusAdminActive
	case 1:
		out = graphql1.BlendPoolStatusActive
	case 2:
		out = graphql1.BlendPoolStatusAdminOnIce
	case 3:
		out = graphql1.BlendPoolStatusOnIce
	case 4:
		out = graphql1.BlendPoolStatusAdminFrozen
	case 5:
		out = graphql1.BlendPoolStatusFrozen
	case 6:
		out = graphql1.BlendPoolStatusSetup
	default:
		return nil
	}
	return &out
}

// buildPool assembles one blend_pools row plus its reserves into a
// pool-wide BlendPool catalog entry.
//
// suppliedUsd/borrowedUsd become nil (not a silently understated sum) as
// soon as any contributing reserve's own suppliedUsd/borrowedUsd is nil,
// mirroring buildPoolPosition's propagation rule: a missing price on one
// reserve makes the pool-wide total genuinely uncomputable, not just
// smaller.
//
// interestApy and netApy are both supplied-USD-weighted means across
// reserves: interestApy = Σ(suppliedUsd_r × supplyApy_r) / Σ suppliedUsd_r.
// netApy additionally folds each reserve's emissionsSupplyApr into the
// weighted term before dividing — i.e. netApy is the supply-side yield an
// LP earns including BLND emissions, NOT the supply-vs-borrow netting
// BlendPoolPosition.netApy computes for an account's actual position. This
// pool-wide catalog view has no "account's borrow exposure" to net against,
// so "net" here means "net of emissions", not "net of debt". Both aggregates
// become nil under the same missing-price/missing-emissions-price
// propagation as suppliedUsd, and both are left nil (rather than a
// division-by-zero) when total suppliedUsd is exactly 0 — there is nothing
// to weight an average over.
func (d *blendAssembly) buildPool(pool blenddata.Pool, reserves []blenddata.Reserve) (*graphql1.BlendPool, error) {
	poolAddr := string(pool.PoolContractID)
	reserveOut := make([]*graphql1.BlendReserve, 0, len(reserves))

	suppliedUsdSum, borrowedUsdSum := 0.0, 0.0
	interestApyNumerator, netApyNumerator := 0.0, 0.0
	suppliedKnown, borrowedKnown, emissionsKnown := true, true, true

	for _, reserve := range reserves {
		rOut, err := d.buildPoolReserve(poolAddr, reserve)
		if err != nil {
			return nil, err
		}
		reserveOut = append(reserveOut, rOut)

		switch {
		case rOut.SuppliedUsd == nil:
			suppliedKnown = false
		case suppliedKnown:
			suppliedUsdSum += *rOut.SuppliedUsd
			if rOut.SupplyApy != nil {
				interestApyNumerator += *rOut.SuppliedUsd * *rOut.SupplyApy
				if rOut.EmissionsSupplyApr != nil {
					netApyNumerator += *rOut.SuppliedUsd * (*rOut.SupplyApy + *rOut.EmissionsSupplyApr)
				} else {
					emissionsKnown = false
				}
			}
		}
		if rOut.BorrowedUsd == nil {
			borrowedKnown = false
		} else if borrowedKnown {
			borrowedUsdSum += *rOut.BorrowedUsd
		}
	}

	var suppliedUsd, borrowedUsd, interestApy, netApy *float64
	if suppliedKnown {
		v := suppliedUsdSum
		suppliedUsd = &v
		if suppliedUsdSum != 0 {
			ia := interestApyNumerator / suppliedUsdSum
			interestApy = &ia
			if emissionsKnown {
				na := netApyNumerator / suppliedUsdSum
				netApy = &na
			}
		}
	}
	if borrowedKnown {
		v := borrowedUsdSum
		borrowedUsd = &v
	}

	backstopUsd, err := d.backstopUsdForPool(poolAddr)
	if err != nil {
		return nil, err
	}

	return &graphql1.BlendPool{
		Address:          poolAddr,
		Name:             pool.Name,
		Status:           blendPoolStatusEnum(pool.Status),
		OracleContractID: addressPtrOrNil(pool.OracleContractID),
		BackstopRate:     pool.BackstopRate,
		MaxPositions:     pool.MaxPositions,
		SuppliedUsd:      suppliedUsd,
		BorrowedUsd:      borrowedUsd,
		BackstopUsd:      backstopUsd,
		InterestApy:      interestApy,
		NetApy:           netApy,
		Reserves:         reserveOut,
		Admin:            addressPtrOrNil(pool.Admin),
		InRewardZone:     pool.InRewardZone,
	}, nil
}

// buildBlendPoolCatalog batch-fetches every reserve/reserve-emission/
// backstop-pool/oracle-price/token-metadata row the given pools touch and
// assembles each into a graphql1.BlendPool, preserving pools' input order
// (both Pools.GetAll and Pools.GetByIDs already return a deterministic
// order — see their godoc).
func (r *Resolver) buildBlendPoolCatalog(ctx context.Context, pools []blenddata.Pool) ([]*graphql1.BlendPool, error) {
	poolIDs := make([]string, 0, len(pools))
	poolByID := make(map[string]blenddata.Pool, len(pools))
	oracleIDSet := map[string]struct{}{}
	for _, p := range pools {
		poolIDs = append(poolIDs, string(p.PoolContractID))
		poolByID[string(p.PoolContractID)] = p
		if p.OracleContractID != "" {
			oracleIDSet[string(p.OracleContractID)] = struct{}{}
		}
	}

	oracleIDs := make([]string, 0, len(oracleIDSet))
	for id := range oracleIDSet {
		oracleIDs = append(oracleIDs, id)
	}

	// Pool-keyed reads plus oracle prices (oracleIDs are known from the input
	// pools) and the backstop LP pair (no input) are all mutually independent, so
	// fetch them concurrently.
	var (
		reserves         []blenddata.Reserve
		reserveEmissions []blenddata.ReserveEmission
		backstopPools    []blenddata.BackstopPool
		oraclePrices     []blenddata.OraclePrice
		backstopLPPrices []blenddata.OraclePrice
	)
	poolGroup, poolCtx := errgroup.WithContext(ctx)
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
	poolGroup.Go(func() (err error) {
		oraclePrices, err = r.models.Blend.OraclePrices.GetByOracles(poolCtx, oracleIDs)
		if err != nil {
			return fmt.Errorf("getting blend oracle prices: %w", err)
		}
		return nil
	})
	poolGroup.Go(func() (err error) {
		backstopLPPrices, err = r.models.Blend.OraclePrices.GetBackstopLPPrices(poolCtx, r.blendBackstopLPContractID)
		if err != nil {
			return fmt.Errorf("getting blend backstop LP prices: %w", err)
		}
		return nil
	})
	if err := poolGroup.Wait(); err != nil {
		return nil, err //nolint:wrapcheck // already wrapped inside the errgroup closures
	}

	reservesByPool := map[string][]blenddata.Reserve{}
	assetIDSet := map[string]struct{}{}
	for _, res := range reserves {
		poolAddr := string(res.PoolContractID)
		reservesByPool[poolAddr] = append(reservesByPool[poolAddr], res)
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

	now := time.Now().Unix()
	lpPrice, blndPrice := findBackstopPrices(ctx, freshPrices(backstopLPPrices, now))
	priceByOracleAsset := freshPriceMap(oraclePrices, now)

	reserveEmissionByPoolToken := make(map[string]blenddata.ReserveEmission, len(reserveEmissions))
	for _, re := range reserveEmissions {
		reserveEmissionByPoolToken[poolTokenKey(string(re.PoolContractID), re.ReserveTokenID)] = re
	}

	backstopPoolByID := make(map[string]blenddata.BackstopPool, len(backstopPools))
	for _, bp := range backstopPools {
		backstopPoolByID[string(bp.PoolContractID)] = bp
	}

	assembly := &blendAssembly{
		poolByID:                   poolByID,
		reserveEmissionByPoolToken: reserveEmissionByPoolToken,
		backstopPoolByID:           backstopPoolByID,
		priceByOracleAsset:         priceByOracleAsset,
		metaByContractID:           metaByContractID,
		lpPrice:                    lpPrice,
		blndPrice:                  blndPrice,
		now:                        now,
	}

	out := make([]*graphql1.BlendPool, 0, len(pools))
	for _, p := range pools {
		bp, err := assembly.buildPool(p, reservesByPool[string(p.PoolContractID)])
		if err != nil {
			return nil, err
		}
		out = append(out, bp)
	}
	return out, nil
}

// getBlendPools is the main implementation for Query.blendPools: the
// pool-wide catalog view across every Blend v2 pool the indexer has ever
// seen.
func (r *Resolver) getBlendPools(ctx context.Context) ([]*graphql1.BlendPool, error) {
	pools, err := r.models.Blend.Pools.GetAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting blend pools: %w", err)
	}
	return r.buildBlendPoolCatalog(ctx, pools)
}

// getBlendPool is the main implementation for Query.blendPool: the pool-wide
// catalog view for a single pool address. Returns (nil, nil) — not an
// error — for a well-formed but unknown pool address, per this field's
// nullable-BlendPool schema contract.
func (r *Resolver) getBlendPool(ctx context.Context, address string) (*graphql1.BlendPool, error) {
	if !utils.IsContractAddress(address) {
		return nil, badUserInputError(ErrMsgInvalidPoolAddress)
	}

	pools, err := r.models.Blend.Pools.GetByIDs(ctx, []string{address})
	if err != nil {
		return nil, fmt.Errorf("getting blend pool %s: %w", address, err)
	}
	if len(pools) == 0 {
		return nil, nil
	}

	out, err := r.buildBlendPoolCatalog(ctx, pools)
	if err != nil {
		return nil, err
	}
	return out[0], nil
}
