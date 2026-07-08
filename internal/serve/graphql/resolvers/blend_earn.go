// Package resolvers — blend_earn.go assembles Query.blendEarnOptions: an
// asset-first "where can I earn this asset" catalog view across every
// enabled reserve in every Blend v2 pool the indexer has seen. It groups the
// exact same reserve-level rate-curve/pricing computation blend_pools.go's
// buildPoolReserve already exercises (via the shared blendAssembly and
// computeReserveRates) by asset instead of by pool — the two views differ
// only in that grouping, and in which reserves are in scope: a disabled
// reserve still shows up as a (disabled) BlendReserve in the pool-wide
// catalog, but accepts no new deposits, so it is never an earn option here.
package resolvers

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/stellar/wallet-backend/internal/data"
	blenddata "github.com/stellar/wallet-backend/internal/data/blend"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	graphql1 "github.com/stellar/wallet-backend/internal/serve/graphql/generated"
	blendrates "github.com/stellar/wallet-backend/internal/services/blend"
)

// buildEarnPoolOption assembles one enabled reserve into a
// BlendEarnPoolOption: the reserve's projected supplyApy and pool-wide
// suppliedUsd (Underlying(BSupply, pB) priced against the pool's own
// oracle), the identical numbers buildPoolReserve computes for this same
// reserve's BlendReserve.supplyApy/suppliedUsd.
func (d *blendAssembly) buildEarnPoolOption(reserve blenddata.Reserve) (*graphql1.BlendEarnPoolOption, error) {
	poolAddr := string(reserve.PoolContractID)
	rr, err := d.computeReserveRates(poolAddr, reserve)
	if err != nil {
		return nil, err
	}

	suppliedTokens := blendrates.Underlying(rr.BSupply, rr.PB)

	oracle := types.AddressBytea("")
	pool, ok := d.poolByID[poolAddr]
	if ok {
		oracle = pool.OracleContractID
	}
	price := d.priceLookup(oracle, reserve.AssetContractID)
	suppliedUsd := usdValueOrNil(suppliedTokens, reserve.Decimals, price)

	supplyApy := rr.SupplyApy
	return &graphql1.BlendEarnPoolOption{
		PoolAddress: poolAddr,
		PoolName:    pool.Name,
		SupplyApy:   &supplyApy,
		SuppliedUsd: suppliedUsd,
	}, nil
}

// sortEarnPoolOptions orders one asset's pool options by suppliedUsd
// descending — the largest existing supply offer first. A missing
// suppliedUsd (no oracle price wired for that pool's own oracle/asset pair)
// sorts last rather than first or dropping out, since "unknown size" isn't
// "biggest" or "smallest" but IS less useful to show first. Ties — including
// nil vs. nil — break by ascending pool address so the order is fully
// deterministic.
func sortEarnPoolOptions(opts []*graphql1.BlendEarnPoolOption) {
	sort.Slice(opts, func(i, j int) bool {
		a, b := opts[i], opts[j]
		switch {
		case a.SuppliedUsd == nil && b.SuppliedUsd == nil:
			return a.PoolAddress < b.PoolAddress
		case a.SuppliedUsd == nil:
			return false
		case b.SuppliedUsd == nil:
			return true
		case *a.SuppliedUsd != *b.SuppliedUsd:
			return *a.SuppliedUsd > *b.SuppliedUsd
		default:
			return a.PoolAddress < b.PoolAddress
		}
	})
}

// buildEarnOption groups one asset's enabled reserves — one per pool that
// lists it — into a BlendEarnOption, with its pools ordered per
// sortEarnPoolOptions. reserves is never empty: callers only invoke this for
// an assetID that has at least one enabled reserve backing it.
func (d *blendAssembly) buildEarnOption(assetID string, reserves []blenddata.Reserve) (*graphql1.BlendEarnOption, error) {
	pools := make([]*graphql1.BlendEarnPoolOption, 0, len(reserves))
	for _, reserve := range reserves {
		opt, err := d.buildEarnPoolOption(reserve)
		if err != nil {
			return nil, err
		}
		pools = append(pools, opt)
	}
	sortEarnPoolOptions(pools)

	meta := d.metaByContractID[assetID]
	decimals := reserves[0].Decimals

	return &graphql1.BlendEarnOption{
		AssetContractID: assetID,
		TokenName:       meta.Name,
		TokenSymbol:     meta.Symbol,
		TokenDecimals:   &decimals,
		Pools:           pools,
	}, nil
}

// getBlendEarnOptions is the main implementation for Query.blendEarnOptions:
// an asset-first "where can I earn this asset" catalog view, batch-fetching
// every enabled reserve across every pool plus the oracle prices and token
// metadata they touch, then grouping by asset. Assets are ordered ascending
// by contract address (a plain C-address string comparison); see
// sortEarnPoolOptions for how each asset's own pools are ordered.
func (r *Resolver) getBlendEarnOptions(ctx context.Context) ([]*graphql1.BlendEarnOption, error) {
	reserves, err := r.models.Blend.Reserves.GetAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting blend reserves: %w", err)
	}
	pools, err := r.models.Blend.Pools.GetAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting blend pools: %w", err)
	}

	poolByID := make(map[string]blenddata.Pool, len(pools))
	oracleIDSet := map[string]struct{}{}
	for _, p := range pools {
		poolByID[string(p.PoolContractID)] = p
		if p.OracleContractID != "" {
			oracleIDSet[string(p.OracleContractID)] = struct{}{}
		}
	}

	reservesByAsset := map[string][]blenddata.Reserve{}
	assetIDSet := map[string]struct{}{}
	for _, res := range reserves {
		if !res.Enabled {
			continue
		}
		assetID := string(res.AssetContractID)
		reservesByAsset[assetID] = append(reservesByAsset[assetID], res)
		assetIDSet[assetID] = struct{}{}
	}
	assetIDs := make([]string, 0, len(assetIDSet))
	for id := range assetIDSet {
		assetIDs = append(assetIDs, id)
	}
	sort.Strings(assetIDs)

	tokenMeta, err := r.models.Contract.GetTokenMetadataByContractIDs(ctx, assetIDs)
	if err != nil {
		return nil, fmt.Errorf("getting blend reserve asset token metadata: %w", err)
	}
	metaByContractID := make(map[string]data.Contract, len(tokenMeta))
	for _, c := range tokenMeta {
		metaByContractID[c.ContractID] = c
	}

	oracleIDs := make([]string, 0, len(oracleIDSet))
	for id := range oracleIDSet {
		oracleIDs = append(oracleIDs, id)
	}
	oraclePrices, err := r.models.Blend.OraclePrices.GetByOracles(ctx, oracleIDs)
	if err != nil {
		return nil, fmt.Errorf("getting blend oracle prices: %w", err)
	}
	priceByOracleAsset := make(map[string]blenddata.OraclePrice, len(oraclePrices))
	for _, op := range oraclePrices {
		priceByOracleAsset[string(op.OracleContractID)+"|"+string(op.AssetContractID)] = op
	}

	assembly := &blendAssembly{
		poolByID:           poolByID,
		priceByOracleAsset: priceByOracleAsset,
		metaByContractID:   metaByContractID,
		now:                time.Now().Unix(),
	}

	out := make([]*graphql1.BlendEarnOption, 0, len(assetIDs))
	for _, assetID := range assetIDs {
		opt, err := assembly.buildEarnOption(assetID, reservesByAsset[assetID])
		if err != nil {
			return nil, err
		}
		out = append(out, opt)
	}
	return out, nil
}
