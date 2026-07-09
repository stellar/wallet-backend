// Package blend — prices.go implements the periodic SEP-40 oracle price
// snapshot task: on a fixed interval, it discovers every (oracle, asset) pair
// some tracked pool's reserve prices against (blenddata.OraclePriceModel.
// GetPriceTargets), fetches each oracle's decimals() once and every target's
// lastprice(asset) (scval.go's buildSep40StellarAsset/decodePriceData), and
// optionally derives the Blend v2 backstop's Comet BLND:USDC LP deposit
// token's BLND and LP-share USD prices from the pool's own on-chain state
// (comet.go) — no on-chain oracle prices a Comet LP token directly. Every row
// from one pass is written in a single blenddata.OraclePriceModel.BatchUpsert
// call.
//
// Comet leg row convention (locked cross-PR contract — PR5 reads these rows):
// both rows are stored under oracle_contract_id = the Comet pool's own
// C-address (a schema convenience key, not a claim the pool implements
// SEP-40). The LP-share row is self-priced: asset_contract_id ==
// oracle_contract_id. The BLND row's asset_contract_id is the real BLND
// token address (cometState.BLNDAddress). Both are recorded at Comet's
// 7-decimal STROOP scale (cometPriceDecimals) with PriceTimestamp set to the
// snapshot's own wall-clock time (unix seconds), since the Comet pool state
// carries no oracle-reported timestamp of its own.
//
// A price the pool contract itself would reject is never persisted: an
// oracle-reported timestamp older than MaxPriceAge is skipped as "stale" and
// a price ≤ 0 as "invalid" (both observable via the fetch-outcome counter,
// staleness additionally via the oldest-price-age gauge).
//
// A failure fetching one oracle's decimals or any of its lastprice targets
// is isolated to that oracle: it never aborts the rest of the pass. All such
// failures (and a Comet leg failure, when configured) are joined with
// errors.Join and returned to the caller for logging; SnapshotOnce still
// upserts whatever rows the pass did manage to collect.
package blend

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/stellar/go-stellar-sdk/support/log"

	blenddata "github.com/stellar/wallet-backend/internal/data/blend"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/services"
)

// MaxPriceAge is how old an oracle-reported price may be and still be
// persisted. It mirrors the Blend v2 pool contract's own rejection window:
// pool.rs::load_price panics on prices older than 24h (and on prices ≤ 0),
// so a price past this age could never be used by the pool it prices for.
const MaxPriceAge = 24 * time.Hour

// PriceSnapshotConfig configures a PriceSnapshotService.
type PriceSnapshotConfig struct {
	// OraclePrices is the blend_oracle_prices data access layer: GetPriceTargets
	// discovers what to fetch, BatchUpsert persists what was fetched.
	OraclePrices *blenddata.OraclePriceModel
	// Metadata performs the SEP-40 RPC simulation calls (decimals, lastprice)
	// against oracle contracts.
	Metadata services.ContractMetadataService
	// Interval is the wait between the end of one snapshot pass and the start
	// of the next.
	Interval time.Duration
	// BackstopLPContractID is the Comet BLND:USDC weighted pool backing the
	// Blend v2 backstop's deposit token, as a strkey C-address. Empty disables
	// the BLND/LP-share derived-pricing leg entirely.
	BackstopLPContractID string
	// RPC reads the Comet pool's ContractData ledger entries (fetchCometState).
	// Required only when BackstopLPContractID is set.
	RPC services.RPCService
	// Metrics receives per-pass duration, per-fetch outcome, and tracked-row-
	// count observations.
	Metrics *metrics.BlendPriceMetrics
}

// PriceSnapshotService periodically snapshots SEP-40 oracle prices — and,
// when configured, the Comet backstop LP/BLND leg — into blend_oracle_prices.
// See the package doc for the persistence and error-isolation contract.
type PriceSnapshotService struct {
	oraclePrices *blenddata.OraclePriceModel
	metadata     services.ContractMetadataService
	interval     time.Duration
	cometID      string
	rpc          services.RPCService
	metrics      *metrics.BlendPriceMetrics
}

// NewPriceSnapshotService validates cfg and constructs a PriceSnapshotService.
// BackstopLPContractID is optional; RPC is required alongside it.
func NewPriceSnapshotService(cfg PriceSnapshotConfig) (*PriceSnapshotService, error) {
	if cfg.OraclePrices == nil {
		return nil, fmt.Errorf("blend: PriceSnapshotConfig.OraclePrices is required")
	}
	if cfg.Metadata == nil {
		return nil, fmt.Errorf("blend: PriceSnapshotConfig.Metadata is required")
	}
	if cfg.Interval <= 0 {
		return nil, fmt.Errorf("blend: PriceSnapshotConfig.Interval must be positive, got %s", cfg.Interval)
	}
	if cfg.BackstopLPContractID != "" && cfg.RPC == nil {
		return nil, fmt.Errorf("blend: PriceSnapshotConfig.RPC is required when BackstopLPContractID is set")
	}
	if cfg.Metrics == nil {
		return nil, fmt.Errorf("blend: PriceSnapshotConfig.Metrics is required")
	}
	return &PriceSnapshotService{
		oraclePrices: cfg.OraclePrices,
		metadata:     cfg.Metadata,
		interval:     cfg.Interval,
		cometID:      cfg.BackstopLPContractID,
		rpc:          cfg.RPC,
		metrics:      cfg.Metrics,
	}, nil
}

// Run snapshots once immediately, then repeatedly until ctx is cancelled,
// waiting s.interval AFTER each pass completes (end-to-start delay). A
// ticker would queue a tick while a slow pass runs — many oracle targets
// plus RPC retries can exceed the interval — and the next pass would then
// start immediately, hammering RPC/DB back-to-back. A failed pass is
// logged, never fatal — the next pass tries again.
func (s *PriceSnapshotService) Run(ctx context.Context) {
	timer := time.NewTimer(0) // fires immediately: the first pass runs at startup
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		if err := s.SnapshotOnce(ctx); err != nil {
			log.Ctx(ctx).Warnf("blend: price snapshot pass failed: %v", err)
		}
		timer.Reset(s.interval)
	}
}

// SnapshotOnce runs a single price-snapshot pass: discover targets, fetch
// every oracle's prices (isolating per-oracle failures), optionally derive
// the Comet leg, and upsert everything collected in one BatchUpsert call.
// Returns an errors.Join of every isolated failure (nil if there were none);
// a non-nil error does not mean no rows were written.
func (s *PriceSnapshotService) SnapshotOnce(ctx context.Context) error {
	start := time.Now()
	defer func() {
		s.metrics.SnapshotDuration.Observe(time.Since(start).Seconds())
	}()

	targets, err := s.oraclePrices.GetPriceTargets(ctx)
	if err != nil {
		return fmt.Errorf("blend: price snapshot: fetching price targets: %w", err)
	}

	byOracle := make(map[string][]blenddata.PriceTarget)
	for _, target := range targets {
		oracle := string(target.OracleContractID)
		byOracle[oracle] = append(byOracle[oracle], target)
	}

	now := time.Now().Unix()
	var rows []blenddata.OraclePrice
	var errs []error
	var oldestAge int64
	for oracle, oracleTargets := range byOracle {
		oracleRows, oracleOldestAge, oracleErr := s.snapshotOracle(ctx, oracle, oracleTargets, now)
		if oracleErr != nil {
			errs = append(errs, fmt.Errorf("blend: price snapshot: oracle %s: %w", oracle, oracleErr))
		}
		if oracleOldestAge > oldestAge {
			oldestAge = oracleOldestAge
		}
		rows = append(rows, oracleRows...)
	}
	s.metrics.OldestPriceAge.Set(float64(oldestAge))

	if s.cometID != "" {
		cometRows, cometErr := s.snapshotComet(ctx)
		if cometErr != nil {
			s.metrics.FetchesTotal.WithLabelValues("error").Inc()
			errs = append(errs, fmt.Errorf("blend: price snapshot: comet leg: %w", cometErr))
		} else {
			s.metrics.FetchesTotal.WithLabelValues("success").Add(float64(len(cometRows)))
			rows = append(rows, cometRows...)
		}
	}

	written := len(rows)
	if len(rows) > 0 {
		if upsertErr := s.oraclePrices.BatchUpsert(ctx, rows); upsertErr != nil {
			errs = append(errs, fmt.Errorf("blend: price snapshot: persisting %d rows: %w", len(rows), upsertErr))
			written = 0
		}
	}
	s.metrics.PricesTracked.Set(float64(written))

	return errors.Join(errs...)
}

// snapshotOracle fetches decimals() once for oracle, then lastprice(asset)
// for each of targets, skipping (not erroring on) a SEP-40 Option::None
// response, a price the pool contract would reject as stale (older than
// MaxPriceAge against now) or invalid (≤ 0). A decimals() failure aborts
// this oracle's whole batch — every target's price is meaningless without
// it — but is isolated to this oracle by the caller. A single target's
// lastprice failure is isolated to that target: the rest of this oracle's
// targets are still attempted. The second return value is the age in
// seconds of the oldest positive price decoded (including skipped-as-stale
// ones — that growth is the dead-oracle signal the gauge exists for;
// invalid ≤ 0 prices are excluded, their timestamps mean nothing).
func (s *PriceSnapshotService) snapshotOracle(ctx context.Context, oracle string, targets []blenddata.PriceTarget, now int64) ([]blenddata.OraclePrice, int64, error) {
	decimalsVal, err := s.metadata.FetchSingleField(ctx, oracle, "decimals")
	if err != nil {
		s.metrics.FetchesTotal.WithLabelValues("error").Inc()
		return nil, 0, fmt.Errorf("fetching decimals: %w", err)
	}
	decimals, ok := u32Val(decimalsVal)
	if !ok {
		s.metrics.FetchesTotal.WithLabelValues("error").Inc()
		return nil, 0, fmt.Errorf("decimals: expected u32, got %v", decimalsVal.Type)
	}

	rows := make([]blenddata.OraclePrice, 0, len(targets))
	var errs []error
	var oldestAge int64
	for _, target := range targets {
		assetArg, buildErr := buildSep40StellarAsset(string(target.AssetContractID))
		if buildErr != nil {
			s.metrics.FetchesTotal.WithLabelValues("error").Inc()
			errs = append(errs, fmt.Errorf("asset %s: building lastprice argument: %w", target.AssetContractID, buildErr))
			continue
		}

		priceVal, fetchErr := s.metadata.FetchSingleField(ctx, oracle, "lastprice", assetArg)
		if fetchErr != nil {
			s.metrics.FetchesTotal.WithLabelValues("error").Inc()
			errs = append(errs, fmt.Errorf("asset %s: fetching lastprice: %w", target.AssetContractID, fetchErr))
			continue
		}

		priceData, decodeErr := decodePriceData(priceVal)
		if decodeErr != nil {
			s.metrics.FetchesTotal.WithLabelValues("error").Inc()
			errs = append(errs, fmt.Errorf("asset %s: decoding lastprice: %w", target.AssetContractID, decodeErr))
			continue
		}
		if priceData == nil {
			s.metrics.FetchesTotal.WithLabelValues("none").Inc()
			continue
		}
		if priceData.Price.Sign() <= 0 {
			s.metrics.FetchesTotal.WithLabelValues("invalid").Inc()
			continue
		}
		if age := now - int64(priceData.Timestamp); age > oldestAge {
			oldestAge = age
		}
		if int64(priceData.Timestamp) < now-int64(MaxPriceAge/time.Second) {
			s.metrics.FetchesTotal.WithLabelValues("stale").Inc()
			continue
		}

		s.metrics.FetchesTotal.WithLabelValues("success").Inc()
		rows = append(rows, blenddata.OraclePrice{
			OracleContractID: target.OracleContractID,
			AssetContractID:  target.AssetContractID,
			Price:            priceData.Price.String(),
			PriceDecimals:    int32(decimals),
			PriceTimestamp:   int64(priceData.Timestamp),
		})
	}

	return rows, oldestAge, errors.Join(errs...)
}

// snapshotComet derives the BLND and Comet LP-share USD prices from the
// configured backstop Comet pool's raw on-chain state (fetchCometState,
// cometValuation) and returns their blend_oracle_prices rows. See the
// package doc for the row-key convention.
func (s *PriceSnapshotService) snapshotComet(ctx context.Context) ([]blenddata.OraclePrice, error) {
	state, err := fetchCometState(ctx, s.rpc, s.cometID)
	if err != nil {
		return nil, fmt.Errorf("fetching comet state: %w", err)
	}

	blndPrice, lpPrice, err := cometValuation(state.BLNDBalance, state.USDCBalance, state.BLNDWeight, state.USDCWeight, state.LPSupply)
	if err != nil {
		return nil, fmt.Errorf("valuing comet pool: %w", err)
	}

	now := time.Now().Unix()
	cometAddr := types.AddressBytea(s.cometID)
	return []blenddata.OraclePrice{
		{
			OracleContractID: cometAddr,
			AssetContractID:  types.AddressBytea(state.BLNDAddress),
			Price:            blndPrice,
			PriceDecimals:    cometPriceDecimals,
			PriceTimestamp:   now,
		},
		{
			// The LP share is self-priced: this row's asset is the Comet pool
			// itself, matching the locked cross-PR row-key convention.
			OracleContractID: cometAddr,
			AssetContractID:  cometAddr,
			Price:            lpPrice,
			PriceDecimals:    cometPriceDecimals,
			PriceTimestamp:   now,
		},
	}, nil
}
