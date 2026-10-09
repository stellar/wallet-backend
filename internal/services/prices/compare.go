package prices

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/metrics"
)

const (
	defaultCompareTopN      = 200
	defaultCompareSpacing   = time.Second
	defaultCompareRetention = 30 * 24 * time.Hour
)

// ComparisonStore persists comparison samples.
type ComparisonStore interface {
	Insert(ctx context.Context, samples []data.PriceComparison) error
	DeleteOlderThan(ctx context.Context, cutoff time.Time) error
}

// ExternalPriceSource returns an external USD price for an external asset id, or nil when it has
// none.
type ExternalPriceSource interface {
	AssetPriceUSD(ctx context.Context, id string) (*float64, error)
}

// ComparisonConfig wires a ComparisonSampler. Zero TopN, Spacing and Retention take defaults.
type ComparisonConfig struct {
	DB     *pgxpool.Pool
	Store  ComparisonStore
	Client ExternalPriceSource
	XLMSAC string
	// ClassicAssets maps a classic asset's SAC address to the CODE-ISSUER id the external source
	// understands. nil means every non-native token is sent as its contract address.
	ClassicAssets func(ctx context.Context) (map[string]string, error)
	Interval      time.Duration
	TopN          int
	Spacing       time.Duration
	Retention     time.Duration
	// Metrics is optional.
	Metrics *metrics.PricesMetrics
}

// ComparisonSampler periodically records our prices next to an external source's for the most
// traded tokens, so the two can be compared offline.
type ComparisonSampler struct {
	cfg ComparisonConfig
}

// NewComparisonSampler applies defaults to the config and returns the sampler.
func NewComparisonSampler(cfg ComparisonConfig) *ComparisonSampler {
	if cfg.TopN <= 0 {
		cfg.TopN = defaultCompareTopN
	}
	if cfg.Spacing <= 0 {
		cfg.Spacing = defaultCompareSpacing
	}
	if cfg.Retention <= 0 {
		cfg.Retention = defaultCompareRetention
	}
	return &ComparisonSampler{cfg: cfg}
}

// Run samples immediately, then again one interval after each pass ends, until ctx is done.
func (s *ComparisonSampler) Run(ctx context.Context) {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		s.pass(ctx)
		timer.Reset(s.cfg.Interval)
	}
}

func (s *ComparisonSampler) pass(ctx context.Context) {
	now := time.Now()
	snap, err := LoadSnapshot(ctx, s.cfg.DB, now)
	if err != nil {
		if ctx.Err() == nil {
			log.Ctx(ctx).Errorf("price comparison: loading snapshot: %v", err)
		}
		return
	}
	if err := s.samplePass(ctx, snap, now); err != nil && ctx.Err() == nil {
		log.Ctx(ctx).Errorf("price comparison: %v", err)
	}
}

// samplePass compares the top tokens of snap against the external source, stores the samples
// and trims those past retention. It returns early with the context error when ctx is done.
func (s *ComparisonSampler) samplePass(ctx context.Context, snap *Snapshot, now time.Time) error {
	tokens := s.selectTokens(snap)
	classic := map[string]string{}
	if s.cfg.ClassicAssets != nil {
		var err error
		if classic, err = s.cfg.ClassicAssets(ctx); err != nil {
			return fmt.Errorf("loading classic asset ids: %w", err)
		}
	}
	samples := make([]data.PriceComparison, 0, len(tokens))
	var missing, failed int
	observe := func(result string) {
		if s.cfg.Metrics != nil {
			s.cfg.Metrics.ComparisonSamplesTotal.WithLabelValues(result).Inc()
		}
	}
	for _, tp := range tokens {
		select {
		case <-ctx.Done():
			return fmt.Errorf("sampling interrupted: %w", ctx.Err())
		case <-time.After(s.cfg.Spacing):
		}
		theirs, err := s.cfg.Client.AssetPriceUSD(ctx, s.externalID(tp.Token, classic))
		if err != nil {
			if ctx.Err() != nil {
				return fmt.Errorf("sampling interrupted: %w", ctx.Err())
			}
			failed++
			log.Ctx(ctx).Warnf("price comparison: external price for %s: %v", tp.Token, err)
			theirs = nil
			observe("external_error")
		} else if theirs == nil {
			missing++
			observe("external_missing")
		} else {
			observe("both")
		}
		ours := tp.PriceUSD
		samples = append(samples, data.PriceComparison{SampledAt: now, Token: tp.Token, Ours: &ours, Theirs: theirs})
	}
	if err := s.cfg.Store.Insert(ctx, samples); err != nil {
		return fmt.Errorf("storing samples: %w", err)
	}
	if err := s.cfg.Store.DeleteOlderThan(ctx, now.Add(-s.cfg.Retention)); err != nil {
		return fmt.Errorf("trimming samples: %w", err)
	}
	log.Ctx(ctx).Infof("price comparison pass: sampled=%d missing=%d errors=%d", len(samples), missing, failed)
	return nil
}

// externalID maps a token to the id the external source understands: XLM for the native SAC,
// CODE-ISSUER for a classic asset, and the contract address for anything else.
func (s *ComparisonSampler) externalID(token string, classic map[string]string) string {
	if token == s.cfg.XLMSAC {
		return StellarExpertNativeID
	}
	if id, ok := classic[token]; ok {
		return id
	}
	return token
}

// selectTokens returns up to TopN tokens ordered by 24-hour volume, descending. The native
// token is kept whenever the snapshot prices it.
func (s *ComparisonSampler) selectTokens(snap *Snapshot) []TokenPrice {
	all := make([]TokenPrice, 0, len(snap.Prices))
	for _, tp := range snap.Prices {
		all = append(all, tp)
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].Volume24hUSD != all[j].Volume24hUSD {
			return all[i].Volume24hUSD > all[j].Volume24hUSD
		}
		return all[i].Token < all[j].Token
	})
	if len(all) <= s.cfg.TopN {
		return all
	}
	top := all[:s.cfg.TopN]
	for _, tp := range top {
		if tp.Token == s.cfg.XLMSAC {
			return top
		}
	}
	xlm, ok := snap.Prices[s.cfg.XLMSAC]
	if !ok {
		return top
	}
	top[len(top)-1] = xlm
	return top
}
