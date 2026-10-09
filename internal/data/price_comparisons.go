package data

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/utils"
)

// PriceComparison pairs our price with the external source's for one token at one instant.
// Either side is nil when that source had no price.
type PriceComparison struct {
	SampledAt time.Time
	Token     string
	Ours      *float64
	Theirs    *float64
}

// PriceComparisonsModel stores side-by-side samples for validation.
type PriceComparisonsModel struct {
	DB      *pgxpool.Pool
	Metrics *metrics.DBMetrics
}

// Insert writes one sampling pass.
func (m *PriceComparisonsModel) Insert(ctx context.Context, samples []PriceComparison) error {
	if len(samples) == 0 {
		return nil
	}
	at := make([]time.Time, len(samples))
	tokens := make([][]byte, len(samples))
	ours := make([]*float64, len(samples))
	theirs := make([]*float64, len(samples))
	for i, s := range samples {
		b, err := addressBytea(s.Token)
		if err != nil {
			return err
		}
		at[i], tokens[i], ours[i], theirs[i] = s.SampledAt, b, s.Ours, s.Theirs
	}
	const q = `
		INSERT INTO price_comparisons (sampled_at, token, ours, theirs)
		SELECT * FROM UNNEST($1::timestamptz[], $2::bytea[], $3::float8[], $4::float8[])
		ON CONFLICT DO NOTHING`
	start := time.Now()
	_, err := m.DB.Exec(ctx, q, at, tokens, ours, theirs)
	m.Metrics.QueryDuration.WithLabelValues("Insert", "price_comparisons").Observe(time.Since(start).Seconds())
	m.Metrics.QueriesTotal.WithLabelValues("Insert", "price_comparisons").Inc()
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("Insert", "price_comparisons", utils.GetDBErrorType(err)).Inc()
		return fmt.Errorf("inserting price_comparisons: %w", err)
	}
	return nil
}

// DeleteOlderThan trims samples past the retention cutoff.
func (m *PriceComparisonsModel) DeleteOlderThan(ctx context.Context, cutoff time.Time) error {
	start := time.Now()
	_, err := m.DB.Exec(ctx, `DELETE FROM price_comparisons WHERE sampled_at < $1`, cutoff)
	m.Metrics.QueryDuration.WithLabelValues("DeleteOlderThan", "price_comparisons").Observe(time.Since(start).Seconds())
	m.Metrics.QueriesTotal.WithLabelValues("DeleteOlderThan", "price_comparisons").Inc()
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("DeleteOlderThan", "price_comparisons", utils.GetDBErrorType(err)).Inc()
		return fmt.Errorf("deleting old price_comparisons: %w", err)
	}
	return nil
}
