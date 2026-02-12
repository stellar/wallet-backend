// Package ingest - configureHypertableSettings applies TimescaleDB chunk interval
// and retention policy settings to hypertables at startup.
package ingest

import (
	"context"
	"fmt"

	"github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/wallet-backend/internal/db"
)

// hypertables lists all TimescaleDB hypertables managed by the ingestion system.
var hypertables = []string{
	"transactions",
	"transactions_accounts",
	"operations",
	"operations_accounts",
	"state_changes",
}

// configureHypertableSettings applies chunk interval and retention policy settings
// to all hypertables. Chunk interval only affects future chunks. Retention policy
// is idempotent: any existing policy is removed before re-adding.
func configureHypertableSettings(ctx context.Context, pool db.ConnectionPool, chunkInterval, retentionPeriod string) error {
	for _, table := range hypertables {
		if _, err := pool.ExecContext(ctx,
			"SELECT set_chunk_time_interval($1::regclass, $2::interval)",
			table, chunkInterval,
		); err != nil {
			return fmt.Errorf("setting chunk interval on %s: %w", table, err)
		}
		log.Ctx(ctx).Infof("Set chunk interval %q on %s", chunkInterval, table)
	}

	if retentionPeriod != "" {
		for _, table := range hypertables {
			if _, err := pool.ExecContext(ctx,
				"SELECT remove_retention_policy($1::regclass, if_exists => true)",
				table,
			); err != nil {
				return fmt.Errorf("removing retention policy on %s: %w", table, err)
			}

			if _, err := pool.ExecContext(ctx,
				"SELECT add_retention_policy($1::regclass, drop_after => $2::interval)",
				table, retentionPeriod,
			); err != nil {
				return fmt.Errorf("adding retention policy on %s: %w", table, err)
			}
			log.Ctx(ctx).Infof("Set retention policy %q on %s", retentionPeriod, table)
		}
	}

	return nil
}
