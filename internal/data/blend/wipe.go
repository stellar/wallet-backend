package blend

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// WipeCurrentState deletes every Blend current-state row, in the same
// transaction as the caller's cursor reset. Classification tables
// (protocol_wasms, protocol_contracts) are deliberately untouched: nothing
// rebuilds them.
func WipeCurrentState(ctx context.Context, dbTx pgx.Tx) error {
	for _, table := range []string{
		"blend_pools",
		"blend_positions",
		"blend_reserves",
		"blend_backstop_positions",
		"blend_backstop_pools",
		"blend_reserve_emissions",
		"blend_emissions",
		"blend_pool_claimed",
		"blend_backstop_claimed",
		"blend_oracle_prices",
		"blend_auctions",
	} {
		if _, err := dbTx.Exec(ctx, "DELETE FROM "+table); err != nil {
			return fmt.Errorf("wiping %s: %w", table, err)
		}
	}
	return nil
}
