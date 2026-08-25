package blend

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// wipeStatement clears every Blend current-state table. Classification tables
// (protocol_wasms, protocol_contracts) are deliberately absent: nothing
// rebuilds them.
//
// TRUNCATE, not DELETE: the caller holds the protocol's ingest_store cursor row
// for this statement's duration, and live ingestion's per-ledger CAS needs that
// same row — so every ledger of every protocol waits on this. TRUNCATE's cost
// is independent of row count, which bounds that wait; a full-table DELETE's is
// not. One statement takes all the table locks at once rather than escalating
// through them.
const wipeStatement = `TRUNCATE
	blend_pools,
	blend_positions,
	blend_reserves,
	blend_backstop_positions,
	blend_backstop_pools,
	blend_reserve_emissions,
	blend_emissions,
	blend_pool_claimed,
	blend_backstop_claimed,
	blend_oracle_prices,
	blend_auctions`

// WipeCurrentState deletes every Blend current-state row, in the same
// transaction as the caller's cursor reset.
func WipeCurrentState(ctx context.Context, dbTx pgx.Tx) error {
	if _, err := dbTx.Exec(ctx, wipeStatement); err != nil {
		return fmt.Errorf("wiping Blend current state: %w", err)
	}
	return nil
}
