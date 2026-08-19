package sep41

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// WipeCurrentState deletes every SEP-41 current-state row. Callers run it in
// the same transaction that resets the protocol's migration cursor, so live
// ingestion — serialized on the cursor row lock — can never observe or fold
// onto a half-wiped table. contract_tokens is deliberately untouched: it is
// classification-owned and nothing would rebuild it.
func WipeCurrentState(ctx context.Context, dbTx pgx.Tx) error {
	for _, table := range []string{"sep41_balances", "sep41_allowances"} {
		if _, err := dbTx.Exec(ctx, "DELETE FROM "+table); err != nil {
			return fmt.Errorf("wiping %s: %w", table, err)
		}
	}
	return nil
}
