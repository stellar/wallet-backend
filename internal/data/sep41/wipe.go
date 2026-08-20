package sep41

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// WipeCurrentState deletes every SEP-41 current-state row, in the same
// transaction as the caller's cursor reset. contract_tokens is deliberately
// untouched: classification owns it and nothing rebuilds it.
func WipeCurrentState(ctx context.Context, dbTx pgx.Tx) error {
	for _, table := range []string{"sep41_balances", "sep41_allowances"} {
		if _, err := dbTx.Exec(ctx, "DELETE FROM "+table); err != nil {
			return fmt.Errorf("wiping %s: %w", table, err)
		}
	}
	return nil
}
