package sep41

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// wipeStatement clears every SEP-41 current-state table. contract_tokens is
// deliberately absent: classification owns it and nothing rebuilds it.
//
// TRUNCATE, not DELETE: the caller holds the protocol's ingest_store cursor row
// for this statement's duration, and live ingestion's per-ledger CAS needs that
// same row — so every ledger of every protocol waits on this. TRUNCATE's cost
// is independent of row count, which bounds that wait; a full-table DELETE's is
// not. Truncating these tables does not touch contract_tokens: their foreign
// key points at it, not the other way around.
const wipeStatement = `TRUNCATE sep41_balances, sep41_allowances`

// WipeCurrentState deletes every SEP-41 current-state row, in the same
// transaction as the caller's cursor reset.
func WipeCurrentState(ctx context.Context, dbTx pgx.Tx) error {
	if _, err := dbTx.Exec(ctx, wipeStatement); err != nil {
		return fmt.Errorf("wiping SEP-41 current state: %w", err)
	}
	return nil
}
