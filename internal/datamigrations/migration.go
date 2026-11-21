// Package datamigrations rewrites history rows that were ingested before a release
// changed what the indexer writes for them: a new column, or a corrected value.
//
// A migration is a named Go type in All. The live ingester records each one in the
// data_migrations table the first time it starts with a binary that knows it, with the
// boundary ledger: the first ledger that binary writes. Rows from the boundary on are
// already correct, so a migration only covers the ledgers below it. An operator then runs
// `data-migrate run <name>`, which walks those ledgers (see Runner) while ingestion and
// the API keep running.
//
// Each ledger must be rewritable on its own, from that ledger's data alone. State that
// depends on earlier ledgers (running totals such as protocol balances) is rebuilt by
// `protocol-migrate current-state --rebuild` instead.
package datamigrations

import (
	"context"

	"github.com/alitto/pond/v2"
	"github.com/jackc/pgx/v5"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/wallet-backend/internal/data"
)

// Deps is what a migration may use to read a ledger and write its rows.
type Deps struct {
	Models            *data.Models
	NetworkPassphrase string
	// Pool runs the per-ledger CPU work (transaction hashing).
	Pool pond.Pool
}

// Migration rewrites the history rows of one ledger at a time.
type Migration interface {
	// Name identifies the migration in data_migrations and on the command line. It never
	// changes once released; a later fix to the same data is a new migration.
	Name() string
	// Tables lists the hypertables Apply writes to. Their compressed chunks are
	// recompressed once the walk has passed them.
	Tables() []string
	// Extract returns the rows to write for one ledger. It does not touch the database.
	Extract(ctx context.Context, deps Deps, ledger xdr.LedgerCloseMeta) ([]any, error)
	// Apply writes the rows Extract returned for a run of consecutive ledgers, inside
	// dbTx. It must be idempotent: a ledger is applied again after an interrupted run.
	Apply(ctx context.Context, deps Deps, dbTx pgx.Tx, rows []any) error
}

// All is every data migration this binary knows.
var All = []Migration{
	transactionsMemo{},
}

// Find returns the migration with the given name.
func Find(name string) (Migration, bool) {
	for _, m := range All {
		if m.Name() == name {
			return m, true
		}
	}
	return nil, false
}
