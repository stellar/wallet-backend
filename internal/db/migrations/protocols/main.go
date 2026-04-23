package protocols

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"sort"

	"github.com/stellar/go-stellar-sdk/support/log"
)

//go:embed *.sql
var protocolFS embed.FS

// Run executes all embedded SQL files in alphabetical order against the database
// inside a single transaction. Protocol SQL files are idempotent
// (INSERT ... ON CONFLICT DO NOTHING), so no migration tracking table is needed.
// Wrapping the batch in a transaction ensures partial failures never leave the
// registration state half-applied — either every file commits together or none do.
func Run(ctx context.Context, sqlDB *sql.DB) (int, error) {
	entries, err := fs.ReadDir(protocolFS, ".")
	if err != nil {
		return 0, fmt.Errorf("reading embedded protocol SQL files: %w", err)
	}

	// Sort entries alphabetically to ensure deterministic execution order
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})

	tx, err := sqlDB.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("beginning protocol migration transaction: %w", err)
	}
	defer func() {
		if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
			log.Ctx(ctx).Warnf("rolling back protocol migration transaction: %v", rbErr)
		}
	}()

	executed := 0
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		content, readErr := fs.ReadFile(protocolFS, entry.Name())
		if readErr != nil {
			return executed, fmt.Errorf("reading protocol SQL file %s: %w", entry.Name(), readErr)
		}

		if _, execErr := tx.ExecContext(ctx, string(content)); execErr != nil {
			return executed, fmt.Errorf("executing protocol SQL file %s: %w", entry.Name(), execErr)
		}

		log.Ctx(ctx).Infof("Executed protocol migration: %s", entry.Name())
		executed++
	}

	if err := tx.Commit(); err != nil {
		return executed, fmt.Errorf("committing protocol migration transaction: %w", err)
	}

	return executed, nil
}
