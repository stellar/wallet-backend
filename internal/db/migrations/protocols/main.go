package protocols

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	"sort"

	"github.com/stellar/go-stellar-sdk/support/log"
)

//go:embed *.sql
var protocolFS embed.FS

// Run executes all embedded SQL files in alphabetical order against the database.
// Protocol SQL files are idempotent (INSERT ... ON CONFLICT DO NOTHING), so no
// migration tracking table is needed.
func Run(ctx context.Context, sqlDB *sql.DB) (int, error) {
	entries, err := fs.ReadDir(protocolFS, ".")
	if err != nil {
		return 0, fmt.Errorf("reading embedded protocol SQL files: %w", err)
	}

	// Sort entries alphabetically to ensure deterministic execution order
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})

	executed := 0
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		content, readErr := fs.ReadFile(protocolFS, entry.Name())
		if readErr != nil {
			return executed, fmt.Errorf("reading protocol SQL file %s: %w", entry.Name(), readErr)
		}

		if _, execErr := sqlDB.ExecContext(ctx, string(content)); execErr != nil {
			return executed, fmt.Errorf("executing protocol SQL file %s: %w", entry.Name(), execErr)
		}

		log.Ctx(ctx).Infof("Executed protocol migration: %s", entry.Name())
		executed++
	}

	return executed, nil
}
