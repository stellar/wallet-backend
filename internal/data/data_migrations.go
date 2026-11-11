package data

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/stellar/wallet-backend/internal/db"
)

const (
	DataMigrationPending    = "pending"
	DataMigrationInProgress = "in_progress"
	DataMigrationDone       = "done"
)

// DataMigration is one row of data_migrations: the progress of a named data migration
// (see internal/datamigrations).
type DataMigration struct {
	Name   string `db:"name"`
	Status string `db:"status"`
	// BoundaryLedger is the first ledger written by a binary that knows the migration.
	BoundaryLedger uint32 `db:"boundary_ledger"`
	// CursorLedger is the lowest ledger rewritten so far.
	CursorLedger uint32     `db:"cursor_ledger"`
	StartedAt    *time.Time `db:"started_at"`
	FinishedAt   *time.Time `db:"finished_at"`
	LastError    *string    `db:"last_error"`
}

type DataMigrationModel struct {
	DB *pgxpool.Pool
}

// InsertIfMissing records a migration with its boundary ledger. An existing row is left
// alone, so the boundary is fixed by the first caller. A row inserted as done has nothing
// to rewrite (a database with no history older than the boundary).
func (m *DataMigrationModel) InsertIfMissing(ctx context.Context, q db.Querier, name, status string, boundary uint32) error {
	const query = `
		INSERT INTO data_migrations (name, status, boundary_ledger, cursor_ledger, finished_at)
		VALUES ($1, $2, $3, $3, CASE WHEN $2 = 'done' THEN NOW() END)
		ON CONFLICT (name) DO NOTHING
	`
	if _, err := q.Exec(ctx, query, name, status, int64(boundary)); err != nil {
		return fmt.Errorf("inserting data migration %s: %w", name, err)
	}
	return nil
}

// Get returns the migration's row, or nil when it has not been recorded yet.
func (m *DataMigrationModel) Get(ctx context.Context, name string) (*DataMigration, error) {
	row, err := db.QueryOne[DataMigration](ctx, m.DB, `SELECT * FROM data_migrations WHERE name = $1`, name)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("getting data migration %s: %w", name, err)
	}
	return &row, nil
}

func (m *DataMigrationModel) List(ctx context.Context) ([]DataMigration, error) {
	rows, err := db.QueryMany[DataMigration](ctx, m.DB, `SELECT * FROM data_migrations ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("listing data migrations: %w", err)
	}
	return rows, nil
}

// Start marks a run as in progress and clears the error of an earlier run.
func (m *DataMigrationModel) Start(ctx context.Context, name string) error {
	const query = `
		UPDATE data_migrations
		SET status = 'in_progress', started_at = COALESCE(started_at, NOW()), last_error = NULL
		WHERE name = $1
	`
	if _, err := m.DB.Exec(ctx, query, name); err != nil {
		return fmt.Errorf("starting data migration %s: %w", name, err)
	}
	return nil
}

// Advance lowers the cursor inside the transaction that rewrote the rows above it. The
// cursor never moves up, so a stale writer cannot undo recorded progress.
func (m *DataMigrationModel) Advance(ctx context.Context, dbTx pgx.Tx, name string, cursor uint32) error {
	const query = `UPDATE data_migrations SET cursor_ledger = LEAST(cursor_ledger, $2) WHERE name = $1`
	if _, err := dbTx.Exec(ctx, query, name, int64(cursor)); err != nil {
		return fmt.Errorf("advancing data migration %s to ledger %d: %w", name, cursor, err)
	}
	return nil
}

// SetError records why a run stopped. The status stays in_progress: the next run resumes.
func (m *DataMigrationModel) SetError(ctx context.Context, name, message string) error {
	if _, err := m.DB.Exec(ctx, `UPDATE data_migrations SET last_error = $2 WHERE name = $1`, name, message); err != nil {
		return fmt.Errorf("recording error of data migration %s: %w", name, err)
	}
	return nil
}

func (m *DataMigrationModel) MarkDone(ctx context.Context, name string) error {
	const query = `UPDATE data_migrations SET status = 'done', finished_at = NOW(), last_error = NULL WHERE name = $1`
	if _, err := m.DB.Exec(ctx, query, name); err != nil {
		return fmt.Errorf("marking data migration %s done: %w", name, err)
	}
	return nil
}

// Chunk is one hypertable chunk.
type Chunk struct {
	Name       string    `db:"name"`
	RangeStart time.Time `db:"range_start"`
}

// ListCompressedChunks returns the compressed chunks of the given hypertables, newest
// first. A partially compressed chunk (one that received DML) is included.
func ListCompressedChunks(ctx context.Context, q db.Querier, hypertables []string) ([]Chunk, error) {
	const query = `
		SELECT chunk_schema || '.' || chunk_name AS name, range_start
		FROM timescaledb_information.chunks
		WHERE hypertable_name = ANY($1) AND is_compressed
		ORDER BY range_start DESC
	`
	chunks, err := db.QueryMany[Chunk](ctx, q, query, hypertables)
	if err != nil {
		return nil, fmt.Errorf("listing compressed chunks of %v: %w", hypertables, err)
	}
	return chunks, nil
}

// CompressChunk (re)compresses one chunk. On a partially compressed chunk this folds the
// rows DML left in the rowstore back into the columnstore; on a fully compressed chunk it
// is a no-op, as it is for a chunk that no longer exists (dropped by retention).
func CompressChunk(ctx context.Context, q db.Querier, chunk string) error {
	const query = `
		SELECT compress_chunk(c, if_not_compressed => true)
		FROM (SELECT to_regclass($1) AS c) AS s
		WHERE c IS NOT NULL
	`
	if _, err := q.Exec(ctx, query, chunk); err != nil {
		return fmt.Errorf("compressing chunk %s: %w", chunk, err)
	}
	return nil
}
