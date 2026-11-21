-- +migrate Up

-- One row per data migration (see internal/datamigrations). The live ingester inserts the
-- row the first time it starts with a binary that knows the migration; the data-migrate
-- command walks cursor_ledger from boundary_ledger down to the oldest ingested ledger.
CREATE TABLE data_migrations (
    name            TEXT PRIMARY KEY,
    status          TEXT NOT NULL CHECK (status IN ('pending', 'in_progress', 'done')),
    -- First ledger written by a binary that knows this migration; rows from it on are
    -- already correct.
    boundary_ledger INTEGER NOT NULL,
    -- Lowest ledger rewritten so far; starts at boundary_ledger.
    cursor_ledger   INTEGER NOT NULL,
    started_at      TIMESTAMPTZ,
    finished_at     TIMESTAMPTZ,
    last_error      TEXT
);

-- +migrate Down

DROP TABLE IF EXISTS data_migrations;
