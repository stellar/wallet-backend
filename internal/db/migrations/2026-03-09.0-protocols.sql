-- +migrate Up
CREATE TABLE protocols (
    id TEXT PRIMARY KEY,
    classification_status TEXT NOT NULL DEFAULT 'not_started',
    history_migration_status TEXT NOT NULL DEFAULT 'not_started',
    current_state_migration_status TEXT NOT NULL DEFAULT 'not_started',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- +migrate Down
DROP TABLE IF EXISTS protocols;
