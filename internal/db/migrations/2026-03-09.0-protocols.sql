-- +migrate Up
CREATE TABLE protocols (
    id TEXT PRIMARY KEY,
    classification_status TEXT NOT NULL DEFAULT 'not_started'
        CHECK (classification_status IN ('not_started', 'in_progress', 'success', 'failed')),
    history_migration_status TEXT NOT NULL DEFAULT 'not_started'
        CHECK (history_migration_status IN ('not_started', 'in_progress', 'success', 'failed')),
    current_state_migration_status TEXT NOT NULL DEFAULT 'not_started'
        CHECK (current_state_migration_status IN ('not_started', 'in_progress', 'success', 'failed')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- +migrate Down
DROP TABLE IF EXISTS protocols;
