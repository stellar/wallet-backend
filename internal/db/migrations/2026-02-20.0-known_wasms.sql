-- +migrate Up
CREATE TABLE known_wasms (
    wasm_hash TEXT PRIMARY KEY,
    protocol_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- +migrate Down
DROP TABLE IF EXISTS known_wasms;
