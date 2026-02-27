-- +migrate Up
CREATE TABLE protocol_wasms (
    wasm_hash TEXT PRIMARY KEY,
    protocol_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- +migrate Down
DROP TABLE IF EXISTS protocol_wasms;
