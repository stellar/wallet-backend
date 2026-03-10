-- +migrate Up
CREATE TABLE protocol_contracts (
    contract_id BYTEA PRIMARY KEY,
    wasm_hash BYTEA NOT NULL REFERENCES protocol_wasms(wasm_hash),
    name TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_protocol_contracts_wasm_hash ON protocol_contracts(wasm_hash);

-- +migrate Down
DROP TABLE IF EXISTS protocol_contracts;
