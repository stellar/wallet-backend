-- +migrate Up
CREATE TABLE protocol_wasms (
    wasm_hash BYTEA PRIMARY KEY,
    protocol_id TEXT REFERENCES protocols(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE protocol_contracts (
    contract_id BYTEA PRIMARY KEY,
    wasm_hash BYTEA NOT NULL REFERENCES protocol_wasms(wasm_hash),
    name TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_protocol_wasms_protocol_id ON protocol_wasms (protocol_id);
CREATE INDEX idx_protocol_contracts_wasm_hash ON protocol_contracts (wasm_hash);


-- +migrate Down
DROP TABLE IF EXISTS protocol_contracts;
DROP TABLE IF EXISTS protocol_wasms;
