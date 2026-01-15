-- +migrate Up

-- Table: contract_tokens
-- Stores Soroban contract tokens with deterministic UUID IDs.
CREATE TABLE contract_tokens (
    id UUID PRIMARY KEY,
    contract_id TEXT UNIQUE NOT NULL,
    type TEXT NOT NULL,
    code TEXT NULL,
    issuer TEXT NULL,
    name TEXT NULL,
    symbol TEXT NULL,
    decimals SMALLINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_contract_tokens_contract_id ON contract_tokens(contract_id);

CREATE TRIGGER contract_tokens_set_updated_at BEFORE UPDATE ON contract_tokens FOR EACH ROW EXECUTE PROCEDURE
  refresh_updated_at_column();

-- +migrate Down

DROP TABLE contract_tokens CASCADE;
