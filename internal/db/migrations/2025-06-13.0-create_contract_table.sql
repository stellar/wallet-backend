-- +migrate Up

-- Table: token_contracts
CREATE TABLE contract_tokens (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    symbol TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TRIGGER contract_tokens_set_updated_at BEFORE UPDATE ON contract_tokens FOR EACH ROW EXECUTE PROCEDURE
  refresh_updated_at_column();

-- +migrate Down

DROP TABLE contract_tokens CASCADE;
