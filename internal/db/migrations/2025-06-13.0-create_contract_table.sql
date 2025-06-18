-- +migrate Up

-- Table: contracts
CREATE TABLE contracts (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    symbol TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TRIGGER contracts_set_updated_at BEFORE UPDATE ON contracts FOR EACH ROW EXECUTE PROCEDURE
  refresh_updated_at_column();

-- +migrate Down

DROP TABLE contracts CASCADE;
