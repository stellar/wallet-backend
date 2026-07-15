-- +migrate Up

-- user backstop positions (UserBalance(pool,user))
CREATE TABLE blend_backstop_positions (
    pool_contract_id     BYTEA NOT NULL,
    user_account_id      BYTEA NOT NULL,
    shares               NUMERIC NOT NULL DEFAULT 0,
    q4w                  JSONB,               -- [{amount, expiration}] queued withdrawals
    last_modified_ledger INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (pool_contract_id, user_account_id)
) WITH (
    fillfactor = 90,
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_vacuum_threshold = 50,
    autovacuum_analyze_scale_factor = 0.01,
    autovacuum_analyze_threshold = 50,
    autovacuum_vacuum_cost_delay = 0
);

-- +migrate Down

DROP TABLE IF EXISTS blend_backstop_positions;
