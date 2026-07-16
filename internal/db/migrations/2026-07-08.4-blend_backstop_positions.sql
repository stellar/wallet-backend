-- +migrate Up

-- user backstop positions (UserBalance(pool,user))
CREATE TABLE blend_backstop_positions (
    pool_contract_id     BYTEA NOT NULL,
    user_account_id      BYTEA NOT NULL,
    shares               TEXT NOT NULL DEFAULT '0',
    q4w                  JSONB,               -- [{amount, expiration}] queued withdrawals
    last_modified_ledger INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (pool_contract_id, user_account_id)
) WITH (
    fillfactor = 80,
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_vacuum_threshold = 50,
    autovacuum_analyze_scale_factor = 0.01,
    autovacuum_analyze_threshold = 50,
    autovacuum_vacuum_cost_delay = 0,
    autovacuum_vacuum_cost_limit = 1000
);
-- BackstopPositionModel.GetByAccount filters on user_account_id alone (the 2nd PK
-- column), so the PK cannot serve it — index the per-account read path.
CREATE INDEX idx_blend_backstop_positions_user ON blend_backstop_positions (user_account_id);

-- +migrate Down

DROP TABLE IF EXISTS blend_backstop_positions;
