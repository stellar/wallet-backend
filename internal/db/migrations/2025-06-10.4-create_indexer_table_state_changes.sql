-- +migrate Up

-- Table: state_changes
-- Stores blockchain state changes with optimized data types for storage efficiency.
CREATE TABLE state_changes (
    to_id BIGINT NOT NULL,
    state_change_order BIGINT NOT NULL,
    state_change_category VARCHAR(50) NOT NULL,     -- Bounded category (BALANCE, ACCOUNT, etc.)
    state_change_reason VARCHAR(100),               -- Bounded reason (CREATE, DEBIT, CREDIT, etc.)
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ledger_created_at TIMESTAMPTZ NOT NULL,
    ledger_number INTEGER NOT NULL,
    account_id CHAR(56) NOT NULL REFERENCES accounts(stellar_address),  -- Stellar address (56 chars)
    operation_id BIGINT NOT NULL,
    tx_hash CHAR(64) NOT NULL REFERENCES transactions(hash),            -- SHA-256 hex (64 chars)
    token_id VARCHAR(100),                          -- Contract/asset IDs (~56-69 chars)
    amount VARCHAR(50),                             -- Numeric amount string
    flags JSONB,
    key_value JSONB,
    offer_id VARCHAR(50),                           -- Offer IDs are numeric
    signer_account_id CHAR(56),                     -- Stellar address (56 chars)
    signer_weights JSONB,
    spender_account_id CHAR(56),                    -- Stellar address (56 chars)
    sponsored_account_id CHAR(56),                  -- Stellar address (56 chars)
    sponsor_account_id CHAR(56),                    -- Stellar address (56 chars)
    deployer_account_id CHAR(56),                   -- Stellar address (56 chars)
    funder_account_id CHAR(56),                     -- Stellar address (56 chars)
    thresholds JSONB,
    trustline_limit JSONB,
    PRIMARY KEY (to_id, state_change_order)
);


-- +migrate Down

-- Tables
DROP TABLE IF EXISTS state_changes CASCADE;
