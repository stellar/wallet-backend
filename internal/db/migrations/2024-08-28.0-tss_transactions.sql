-- +migrate Up

CREATE TABLE tss_transactions (
    transaction_hash TEXT PRIMARY KEY,
    transaction_xdr TEXT,
    webhook_url TEXT,
    current_status TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    claimed_until TIMESTAMPTZ
);

CREATE TABLE tss_transaction_submission_tries (
    try_transaction_hash TEXT PRIMARY_KEY,
    original_transaction_hash TEXT,
    try_transaction_xdr TEXT,
    status INTEGER,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_tx_current_status ON tss_transactions(current_status);
CREATE INDEX idx_claimed_until ON tss_transactions(claimed_until);

CREATE INDEX idx_original_transaction_hash ON tss_transaction_submission_tries(original_transaction_hash);
CREATE INDEX idx_last_updated ON tss_transaction_submission_tries(updated_at);

-- +migrate Down

DROP INDEX IF EXISTS idx_tx_current_status;
DROP INDEX IF EXISTS idx_claimed_until;
DROP TABLE tss_transactions;

DROP INDEX IF EXISTS idx_original_transaction_hash;
DROP INDEX IF EXISTS idx_last_updated;
DROP TABLE tss_transaction_submission_tries;
