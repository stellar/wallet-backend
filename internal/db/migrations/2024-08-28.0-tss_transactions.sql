-- +migrate Up

CREATE TABLE tss_transactions (
    transaction_hash TEXT PRIMARY KEY,
    transaction_xdr TEXT NOT NULL,
    webhook_url TEXT  NOT NULL,
    current_status TEXT  NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    claimed_until TIMESTAMPTZ
);

CREATE TABLE tss_transaction_submission_tries (
    try_transaction_hash TEXT PRIMARY KEY,
    original_transaction_hash TEXT  NOT NULL,
    try_transaction_xdr TEXT  NOT NULL,
    status INTEGER  NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT NOW() NOT NULL
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
