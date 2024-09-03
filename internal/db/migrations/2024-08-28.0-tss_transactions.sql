-- +migrate Up

CREATE TABLE tss_transactions (
    transaction_hash VARCHAR(70) PRIMARY KEY,
    transaction_xdr VARCHAR(400),
    webhook_url VARCHAR(250),
    current_status VARCHAR(50),
    creation_time TIMESTAMPTZ DEFAULT NOW(),
    last_updated_time TIMESTAMPTZ DEFAULT NOW(),
    claimed_until TIMESTAMPTZ
);

CREATE TABLE tss_transaction_submission_tries (
    original_transaction_hash VARCHAR(70),
    try_transaction_hash VARCHAR(70),
    try_transaction_xdr VARCHAR(400),
    status VARCHAR(50),
    last_updated TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_tx_current_status ON tss_transactions(current_status);
CREATE INDEX idx_claimed_until ON tss_transactions(claimed_until);

CREATE INDEX idx_original_transaction_hash ON tss_transaction_submission_tries(original_transaction_hash);
CREATE INDEX idx_try_transaction_hash ON tss_transaction_submission_tries(try_transaction_hash);
CREATE INDEX idx_last_updated ON tss_transaction_submission_tries(last_updated);

-- +migrate Down

DROP INDEX IF EXISTS idx_tx_current_status;
DROP INDEX IF EXISTS idx_claimed_until;
DROP TABLE tss_transactions;

DROP INDEX IF EXISTS idx_original_transaction_hash;
DROP INDEX IF EXISTS idx_try_transaction_hash;
DROP INDEX IF EXISTS idx_last_updated;
DROP TABLE tss_transaction_submission_tries;
