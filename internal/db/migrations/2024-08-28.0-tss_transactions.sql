-- +migrate Up

CREATE TABLE tss_transactions (
    transaction_xdr VARCHAR(400) PRIMARY KEY,
    webhook_url VARCHAR(250),
    current_status VARCHAR(50),
    creation_time TIMESTAMPTZ DEFAULT NOW(),
    last_updated_time TIMESTAMPTZ DEFAULT NOW(),
    claimed_until TIMESTAMPTZ
);

CREATE INDEX idx_tx_current_status ON tss_transactions(current_status);
CREATE INDEX idx_claimed_until ON tss_transactions(claimed_until);

-- +migrate Down

DROP INDEX IF EXISTS idx_tx_current_status;
DROP INDEX IF EXISTS idx_claimed_until;
DROP TABLE tss_transactions
