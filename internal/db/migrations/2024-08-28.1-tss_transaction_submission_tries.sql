-- +migrate Up

CREATE TABLE tss_transaction_submission_tries (
    original_transaction_xdr VARCHAR(400),
    try_transaction_xdr VARCHAR(400),
    incoming_status VARCHAR(50),
    outgoing_status VARCHAR(50),
    last_updated TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_original_transaction_xdr ON tss_transaction_submission_tries(original_transaction_xdr);
CREATE INDEX idx_try_transaction_xdr ON tss_transaction_submission_tries(try_transaction_xdr);
CREATE INDEX idx_last_updated ON tss_transaction_submission_tries(last_updated);

-- +migrate Down

DROP INDEX IF EXISTS idx_original_transaction_xdr;
DROP INDEX IF EXISTS idx_try_transaction_xdr;
DROP INDEX IF EXISTS idx_last_updated;
DROP TABLE tss_transaction_submission_tries;
