-- +migrate Up
ALTER TABLE tss_transaction_submission_tries
    RENAME COLUMN status TO code;

ALTER TABLE tss_transaction_submission_tries
    ADD column status TEXT NOT NULL,
    ADD COLUMN result_xdr TEXT NOT NULL;

-- +migrate Down
ALTER TABLE tss_transaction_submission_tries
    DROP COLUMN status,
    DROP COLUMN result_xdr;

ALTER TABLE tss_transaction_submission_tries
    RENAME COLUMN code TO status;
