-- +migrate Up

ALTER TABLE tss_transaction_submission_tries
    DROP column status,
    ADD column status TEXT NOT NULL,
    ADD COLUMN code INTEGER NOT NULL,
    ADD COLUMN result_xdr TEXT NOT NULL;

-- +migrate Down

ALTER TABLE tss_transaction_submission_tries
    DROP column status,
    DROP COLUMN code,
    DROP COLUMN result_xdr,
    ADD column status INTEGER NOT NULL;
