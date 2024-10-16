-- +migrate Up

ALTER TABLE tss_transaction_submission_tries
    ALTER COLUMN status TYPE TEXT,
    ALTER COLUMN status SET NOT NULL,
    ADD COLUMN code INTEGER NOT NULL,
    ADD COLUMN result_xdr TEXT NOT NULL;

-- +migrate Down

ALTER TABLE tss_transaction_submission_tries
    ALTER COLUMN status TYPE INTEGER,
    ALTER COLUMN status SET NOT NULL,
    DROP COLUMN code,
    DROP COLUMN result_xdr;
