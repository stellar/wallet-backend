-- +migrate Up

ALTER TABLE tss_transactions
ALTER COLUMN webhook_url DROP NOT NULL;


-- +migrate Down

ALTER TABLE tss_transactions
ALTER COLUMN webhook_url SET NOT NULL;
