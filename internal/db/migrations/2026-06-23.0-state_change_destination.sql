-- +migrate Up

ALTER TABLE state_changes ADD COLUMN destination_account_id BYTEA;

-- +migrate Down

ALTER TABLE state_changes DROP COLUMN IF EXISTS destination_account_id;
