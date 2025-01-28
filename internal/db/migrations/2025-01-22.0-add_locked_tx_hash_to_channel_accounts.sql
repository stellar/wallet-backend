-- +migrate Up

ALTER TABLE channel_accounts
ADD COLUMN locked_tx_hash VARCHAR(64);
CREATE INDEX idx_locked_tx_hash ON channel_accounts(locked_tx_hash);
-- +migrate Down
DROP INDEX IF EXISTS idx_locked_tx_hash;
ALTER TABLE channel_accounts
DROP COLUMN locked_tx_hash;
