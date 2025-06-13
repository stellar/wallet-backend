-- Create triggers for the existing updated_at columns.


-- +migrate Up

-- TRIGGER: keypairs.updated_at
CREATE TRIGGER keypairs_set_updated_at BEFORE UPDATE ON keypairs FOR EACH ROW EXECUTE PROCEDURE refresh_updated_at_column();
-- TRIGGER: channel_accounts.updated_at
CREATE TRIGGER channel_accounts_set_updated_at BEFORE UPDATE ON channel_accounts FOR EACH ROW EXECUTE PROCEDURE refresh_updated_at_column();


-- +migrate Down

-- TRIGGER: keypairs.updated_at
DROP TRIGGER keypairs_set_updated_at ON keypairs;
-- TRIGGER: channel_accounts.updated_at
DROP TRIGGER channel_accounts_set_updated_at ON channel_accounts;
