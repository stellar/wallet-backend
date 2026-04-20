-- Create triggers for the existing updated_at columns.


-- +migrate Up

-- TRIGGER: keypairs.updated_at
CREATE TRIGGER keypairs_set_updated_at BEFORE UPDATE ON keypairs FOR EACH ROW EXECUTE PROCEDURE refresh_updated_at_column();

-- +migrate Down

-- TRIGGER: keypairs.updated_at
DROP TRIGGER keypairs_set_updated_at ON keypairs;
