-- Add function used to refresh the updated_at column automatically.


-- +migrate Up

-- +migrate StatementBegin
CREATE OR REPLACE FUNCTION refresh_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';
-- +migrate StatementEnd


-- +migrate Down

DROP FUNCTION refresh_updated_at_column;
