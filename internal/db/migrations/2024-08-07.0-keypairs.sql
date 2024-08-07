-- +migrate Up
CREATE TABLE keypairs (
    public_key VARCHAR(64) PRIMARY KEY,
    encrypted_private_key bytea NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- +migrate Down
DROP TABLE keypairs;
