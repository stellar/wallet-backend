-- +migrate Up

CREATE TABLE channel_accounts (
    public_key VARCHAR(64) PRIMARY KEY NOT NULL,
    encrypted_private_key VARCHAR(256) NOT NULL,
    created_at timestamp with time zone NOT NULL DEFAULT NOW(),
    updated_at timestamp with time zone NOT NULL DEFAULT NOW(),
    locked_at timestamp NULL,
    locked_until timestamp NULL
);

-- +migrate Down

DROP TABLE channel_accounts;
