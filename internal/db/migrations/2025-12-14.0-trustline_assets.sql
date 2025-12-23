-- +migrate Up

-- Table: trustline_assets
-- Stores classic Stellar trustline assets with auto-incrementing IDs for memory-efficient storage.
CREATE TABLE trustline_assets (
    id BIGSERIAL PRIMARY KEY,
    code TEXT NOT NULL,
    issuer TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(code, issuer)
);

CREATE INDEX idx_trustline_assets_code_issuer ON trustline_assets(code, issuer);

-- +migrate Down

DROP TABLE trustline_assets CASCADE;
