-- +migrate Up

-- Table: trustline_assets
-- Stores classic Stellar trustline assets with deterministic UUID IDs (UUID v5 from CODE:ISSUER).
CREATE TABLE trustline_assets (
    id UUID PRIMARY KEY,
    code TEXT NOT NULL,
    issuer TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(code, issuer)
);

CREATE INDEX idx_trustline_assets_code_issuer ON trustline_assets(code, issuer);

-- +migrate Down

DROP TABLE trustline_assets CASCADE;
