-- +migrate Up

-- Table: operations
CREATE TABLE operations (
    id BIGINT PRIMARY KEY,
    operation_type TEXT NOT NULL CHECK (
        operation_type IN (
            'CREATE_ACCOUNT', 'PAYMENT', 'PATH_PAYMENT_STRICT_RECEIVE',
            'MANAGE_SELL_OFFER', 'CREATE_PASSIVE_SELL_OFFER', 'SET_OPTIONS',
            'CHANGE_TRUST', 'ALLOW_TRUST', 'ACCOUNT_MERGE', 'INFLATION',
            'MANAGE_DATA', 'BUMP_SEQUENCE', 'MANAGE_BUY_OFFER',
            'PATH_PAYMENT_STRICT_SEND', 'CREATE_CLAIMABLE_BALANCE',
            'CLAIM_CLAIMABLE_BALANCE', 'BEGIN_SPONSORING_FUTURE_RESERVES',
            'END_SPONSORING_FUTURE_RESERVES', 'REVOKE_SPONSORSHIP',
            'CLAWBACK', 'CLAWBACK_CLAIMABLE_BALANCE', 'SET_TRUST_LINE_FLAGS',
            'LIQUIDITY_POOL_DEPOSIT', 'LIQUIDITY_POOL_WITHDRAW',
            'INVOKE_HOST_FUNCTION', 'EXTEND_FOOTPRINT_TTL', 'RESTORE_FOOTPRINT'
        )
    ),
    operation_xdr TEXT,
    result_code TEXT NOT NULL,
    successful BOOLEAN NOT NULL,
    ledger_number INTEGER NOT NULL,
    ledger_created_at TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_operations_ledger_created_at ON operations(ledger_created_at);

-- Table: operations_accounts
CREATE TABLE operations_accounts (
    operation_id BIGINT NOT NULL REFERENCES operations(id) ON DELETE CASCADE,
    account_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (account_id, operation_id)
);

CREATE INDEX idx_operations_accounts_operation_id ON operations_accounts(operation_id);

-- +migrate Down

-- Tables
DROP TABLE IF EXISTS operations_accounts CASCADE;
DROP TABLE IF EXISTS operations CASCADE;
