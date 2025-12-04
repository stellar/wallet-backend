-- +migrate Up

-- =============================================================================
-- STEP 1: Drop foreign key constraints that reference the tables we're converting
-- TimescaleDB hypertables have limitations with foreign keys pointing TO them
--
-- Note: The transactions_accounts and operations_accounts tables no longer have
-- foreign keys to the accounts table - those were removed in the base schema
-- to allow recording all transaction participants.
-- =============================================================================

-- Drop FK from transactions_accounts to transactions
ALTER TABLE transactions_accounts DROP CONSTRAINT IF EXISTS transactions_accounts_tx_hash_fkey;

-- Drop FK from operations to transactions
ALTER TABLE operations DROP CONSTRAINT IF EXISTS operations_tx_hash_fkey;

-- Drop FK from operations_accounts to operations
ALTER TABLE operations_accounts DROP CONSTRAINT IF EXISTS operations_accounts_operation_id_fkey;

-- Drop FK from state_changes to transactions
ALTER TABLE state_changes DROP CONSTRAINT IF EXISTS state_changes_tx_hash_fkey;

-- Drop FK from state_changes to accounts
ALTER TABLE state_changes DROP CONSTRAINT IF EXISTS state_changes_account_id_fkey;

-- =============================================================================
-- STEP 2: Modify primary keys to include time dimension for hypertables
-- TimescaleDB requires the time dimension to be part of the primary key
-- =============================================================================

-- Modify transactions primary key to be composite (hash, ledger_created_at)
ALTER TABLE transactions DROP CONSTRAINT transactions_pkey;
ALTER TABLE transactions ADD PRIMARY KEY (hash, ledger_created_at);

-- Modify operations primary key to be composite (id, ledger_created_at)
ALTER TABLE operations DROP CONSTRAINT operations_pkey;
ALTER TABLE operations ADD PRIMARY KEY (id, ledger_created_at);

-- Modify state_changes primary key to include ledger_created_at
ALTER TABLE state_changes DROP CONSTRAINT state_changes_pkey;
ALTER TABLE state_changes ADD PRIMARY KEY (to_id, state_change_order, ledger_created_at);

-- =============================================================================
-- STEP 3: Convert tables to hypertables
-- Using 1 month chunk interval as specified
-- =============================================================================

-- Convert transactions to hypertable
SELECT create_hypertable('transactions', 'ledger_created_at',
    chunk_time_interval => INTERVAL '1 month',
    migrate_data => true
);

-- Convert operations to hypertable
SELECT create_hypertable('operations', 'ledger_created_at',
    chunk_time_interval => INTERVAL '1 month',
    migrate_data => true
);

-- Convert state_changes to hypertable
SELECT create_hypertable('state_changes', 'ledger_created_at',
    chunk_time_interval => INTERVAL '1 month',
    migrate_data => true
);

-- =============================================================================
-- STEP 4: Enable compression on hypertables
-- Compress chunks older than 7 days to save storage
-- =============================================================================

-- Enable compression on transactions
ALTER TABLE transactions SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'ledger_number',
    timescaledb.compress_orderby = 'ledger_created_at DESC, hash'
);

-- Enable compression on operations
ALTER TABLE operations SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'ledger_number',
    timescaledb.compress_orderby = 'ledger_created_at DESC, id'
);

-- Enable compression on state_changes
-- Segment by account_id for better query performance on account history
ALTER TABLE state_changes SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'account_id',
    timescaledb.compress_orderby = 'ledger_created_at DESC, to_id, state_change_order'
);

-- =============================================================================
-- STEP 5: Add compression policies
-- Automatically compress chunks older than 7 days
-- =============================================================================

SELECT add_compression_policy('transactions', INTERVAL '7 days');
SELECT add_compression_policy('operations', INTERVAL '7 days');
SELECT add_compression_policy('state_changes', INTERVAL '7 days');

-- =============================================================================
-- STEP 6: Recreate indexes optimized for time-series queries
-- TimescaleDB automatically creates indexes on the time dimension
-- =============================================================================

-- Add additional useful indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_transactions_ledger_number ON transactions(ledger_number, ledger_created_at DESC);
CREATE INDEX IF NOT EXISTS idx_operations_ledger_number ON operations(ledger_number, ledger_created_at DESC);
CREATE INDEX IF NOT EXISTS idx_state_changes_ledger_number ON state_changes(ledger_number, ledger_created_at DESC);

-- Composite index for state_changes account queries
CREATE INDEX IF NOT EXISTS idx_state_changes_account_time ON state_changes(account_id, ledger_created_at DESC);

-- +migrate Down

-- =============================================================================
-- REVERSE: Remove compression policies
-- =============================================================================
SELECT remove_compression_policy('transactions', if_exists => true);
SELECT remove_compression_policy('operations', if_exists => true);
SELECT remove_compression_policy('state_changes', if_exists => true);

-- =============================================================================
-- REVERSE: Decompress all chunks before converting back to regular tables
-- =============================================================================
SELECT decompress_chunk(c, true)
FROM show_chunks('transactions') c;

SELECT decompress_chunk(c, true)
FROM show_chunks('operations') c;

SELECT decompress_chunk(c, true)
FROM show_chunks('state_changes') c;

-- =============================================================================
-- REVERSE: Drop indexes
-- =============================================================================
DROP INDEX IF EXISTS idx_transactions_ledger_number;
DROP INDEX IF EXISTS idx_operations_ledger_number;
DROP INDEX IF EXISTS idx_state_changes_ledger_number;
DROP INDEX IF EXISTS idx_state_changes_account_time;

-- =============================================================================
-- REVERSE: Convert hypertables back to regular tables
-- Note: This requires creating new tables, copying data, and dropping old ones
-- =============================================================================

-- Transactions table
CREATE TABLE transactions_new (
    hash TEXT NOT NULL,
    to_id BIGINT NOT NULL,
    envelope_xdr TEXT,
    result_xdr TEXT,
    meta_xdr TEXT,
    ledger_number INTEGER NOT NULL,
    ledger_created_at TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (hash)
);

INSERT INTO transactions_new SELECT hash, to_id, envelope_xdr, result_xdr, meta_xdr, ledger_number, ledger_created_at, ingested_at FROM transactions;
DROP TABLE transactions CASCADE;
ALTER TABLE transactions_new RENAME TO transactions;
CREATE INDEX idx_transactions_ledger_created_at ON transactions(ledger_created_at);

-- Operations table
CREATE TABLE operations_new (
    id BIGINT NOT NULL,
    tx_hash TEXT NOT NULL,
    operation_type TEXT NOT NULL,
    operation_xdr TEXT,
    ledger_number INTEGER NOT NULL,
    ledger_created_at TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id)
);

INSERT INTO operations_new SELECT id, tx_hash, operation_type, operation_xdr, ledger_number, ledger_created_at, ingested_at FROM operations;
DROP TABLE operations CASCADE;
ALTER TABLE operations_new RENAME TO operations;
CREATE INDEX idx_operations_tx_hash ON operations(tx_hash);
CREATE INDEX idx_operations_operation_type ON operations(operation_type);
CREATE INDEX idx_operations_ledger_created_at ON operations(ledger_created_at);

-- State changes table
CREATE TABLE state_changes_new (
    to_id BIGINT NOT NULL,
    state_change_order BIGINT NOT NULL,
    state_change_category TEXT NOT NULL,
    state_change_reason TEXT,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ledger_created_at TIMESTAMPTZ NOT NULL,
    ledger_number INTEGER NOT NULL,
    account_id TEXT NOT NULL,
    operation_id BIGINT NOT NULL,
    tx_hash TEXT NOT NULL,
    token_id TEXT,
    amount TEXT,
    flags JSONB,
    key_value JSONB,
    offer_id TEXT,
    signer_account_id TEXT,
    signer_weights JSONB,
    spender_account_id TEXT,
    sponsored_account_id TEXT,
    sponsor_account_id TEXT,
    deployer_account_id TEXT,
    funder_account_id TEXT,
    thresholds JSONB,
    trustline_limit JSONB,
    PRIMARY KEY (to_id, state_change_order)
);

INSERT INTO state_changes_new SELECT * FROM state_changes;
DROP TABLE state_changes CASCADE;
ALTER TABLE state_changes_new RENAME TO state_changes;
CREATE INDEX idx_state_changes_account_id ON state_changes(account_id);
CREATE INDEX idx_state_changes_tx_hash ON state_changes(tx_hash);
CREATE INDEX idx_state_changes_operation_id ON state_changes(operation_id);
CREATE INDEX idx_state_changes_ledger_created_at ON state_changes(ledger_created_at);

-- =============================================================================
-- REVERSE: Restore foreign key constraints
-- =============================================================================
ALTER TABLE transactions_accounts ADD CONSTRAINT transactions_accounts_tx_hash_fkey
    FOREIGN KEY (tx_hash) REFERENCES transactions(hash) ON DELETE CASCADE;

ALTER TABLE operations ADD CONSTRAINT operations_tx_hash_fkey
    FOREIGN KEY (tx_hash) REFERENCES transactions(hash);

ALTER TABLE operations_accounts ADD CONSTRAINT operations_accounts_operation_id_fkey
    FOREIGN KEY (operation_id) REFERENCES operations(id) ON DELETE CASCADE;

ALTER TABLE state_changes ADD CONSTRAINT state_changes_tx_hash_fkey
    FOREIGN KEY (tx_hash) REFERENCES transactions(hash);

ALTER TABLE state_changes ADD CONSTRAINT state_changes_account_id_fkey
    FOREIGN KEY (account_id) REFERENCES accounts(stellar_address);
