-- +migrate Up

-- Table: state_changes (TimescaleDB hypertable with columnstore)
-- Note: FK to transactions removed (hypertable FKs not supported)
CREATE TABLE state_changes (
    to_id BIGINT NOT NULL,
    operation_id BIGINT NOT NULL,
    state_change_id BIGINT NOT NULL,
    state_change_category TEXT NOT NULL CHECK (
        state_change_category IN (
            'BALANCE', 'ACCOUNT', 'SIGNER', 'SIGNATURE_THRESHOLD',
            'DATA_ENTRY', 'HOME_DOMAIN', 'ALLOWANCE', 'FLAGS', 'TRUSTLINE',
            'BALANCE_AUTHORIZATION'
        )
    ),
    state_change_reason TEXT NOT NULL CHECK (
        state_change_reason IN (
            'CREATE', 'MERGE', 'DEBIT', 'CREDIT', 'MINT', 'BURN',
            'ADD', 'REMOVE', 'UPDATE', 'SET', 'CLEAR'
        )
    ),
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ledger_number INTEGER NOT NULL,
    account_id BYTEA NOT NULL,
    token_id BYTEA,
    amount TEXT,
    signer_account_id BYTEA,
    spender_account_id BYTEA,
    creator_account_id BYTEA,
    destination_account_id BYTEA,
    liquidity_pool_id TEXT,
    signer_weight_old SMALLINT,
    signer_weight_new SMALLINT,
    threshold TEXT,
    threshold_old SMALLINT,
    threshold_new SMALLINT,
    trustline_limit_old TEXT,
    trustline_limit_new TEXT,
    flags SMALLINT,
    data_entry_name TEXT,
    key_value JSONB,
    to_muxed_id TEXT,
    ledger_created_at TIMESTAMPTZ NOT NULL,
    -- The root GraphQL connection sorts by (ledger_created_at DESC, to_id DESC, operation_id
    -- DESC, state_change_id DESC). This PK's column order matches that sort key directly, so
    -- a top-N first page is served by scanning the PK itself -- forward or backward, no
    -- heapsort -- which also makes it this table's replacement for the default single-column
    -- index TimescaleDB would otherwise auto-create on the partition column.
    -- Operation-scoped lookups ride this PK too: TOID encoding makes a state change's parent
    -- transaction to_id derivable from its operation_id (to_id = operation_id & ~x'FFF'), so
    -- StateChangeModel.BatchGetByOperationID and BatchGetByOperationIDs bind
    -- (ledger_created_at, to_id, operation_id) as a three-column prefix.
    PRIMARY KEY (ledger_created_at, to_id, operation_id, state_change_id)
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'ledger_created_at',
    tsdb.chunk_interval = '1 day',
    tsdb.orderby = 'ledger_created_at DESC, to_id DESC, operation_id DESC, state_change_id DESC',
    tsdb.segmentby = 'account_id',
    tsdb.sparse_index = 'bloom(state_change_category),bloom(state_change_reason)'
);

SELECT enable_chunk_skipping('state_changes', 'to_id');
SELECT enable_chunk_skipping('state_changes', 'operation_id');

-- TimescaleDB's default single-column index on the partition column; the reordered PK above
-- is now a superset of it (ledger_created_at is its leading column), so it's redundant.
DROP INDEX IF EXISTS state_changes_ledger_created_at_idx;

-- Serves StateChangeModel.BatchGetByAccountAddress and BatchGetAccountStateChangesByToIDs: the
-- trailing columns repeat the PK's sort key, so one account's page is a prefix scan with no
-- heapsort. The category- and reason-filtered variants of those queries share it: those two
-- columns are left out because an account's rows are few enough to scan and filter on the
-- active chunk, and compressed chunks prune on the bloom sparse indexes declared above.
CREATE INDEX idx_state_changes_account_id ON state_changes(account_id, ledger_created_at DESC, to_id DESC, operation_id DESC, state_change_id DESC);

-- +migrate Down

-- Tables
DROP TABLE IF EXISTS state_changes CASCADE;
