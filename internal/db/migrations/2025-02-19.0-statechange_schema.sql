-- +migrate Up


CREATE TYPE statechangetype_enum AS ENUM (
    'DEBIT',
    'CREDIT',
    'HOMEDOMAIN',
    'FLAGS',
    'SIGNERS',
    'DATAENTRY',
    'ALLOWANCE',
    'BALANCEAUTHORIZATION',
    'THRESHOLDS',
    'AUTHORIZATION'
);

CREATE TYPE statechangereason_enum AS ENUM (
    'CREATEACCOUNT',
    'MERGEACCOUNT',
    'PAYMENT',
    'CREATECLAIMABLEBALANCE',
    'CLAIMCLAIMABLEBALANCE',
    'CLAWBACK',
    'LIQUIDITYPOOLDEPOSIT',
    'LIQUIDITYPOOLWITHDRAW',
    'OFFERS',
    'PATHPAYMENT',
    'FEE'
    'TRANSFER',
    'CONTRACT_TRANSFER',
    'CONTRACT_MINT',
    'CONTRACT_BURN',
    'CONTRACT_CLAWBACK'
    'FLAGS_CLEARED',
    'FLAGS_SET',
    'SIGNERS_ADDED',
    'SIGNERS_UPDATED',
    'SINGERS_REMOVED',
    'ALLOWANCE_SET',
    'ALLOWANCE_CONSUMED'
);

-- Transactions Table
CREATE TABLE transactions (
    txhash VARCHAR(64) PRIMARY KEY,
    transactionxdr TEXT NOT NULL,
    txmeta TEXT,
    ts TIMESTAMP NOT NULL
);

-- Operations Table
CREATE TABLE operations (
    toid BIGINT PRIMARY KEY,
    txhash VARCHAR(64) NOT NULL REFERENCES transactions(txhash) ON DELETE CASCADE,
    operationindex INTEGER NOT NULL,
    operationtype INTEGER NOT NULL,
    ts TIMESTAMP NOT NULL
);

-- State Changes Table
CREATE TABLE statechanges (
    id SERIAL PRIMARY KEY,
    acctid TEXT NOT NULL,
    operationtoid BIGINT REFERENCES operations(toid) ON DELETE CASCADE,
    txhash VARCHAR(64) NOT NULL REFERENCES transactions(txhash) ON DELETE CASCADE,
    statechangetype statechangetype_enum NOT NULL,
    statechangereason statechangereason_enum NOT NULL,
    changemetadata JSONB,
    asset TEXT,
    amount FLOAT,
    lowthreshold INTEGER,
    mediumthreshold INTEGER,
    highthreshold INTEGER,
    flags_cleared TEXT[],
    flags_set TEXT[],
    homedomain TEXT,
    signer_added JSONB,
    signer_removed JSONB,
    signer_updated JSONB,
    data_entry JSONB,
    balance_authorization BOOLEAN,
    contract_address TEXT,
    function_name TEXT,
    ts TIMESTAMP NOT NULL
);

-- Indexes for optimized queries
CREATE INDEX idx_transactions_ts ON transactions (ts);

CREATE INDEX idx_operations_txhash ON operations (txhash);
CREATE INDEX idx_operations_operationtype ON operations (operationtype);
CREATE INDEX idx_operations_ts ON operations (ts);

CREATE INDEX idx_statechanges_acctid ON statechanges (acctid);
CREATE INDEX idx_statechanges_operationtoid ON statechanges (operationtoid);
CREATE INDEX idx_statechanges_txhash ON statechanges (txhash);
CREATE INDEX idx_statechanges_statechangetype ON statechanges (statechangetype);
CREATE INDEX idx_statechanges_statechangereason ON statechanges (statechangereason);
CREATE INDEX idx_statechanges_ts ON statechanges (ts);

-- +migrate Down

DROP INDEX IF EXISTS idx_transactions_ts;
DROP INDEX IF EXISTS idx_operations_txhash;
DROP INDEX IF EXISTS idx_operations_operationtype;
DROP INDEX IF EXISTS idx_operations_ts;
DROP INDEX IF EXISTS idx_statechanges_acctid;
DROP INDEX IF EXISTS idx_statechanges_operationtoid;
DROP INDEX IF EXISTS idx_statechanges_txhash;
DROP INDEX IF EXISTS idx_statechanges_statechangetype;
DROP INDEX IF EXISTS idx_statechanges_statechangereason;
DROP INDEX IF EXISTS idx_statechanges_ts;

DROP TABLE IF EXISTS statechanges;
DROP TABLE IF EXISTS operations;
DROP TABLE IF EXISTS transactions;

DROP TYPE IF EXISTS operationtype_enum;
DROP TYPE IF EXISTS statechangetype_enum;
DROP TYPE IF EXISTS statechangereason_enum;
