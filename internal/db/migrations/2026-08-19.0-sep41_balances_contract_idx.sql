-- +migrate Up

-- Contract-first repair scans: ListPairs pages (account_id, contract_id) filtered by
-- contract_id (`protocol-repair current-state --contract`). The PK leads with
-- account_id, so without this a contract-scoped run walks the whole table.
CREATE INDEX idx_sep41_balances_contract_account ON sep41_balances (contract_id, account_id);

-- +migrate Down

DROP INDEX IF EXISTS idx_sep41_balances_contract_account;
