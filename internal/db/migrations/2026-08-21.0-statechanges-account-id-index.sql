-- +migrate Up

-- The single authority for this index change. 2025-06-10.4 creates
-- idx_state_changes_operation_id and idx_state_changes_account_category as it
-- always did -- sql-migrate keys applied migrations by filename, so editing it
-- in place would reshape only fresh databases and leave every existing one
-- behind. Every database therefore reaches the final shape the same way: build
-- the original pair there, converge here.
--
-- idx_state_changes_account_id serves StateChangeModel.BatchGetByAccountAddress
-- and BatchGetAccountStateChangesByToIDs: its trailing columns repeat the PK's
-- sort key, so one account's page is a prefix scan with no heapsort. The
-- category- and reason-filtered variants share it -- those two columns are left
-- out because an account's rows are few enough to scan and filter on the active
-- chunk, and compressed chunks prune on the bloom sparse indexes 2025-06-10.4
-- declares. The dropped indexes are superseded: operation-scoped lookups derive
-- their PK prefix from the TOID encoding (to_id = operation_id & ~x'FFF').
CREATE INDEX IF NOT EXISTS idx_state_changes_account_id ON state_changes(account_id, ledger_created_at DESC, to_id DESC, operation_id DESC, state_change_id DESC);
DROP INDEX IF EXISTS idx_state_changes_operation_id;
DROP INDEX IF EXISTS idx_state_changes_account_category;

-- +migrate Down

CREATE INDEX IF NOT EXISTS idx_state_changes_operation_id ON state_changes(operation_id);
CREATE INDEX IF NOT EXISTS idx_state_changes_account_category ON state_changes(account_id, state_change_category, state_change_reason, ledger_created_at DESC, to_id DESC, operation_id DESC, state_change_id DESC);
DROP INDEX IF EXISTS idx_state_changes_account_id;
