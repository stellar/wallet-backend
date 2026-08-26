-- +migrate Up

-- Converges environments that applied 2025-06-10.4 while it still carried
-- idx_state_changes_operation_id and idx_state_changes_account_category:
-- sql-migrate keys applied migrations by filename, so the in-place edit of
-- that file never re-runs on a database already past it. Fresh databases
-- build the final shape from 2025-06-10.4 itself, making every statement
-- here a no-op for them.
--
-- idx_state_changes_account_id serves StateChangeModel.BatchGetByAccountAddress
-- and BatchGetAccountStateChangesByToIDs as a heapsort-free prefix scan (see
-- 2025-06-10.4 for the full rationale). The dropped indexes are superseded:
-- operation-scoped lookups derive their PK prefix from the TOID encoding
-- (to_id = operation_id & ~x'FFF'), and the category/reason-filtered account
-- queries scan-and-filter on the active chunk with the bloom sparse indexes
-- pruning compressed chunks.
CREATE INDEX IF NOT EXISTS idx_state_changes_account_id ON state_changes(account_id, ledger_created_at DESC, to_id DESC, operation_id DESC, state_change_id DESC);
DROP INDEX IF EXISTS idx_state_changes_operation_id;
DROP INDEX IF EXISTS idx_state_changes_account_category;

-- +migrate Down

CREATE INDEX IF NOT EXISTS idx_state_changes_operation_id ON state_changes(operation_id);
CREATE INDEX IF NOT EXISTS idx_state_changes_account_category ON state_changes(account_id, state_change_category, state_change_reason, ledger_created_at DESC, to_id DESC, operation_id DESC, state_change_id DESC);
DROP INDEX IF EXISTS idx_state_changes_account_id;
