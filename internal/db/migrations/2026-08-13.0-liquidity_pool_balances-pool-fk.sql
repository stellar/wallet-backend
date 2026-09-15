-- +migrate Up notransaction

-- A pool-share balance is read alongside its pool's reserves, and ingestion
-- writes both tables in one transaction with the pool row first, so a balance
-- row whose pool is missing can only be produced by an ingestion bug. The
-- constraint turns that bug into a loud failure at the write instead of a
-- silently broken join at read time, matching the trustline_balances →
-- trustline_assets constraint. DEFERRABLE so the commit-time check covers
-- same-transaction pool deletions: a pool is deleted only once every share
-- balance referencing it is deleted, and both happen in the same persist
-- transaction.
--
-- Applied statement-by-statement (notransaction) so validation runs in its
-- own transaction: ADD CONSTRAINT NOT VALID is a brief metadata-only change,
-- and VALIDATE takes only SHARE UPDATE EXCLUSIVE — ingestion keeps writing
-- through the scan. Orphaned share rows are cleared first: rows whose pool
-- is gone are unreadable anyway (the read path joins liquidity_pools), and a
-- single surviving orphan would abort validation and halt the deploy.
DELETE FROM liquidity_pool_balances lpb
    WHERE NOT EXISTS (SELECT 1 FROM liquidity_pools lp WHERE lp.pool_id = lpb.pool_id);

-- Re-runnable after a partial failure: without a wrapping transaction, a
-- failed later statement leaves earlier ones applied.
ALTER TABLE liquidity_pool_balances
    DROP CONSTRAINT IF EXISTS liquidity_pool_balances_pool_id_fkey;

ALTER TABLE liquidity_pool_balances
    ADD CONSTRAINT liquidity_pool_balances_pool_id_fkey
    FOREIGN KEY (pool_id) REFERENCES liquidity_pools (pool_id)
    DEFERRABLE INITIALLY DEFERRED
    NOT VALID;

ALTER TABLE liquidity_pool_balances
    VALIDATE CONSTRAINT liquidity_pool_balances_pool_id_fkey;

-- +migrate Down

ALTER TABLE liquidity_pool_balances
    DROP CONSTRAINT IF EXISTS liquidity_pool_balances_pool_id_fkey;
