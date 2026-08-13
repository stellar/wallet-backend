-- +migrate Up

-- A pool-share balance is read alongside its pool's reserves, and ingestion
-- writes both tables in one transaction with the pool row first, so a balance
-- row whose pool is missing can only be produced by an ingestion bug. The
-- constraint turns that bug into a loud failure at the write instead of a
-- silently broken join at read time, matching the trustline_balances →
-- trustline_assets constraint. DEFERRABLE so the commit-time check covers
-- same-transaction pool deletions: a pool is deleted only once every share
-- balance referencing it is deleted, and both happen in the same persist
-- transaction.
ALTER TABLE liquidity_pool_balances
    ADD CONSTRAINT liquidity_pool_balances_pool_id_fkey
    FOREIGN KEY (pool_id) REFERENCES liquidity_pools (pool_id)
    DEFERRABLE INITIALLY DEFERRED;

-- +migrate Down

ALTER TABLE liquidity_pool_balances
    DROP CONSTRAINT IF EXISTS liquidity_pool_balances_pool_id_fkey;
