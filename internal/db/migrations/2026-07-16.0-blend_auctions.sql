-- +migrate Up

-- Active auctions (Auction(user, auction_type) TEMPORARY entries on pools). A row
-- exists while the auction is open; entry deletion (fill/cancel) deletes the row.
-- TTL eviction is not surfaced to the indexer, so a never-filled auction's row can
-- outlive the entry's ~45d TTL — readers should treat a very old start_block as stale.
CREATE TABLE blend_auctions (
    pool_contract_id     BYTEA NOT NULL,
    user_account_id      BYTEA NOT NULL,     -- auction owner: liquidated user; the backstop for bad-debt/interest
    auction_type         INTEGER NOT NULL,   -- 0 user liquidation, 1 bad debt, 2 interest
    bid                  JSONB NOT NULL,     -- {asset C-address: amount string} the filler pays
    lot                  JSONB NOT NULL,     -- {asset C-address: amount string} the filler receives
    start_block          INTEGER NOT NULL,   -- AuctionData.block: start ledger anchoring Dutch-auction scaling
    last_modified_ledger INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (pool_contract_id, user_account_id, auction_type)
) WITH (
    fillfactor = 80,
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_vacuum_threshold = 50,
    autovacuum_analyze_scale_factor = 0.01,
    autovacuum_analyze_threshold = 50,
    autovacuum_vacuum_cost_delay = 0,
    autovacuum_vacuum_cost_limit = 1000
);
-- account-first lookup ("is this account being liquidated anywhere")
CREATE INDEX idx_blend_auctions_user ON blend_auctions (user_account_id);

-- +migrate Down

DROP TABLE IF EXISTS blend_auctions;
