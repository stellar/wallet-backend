-- +migrate Up

ALTER TABLE ingest_payments
  ADD COLUMN src_asset_type text NOT NULL,
  ADD COLUMN dest_asset_type text NOT NULL,
  ADD COLUMN memo_type text NULL;

-- +migrate Down

ALTER TABLE ingest_payments
  DROP COLUMN src_asset_type,
  DROP COLUMN dest_asset_type,
  DROP COLUMN memo_type;
