-- +migrate Up

CREATE TABLE ingest_store (
  key varchar(255) NOT NULL,
  value varchar(255) NOT NULL,
  PRIMARY KEY (key)
);

CREATE TABLE ingest_payments (
  operation_id bigint NOT NULL,
  operation_type text NOT NULL,
  transaction_id bigint NOT NULL,
  transaction_hash text NOT NULL,
  from_address text NOT NULL,
  to_address text NOT NULL,
  src_asset_code text NOT NULL,
  src_asset_issuer text NOT NULL,
  src_amount bigint NOT NULL,
  dest_asset_code text NOT NULL,
  dest_asset_issuer text NOT NULL,
  dest_amount bigint NOT NULL,
  created_at timestamp with time zone NOT NULL,
  memo text NULL,
  PRIMARY KEY (operation_id)
);

CREATE INDEX from_address_idx ON ingest_payments (from_address);
CREATE INDEX to_address_idx ON ingest_payments (to_address);

-- +migrate Down

DROP TABLE ingest_payments;

DROP TABLE ingest_store;
