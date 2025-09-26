-- +migrate Up

CREATE TABLE ingest_store (
  key varchar(255) NOT NULL,
  value varchar(255) NOT NULL,
  PRIMARY KEY (key)
);

-- +migrate Down

DROP TABLE ingest_store;
