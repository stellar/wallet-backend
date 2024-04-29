-- +migrate Up

CREATE TABLE accounts (
  stellar_address text NOT NULL,
  created_at timestamp with time zone NOT NULL DEFAULT NOW(),
  PRIMARY KEY (stellar_address)
);

-- +migrate Down

DROP TABLE accounts;
