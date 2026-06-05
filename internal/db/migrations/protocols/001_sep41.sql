-- Idempotent SEP-41 protocol registration. Executed by internal/db/migrations/protocols/main.go,
-- which runs the file verbatim on every protocol-setup invocation.
INSERT INTO protocols (id) VALUES ('SEP41') ON CONFLICT (id) DO NOTHING;
