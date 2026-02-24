---
context: rubenv/sql-migrate splits migration files on semicolons by default; stored procedures and DO blocks containing semicolons must be wrapped in -- StatementBegin / -- StatementEnd comments
type: gotcha
created: 2026-02-24
---

# multi-statement migration DDL requires StatementBegin and StatementEnd delimiters to prevent parser splitting on semicolons

`rubenv/sql-migrate` parses migration files by splitting on semicolons. Each semicolon-terminated statement is sent to PostgreSQL as a separate command. This works correctly for simple DDL like `CREATE TABLE ... ;` or `ALTER TABLE ... ;`.

The problem arises with multi-statement DDL blocks: stored procedures, DO blocks, and functions contain internal semicolons that delimit statements *within* the block — not at the block's boundary. If sql-migrate splits these on every semicolon, each fragment is sent to PostgreSQL separately, producing syntax errors.

The fix is the `-- StatementBegin` / `-- StatementEnd` comment markers. When sql-migrate encounters `-- StatementBegin`, it stops splitting on semicolons and accumulates everything until `-- StatementEnd`, then sends the entire block as a single statement.

Example:
```sql
-- StatementBegin
CREATE OR REPLACE FUNCTION ... RETURNS void AS $$
BEGIN
  INSERT INTO foo VALUES (1);  -- internal semicolon — safe inside markers
  UPDATE bar SET x = 2;        -- another internal semicolon — safe
END;
$$ LANGUAGE plpgsql;
-- StatementEnd
```

Without these markers, the migration would fail silently or with a cryptic PostgreSQL error about incomplete syntax. This is particularly easy to miss because simple migrations work fine, and the gotcha only surfaces when adding more complex DDL.

---

Relevant Notes:
- [[go:embed compiles SQL migrations into the binary at build time making migration files unavailable at runtime]] — the migration system that uses this parser
- [[migrate down requires explicit count to prevent accidental full rollback while migrate up treats zero as apply-all]] — the other migration CLI gotcha

Areas:
- [[entries/data-layer]]
