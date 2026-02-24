---
context: Several indexer types (ContractID, AssetCode) are stored as BYTEA; custom Scanner/Valuer methods let sqlx handle conversion without manual hex encoding at call sites
type: pattern
status: active
subsystem: ingestion
areas: [ingestion, indexer]
created: 2026-02-24
---

# BYTEA custom types implement sql Scanner and driver Valuer for transparent binary database round-tripping

Several types in the indexer package that map to PostgreSQL `BYTEA` columns implement both `database/sql.Scanner` (for reading) and `database/sql/driver.Valuer` (for writing), enabling transparent round-trip serialization without manual encoding/decoding at call sites.

Types using this pattern include `ContractID` (32-byte Soroban contract identifier) and binary-encoded asset codes. The implementation:

```go
// Writing to DB: return raw []byte
func (c ContractID) Value() (driver.Value, error) {
    return c[:], nil
}

// Reading from DB: accept []byte and copy
func (c *ContractID) Scan(value interface{}) error {
    b, ok := value.([]byte)
    if !ok { return errors.New("expected []byte") }
    copy(c[:], b)
    return nil
}
```

With these implemented, `sqlx` calls `Value()` automatically during `db.Exec()` and `Scan()` during `db.Get()` / `db.Select()` — no hex encoding, `encode/hex`, or manual `[]byte` casts at call sites. The DB schema uses `BYTEA` for these columns and PostgreSQL handles the binary storage transparently.

---

Relevant Notes:
- [[the unified StateChange struct uses nullable fields and a category discriminator to store all state change types in a single database table]] — StateChange contains ContractID fields using this pattern

Areas:
- [[entries/ingestion]]
