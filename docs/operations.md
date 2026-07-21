# Operations

**Hypertable index usage:** `pg_stat_user_indexes` on the `public` schema always reports `idx_scan = 0` for hypertable indexes — scans accrue on the per-chunk indexes in `_timescaledb_internal`, and the parent's index entry is only a template. To measure real usage (e.g. for an unused-index audit), aggregate chunk-index stats:

```sql
SELECT ht.table_name AS hypertable, regexp_replace(sui.indexrelname, '^_hyper_\d+_\d+_chunk_', '') AS index_name,
       sum(sui.idx_scan) AS scans
FROM pg_stat_user_indexes sui
JOIN _timescaledb_catalog.chunk c ON c.schema_name = sui.schemaname AND c.table_name = sui.relname
JOIN _timescaledb_catalog.hypertable ht ON ht.id = c.hypertable_id
GROUP BY 1, 2 ORDER BY 1, 3 DESC;
```

Every index in the schema must have a nameable consumer query; secondary indexes duplicating a primary key's column set are not kept (the PK's column order is chosen to serve its consumers directly — B-tree scans run forward or backward, so ASC/DESC variants of the same key are one index, not two).
