# Data Layer Architecture

## TimescaleDB Setup

<!-- TODO: Read internal/ingest/timescaledb.go (or wherever hypertable creation happens).
     Document:
     - Which tables are hypertables and their partition columns
     - Chunk interval configuration
     - Compression policies (if any)
     - Continuous aggregates (if any)
     - The SQL that creates hypertables (from migrations) -->

## Model Interface Pattern

<!-- TODO: Read internal/data/models.go.
     Show the pattern:
       type FooModelInterface interface {
           Method1(ctx, ...) error
           Method2(ctx, ...) (Result, error)
       }
       type FooModel struct { db *sql.DB }
       // implements FooModelInterface

     List all model interfaces and their key methods.
     Show how they're aggregated in the Models struct. -->

## Migration System

<!-- TODO: Read internal/db/migrations/main.go (or wherever migrations are embedded).
     Document:
     - The //go:embed pattern for SQL migration files
     - How rubenv/sql-migrate is configured
     - Migration naming convention: YYYY-MM-DD.N-description.sql
     - How to create a new migration
     - The migrate up / migrate down commands -->

## Connection Pool Architecture

<!-- TODO: Read internal/db/db.go for the ConnectionPool interface.
     Document:
     - Pool configuration (max open, idle, lifetimes)
     - How the pool is created and injected into services
     - Transaction helpers (if any)
     - How tests get isolated database connections via dbtest.Open(t) -->

## Query Patterns

<!-- TODO: Identify the query builder used (likely Squirrel or raw SQL).
     Document:
     - Common query patterns (pagination, filtering, joins)
     - How parameterized queries are used
     - Any query helper utilities
     - Error wrapping conventions for DB errors -->
