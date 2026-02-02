// Package dbtest provides test database utilities with TimescaleDB support.
package dbtest

import (
	"net/http"
	"testing"

	migrate "github.com/rubenv/sql-migrate"
	"github.com/stellar/go-stellar-sdk/support/db/dbtest"
	"github.com/stellar/go-stellar-sdk/support/db/schema"

	"github.com/stellar/wallet-backend/internal/db/migrations"
)

// Open opens a test database with migrations applied.
// Requires TimescaleDB extension to be available on the PostgreSQL server.
func Open(t *testing.T) *dbtest.DB {
	db := dbtest.Postgres(t)
	conn := db.Open()
	defer conn.Close()

	// Enable TimescaleDB extension before running migrations
	_, err := conn.Exec("CREATE EXTENSION IF NOT EXISTS timescaledb")
	if err != nil {
		t.Fatal(err)
	}

	migrateDirection := schema.MigrateUp
	m := migrate.HttpFileSystemMigrationSource{FileSystem: http.FS(migrations.FS)}
	_, err = schema.Migrate(conn.DB, m, migrateDirection, 0)
	if err != nil {
		t.Fatal(err)
	}

	return db
}

// OpenWithoutMigrations opens a test database without running migrations
// but still enables TimescaleDB extension for manual migration testing.
func OpenWithoutMigrations(t *testing.T) *dbtest.DB {
	db := dbtest.Postgres(t)
	conn := db.Open()
	defer conn.Close()

	// Enable TimescaleDB extension
	_, err := conn.Exec("CREATE EXTENSION IF NOT EXISTS timescaledb")
	if err != nil {
		t.Fatal(err)
	}

	return db
}

// OpenB opens a test database for benchmarks with migrations applied.
// Requires TimescaleDB extension to be available on the PostgreSQL server.
func OpenB(b *testing.B) *dbtest.DB {
	db := dbtest.Postgres(b)
	conn := db.Open()
	defer conn.Close()

	// Enable TimescaleDB extension before running migrations
	_, err := conn.Exec("CREATE EXTENSION IF NOT EXISTS timescaledb")
	if err != nil {
		b.Fatal(err)
	}

	migrateDirection := schema.MigrateUp
	m := migrate.HttpFileSystemMigrationSource{FileSystem: http.FS(migrations.FS)}
	_, err = schema.Migrate(conn.DB, m, migrateDirection, 0)
	if err != nil {
		b.Fatal(err)
	}

	return db
}
