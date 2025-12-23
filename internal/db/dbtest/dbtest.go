package dbtest

import (
	"net/http"
	"testing"

	migrate "github.com/rubenv/sql-migrate"
	"github.com/stellar/go/support/db/dbtest"
	"github.com/stellar/go/support/db/schema"

	"github.com/stellar/wallet-backend/internal/db/migrations"
)

func Open(t *testing.T) *dbtest.DB {
	db := dbtest.Postgres(t)
	conn := db.Open()
	defer conn.Close()

	migrateDirection := schema.MigrateUp
	m := migrate.HttpFileSystemMigrationSource{FileSystem: http.FS(migrations.FS)}
	_, err := schema.Migrate(conn.DB, m, migrateDirection, 0)
	if err != nil {
		t.Fatal(err)
	}

	return db
}

func OpenWithoutMigrations(t *testing.T) *dbtest.DB {
	db := dbtest.Postgres(t)
	return db
}

// OpenB opens a test database for benchmarks with migrations applied.
func OpenB(b *testing.B) *dbtest.DB {
	db := dbtest.Postgres(b)
	conn := db.Open()
	defer conn.Close()

	migrateDirection := schema.MigrateUp
	m := migrate.HttpFileSystemMigrationSource{FileSystem: http.FS(migrations.FS)}
	_, err := schema.Migrate(conn.DB, m, migrateDirection, 0)
	if err != nil {
		b.Fatal(err)
	}

	return db
}
