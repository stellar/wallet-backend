package dbtest

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/jmoiron/sqlx"

	migrate "github.com/rubenv/sql-migrate"
	"github.com/stellar/go/support/db/schema"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/stellar/wallet-backend/internal/db/migrations"
)

type DB struct {
	DSN       string
	container testcontainers.Container
	t         testing.TB
}

func (db *DB) Close() {
	if db.container != nil {
		ctx := context.Background()
		if err := db.container.Terminate(ctx); err != nil {
			db.t.Logf("Warning: failed to terminate container: %v", err)
		}
	}
}

func (db *DB) Open() *sqlx.DB {
	conn, err := sqlx.Open("postgres", db.DSN)
	require.NoError(db.t, err)
	return conn
}

// openContainer creates and starts a TimescaleDB container for testing.
func openContainer(tb testing.TB, image string) *DB {
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        image,
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":             "postgres",
			"POSTGRES_PASSWORD":         "postgres",
			"POSTGRES_DB":               "testdb",
			"POSTGRES_HOST_AUTH_METHOD": "trust",
		},
		WaitingFor: wait.ForAll(
			wait.ForListeningPort("5432/tcp"),
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
		),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(tb, err)

	tb.Cleanup(func() {
		if container != nil {
			if termErr := container.Terminate(ctx); termErr != nil {
				tb.Logf("Warning: failed to terminate container in cleanup: %v", termErr)
			}
		}
	})

	host, err := container.Host(ctx)
	require.NoError(tb, err)

	port, err := container.MappedPort(ctx, "5432")
	require.NoError(tb, err)

	dsn := fmt.Sprintf("postgres://postgres:postgres@%s:%s/testdb?sslmode=disable", host, port.Port())

	return &DB{
		DSN:       dsn,
		container: container,
		t:         tb,
	}
}

func Open(t *testing.T) *DB {
	db := openContainer(t, "timescale/timescaledb:latest-pg17")

	conn, err := sqlx.Open("postgres", db.DSN)
	require.NoError(t, err)
	defer conn.Close()

	migrateDirection := schema.MigrateUp
	m := migrate.HttpFileSystemMigrationSource{FileSystem: http.FS(migrations.FS)}
	_, err = schema.Migrate(conn.DB, m, migrateDirection, 0)
	require.NoError(t, err)

	return db
}

func OpenWithoutMigrations(t *testing.T) *DB {
	return openContainer(t, "timescale/timescaledb:latest-pg17")
}

// OpenB opens a test database for benchmarks with migrations applied.
func OpenB(b *testing.B) *DB {
	db := openContainer(b, "timescale/timescaledb:latest-pg17")

	conn, err := sqlx.Open("postgres", db.DSN)
	require.NoError(b, err)
	defer conn.Close()

	migrateDirection := schema.MigrateUp
	m := migrate.HttpFileSystemMigrationSource{FileSystem: http.FS(migrations.FS)}
	_, err = schema.Migrate(conn.DB, m, migrateDirection, 0)
	require.NoError(b, err)

	return db
}
