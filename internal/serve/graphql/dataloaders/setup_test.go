package dataloaders

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	godbtest "github.com/stellar/go-stellar-sdk/support/db/dbtest"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
)

var (
	testCtx              context.Context
	testDBConnectionPool *pgxpool.Pool
	testDBT              *godbtest.DB
)

// TestMain opens a single migrated TimescaleDB instance for the package. Loader tests
// insert their own isolated high-to_id fixtures and clean them up with t.Cleanup, so no
// shared seed is needed.
func TestMain(m *testing.M) {
	testCtx = context.Background()

	testDBT = dbtest.Open(&testing.T{})
	var err error
	testDBConnectionPool, err = db.OpenDBConnectionPool(testCtx, testDBT.DSN)
	if err != nil {
		panic(err)
	}

	code := m.Run()

	testDBConnectionPool.Close()
	testDBT.Close()

	os.Exit(code)
}
