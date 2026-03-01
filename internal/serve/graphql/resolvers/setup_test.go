package resolvers

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

func TestMain(m *testing.M) {
	testCtx = context.Background()

	testDBT = dbtest.Open(&testing.T{})
	var err error
	testDBConnectionPool, err = db.OpenDBConnectionPool(testCtx, testDBT.DSN)
	if err != nil {
		panic(err)
	}

	setupDB(testCtx, &testing.T{}, testDBConnectionPool)

	code := m.Run()

	cleanUpDB(testCtx, &testing.T{}, testDBConnectionPool)
	testDBConnectionPool.Close()
	testDBT.Close()

	os.Exit(code)
}
