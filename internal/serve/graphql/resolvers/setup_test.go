package resolvers

import (
	"context"
	"os"
	"testing"

	godbtest "github.com/stellar/go-stellar-sdk/support/db/dbtest"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
)

var (
	testCtx              context.Context
	testDBConnectionPool db.ConnectionPool
	testDBT              *godbtest.DB
)

func TestMain(m *testing.M) {
	testCtx = context.Background()

	testDBT = dbtest.Open(&testing.T{})
	var err error
	testDBConnectionPool, err = db.OpenDBConnectionPool(testDBT.DSN)
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
