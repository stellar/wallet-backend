package resolvers

import (
	"context"
	"crypto/rand"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stellar/go-stellar-sdk/strkey"
	godbtest "github.com/stellar/go-stellar-sdk/support/db/dbtest"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/indexer/types"
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

// execTestDB runs a raw SQL exec against the shared test pool. Used by tests
// that seed fixtures without going through the data-layer interfaces.
func execTestDB(t *testing.T, sql string, args ...any) {
	t.Helper()
	_, err := testDBConnectionPool.Exec(testCtx, sql, args...)
	require.NoError(t, err)
}

// mustAddressBytes returns the 33-byte BYTEA encoding of a Stellar strkey
// address, matching the types.AddressBytea.Value serialization. Used to build
// the bytes for ad-hoc SQL inserts in tests.
func mustAddressBytes(t *testing.T, addr string) []byte {
	t.Helper()
	a := types.AddressBytea(addr)
	v, err := a.Value()
	require.NoError(t, err)
	b, ok := v.([]byte)
	require.True(t, ok)
	return b
}

// randomContractAddress generates a random valid C-address for testing
// (e.g. token/pool contract identifiers, which are C-addresses, not G-addresses).
func randomContractAddress(t *testing.T) string {
	t.Helper()
	var raw [32]byte
	_, err := rand.Read(raw[:])
	require.NoError(t, err)
	addr, err := strkey.Encode(strkey.VersionByteContract, raw[:])
	require.NoError(t, err)
	return addr
}
