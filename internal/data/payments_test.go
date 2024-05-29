package data

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/stellar/go/keypair"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAddPayment(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	m := &PaymentModel{
		DB: dbConnectionPool,
	}
	ctx := context.Background()

	const (
		fromAddress = "GCYQBVCREYSLKHHOWLT27VNZNGVIXXAPYVNNOWMQV67WVDD4PP2VZAX7"
		toAddress   = "GDDEAH46MNFO6JD7NTQ5FWJBC4ZSA47YEK3RKFHQWADYTS6NDVD5CORW"
	)
	payment := Payment{
		OperationID:     2120562792996865,
		OperationType:   "OperationTypePayment",
		TransactionID:   2120562792996864,
		TransactionHash: "a3daffa64dc46db84888b1206dc8014a480042e7fe8b19fd5d05465709f4e887",
		FromAddress:     fromAddress,
		ToAddress:       toAddress,
		SrcAssetCode:    "USDC",
		SrcAssetIssuer:  "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5",
		SrcAmount:       500000000,
		DestAssetCode:   "USDC",
		DestAssetIssuer: "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5",
		DestAmount:      500000000,
		CreatedAt:       time.Date(2023, 12, 15, 1, 0, 0, 0, time.UTC),
		Memo:            nil,
	}

	addPayment := func() {
		err := db.RunInTransaction(ctx, m.DB, nil, func(dbTx db.Transaction) error {
			return m.AddPayment(ctx, dbTx, payment)
		})
		require.NoError(t, err)
	}

	fetchPaymentInserted := func() bool {
		var inserted bool
		err := dbConnectionPool.QueryRowxContext(ctx, "SELECT EXISTS(SELECT 1 FROM ingest_payments)").Scan(&inserted)
		require.NoError(t, err)

		return inserted
	}

	cleanUpDB := func() {
		_, err := dbConnectionPool.ExecContext(ctx, `DELETE FROM accounts; DELETE FROM ingest_payments;`)
		require.NoError(t, err)
	}

	t.Run("unkown_address", func(t *testing.T) {
		addPayment()

		inserted := fetchPaymentInserted()
		assert.False(t, inserted)

		cleanUpDB()
	})

	t.Run("from_known_address", func(t *testing.T) {
		_, err := dbConnectionPool.ExecContext(ctx, `INSERT INTO accounts (stellar_address) VALUES ($1)`, fromAddress)
		require.NoError(t, err)

		addPayment()

		inserted := fetchPaymentInserted()
		assert.True(t, inserted)

		cleanUpDB()
	})

	t.Run("to_known_address", func(t *testing.T) {
		_, err := dbConnectionPool.ExecContext(ctx, `INSERT INTO accounts (stellar_address) VALUES ($1)`, toAddress)
		require.NoError(t, err)

		addPayment()

		inserted := fetchPaymentInserted()
		assert.True(t, inserted)

		cleanUpDB()
	})
}

func TestSubscribeAddress(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	m := &PaymentModel{
		DB: dbConnectionPool,
	}

	ctx := context.Background()
	address := keypair.MustRandom().Address()
	err = m.SubscribeAddress(ctx, address)
	require.NoError(t, err)

	var dbAddress sql.NullString
	err = m.DB.GetContext(ctx, &dbAddress, "SELECT stellar_address FROM accounts LIMIT 1")
	require.NoError(t, err)

	assert.True(t, dbAddress.Valid)
	assert.Equal(t, address, dbAddress.String)
}
