package data

import (
	"context"
	"testing"
	"time"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPaymentModelAddPayment(t *testing.T) {
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

func TestPaymentModelGetLatestLedgerSynced(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()
	m := &PaymentModel{
		DB: dbConnectionPool,
	}

	const key = "ingest_store_key"
	lastSyncedLedger, err := m.GetLatestLedgerSynced(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, uint32(0), lastSyncedLedger)

	_, err = dbConnectionPool.ExecContext(ctx, `INSERT INTO ingest_store (key, value) VALUES ($1, $2)`, key, 123)
	require.NoError(t, err)

	lastSyncedLedger, err = m.GetLatestLedgerSynced(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, uint32(123), lastSyncedLedger)
}

func TestPaymentModelUpdateLatestLedgerSynced(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()
	m := &PaymentModel{
		DB: dbConnectionPool,
	}

	const key = "ingest_store_key"
	err = m.UpdateLatestLedgerSynced(ctx, key, 123)
	require.NoError(t, err)

	var lastSyncedLedger uint32
	err = m.DB.GetContext(ctx, &lastSyncedLedger, `SELECT value FROM ingest_store WHERE key = $1`, key)
	require.NoError(t, err)
	assert.Equal(t, uint32(123), lastSyncedLedger)
}
