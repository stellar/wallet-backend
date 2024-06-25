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

func TestPaymentModelGetPayments(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	ctx := context.Background()
	m := &PaymentModel{
		DB: dbConnectionPool,
	}

	dbPayments := []Payment{
		{OperationID: 1, OperationType: "OperationTypePayment", TransactionID: 11, TransactionHash: "c370ff20144e4c96b17432b8d14664c1", FromAddress: "GAZ37ZO4TU3H", ToAddress: "GDD2HQO6IOFT", SrcAssetCode: "XLM", SrcAssetIssuer: "", SrcAmount: 10, DestAssetCode: "XLM", DestAssetIssuer: "", DestAmount: 10, CreatedAt: time.Date(2024, 6, 21, 0, 0, 0, 0, time.UTC), Memo: nil},
		{OperationID: 2, OperationType: "OperationTypePayment", TransactionID: 22, TransactionHash: "30850d8fc7d1439782885103390cd975", FromAddress: "GBZ5Q56JKHJQ", ToAddress: "GASV72SENBSY", SrcAssetCode: "XLM", SrcAssetIssuer: "", SrcAmount: 20, DestAssetCode: "XLM", DestAssetIssuer: "", DestAmount: 20, CreatedAt: time.Date(2024, 6, 22, 0, 0, 0, 0, time.UTC), Memo: nil},
		{OperationID: 3, OperationType: "OperationTypePayment", TransactionID: 33, TransactionHash: "d9521ed7057d4d1e9b9dd22ab515cbf1", FromAddress: "GAYFAYPOECBT", ToAddress: "GDWDPNMALNIT", SrcAssetCode: "XLM", SrcAssetIssuer: "", SrcAmount: 30, DestAssetCode: "XLM", DestAssetIssuer: "", DestAmount: 30, CreatedAt: time.Date(2024, 6, 23, 0, 0, 0, 0, time.UTC), Memo: nil},
		{OperationID: 4, OperationType: "OperationTypePayment", TransactionID: 44, TransactionHash: "2af98496a86741c6a6814200e06027fd", FromAddress: "GACKTNR2QQXU", ToAddress: "GBZ5KUZHAAVI", SrcAssetCode: "USDC", SrcAssetIssuer: "GAHLU7PDIQMZ", SrcAmount: 40, DestAssetCode: "USDC", DestAssetIssuer: "GAHLU7PDIQMZ", DestAmount: 40, CreatedAt: time.Date(2024, 6, 24, 0, 0, 0, 0, time.UTC), Memo: nil},
		{OperationID: 5, OperationType: "OperationTypePayment", TransactionID: 55, TransactionHash: "edfab36f9f104c4fb74b549de44cfbcc", FromAddress: "GA4CMYJEC5W5", ToAddress: "GAZ37ZO4TU3H", SrcAssetCode: "USDC", SrcAssetIssuer: "GAHLU7PDIQMZ", SrcAmount: 50, DestAssetCode: "USDC", DestAssetIssuer: "GAHLU7PDIQMZ", DestAmount: 50, CreatedAt: time.Date(2024, 6, 25, 0, 0, 0, 0, time.UTC), Memo: nil},
	}

	const query = `INSERT INTO ingest_payments (operation_id, operation_type, transaction_id, transaction_hash, from_address, to_address, src_asset_code, src_asset_issuer, src_amount, dest_asset_code, dest_asset_issuer, dest_amount, created_at, memo) VALUES (:operation_id, :operation_type, :transaction_id, :transaction_hash, :from_address, :to_address, :src_asset_code, :src_asset_issuer, :src_amount, :dest_asset_code, :dest_asset_issuer, :dest_amount, :created_at, :memo);`
	_, err = dbConnectionPool.NamedExecContext(ctx, query, dbPayments)
	require.NoError(t, err)

	t.Run("no_filter_desc", func(t *testing.T) {
		payments, err := m.GetPayments(ctx, "", 0, 0, DESC, 2)
		require.NoError(t, err)

		assert.Equal(t, []Payment{
			dbPayments[4],
			dbPayments[3],
		}, payments)
	})

	t.Run("no_filter_asc", func(t *testing.T) {
		payments, err := m.GetPayments(ctx, "", 0, 0, ASC, 2)
		require.NoError(t, err)

		assert.Equal(t, []Payment{
			dbPayments[0],
			dbPayments[1],
		}, payments)
	})

	t.Run("after_id_desc", func(t *testing.T) {
		payments, err := m.GetPayments(ctx, "", 0, dbPayments[3].OperationID, DESC, 2)
		require.NoError(t, err)

		assert.Equal(t, []Payment{
			dbPayments[2],
			dbPayments[1],
		}, payments)
	})

	t.Run("after_id_asc", func(t *testing.T) {
		payments, err := m.GetPayments(ctx, "", 0, dbPayments[3].OperationID, ASC, 2)
		require.NoError(t, err)

		assert.Equal(t, []Payment{
			dbPayments[4],
		}, payments)
	})

	t.Run("before_id_desc", func(t *testing.T) {
		payments, err := m.GetPayments(ctx, "", dbPayments[2].OperationID, 0, DESC, 2)
		require.NoError(t, err)

		assert.Equal(t, []Payment{
			dbPayments[4],
			dbPayments[3],
		}, payments)
	})

	t.Run("before_id_asc", func(t *testing.T) {
		payments, err := m.GetPayments(ctx, "", dbPayments[2].OperationID, 0, ASC, 2)
		require.NoError(t, err)

		assert.Equal(t, []Payment{
			dbPayments[0],
			dbPayments[1],
		}, payments)
	})

	t.Run("before_id_after_id_asc", func(t *testing.T) {
		payments, err := m.GetPayments(ctx, "", dbPayments[4].OperationID, dbPayments[2].OperationID, ASC, 2)
		require.NoError(t, err)

		assert.Equal(t, []Payment{
			dbPayments[3],
		}, payments)
	})
}
