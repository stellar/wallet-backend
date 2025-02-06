package data

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/stellar/go/xdr"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPaymentModelAddPayment(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()
	sqlxDB, err := dbConnectionPool.SqlxDB(context.Background())
	require.NoError(t, err)
	metricsService := metrics.NewMetricsService(sqlxDB)

	m := &PaymentModel{
		DB:             dbConnectionPool,
		MetricsService: metricsService,
	}
	ctx := context.Background()

	const (
		fromAddress = "GCYQBVCREYSLKHHOWLT27VNZNGVIXXAPYVNNOWMQV67WVDD4PP2VZAX7"
		toAddress   = "GDDEAH46MNFO6JD7NTQ5FWJBC4ZSA47YEK3RKFHQWADYTS6NDVD5CORW"
	)
	payment := Payment{
		OperationID:     "2120562792996865",
		OperationType:   xdr.OperationTypePayment.String(),
		TransactionID:   "2120562792996864",
		TransactionHash: "a3daffa64dc46db84888b1206dc8014a480042e7fe8b19fd5d05465709f4e887",
		FromAddress:     fromAddress,
		ToAddress:       toAddress,
		SrcAssetCode:    "USDC",
		SrcAssetIssuer:  "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5",
		SrcAssetType:    xdr.AssetTypeAssetTypeCreditAlphanum4.String(),
		SrcAmount:       500000000,
		DestAssetCode:   "USDC",
		DestAssetIssuer: "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5",
		DestAssetType:   xdr.AssetTypeAssetTypeCreditAlphanum4.String(),
		DestAmount:      500000000,
		CreatedAt:       time.Date(2023, 12, 15, 1, 0, 0, 0, time.UTC),
		Memo:            nil,
		MemoType:        xdr.MemoTypeMemoNone.String(),
	}

	addPayment := func(p Payment) {
		err := db.RunInTransaction(ctx, m.DB, nil, func(dbTx db.Transaction) error {
			return m.AddPayment(ctx, dbTx, p)
		})
		require.NoError(t, err)
	}

	fetchPayment := func() (Payment, error) {
		var dbPayment Payment
		err := dbConnectionPool.GetContext(ctx, &dbPayment, "SELECT * FROM ingest_payments")
		return dbPayment, err
	}

	cleanUpDB := func() {
		_, err := dbConnectionPool.ExecContext(ctx, `DELETE FROM accounts; DELETE FROM ingest_payments;`)
		require.NoError(t, err)
	}

	t.Run("unkown_address", func(t *testing.T) {
		addPayment(payment)

		_, err := fetchPayment()
		assert.ErrorIs(t, err, sql.ErrNoRows)

		cleanUpDB()
	})

	t.Run("from_known_address", func(t *testing.T) {
		_, err := dbConnectionPool.ExecContext(ctx, `INSERT INTO accounts (stellar_address) VALUES ($1)`, fromAddress)
		require.NoError(t, err)

		addPayment(payment)

		dbPayment, err := fetchPayment()
		require.NoError(t, err)
		assert.Equal(t, payment, dbPayment)

		cleanUpDB()
	})

	t.Run("to_known_address", func(t *testing.T) {
		_, err := dbConnectionPool.ExecContext(ctx, `INSERT INTO accounts (stellar_address) VALUES ($1)`, toAddress)
		require.NoError(t, err)

		addPayment(payment)

		dbPayment, err := fetchPayment()
		require.NoError(t, err)
		assert.Equal(t, payment, dbPayment)

		cleanUpDB()
	})

	t.Run("to_known_address_update_on_reingestion", func(t *testing.T) {
		updatedPayment := Payment{
			OperationID:     payment.OperationID, // Same OperationID
			OperationType:   xdr.OperationTypePathPaymentStrictSend.String(),
			TransactionID:   "2120562792996865",
			TransactionHash: "a3daffa64dc46db84888b1206dc8014a480042e7fe8b19fd5d05465709f4e888",
			FromAddress:     fromAddress,
			ToAddress:       toAddress,
			SrcAssetCode:    "XLM",
			SrcAssetIssuer:  "",
			SrcAssetType:    xdr.AssetTypeAssetTypeCreditAlphanum12.String(),
			SrcAmount:       300000000,
			DestAssetCode:   "ARST",
			DestAssetIssuer: "GB7TAYRUZGE6TVT7NHP5SMIZRNQA6PLM423EYISAOAP3MKYIQMVYP2JO",
			DestAssetType:   xdr.AssetTypeAssetTypeCreditAlphanum12.String(),
			DestAmount:      700000000,
			CreatedAt:       time.Date(2023, 12, 16, 1, 0, 0, 0, time.UTC),
			Memo:            utils.PointOf("diff"),
			MemoType:        xdr.MemoTypeMemoText.String(),
		}

		_, err := dbConnectionPool.ExecContext(ctx, `INSERT INTO accounts (stellar_address) VALUES ($1)`, toAddress)
		require.NoError(t, err)

		addPayment(payment)
		addPayment(updatedPayment)

		dbPayment, err := fetchPayment()
		require.NoError(t, err)
		assert.Equal(t, updatedPayment, dbPayment)

		cleanUpDB()
	})
}

func TestPaymentModelGetLatestLedgerSynced(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()
	sqlxDB, err := dbConnectionPool.SqlxDB(context.Background())
	require.NoError(t, err)
	metricsService := metrics.NewMetricsService(sqlxDB)

	ctx := context.Background()
	m := &PaymentModel{
		DB:             dbConnectionPool,
		MetricsService: metricsService,
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
	sqlxDB, err := dbConnectionPool.SqlxDB(context.Background())
	require.NoError(t, err)
	metricsService := metrics.NewMetricsService(sqlxDB)

	ctx := context.Background()
	m := &PaymentModel{
		DB:             dbConnectionPool,
		MetricsService: metricsService,
	}

	const key = "ingest_store_key"
	err = m.UpdateLatestLedgerSynced(ctx, key, 123)
	require.NoError(t, err)

	var lastSyncedLedger uint32
	err = m.DB.GetContext(ctx, &lastSyncedLedger, `SELECT value FROM ingest_store WHERE key = $1`, key)
	require.NoError(t, err)
	assert.Equal(t, uint32(123), lastSyncedLedger)
}

func TestPaymentModelGetPaymentsPaginated(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()
	sqlxDB, err := dbConnectionPool.SqlxDB(context.Background())
	require.NoError(t, err)
	metricsService := metrics.NewMetricsService(sqlxDB)

	ctx := context.Background()
	m := &PaymentModel{
		DB:             dbConnectionPool,
		MetricsService: metricsService,
	}

	dbPayments := []Payment{
		{OperationID: "1", OperationType: xdr.OperationTypePayment.String(), TransactionID: "11", TransactionHash: "c370ff20144e4c96b17432b8d14664c1", FromAddress: "GAZ37ZO4TU3H", ToAddress: "GDD2HQO6IOFT", SrcAssetCode: "XLM", SrcAssetIssuer: "", SrcAssetType: xdr.AssetTypeAssetTypeNative.String(), SrcAmount: 10, DestAssetCode: "XLM", DestAssetIssuer: "", DestAssetType: xdr.AssetTypeAssetTypeNative.String(), DestAmount: 10, CreatedAt: time.Date(2024, 6, 21, 0, 0, 0, 0, time.UTC), Memo: nil, MemoType: xdr.MemoTypeMemoNone.String()},
		{OperationID: "2", OperationType: xdr.OperationTypePayment.String(), TransactionID: "22", TransactionHash: "30850d8fc7d1439782885103390cd975", FromAddress: "GBZ5Q56JKHJQ", ToAddress: "GASV72SENBSY", SrcAssetCode: "XLM", SrcAssetIssuer: "", SrcAssetType: xdr.AssetTypeAssetTypeNative.String(), SrcAmount: 20, DestAssetCode: "XLM", DestAssetIssuer: "", DestAssetType: xdr.AssetTypeAssetTypeNative.String(), DestAmount: 20, CreatedAt: time.Date(2024, 6, 22, 0, 0, 0, 0, time.UTC), Memo: nil, MemoType: xdr.MemoTypeMemoNone.String()},
		{OperationID: "3", OperationType: xdr.OperationTypePayment.String(), TransactionID: "33", TransactionHash: "d9521ed7057d4d1e9b9dd22ab515cbf1", FromAddress: "GAYFAYPOECBT", ToAddress: "GDWDPNMALNIT", SrcAssetCode: "XLM", SrcAssetIssuer: "", SrcAssetType: xdr.AssetTypeAssetTypeNative.String(), SrcAmount: 30, DestAssetCode: "XLM", DestAssetIssuer: "", DestAssetType: xdr.AssetTypeAssetTypeNative.String(), DestAmount: 30, CreatedAt: time.Date(2024, 6, 23, 0, 0, 0, 0, time.UTC), Memo: nil, MemoType: xdr.MemoTypeMemoNone.String()},
		{OperationID: "4", OperationType: xdr.OperationTypePayment.String(), TransactionID: "44", TransactionHash: "2af98496a86741c6a6814200e06027fd", FromAddress: "GACKTNR2QQXU", ToAddress: "GBZ5KUZHAAVI", SrcAssetCode: "USDC", SrcAssetIssuer: "GAHLU7PDIQMZ", SrcAssetType: xdr.AssetTypeAssetTypeCreditAlphanum4.String(), SrcAmount: 40, DestAssetCode: "USDC", DestAssetIssuer: "GAHLU7PDIQMZ", DestAssetType: xdr.AssetTypeAssetTypeCreditAlphanum4.String(), DestAmount: 40, CreatedAt: time.Date(2024, 6, 24, 0, 0, 0, 0, time.UTC), Memo: nil, MemoType: xdr.MemoTypeMemoNone.String()},
		{OperationID: "5", OperationType: xdr.OperationTypePayment.String(), TransactionID: "55", TransactionHash: "edfab36f9f104c4fb74b549de44cfbcc", FromAddress: "GA4CMYJEC5W5", ToAddress: "GAZ37ZO4TU3H", SrcAssetCode: "USDC", SrcAssetIssuer: "GAHLU7PDIQMZ", SrcAssetType: xdr.AssetTypeAssetTypeCreditAlphanum4.String(), SrcAmount: 50, DestAssetCode: "USDC", DestAssetIssuer: "GAHLU7PDIQMZ", DestAssetType: xdr.AssetTypeAssetTypeCreditAlphanum4.String(), DestAmount: 50, CreatedAt: time.Date(2024, 6, 25, 0, 0, 0, 0, time.UTC), Memo: nil, MemoType: xdr.MemoTypeMemoNone.String()},
	}
	InsertTestPayments(t, ctx, dbPayments, dbConnectionPool)

	t.Run("no_filter_desc", func(t *testing.T) {
		payments, prevExists, nextExists, err := m.GetPaymentsPaginated(ctx, "", "", "", DESC, 2)
		require.NoError(t, err)

		assert.False(t, prevExists)
		assert.True(t, nextExists)

		assert.Equal(t, []Payment{
			dbPayments[4],
			dbPayments[3],
		}, payments)
	})

	t.Run("no_filter_asc", func(t *testing.T) {
		payments, prevExists, nextExists, err := m.GetPaymentsPaginated(ctx, "", "", "", ASC, 2)
		require.NoError(t, err)

		assert.False(t, prevExists)
		assert.True(t, nextExists)

		assert.Equal(t, []Payment{
			dbPayments[0],
			dbPayments[1],
		}, payments)
	})

	t.Run("filter_address", func(t *testing.T) {
		payments, prevExists, nextExists, err := m.GetPaymentsPaginated(ctx, dbPayments[1].FromAddress, "", "", DESC, 2)
		require.NoError(t, err)

		assert.False(t, prevExists)
		assert.False(t, nextExists)

		assert.Equal(t, []Payment{
			dbPayments[1],
		}, payments)
	})

	t.Run("filter_after_id_desc", func(t *testing.T) {
		payments, prevExists, nextExists, err := m.GetPaymentsPaginated(ctx, "", "", dbPayments[3].OperationID, DESC, 2)
		require.NoError(t, err)

		assert.True(t, prevExists)
		assert.True(t, nextExists)

		assert.Equal(t, []Payment{
			dbPayments[2],
			dbPayments[1],
		}, payments)
	})

	t.Run("filter_after_id_asc", func(t *testing.T) {
		payments, prevExists, nextExists, err := m.GetPaymentsPaginated(ctx, "", "", dbPayments[3].OperationID, ASC, 2)
		require.NoError(t, err)

		assert.True(t, prevExists)
		assert.False(t, nextExists)

		assert.Equal(t, []Payment{
			dbPayments[4],
		}, payments)
	})

	t.Run("filter_before_id_desc", func(t *testing.T) {
		payments, prevExists, nextExists, err := m.GetPaymentsPaginated(ctx, "", dbPayments[2].OperationID, "", DESC, 2)
		require.NoError(t, err)

		assert.False(t, prevExists)
		assert.True(t, nextExists)

		assert.Equal(t, []Payment{
			dbPayments[4],
			dbPayments[3],
		}, payments)
	})

	t.Run("filter_before_id_asc", func(t *testing.T) {
		payments, prevExists, nextExists, err := m.GetPaymentsPaginated(ctx, "", dbPayments[2].OperationID, "", ASC, 2)
		require.NoError(t, err)

		assert.False(t, prevExists)
		assert.True(t, nextExists)

		assert.Equal(t, []Payment{
			dbPayments[0],
			dbPayments[1],
		}, payments)
	})

	t.Run("filter_before_id_after_id_asc", func(t *testing.T) {
		_, _, _, err := m.GetPaymentsPaginated(ctx, "", dbPayments[4].OperationID, dbPayments[2].OperationID, ASC, 2)
		assert.ErrorContains(t, err, "at most one cursor may be provided, got afterId and beforeId")
	})
}
