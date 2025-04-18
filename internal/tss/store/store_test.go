package store

import (
	"context"
	"testing"

	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/tss"
)

func TestUpsertTransaction(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := metrics.NewMockMetricsService()
	store, err := NewStore(dbConnectionPool, mockMetricsService)
	require.NoError(t, err)

	t.Run("insert", func(t *testing.T) {
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "tss_transactions", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "SELECT", "tss_transactions").Once()
		mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "tss_transactions", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "INSERT", "tss_transactions").Once()
		defer mockMetricsService.AssertExpectations(t)

		err = store.UpsertTransaction(context.Background(), "www.stellar.org", "hash", "xdr", tss.RPCTXStatus{OtherStatus: tss.NewStatus})
		require.NoError(t, err)

		var tx Transaction
		tx, err = store.GetTransaction(context.Background(), "hash")
		require.NoError(t, err)
		assert.Equal(t, "xdr", tx.XDR)
		assert.Equal(t, string(tss.NewStatus), tx.Status)
	})

	t.Run("update", func(t *testing.T) {
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "tss_transactions", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "SELECT", "tss_transactions").Once()
		mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "tss_transactions", mock.AnythingOfType("float64")).Times(2)
		mockMetricsService.On("IncDBQuery", "INSERT", "tss_transactions").Times(2)
		defer mockMetricsService.AssertExpectations(t)

		err = store.UpsertTransaction(context.Background(), "www.stellar.org", "hash", "xdr", tss.RPCTXStatus{OtherStatus: tss.NewStatus})
		require.NoError(t, err)
		err = store.UpsertTransaction(context.Background(), "www.stellar.org", "hash", "xdr", tss.RPCTXStatus{RPCStatus: entities.SuccessStatus})
		require.NoError(t, err)

		tx, err := store.GetTransaction(context.Background(), "hash")
		require.NoError(t, err)
		assert.Equal(t, "xdr", tx.XDR)
		assert.Equal(t, string(entities.SuccessStatus), tx.Status)

		var numRows int
		err = dbConnectionPool.GetContext(context.Background(), &numRows, `SELECT count(*) FROM tss_transactions WHERE transaction_hash = $1`, "hash")
		require.NoError(t, err)
		assert.Equal(t, numRows, 1)
	})
}

func TestUpsertTry(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := metrics.NewMockMetricsService()
	store, err := NewStore(dbConnectionPool, mockMetricsService)
	require.NoError(t, err)

	t.Run("insert", func(t *testing.T) {
		mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "tss_transaction_submission_tries", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "INSERT", "tss_transaction_submission_tries").Once()
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "tss_transaction_submission_tries", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "SELECT", "tss_transaction_submission_tries").Once()
		defer mockMetricsService.AssertExpectations(t)

		status := tss.RPCTXStatus{OtherStatus: tss.NewStatus}
		code := tss.RPCTXCode{OtherCodes: tss.NewCode}
		resultXDR := "ABCD//"
		err = store.UpsertTry(context.Background(), "hash", "feebumptxhash", "feebumptxxdr", status, code, resultXDR)
		require.NoError(t, err)

		var try Try
		try, err = store.GetTry(context.Background(), "feebumptxhash")
		require.NoError(t, err)
		assert.Equal(t, "hash", try.OrigTxHash)
		assert.Equal(t, status.Status(), try.Status)
		assert.Equal(t, code.Code(), int(try.Code))
		assert.Equal(t, resultXDR, try.ResultXDR)
		assert.Equal(t, status.Status(), try.Status)
	})

	t.Run("update_other_code", func(t *testing.T) {
		mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "tss_transaction_submission_tries", mock.AnythingOfType("float64")).Times(2)
		mockMetricsService.On("IncDBQuery", "INSERT", "tss_transaction_submission_tries").Times(2)
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "tss_transaction_submission_tries", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "SELECT", "tss_transaction_submission_tries").Once()
		defer mockMetricsService.AssertExpectations(t)

		status := tss.RPCTXStatus{OtherStatus: tss.NewStatus}
		code := tss.RPCTXCode{OtherCodes: tss.NewCode}
		resultXDR := "ABCD//"
		err = store.UpsertTry(context.Background(), "hash", "feebumptxhash", "feebumptxxdr", status, code, resultXDR)
		require.NoError(t, err)
		code = tss.RPCTXCode{OtherCodes: tss.RPCFailCode}
		err = store.UpsertTry(context.Background(), "hash", "feebumptxhash", "feebumptxxdr", status, code, resultXDR)
		require.NoError(t, err)

		var try Try
		try, err = store.GetTry(context.Background(), "feebumptxhash")
		require.NoError(t, err)
		assert.Equal(t, "hash", try.OrigTxHash)
		assert.Equal(t, status.Status(), try.Status)
		assert.Equal(t, code.Code(), int(try.Code))
		assert.Equal(t, resultXDR, try.ResultXDR)

		var numRows int
		err = dbConnectionPool.GetContext(context.Background(), &numRows, `SELECT count(*) FROM tss_transaction_submission_tries  WHERE try_transaction_hash = $1`, "feebumptxhash")
		require.NoError(t, err)
		assert.Equal(t, numRows, 1)
	})

	t.Run("update_tx_code", func(t *testing.T) {
		mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "tss_transaction_submission_tries", mock.AnythingOfType("float64")).Times(2)
		mockMetricsService.On("IncDBQuery", "INSERT", "tss_transaction_submission_tries").Times(2)
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "tss_transaction_submission_tries", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "SELECT", "tss_transaction_submission_tries").Once()
		defer mockMetricsService.AssertExpectations(t)

		status := tss.RPCTXStatus{RPCStatus: entities.ErrorStatus}
		code := tss.RPCTXCode{TxResultCode: xdr.TransactionResultCodeTxInsufficientFee}
		resultXDR := "ABCD//"
		err = store.UpsertTry(context.Background(), "hash", "feebumptxhash", "feebumptxxdr", status, code, resultXDR)
		require.NoError(t, err)
		code = tss.RPCTXCode{TxResultCode: xdr.TransactionResultCodeTxSuccess}
		err = store.UpsertTry(context.Background(), "hash", "feebumptxhash", "feebumptxxdr", status, code, resultXDR)
		require.NoError(t, err)

		try, err := store.GetTry(context.Background(), "feebumptxhash")
		require.NoError(t, err)
		assert.Equal(t, "hash", try.OrigTxHash)
		assert.Equal(t, status.Status(), try.Status)
		assert.Equal(t, code.Code(), int(try.Code))
		assert.Equal(t, resultXDR, try.ResultXDR)

		var numRows int
		err = dbConnectionPool.GetContext(context.Background(), &numRows, `SELECT count(*) FROM tss_transaction_submission_tries  WHERE try_transaction_hash = $1`, "feebumptxhash")
		require.NoError(t, err)
		assert.Equal(t, numRows, 1)
	})
}

func TestGetTransaction(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := metrics.NewMockMetricsService()
	store, err := NewStore(dbConnectionPool, mockMetricsService)
	require.NoError(t, err)

	t.Run("transaction_exists", func(t *testing.T) {
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "tss_transactions", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "SELECT", "tss_transactions").Once()
		mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "tss_transactions", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "INSERT", "tss_transactions").Once()
		defer mockMetricsService.AssertExpectations(t)

		status := tss.RPCTXStatus{OtherStatus: tss.NewStatus}
		err = store.UpsertTransaction(context.Background(), "localhost:8000", "hash", "xdr", status)
		require.NoError(t, err)

		tx, err := store.GetTransaction(context.Background(), "hash")
		require.NoError(t, err)
		assert.Equal(t, "xdr", tx.XDR)
	})
	t.Run("transaction_does_not_exist", func(t *testing.T) {
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "tss_transactions", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "SELECT", "tss_transactions").Once()
		defer mockMetricsService.AssertExpectations(t)

		tx, err := store.GetTransaction(context.Background(), "doesnotexist")
		require.NoError(t, err)
		assert.Empty(t, tx)
	})
}

func TestGetTry(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := metrics.NewMockMetricsService()
	store, err := NewStore(dbConnectionPool, mockMetricsService)
	require.NoError(t, err)

	t.Run("try_exists", func(t *testing.T) {
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "tss_transaction_submission_tries", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "SELECT", "tss_transaction_submission_tries").Once()
		mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "tss_transaction_submission_tries", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "INSERT", "tss_transaction_submission_tries").Once()
		defer mockMetricsService.AssertExpectations(t)

		status := tss.RPCTXStatus{OtherStatus: tss.NewStatus}
		code := tss.RPCTXCode{OtherCodes: tss.NewCode}
		resultXDR := "ABCD//"
		err = store.UpsertTry(context.Background(), "hash", "feebumptxhash", "feebumptxxdr", status, code, resultXDR)
		require.NoError(t, err)

		try, err := store.GetTry(context.Background(), "feebumptxhash")
		require.NoError(t, err)
		assert.Equal(t, "hash", try.OrigTxHash)
		assert.Equal(t, status.Status(), try.Status)
		assert.Equal(t, code.Code(), int(try.Code))
		assert.Equal(t, resultXDR, try.ResultXDR)
	})
	t.Run("try_does_not_exist", func(t *testing.T) {
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "tss_transaction_submission_tries", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "SELECT", "tss_transaction_submission_tries").Once()
		defer mockMetricsService.AssertExpectations(t)

		try, err := store.GetTry(context.Background(), "doesnotexist")
		require.NoError(t, err)
		assert.Empty(t, try)
	})
}

func TestGetTryByXDR(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := metrics.NewMockMetricsService()
	store, err := NewStore(dbConnectionPool, mockMetricsService)
	require.NoError(t, err)

	t.Run("try_exists", func(t *testing.T) {
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "tss_transaction_submission_tries", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "SELECT", "tss_transaction_submission_tries").Once()
		mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "tss_transaction_submission_tries", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "INSERT", "tss_transaction_submission_tries").Once()
		defer mockMetricsService.AssertExpectations(t)

		status := tss.RPCTXStatus{OtherStatus: tss.NewStatus}
		code := tss.RPCTXCode{OtherCodes: tss.NewCode}
		resultXDR := "ABCD//"
		err = store.UpsertTry(context.Background(), "hash", "feebumptxhash", "feebumptxxdr", status, code, resultXDR)
		require.NoError(t, err)

		try, err := store.GetTryByXDR(context.Background(), "feebumptxxdr")
		require.NoError(t, err)
		assert.Equal(t, "hash", try.OrigTxHash)
		assert.Equal(t, status.Status(), try.Status)
		assert.Equal(t, code.Code(), int(try.Code))
		assert.Equal(t, resultXDR, try.ResultXDR)
	})
	t.Run("try_does_not_exist", func(t *testing.T) {
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "tss_transaction_submission_tries", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "SELECT", "tss_transaction_submission_tries").Once()
		defer mockMetricsService.AssertExpectations(t)

		try, err := store.GetTryByXDR(context.Background(), "doesnotexist")
		require.NoError(t, err)
		assert.Empty(t, try)
	})
}

func TestGetTransactionsWithStatus(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := metrics.NewMockMetricsService()
	store, err := NewStore(dbConnectionPool, mockMetricsService)
	require.NoError(t, err)

	t.Run("transactions_do_not_exist", func(t *testing.T) {
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "tss_transactions", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "SELECT", "tss_transactions").Once()
		defer mockMetricsService.AssertExpectations(t)

		status := tss.RPCTXStatus{OtherStatus: tss.NewStatus}
		var txns []Transaction
		txns, err = store.GetTransactionsWithStatus(context.Background(), status)
		require.NoError(t, err)
		assert.Empty(t, txns)
	})

	t.Run("transactions_exist", func(t *testing.T) {
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "tss_transactions", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "SELECT", "tss_transactions").Once()
		mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "tss_transactions", mock.AnythingOfType("float64")).Times(2)
		mockMetricsService.On("IncDBQuery", "INSERT", "tss_transactions").Times(2)
		defer mockMetricsService.AssertExpectations(t)

		status := tss.RPCTXStatus{OtherStatus: tss.NewStatus}
		err = store.UpsertTransaction(context.Background(), "localhost:8000", "hash1", "xdr1", status)
		require.NoError(t, err)
		err = store.UpsertTransaction(context.Background(), "localhost:8000", "hash2", "xdr2", status)
		require.NoError(t, err)

		txns, err := store.GetTransactionsWithStatus(context.Background(), status)
		require.NoError(t, err)
		assert.Equal(t, 2, len(txns))
		assert.Equal(t, "hash1", txns[0].Hash)
		assert.Equal(t, "hash2", txns[1].Hash)
	})
}

func TestGetLatestTry(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := metrics.NewMockMetricsService()
	store, err := NewStore(dbConnectionPool, mockMetricsService)
	require.NoError(t, err)

	t.Run("tries_do_not_exist", func(t *testing.T) {
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "tss_transaction_submission_tries", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "SELECT", "tss_transaction_submission_tries").Once()
		defer mockMetricsService.AssertExpectations(t)

		var try Try
		try, err = store.GetLatestTry(context.Background(), "hash")
		require.NoError(t, err)
		assert.Empty(t, try)
	})

	t.Run("tries_exist", func(t *testing.T) {
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "tss_transaction_submission_tries", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "SELECT", "tss_transaction_submission_tries").Once()
		mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "tss_transaction_submission_tries", mock.AnythingOfType("float64")).Times(2)
		mockMetricsService.On("IncDBQuery", "INSERT", "tss_transaction_submission_tries").Times(2)
		defer mockMetricsService.AssertExpectations(t)

		status := tss.RPCTXStatus{OtherStatus: tss.NewStatus}
		code := tss.RPCTXCode{OtherCodes: tss.NewCode}
		resultXDR := "ABCD//"
		err = store.UpsertTry(context.Background(), "hash", "feebumptxhash1", "feebumptxxdr1", status, code, resultXDR)
		require.NoError(t, err)
		err = store.UpsertTry(context.Background(), "hash", "feebumptxhash2", "feebumptxxdr2", status, code, resultXDR)
		require.NoError(t, err)

		try, err := store.GetLatestTry(context.Background(), "hash")
		require.NoError(t, err)
		assert.Equal(t, "feebumptxhash2", try.Hash)
	})
}
