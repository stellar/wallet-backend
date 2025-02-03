package store

import (
	"context"
	"testing"

	"github.com/stellar/go/xdr"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/tss"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpsertTransaction(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()
	store, _ := NewStore(dbConnectionPool)
	t.Run("insert", func(t *testing.T) {
		_ = store.UpsertTransaction(context.Background(), "www.stellar.org", "hash", "xdr", tss.RPCTXStatus{OtherStatus: tss.NewStatus})

		tx, _ := store.GetTransaction(context.Background(), "hash")
		assert.Equal(t, "xdr", tx.XDR)
		assert.Equal(t, string(tss.NewStatus), tx.Status)
	})

	t.Run("update", func(t *testing.T) {
		_ = store.UpsertTransaction(context.Background(), "www.stellar.org", "hash", "xdr", tss.RPCTXStatus{OtherStatus: tss.NewStatus})
		_ = store.UpsertTransaction(context.Background(), "www.stellar.org", "hash", "xdr", tss.RPCTXStatus{RPCStatus: entities.SuccessStatus})

		tx, _ := store.GetTransaction(context.Background(), "hash")
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
	store, _ := NewStore(dbConnectionPool)
	t.Run("insert", func(t *testing.T) {
		status := tss.RPCTXStatus{OtherStatus: tss.NewStatus}
		code := tss.RPCTXCode{OtherCodes: tss.NewCode}
		resultXDR := "ABCD//"
		err = store.UpsertTry(context.Background(), "hash", "feebumptxhash", "feebumptxxdr", status, code, resultXDR)

		try, _ := store.GetTry(context.Background(), "feebumptxhash")
		assert.Equal(t, "hash", try.OrigTxHash)
		assert.Equal(t, status.Status(), try.Status)
		assert.Equal(t, code.Code(), int(try.Code))
		assert.Equal(t, resultXDR, try.ResultXDR)
		assert.Equal(t, status.Status(), try.Status)
	})

	t.Run("update_other_code", func(t *testing.T) {
		status := tss.RPCTXStatus{OtherStatus: tss.NewStatus}
		code := tss.RPCTXCode{OtherCodes: tss.NewCode}
		resultXDR := "ABCD//"
		_ = store.UpsertTry(context.Background(), "hash", "feebumptxhash", "feebumptxxdr", status, code, resultXDR)
		code = tss.RPCTXCode{OtherCodes: tss.RPCFailCode}
		_ = store.UpsertTry(context.Background(), "hash", "feebumptxhash", "feebumptxxdr", status, code, resultXDR)

		try, _ := store.GetTry(context.Background(), "feebumptxhash")
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
		status := tss.RPCTXStatus{RPCStatus: entities.ErrorStatus}
		code := tss.RPCTXCode{TxResultCode: xdr.TransactionResultCodeTxInsufficientFee}
		resultXDR := "ABCD//"
		_ = store.UpsertTry(context.Background(), "hash", "feebumptxhash", "feebumptxxdr", status, code, resultXDR)
		code = tss.RPCTXCode{TxResultCode: xdr.TransactionResultCodeTxSuccess}
		_ = store.UpsertTry(context.Background(), "hash", "feebumptxhash", "feebumptxxdr", status, code, resultXDR)

		try, _ := store.GetTry(context.Background(), "feebumptxhash")
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
	store, _ := NewStore(dbConnectionPool)
	t.Run("transaction_exists", func(t *testing.T) {
		status := tss.RPCTXStatus{OtherStatus: tss.NewStatus}
		_ = store.UpsertTransaction(context.Background(), "localhost:8000", "hash", "xdr", status)

		tx, err := store.GetTransaction(context.Background(), "hash")

		assert.Equal(t, "xdr", tx.XDR)
		assert.Empty(t, err)

	})
	t.Run("transaction_does_not_exist", func(t *testing.T) {
		tx, _ := store.GetTransaction(context.Background(), "doesnotexist")
		assert.Equal(t, Transaction{}, tx)
		assert.Empty(t, err)
	})
}

func TestGetTry(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()
	store, _ := NewStore(dbConnectionPool)
	t.Run("try_exists", func(t *testing.T) {
		status := tss.RPCTXStatus{OtherStatus: tss.NewStatus}
		code := tss.RPCTXCode{OtherCodes: tss.NewCode}
		resultXDR := "ABCD//"
		_ = store.UpsertTry(context.Background(), "hash", "feebumptxhash", "feebumptxxdr", status, code, resultXDR)

		try, err := store.GetTry(context.Background(), "feebumptxhash")

		assert.Equal(t, "hash", try.OrigTxHash)
		assert.Equal(t, status.Status(), try.Status)
		assert.Equal(t, code.Code(), int(try.Code))
		assert.Equal(t, resultXDR, try.ResultXDR)
		assert.Empty(t, err)

	})
	t.Run("try_does_not_exist", func(t *testing.T) {
		try, _ := store.GetTry(context.Background(), "doesnotexist")
		assert.Equal(t, Try{}, try)
		assert.Empty(t, err)
	})
}

func TestGetTryByXDR(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()
	store, _ := NewStore(dbConnectionPool)
	t.Run("try_exists", func(t *testing.T) {
		status := tss.RPCTXStatus{OtherStatus: tss.NewStatus}
		code := tss.RPCTXCode{OtherCodes: tss.NewCode}
		resultXDR := "ABCD//"
		_ = store.UpsertTry(context.Background(), "hash", "feebumptxhash", "feebumptxxdr", status, code, resultXDR)

		try, err := store.GetTryByXDR(context.Background(), "feebumptxxdr")

		assert.Equal(t, "hash", try.OrigTxHash)
		assert.Equal(t, status.Status(), try.Status)
		assert.Equal(t, code.Code(), int(try.Code))
		assert.Equal(t, resultXDR, try.ResultXDR)
		assert.Empty(t, err)

	})
	t.Run("try_does_not_exist", func(t *testing.T) {
		try, _ := store.GetTryByXDR(context.Background(), "doesnotexist")
		assert.Equal(t, Try{}, try)
		assert.Empty(t, err)
	})
}

func TestGetTransactionsWithStatus(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()
	store, _ := NewStore(dbConnectionPool)

	t.Run("transactions_do_not_exist", func(t *testing.T) {
		status := tss.RPCTXStatus{OtherStatus: tss.NewStatus}
		txns, err := store.GetTransactionsWithStatus(context.Background(), status)
		assert.Equal(t, 0, len(txns))
		assert.Empty(t, err)
	})

	t.Run("transactions_exist", func(t *testing.T) {
		status := tss.RPCTXStatus{OtherStatus: tss.NewStatus}
		_ = store.UpsertTransaction(context.Background(), "localhost:8000", "hash1", "xdr1", status)
		_ = store.UpsertTransaction(context.Background(), "localhost:8000", "hash2", "xdr2", status)

		txns, err := store.GetTransactionsWithStatus(context.Background(), status)

		assert.Equal(t, 2, len(txns))
		assert.Equal(t, "hash1", txns[0].Hash)
		assert.Equal(t, "hash2", txns[1].Hash)
		assert.Empty(t, err)
	})
}

func TestGetLatestTry(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()
	store, _ := NewStore(dbConnectionPool)

	t.Run("tries_do_not_exist", func(t *testing.T) {
		try, err := store.GetLatestTry(context.Background(), "hash")

		assert.Equal(t, Try{}, try)
		assert.Empty(t, err)
	})

	t.Run("tries_exist", func(t *testing.T) {
		status := tss.RPCTXStatus{OtherStatus: tss.NewStatus}
		code := tss.RPCTXCode{OtherCodes: tss.NewCode}
		resultXDR := "ABCD//"
		_ = store.UpsertTry(context.Background(), "hash", "feebumptxhash1", "feebumptxxdr1", status, code, resultXDR)
		_ = store.UpsertTry(context.Background(), "hash", "feebumptxhash2", "feebumptxxdr2", status, code, resultXDR)

		try, err := store.GetLatestTry(context.Background(), "hash")

		assert.Equal(t, "feebumptxhash2", try.Hash)
		assert.Empty(t, err)
	})

}
