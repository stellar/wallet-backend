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

		var status string
		err = dbConnectionPool.GetContext(context.Background(), &status, `SELECT current_status FROM tss_transactions WHERE transaction_hash = $1`, "hash")
		require.NoError(t, err)
		assert.Equal(t, status, string(tss.NewStatus))
	})

	t.Run("update", func(t *testing.T) {
		_ = store.UpsertTransaction(context.Background(), "www.stellar.org", "hash", "xdr", tss.RPCTXStatus{OtherStatus: tss.NewStatus})
		_ = store.UpsertTransaction(context.Background(), "www.stellar.org", "hash", "xdr", tss.RPCTXStatus{RPCStatus: entities.SuccessStatus})

		var status string
		err = dbConnectionPool.GetContext(context.Background(), &status, `SELECT current_status FROM tss_transactions WHERE transaction_hash = $1`, "hash")
		require.NoError(t, err)
		assert.Equal(t, status, string(entities.SuccessStatus))

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
		code := tss.RPCTXCode{OtherCodes: tss.NewCode}
		_ = store.UpsertTry(context.Background(), "hash", "feebumptxhash", "feebumptxxdr", code)

		var status int
		err = dbConnectionPool.GetContext(context.Background(), &status, `SELECT status FROM tss_transaction_submission_tries WHERE try_transaction_hash = $1`, "feebumptxhash")
		require.NoError(t, err)
		assert.Equal(t, status, int(tss.NewCode))
	})

	t.Run("update_other_code", func(t *testing.T) {
		code := tss.RPCTXCode{OtherCodes: tss.NewCode}
		_ = store.UpsertTry(context.Background(), "hash", "feebumptxhash", "feebumptxxdr", code)
		code = tss.RPCTXCode{OtherCodes: tss.RPCFailCode}
		_ = store.UpsertTry(context.Background(), "hash", "feebumptxhash", "feebumptxxdr", code)
		var status int
		err = dbConnectionPool.GetContext(context.Background(), &status, `SELECT status FROM tss_transaction_submission_tries WHERE try_transaction_hash = $1`, "feebumptxhash")
		require.NoError(t, err)
		assert.Equal(t, status, int(tss.RPCFailCode))

		var numRows int
		err = dbConnectionPool.GetContext(context.Background(), &numRows, `SELECT count(*) FROM tss_transaction_submission_tries  WHERE try_transaction_hash = $1`, "feebumptxhash")
		require.NoError(t, err)
		assert.Equal(t, numRows, 1)
	})

	t.Run("update_tx_code", func(t *testing.T) {
		code := tss.RPCTXCode{OtherCodes: tss.NewCode}
		_ = store.UpsertTry(context.Background(), "hash", "feebumptxhash", "feebumptxxdr", code)
		code = tss.RPCTXCode{TxResultCode: xdr.TransactionResultCodeTxSuccess}
		_ = store.UpsertTry(context.Background(), "hash", "feebumptxhash", "feebumptxxdr", code)
		var status int
		err = dbConnectionPool.GetContext(context.Background(), &status, `SELECT status FROM tss_transaction_submission_tries WHERE try_transaction_hash = $1`, "feebumptxhash")
		require.NoError(t, err)
		assert.Equal(t, status, int(xdr.TransactionResultCodeTxSuccess))

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
	store := NewStore(dbConnectionPool)
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
	store := NewStore(dbConnectionPool)
	t.Run("try_exists", func(t *testing.T) {
		code := tss.RPCTXCode{OtherCodes: tss.NewCode}
		_ = store.UpsertTry(context.Background(), "hash", "feebumptxhash", "feebumptxxdr", code)
		try, err := store.GetTry(context.Background(), "feebumptxhash")
		assert.Equal(t, try.OrigTxHash, "hash")
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
	store := NewStore(dbConnectionPool)
	t.Run("try_exists", func(t *testing.T) {
		code := tss.RPCTXCode{OtherCodes: tss.NewCode}
		_ = store.UpsertTry(context.Background(), "hash", "feebumptxhash", "feebumptxxdr", code)
		try, err := store.GetTryByXDR(context.Background(), "feebumptxxdr")
		assert.Equal(t, try.OrigTxHash, "hash")
		assert.Empty(t, err)

	})
	t.Run("try_does_not_exist", func(t *testing.T) {
		try, _ := store.GetTryByXDR(context.Background(), "doesnotexist")
		assert.Equal(t, Try{}, try)
		assert.Empty(t, err)
	})
}
