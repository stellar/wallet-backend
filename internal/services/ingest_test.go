package services

import (
	"context"
	"testing"

	"github.com/stellar/go/xdr"
	"github.com/stellar/wallet-backend/internal/apptracker"
	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/tss"
	tssrouter "github.com/stellar/wallet-backend/internal/tss/router"
	tssstore "github.com/stellar/wallet-backend/internal/tss/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestGetLedgerTransactions(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()
	models, _ := data.NewModels(dbConnectionPool)
	mockAppTracker := apptracker.MockAppTracker{}
	mockRPCService := RPCServiceMock{}
	mockRouter := tssrouter.MockRouter{}
	tssStore, _ := tssstore.NewStore(dbConnectionPool)
	ingestService, _ := NewIngestService(models, "ingestionLedger", &mockAppTracker, &mockRPCService, &mockRouter, tssStore)
	t.Run("all_ledger_transactions_in_single_gettransactions_call", func(t *testing.T) {
		rpcGetTransactionsResult := entities.RPCGetTransactionsResult{
			Cursor: "51",
			Transactions: []entities.Transaction{
				{
					Status: entities.SuccessStatus,
					Hash:   "hash1",
					Ledger: 1,
				},
				{
					Status: entities.FailedStatus,
					Hash:   "hash2",
					Ledger: 2,
				},
			},
		}
		mockRPCService.
			On("GetTransactions", int64(1), "", 50).
			Return(rpcGetTransactionsResult, nil).
			Once()

		txns, err := ingestService.GetLedgerTransactions(1)
		assert.Equal(t, 1, len(txns))
		assert.Equal(t, txns[0].Hash, "hash1")
		assert.Empty(t, err)
	})

	t.Run("ledger_transactions_split_between_multiple_gettransactions_calls", func(t *testing.T) {
		rpcGetTransactionsResult1 := entities.RPCGetTransactionsResult{
			Cursor: "51",
			Transactions: []entities.Transaction{
				{
					Status: entities.SuccessStatus,
					Hash:   "hash1",
					Ledger: 1,
				},
				{
					Status: entities.FailedStatus,
					Hash:   "hash2",
					Ledger: 1,
				},
			},
		}
		rpcGetTransactionsResult2 := entities.RPCGetTransactionsResult{
			Cursor: "51",
			Transactions: []entities.Transaction{
				{
					Status: entities.SuccessStatus,
					Hash:   "hash3",
					Ledger: 1,
				},
				{
					Status: entities.FailedStatus,
					Hash:   "hash4",
					Ledger: 2,
				},
			},
		}

		mockRPCService.
			On("GetTransactions", int64(1), "", 50).
			Return(rpcGetTransactionsResult1, nil).
			Once()

		mockRPCService.
			On("GetTransactions", int64(1), "51", 50).
			Return(rpcGetTransactionsResult2, nil).
			Once()

		txns, err := ingestService.GetLedgerTransactions(1)
		assert.Equal(t, 3, len(txns))
		assert.Equal(t, txns[0].Hash, "hash1")
		assert.Equal(t, txns[1].Hash, "hash2")
		assert.Equal(t, txns[2].Hash, "hash3")
		assert.Empty(t, err)
	})

}

func TestProcessTSSTransactions(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()
	models, _ := data.NewModels(dbConnectionPool)
	mockAppTracker := apptracker.MockAppTracker{}
	mockRPCService := RPCServiceMock{}
	mockRouter := tssrouter.MockRouter{}
	tssStore, _ := tssstore.NewStore(dbConnectionPool)
	ingestService, _ := NewIngestService(models, "ingestionLedger", &mockAppTracker, &mockRPCService, &mockRouter, tssStore)

	t.Run("routes_to_tss_router", func(t *testing.T) {

		transactions := []entities.Transaction{
			{
				Status:              entities.SuccessStatus,
				Hash:                "feebumphash",
				ApplicationOrder:    1,
				FeeBump:             true,
				EnvelopeXDR:         "feebumpxdr",
				ResultXDR:           "AAAAAAAAAMj////9AAAAAA==",
				ResultMetaXDR:       "meta",
				Ledger:              123456,
				DiagnosticEventsXDR: "diag",
				CreatedAt:           1695939098,
			},
		}

		_ = tssStore.UpsertTransaction(context.Background(), "localhost:8000/webhook", "hash", "xdr", tss.RPCTXStatus{OtherStatus: tss.NewStatus})
		_ = tssStore.UpsertTry(context.Background(), "hash", "feebumphash", "feebumpxdr", tss.RPCTXCode{OtherCodes: tss.NewCode})

		mockRouter.
			On("Route", mock.AnythingOfType("tss.Payload")).
			Return(nil).
			Once()

		err := ingestService.processTSSTransactions(context.Background(), transactions)
		assert.Empty(t, err)

		updatedTX, _ := tssStore.GetTransaction(context.Background(), "hash")
		assert.Equal(t, string(entities.SuccessStatus), updatedTX.Status)
		updatedTry, _ := tssStore.GetTry(context.Background(), "feebumphash")
		assert.Equal(t, int32(xdr.TransactionResultCodeTxTooLate), updatedTry.Code)
	})
}
