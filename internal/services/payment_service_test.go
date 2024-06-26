package services

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPaymentServiceGetPaymentsPaginated(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	models, err := data.NewModels(dbConnectionPool)
	require.NoError(t, err)
	service := &PaymentService{
		Models:        models,
		ServerBaseURL: "http://testing.com",
	}
	ctx := context.Background()

	dbPayments := []data.Payment{
		{OperationID: 1, OperationType: "OperationTypePayment", TransactionID: 11, TransactionHash: "c370ff20144e4c96b17432b8d14664c1", FromAddress: "GAZ37ZO4TU3H", ToAddress: "GDD2HQO6IOFT", SrcAssetCode: "XLM", SrcAssetIssuer: "", SrcAmount: 10, DestAssetCode: "XLM", DestAssetIssuer: "", DestAmount: 10, CreatedAt: time.Date(2024, 6, 21, 0, 0, 0, 0, time.UTC), Memo: nil},
		{OperationID: 2, OperationType: "OperationTypePayment", TransactionID: 22, TransactionHash: "30850d8fc7d1439782885103390cd975", FromAddress: "GBZ5Q56JKHJQ", ToAddress: "GASV72SENBSY", SrcAssetCode: "XLM", SrcAssetIssuer: "", SrcAmount: 20, DestAssetCode: "XLM", DestAssetIssuer: "", DestAmount: 20, CreatedAt: time.Date(2024, 6, 22, 0, 0, 0, 0, time.UTC), Memo: nil},
		{OperationID: 3, OperationType: "OperationTypePayment", TransactionID: 33, TransactionHash: "d9521ed7057d4d1e9b9dd22ab515cbf1", FromAddress: "GAYFAYPOECBT", ToAddress: "GDWDPNMALNIT", SrcAssetCode: "XLM", SrcAssetIssuer: "", SrcAmount: 30, DestAssetCode: "XLM", DestAssetIssuer: "", DestAmount: 30, CreatedAt: time.Date(2024, 6, 23, 0, 0, 0, 0, time.UTC), Memo: nil},
		{OperationID: 4, OperationType: "OperationTypePayment", TransactionID: 44, TransactionHash: "2af98496a86741c6a6814200e06027fd", FromAddress: "GACKTNR2QQXU", ToAddress: "GBZ5KUZHAAVI", SrcAssetCode: "USDC", SrcAssetIssuer: "GAHLU7PDIQMZ", SrcAmount: 40, DestAssetCode: "USDC", DestAssetIssuer: "GAHLU7PDIQMZ", DestAmount: 40, CreatedAt: time.Date(2024, 6, 24, 0, 0, 0, 0, time.UTC), Memo: nil},
		{OperationID: 5, OperationType: "OperationTypePayment", TransactionID: 55, TransactionHash: "edfab36f9f104c4fb74b549de44cfbcc", FromAddress: "GA4CMYJEC5W5", ToAddress: "GAZ37ZO4TU3H", SrcAssetCode: "USDC", SrcAssetIssuer: "GAHLU7PDIQMZ", SrcAmount: 50, DestAssetCode: "USDC", DestAssetIssuer: "GAHLU7PDIQMZ", DestAmount: 50, CreatedAt: time.Date(2024, 6, 25, 0, 0, 0, 0, time.UTC), Memo: nil},
	}
	data.InsertTestPayments(t, ctx, dbPayments, dbConnectionPool)

	t.Run("page_1", func(t *testing.T) {
		payments, pagination, err := service.GetPaymentsPaginated(ctx, "", 0, 0, data.DESC, 2)
		require.NoError(t, err)

		assert.Equal(t, []data.Payment{
			dbPayments[4],
			dbPayments[3],
		}, payments)
		assert.Equal(t, entities.Pagination{
			Links: entities.PaginationLinks{
				Self: "http://testing.com?limit=2&sort=DESC",
				Prev: "",
				Next: fmt.Sprintf("http://testing.com?afterId=%d&limit=2&sort=DESC", dbPayments[3].OperationID),
			},
		}, pagination)
	})

	t.Run("page_2_after", func(t *testing.T) {
		payments, pagination, err := service.GetPaymentsPaginated(ctx, "", 0, dbPayments[3].OperationID, data.DESC, 2)
		require.NoError(t, err)

		assert.Equal(t, []data.Payment{
			dbPayments[2],
			dbPayments[1],
		}, payments)
		assert.Equal(t, entities.Pagination{
			Links: entities.PaginationLinks{
				Self: fmt.Sprintf("http://testing.com?afterId=%d&limit=2&sort=DESC", dbPayments[3].OperationID),
				Prev: fmt.Sprintf("http://testing.com?beforeId=%d&limit=2&sort=DESC", dbPayments[2].OperationID),
				Next: fmt.Sprintf("http://testing.com?afterId=%d&limit=2&sort=DESC", dbPayments[1].OperationID),
			},
		}, pagination)
	})

	t.Run("page_3_after", func(t *testing.T) {
		payments, pagination, err := service.GetPaymentsPaginated(ctx, "", 0, dbPayments[1].OperationID, data.DESC, 2)
		require.NoError(t, err)

		assert.Equal(t, []data.Payment{
			dbPayments[0],
		}, payments)
		assert.Equal(t, entities.Pagination{
			Links: entities.PaginationLinks{
				Self: fmt.Sprintf("http://testing.com?afterId=%d&limit=2&sort=DESC", dbPayments[1].OperationID),
				Prev: fmt.Sprintf("http://testing.com?beforeId=%d&limit=2&sort=DESC", dbPayments[0].OperationID),
				Next: "",
			},
		}, pagination)
	})

	t.Run("page_2_before", func(t *testing.T) {
		payments, pagination, err := service.GetPaymentsPaginated(ctx, "", dbPayments[0].OperationID, 0, data.DESC, 2)
		require.NoError(t, err)

		assert.Equal(t, []data.Payment{
			dbPayments[2],
			dbPayments[1],
		}, payments)
		assert.Equal(t, entities.Pagination{
			Links: entities.PaginationLinks{
				Self: fmt.Sprintf("http://testing.com?beforeId=%d&limit=2&sort=DESC", dbPayments[0].OperationID),
				Prev: fmt.Sprintf("http://testing.com?beforeId=%d&limit=2&sort=DESC", dbPayments[2].OperationID),
				Next: fmt.Sprintf("http://testing.com?afterId=%d&limit=2&sort=DESC", dbPayments[1].OperationID),
			},
		}, pagination)
	})

	t.Run("page_1_before", func(t *testing.T) {
		payments, pagination, err := service.GetPaymentsPaginated(ctx, "", dbPayments[2].OperationID, 0, data.DESC, 2)
		require.NoError(t, err)

		assert.Equal(t, []data.Payment{
			dbPayments[4],
			dbPayments[3],
		}, payments)
		assert.Equal(t, entities.Pagination{
			Links: entities.PaginationLinks{
				Self: fmt.Sprintf("http://testing.com?beforeId=%d&limit=2&sort=DESC", dbPayments[2].OperationID),
				Prev: "",
				Next: fmt.Sprintf("http://testing.com?afterId=%d&limit=2&sort=DESC", dbPayments[3].OperationID),
			},
		}, pagination)
	})
}
