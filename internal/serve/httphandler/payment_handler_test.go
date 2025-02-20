package httphandler

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-chi/chi"
	"github.com/stellar/go/xdr"
	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/services"
	"github.com/stellar/wallet-backend/internal/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestPaymentHandlerGetPayments(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	setupTest := func() (*PaymentHandler, *metrics.MockMetricsService) {
		mockMetricsService := metrics.NewMockMetricsService()
		models, err := data.NewModels(dbConnectionPool, mockMetricsService)
		require.NoError(t, err)
		paymentService, err := services.NewPaymentService(models, "http://testing.com")
		require.NoError(t, err)
		handler := &PaymentHandler{
			PaymentService: paymentService,
		}
		return handler, mockMetricsService
	}

	// Setup router and test data
	setupRouter := func(handler *PaymentHandler) *chi.Mux {
		r := chi.NewRouter()
		r.Route("/payments", func(r chi.Router) {
			r.Get("/", handler.GetPayments)
		})
		return r
	}

	ctx := context.Background()

	dbPayments := []data.Payment{
		{
			OperationID:     "1",
			OperationType:   xdr.OperationTypePayment.String(),
			TransactionID:   "11",
			TransactionHash: "c370ff20144e4c96b17432b8d14664c1",
			FromAddress:     "GD73EG2IJJQQTCD33JKPKEGS76CJJ4TQ7NHDQYMS4D3Z5FBHPML6M66W",
			ToAddress:       "GCJ4LXZIQRSS5Z7YVIH5YLA7RXMYB64DQN3XMKWEBHUUAFXIXOL3GYVT",
			SrcAssetCode:    "XLM",
			SrcAssetIssuer:  "",
			SrcAssetType:    xdr.AssetTypeAssetTypeNative.String(),
			SrcAmount:       10,
			DestAssetCode:   "XLM",
			DestAssetIssuer: "",
			DestAssetType:   xdr.AssetTypeAssetTypeNative.String(),
			DestAmount:      10,
			CreatedAt:       time.Date(2024, 6, 21, 0, 0, 0, 0, time.UTC),
			Memo:            utils.PointOf("test"),
			MemoType:        xdr.MemoTypeMemoText.String(),
		},
		{
			OperationID:     "2",
			OperationType:   xdr.OperationTypePayment.String(),
			TransactionID:   "22",
			TransactionHash: "30850d8fc7d1439782885103390cd975",
			FromAddress:     "GASP7HTICNNA2U5RKMPRQELEUJFO7PBB3AKKRGTAG23QVG255ESPZW2L",
			ToAddress:       "GDB4RW6QFWMGHGI6JTIKMGVUUQO7NNOLSFDMCOMUCCWHMAMFL3FH4Q2J",
			SrcAssetCode:    "USDC",
			SrcAssetIssuer:  "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5",
			SrcAssetType:    xdr.AssetTypeAssetTypeCreditAlphanum4.String(),
			SrcAmount:       20,
			DestAssetCode:   "USDC",
			DestAssetIssuer: "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5",
			DestAssetType:   xdr.AssetTypeAssetTypeCreditAlphanum4.String(),
			DestAmount:      20,
			CreatedAt:       time.Date(2024, 6, 22, 0, 0, 0, 0, time.UTC),
			Memo:            utils.PointOf("123"),
			MemoType:        xdr.MemoTypeMemoId.String(),
		},
		{
			OperationID:     "3",
			OperationType:   xdr.OperationTypePathPaymentStrictSend.String(),
			TransactionID:   "33",
			TransactionHash: "d9521ed7057d4d1e9b9dd22ab515cbf1",
			FromAddress:     "GCXBGEYNIEIUJ56YX5UVBM27NTKCBMLDD2NEPTTXZGQMBA2EOKG5VA2W",
			ToAddress:       "GAX6VPTVC2YNJM52OYMJAZKTQMSLNQ6NKYYU77KSGRVHINZ2D3EUJWAN",
			SrcAssetCode:    "XLM",
			SrcAssetIssuer:  "",
			SrcAssetType:    xdr.AssetTypeAssetTypeNative.String(),
			SrcAmount:       300,
			DestAssetCode:   "USDC",
			DestAssetIssuer: "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5",
			DestAssetType:   xdr.AssetTypeAssetTypeCreditAlphanum4.String(),
			DestAmount:      30,
			CreatedAt:       time.Date(2024, 6, 23, 0, 0, 0, 0, time.UTC),
			Memo:            nil,
			MemoType:        xdr.MemoTypeMemoNone.String(),
		},
	}
	data.InsertTestPayments(t, ctx, dbPayments, dbConnectionPool)

	t.Run("no_filters", func(t *testing.T) {
		handler, mockMetricsService := setupTest()
		r := setupRouter(handler)

		mockMetricsService.On("IncDBQuery", "SELECT", "ingest_payments").Return().Times(2)
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "ingest_payments", mock.Anything).Return().Times(2)

		// Prepare request
		req, err := http.NewRequest(http.MethodGet, "/payments", nil)
		require.NoError(t, err)

		// Serve request
		rr := httptest.NewRecorder()
		r.ServeHTTP(rr, req)

		// Assert 200 response
		assert.Equal(t, http.StatusOK, rr.Code)

		resp := rr.Result()
		respBody, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		expectedRespBody := `{
			"_links": {
				"next": "",
				"prev": "",
				"self": "http://testing.com?limit=50&sort=DESC"
			},
			"payments": [
				{
					"createdAt": "2024-06-23T00:00:00Z",
					"destAmount": 30,
					"destAssetCode": "USDC",
					"destAssetIssuer": "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5",
					"destAssetType": "AssetTypeAssetTypeCreditAlphanum4",
					"fromAddress": "GCXBGEYNIEIUJ56YX5UVBM27NTKCBMLDD2NEPTTXZGQMBA2EOKG5VA2W",
					"memo": null,
					"memoType": "MemoTypeMemoNone",
					"operationId": "3",
					"operationType": "OperationTypePathPaymentStrictSend",
					"srcAmount": 300,
					"srcAssetCode": "XLM",
					"srcAssetIssuer": "",
					"srcAssetType": "AssetTypeAssetTypeNative",
					"toAddress": "GAX6VPTVC2YNJM52OYMJAZKTQMSLNQ6NKYYU77KSGRVHINZ2D3EUJWAN",
					"transactionHash": "d9521ed7057d4d1e9b9dd22ab515cbf1",
					"transactionId": "33"
				},
				{
					"createdAt": "2024-06-22T00:00:00Z",
					"destAmount": 20,
					"destAssetCode": "USDC",
					"destAssetIssuer": "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5",
					"destAssetType": "AssetTypeAssetTypeCreditAlphanum4",
					"fromAddress": "GASP7HTICNNA2U5RKMPRQELEUJFO7PBB3AKKRGTAG23QVG255ESPZW2L",
					"memo": "123",
					"memoType": "MemoTypeMemoId",
					"operationId": "2",
					"operationType": "OperationTypePayment",
					"srcAmount": 20,
					"srcAssetCode": "USDC",
					"srcAssetIssuer": "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5",
					"srcAssetType": "AssetTypeAssetTypeCreditAlphanum4",
					"toAddress": "GDB4RW6QFWMGHGI6JTIKMGVUUQO7NNOLSFDMCOMUCCWHMAMFL3FH4Q2J",
					"transactionHash": "30850d8fc7d1439782885103390cd975",
					"transactionId": "22"
				},
				{
					"createdAt": "2024-06-21T00:00:00Z",
					"destAmount": 10,
					"destAssetCode": "XLM",
					"destAssetIssuer": "",
					"destAssetType": "AssetTypeAssetTypeNative",
					"fromAddress": "GD73EG2IJJQQTCD33JKPKEGS76CJJ4TQ7NHDQYMS4D3Z5FBHPML6M66W",
					"memo": "test",
					"memoType": "MemoTypeMemoText",
					"operationId": "1",
					"operationType": "OperationTypePayment",
					"srcAmount": 10,
					"srcAssetCode": "XLM",
					"srcAssetIssuer": "",
					"srcAssetType": "AssetTypeAssetTypeNative",
					"toAddress": "GCJ4LXZIQRSS5Z7YVIH5YLA7RXMYB64DQN3XMKWEBHUUAFXIXOL3GYVT",
					"transactionHash": "c370ff20144e4c96b17432b8d14664c1",
					"transactionId": "11"
				}
			]
		}`
		assert.JSONEq(t, expectedRespBody, string(respBody))
		mockMetricsService.AssertExpectations(t)
	})

	t.Run("filter_address", func(t *testing.T) {
		handler, mockMetricsService := setupTest()
		r := setupRouter(handler)

		mockMetricsService.On("IncDBQuery", "SELECT", "ingest_payments").Return().Times(2)
		mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "ingest_payments", mock.Anything).Return().Times(2)

		// Prepare request
		req, err := http.NewRequest(http.MethodGet, "/payments?address=GASP7HTICNNA2U5RKMPRQELEUJFO7PBB3AKKRGTAG23QVG255ESPZW2L", nil)
		require.NoError(t, err)

		// Serve request
		rr := httptest.NewRecorder()
		r.ServeHTTP(rr, req)

		// Assert 200 response
		assert.Equal(t, http.StatusOK, rr.Code)

		resp := rr.Result()
		respBody, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		expectedRespBody := `{
			"_links": {
				"next": "",
				"prev": "",
				"self": "http://testing.com?address=GASP7HTICNNA2U5RKMPRQELEUJFO7PBB3AKKRGTAG23QVG255ESPZW2L&limit=50&sort=DESC"
			},
			"payments": [
				{
					"createdAt": "2024-06-22T00:00:00Z",
					"destAmount": 20,
					"destAssetCode": "USDC",
					"destAssetIssuer": "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5",
					"destAssetType": "AssetTypeAssetTypeCreditAlphanum4",
					"fromAddress": "GASP7HTICNNA2U5RKMPRQELEUJFO7PBB3AKKRGTAG23QVG255ESPZW2L",
					"memo": "123",
					"memoType": "MemoTypeMemoId",
					"operationId": "2",
					"operationType": "OperationTypePayment",
					"srcAmount": 20,
					"srcAssetCode": "USDC",
					"srcAssetIssuer": "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5",
					"srcAssetType": "AssetTypeAssetTypeCreditAlphanum4",
					"toAddress": "GDB4RW6QFWMGHGI6JTIKMGVUUQO7NNOLSFDMCOMUCCWHMAMFL3FH4Q2J",
					"transactionHash": "30850d8fc7d1439782885103390cd975",
					"transactionId": "22"
				}
			]
		}`
		assert.JSONEq(t, expectedRespBody, string(respBody))
		mockMetricsService.AssertExpectations(t)
	})

	t.Run("invalid_params_1", func(t *testing.T) {
		handler, mockMetricsService := setupTest()
		r := setupRouter(handler)

		// Prepare request
		req, err := http.NewRequest(http.MethodGet, "/payments?address=12345&limit=0&sort=BS", nil)
		require.NoError(t, err)

		// Serve request
		rr := httptest.NewRecorder()
		r.ServeHTTP(rr, req)

		// Assert 400 response
		assert.Equal(t, http.StatusBadRequest, rr.Code)

		resp := rr.Result()
		respBody, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		expectedRespBody := `{
			"error": "Validation error.",
			"extras": {
				"limit": "Should be greater than 0",
				"address": "Invalid public key provided",
				"sort": "Unexpected value \"BS\". Expected one of the following values: ASC, DESC"
			}
		}`
		assert.JSONEq(t, expectedRespBody, string(respBody))
		mockMetricsService.AssertExpectations(t)
	})

	t.Run("invalid_params_2", func(t *testing.T) {
		handler, mockMetricsService := setupTest()
		r := setupRouter(handler)

		// Prepare request
		req, err := http.NewRequest(http.MethodGet, "/payments?limit=210", nil)
		require.NoError(t, err)

		// Serve request
		rr := httptest.NewRecorder()
		r.ServeHTTP(rr, req)

		// Assert 400 response
		assert.Equal(t, http.StatusBadRequest, rr.Code)

		resp := rr.Result()
		respBody, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		expectedRespBody := `{
			"error": "Validation error.",
			"extras": {
				"limit": "Should be less than or equal to 200"
			}
		}`
		assert.JSONEq(t, expectedRespBody, string(respBody))
		mockMetricsService.AssertExpectations(t)
	})
}
