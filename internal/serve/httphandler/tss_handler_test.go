package httphandler

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path"
	"strings"
	"testing"

	"github.com/go-chi/chi"
	xdr3 "github.com/stellar/go-xdr/xdr3"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/wallet-backend/internal/apptracker"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/tss"
	"github.com/stellar/wallet-backend/internal/tss/router"
	tssservices "github.com/stellar/wallet-backend/internal/tss/services"
	"github.com/stellar/wallet-backend/internal/tss/store"
	"github.com/stellar/wallet-backend/internal/tss/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestBuildTransactions(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()
	store, _ := store.NewStore(dbConnectionPool)
	mockRouter := router.MockRouter{}
	mockAppTracker := apptracker.MockAppTracker{}
	mockTxService := tssservices.TransactionServiceMock{}

	handler := &TSSHandler{
		Router:             &mockRouter,
		Store:              store,
		AppTracker:         &mockAppTracker,
		NetworkPassphrase:  "testnet passphrase",
		TransactionService: &mockTxService,
	}

	srcAccount := keypair.MustRandom().Address()
	p := txnbuild.Payment{
		Destination:   keypair.MustRandom().Address(),
		Amount:        "10",
		Asset:         txnbuild.NativeAsset{},
		SourceAccount: srcAccount,
	}
	op, _ := p.BuildXDR()

	var buf strings.Builder
	enc := xdr3.NewEncoder(&buf)
	_ = op.EncodeTo(enc)

	opXDR := buf.String()
	opXDRBase64 := base64.StdEncoding.EncodeToString([]byte(opXDR))

	const endpoint = "/tss/transactions"

	t.Run("tx_signing_fails", func(t *testing.T) {
		reqBody := fmt.Sprintf(`{
			"transactions": [{"operations": [%q], "timeout": 100}]
		}`, opXDRBase64)
		rw := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, endpoint, strings.NewReader(reqBody))

		expectedOps, _ := utils.BuildOperations([]string{opXDRBase64})

		err := errors.New("unable to find channel account")
		mockTxService.
			On("BuildAndSignTransactionWithChannelAccount", context.Background(), expectedOps, int64(100)).
			Return(nil, err).
			Once()

		mockAppTracker.
			On("CaptureException", err).
			Return().
			Once()

		http.HandlerFunc(handler.BuildTransactions).ServeHTTP(rw, req)
		resp := rw.Result()
		respBody, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		expectedRespBody := `{"error": "An error occurred while processing this request."}`
		assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
		assert.JSONEq(t, expectedRespBody, string(respBody))
	})

	t.Run("happy_path", func(t *testing.T) {
		reqBody := fmt.Sprintf(`{
			"transactions": [{"operations": [%q], "timeout": 100}]
		}`, opXDRBase64)
		rw := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, endpoint, strings.NewReader(reqBody))

		expectedOps, _ := utils.BuildOperations([]string{opXDRBase64})
		tx := utils.BuildTestTransaction()

		mockTxService.
			On("BuildAndSignTransactionWithChannelAccount", context.Background(), expectedOps, int64(100)).
			Return(tx, nil).
			Once()

		http.HandlerFunc(handler.BuildTransactions).ServeHTTP(rw, req)
		resp := rw.Result()
		respBody, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		var buildTxResp BuildTransactionsResponse
		_ = json.Unmarshal(respBody, &buildTxResp)
		expectedTxXDR, _ := tx.Base64()
		assert.Equal(t, expectedTxXDR, buildTxResp.TransactionXDRs[0])
	})

}

func TestSubmitTransactions(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()
	store, _ := store.NewStore(dbConnectionPool)
	mockRouter := router.MockRouter{}
	mockAppTracker := apptracker.MockAppTracker{}
	txServiceMock := tssservices.TransactionServiceMock{}

	handler := &TSSHandler{
		Router:             &mockRouter,
		Store:              store,
		AppTracker:         &mockAppTracker,
		NetworkPassphrase:  "testnet passphrase",
		TransactionService: &txServiceMock,
	}

	const endpoint = "/tss/transactions"

	t.Run("invalid_request_bodies", func(t *testing.T) {
		reqBody := `{}`
		rw := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, endpoint, strings.NewReader(reqBody))

		http.HandlerFunc(handler.SubmitTransactions).ServeHTTP(rw, req)
		resp := rw.Result()
		respBody, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		expectedRespBody := `
		{
			"error": "Validation error.",
			"extras": {
				"transactions": "This field is required",
				"webhookURL": "This field is required"
			}
		}`

		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
		assert.JSONEq(t, expectedRespBody, string(respBody))

		reqBody = fmt.Sprintf(`{
					"webhook": "localhost:8080",
					"transactions": [%q]
				}`, "ABCD")
		rw = httptest.NewRecorder()
		req = httptest.NewRequest(http.MethodPost, endpoint, strings.NewReader(reqBody))

		http.HandlerFunc(handler.SubmitTransactions).ServeHTTP(rw, req)

		resp = rw.Result()
		respBody, err = io.ReadAll(resp.Body)
		require.NoError(t, err)

		expectedRespBody = `{"error": "bad transaction xdr"}`
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
		assert.JSONEq(t, expectedRespBody, string(respBody))

	})

	t.Run("happy_path", func(t *testing.T) {
		tx := utils.BuildTestTransaction()
		txXDR, _ := tx.Base64()
		reqBody := fmt.Sprintf(`{
			"webhook": "localhost:8080",
			"transactions": [%q]
		}`, txXDR)

		rw := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, endpoint, strings.NewReader(reqBody))

		mockRouter.
			On("Route", mock.Anything).
			Return(nil).
			Once()

		http.HandlerFunc(handler.SubmitTransactions).ServeHTTP(rw, req)
		resp := rw.Result()
		respBody, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		var txSubmissionResp TransactionSubmissionResponse
		_ = json.Unmarshal(respBody, &txSubmissionResp)

		assert.Equal(t, 1, len(txSubmissionResp.TransactionHashes))

		mockRouter.AssertNumberOfCalls(t, "Route", 1)
	})
}

func TestGetTransaction(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()
	store, _ := store.NewStore(dbConnectionPool)
	mockRouter := router.MockRouter{}
	mockAppTracker := apptracker.MockAppTracker{}
	txServiceMock := tssservices.TransactionServiceMock{}

	handler := &TSSHandler{
		Router:             &mockRouter,
		Store:              store,
		AppTracker:         &mockAppTracker,
		NetworkPassphrase:  "testnet passphrase",
		TransactionService: &txServiceMock,
	}

	endpoint := "/tss/transactions"

	r := chi.NewRouter()
	r.Route(endpoint, func(r chi.Router) {
		r.Get("/{transactionhash}", handler.GetTransaction)
	})

	clearTransactions := func(ctx context.Context) {
		_, err = dbConnectionPool.ExecContext(ctx, "TRUNCATE tss_transactions")
		require.NoError(t, err)
	}

	t.Run("returns_transaction", func(t *testing.T) {
		txHash := "hash"
		ctx := context.Background()
		_ = store.UpsertTransaction(ctx, "localhost:8080/webhook", txHash, "xdr", tss.RPCTXStatus{OtherStatus: tss.NewStatus})
		req, err := http.NewRequest(http.MethodGet, path.Join(endpoint, txHash), nil)
		require.NoError(t, err)

		// Serve request
		rw := httptest.NewRecorder()
		r.ServeHTTP(rw, req)
		resp := rw.Result()
		respBody, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		var getTxResp GetTransactionResponse
		_ = json.Unmarshal(respBody, &getTxResp)

		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Equal(t, "hash", getTxResp.Hash)
		assert.Equal(t, "xdr", getTxResp.XDR)
		assert.Equal(t, "NEW", getTxResp.Status)

		clearTransactions(ctx)
	})

	t.Run("return_empty_transaction", func(t *testing.T) {
		txHash := "hash"
		req, err := http.NewRequest(http.MethodGet, path.Join(endpoint, txHash), nil)
		require.NoError(t, err)

		// Serve request
		rw := httptest.NewRecorder()
		r.ServeHTTP(rw, req)
		resp := rw.Result()
		respBody, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		var getTxResp GetTransactionResponse
		_ = json.Unmarshal(respBody, &getTxResp)

		assert.Equal(t, http.StatusNotFound, resp.StatusCode)
		assert.Empty(t, getTxResp.Hash)
		assert.Empty(t, getTxResp.XDR)
		assert.Empty(t, getTxResp.Status)

	})

}
