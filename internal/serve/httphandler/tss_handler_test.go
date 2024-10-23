package httphandler

import (
	"context"
	"encoding/base64"
	"encoding/json"
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
	"github.com/stellar/wallet-backend/internal/tss/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestSubmitTransactions(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()
	store, _ := store.NewStore(dbConnectionPool)
	mockRouter := router.MockRouter{}
	mockAppTracker := apptracker.MockAppTracker{}

	handler := &TSSHandler{
		Router:            &mockRouter,
		Store:             store,
		AppTracker:        &mockAppTracker,
		NetworkPassphrase: "testnet passphrase",
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
				"transactions": [{"operations": [%q]}]
			}`, "ABCD")
		rw = httptest.NewRecorder()
		req = httptest.NewRequest(http.MethodPost, endpoint, strings.NewReader(reqBody))

		http.HandlerFunc(handler.SubmitTransactions).ServeHTTP(rw, req)

		resp = rw.Result()
		respBody, err = io.ReadAll(resp.Body)
		require.NoError(t, err)

		expectedRespBody = `{"error": "bad operation xdr"}`
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
		assert.JSONEq(t, expectedRespBody, string(respBody))

	})

	t.Run("happy_path", func(t *testing.T) {
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
		reqBody := fmt.Sprintf(`{
			"webhook": "localhost:8080",
			"transactions": [{"operations": [%q]}]
		}`, opXDRBase64)

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

	handler := &TSSHandler{
		Router:            &mockRouter,
		Store:             store,
		AppTracker:        &mockAppTracker,
		NetworkPassphrase: "testnet passphrase",
	}

	endpoint := "/tss/transactions"

	r := chi.NewRouter()
	//r.Get("/tss/{transactionhash}", handler.GetTransaction)
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

		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Empty(t, getTxResp.Hash)
		assert.Empty(t, getTxResp.XDR)
		assert.Empty(t, getTxResp.Status)

	})

}
