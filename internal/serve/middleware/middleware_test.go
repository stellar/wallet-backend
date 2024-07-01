package middleware

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi"
	"github.com/stellar/go/support/log"
	"github.com/stellar/wallet-backend/internal/serve/auth"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestSignatureMiddleware(t *testing.T) {
	signatureVerifierMock := auth.MockSignatureVerifier{}
	defer signatureVerifierMock.AssertExpectations(t)

	r := chi.NewRouter()
	r.Group(func(r chi.Router) {
		r.Use(SignatureMiddleware(&signatureVerifierMock))

		r.Get("/authenticated", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, err := w.Write(json.RawMessage(`{"status":"ok"}`))
			require.NoError(t, err)
		})

		r.Post("/authenticated", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, err := w.Write(json.RawMessage(`{"status":"ok"}`))
			require.NoError(t, err)
		})
	})

	r.Get("/unauthenticated", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, err := w.Write(json.RawMessage(`{"status":"ok"}`))
		require.NoError(t, err)
	})

	t.Run("returns_Unauthorized_error_when_no_header_is_sent", func(t *testing.T) {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/authenticated", nil)
		r.ServeHTTP(w, req)

		resp := w.Result()
		respBody, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
		assert.JSONEq(t, `{"error":"Not authorized."}`, string(respBody))
	})

	t.Run("returns_Unauthorized_when_a_unexpected_error_occurs_validating_the_token", func(t *testing.T) {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/authenticated", nil)
		req.Header.Set("Signature", "signature")

		getEntries := log.DefaultLogger.StartTest(log.ErrorLevel)
		signatureVerifierMock.
			On("VerifySignature", mock.Anything, "signature", []byte{}).
			Return(errors.New("unexpected error")).
			Once()

		r.ServeHTTP(w, req)

		resp := w.Result()
		respBody, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
		assert.JSONEq(t, `{"error": "Not authorized."}`, string(respBody))

		entries := getEntries()
		require.Len(t, entries, 1)
		assert.Equal(t, entries[0].Message, "checking request signature: unexpected error")
	})

	t.Run("returns_the_response_successfully", func(t *testing.T) {
		// Without body - GET requests
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/authenticated", nil)
		req.Header.Set("X-Stellar-Signature", "signature")

		signatureVerifierMock.
			On("VerifySignature", mock.Anything, "signature", []byte{}).
			Return(nil).
			Once()

		r.ServeHTTP(w, req)

		resp := w.Result()
		respBody, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.JSONEq(t, `{"status":"ok"}`, string(respBody))

		// With body - POST, PUT, PATCH requests
		reqBody := `{"status": "ok"}`
		w = httptest.NewRecorder()
		req = httptest.NewRequest(http.MethodPost, "/authenticated", strings.NewReader(reqBody))
		req.Header.Set("X-Stellar-Signature", "signature")

		signatureVerifierMock.
			On("VerifySignature", mock.Anything, "signature", []byte(reqBody)).
			Return(nil).
			Once()

		r.ServeHTTP(w, req)

		resp = w.Result()
		respBody, err = io.ReadAll(resp.Body)
		require.NoError(t, err)

		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.JSONEq(t, `{"status":"ok"}`, string(respBody))
	})

	t.Run("doesn't_return_Unauthorized_for_unauthenticated_routes", func(t *testing.T) {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/unauthenticated", nil)
		r.ServeHTTP(w, req)

		resp := w.Result()
		respBody, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.JSONEq(t, `{"status":"ok"}`, string(respBody))
	})
}

func TestRecoverHandler(t *testing.T) {
	getEntries := log.DefaultLogger.StartTest(log.ErrorLevel)

	// setup
	r := chi.NewRouter()
	r.Use(RecoverHandler)
	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		panic("test panic")
	})

	// test
	req, err := http.NewRequest("GET", "/", nil)
	require.NoError(t, err)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	// assert response
	assert.Equal(t, http.StatusInternalServerError, rr.Code)
	wantJson := `{
		"error": "An error occurred while processing this request."
	}`
	assert.JSONEq(t, wantJson, rr.Body.String())

	entries := getEntries()
	require.Len(t, entries, 2)
	assert.Contains(t, entries[0].Message, "panic: test panic", "should log the panic message")
}
