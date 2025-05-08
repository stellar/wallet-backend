package middleware

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi"
	"github.com/golang-jwt/jwt/v4"
	"github.com/stellar/go/support/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/apptracker"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/pkg/wbclient/auth"
)

func TestAuthenticationMiddleware(t *testing.T) {
	testCases := []struct {
		name            string
		setupRequest    func() *http.Request
		setupMocks      func(t *testing.T, mJWTokenParser *auth.MockJWTTokenParser, mAppTracker *apptracker.MockAppTracker, mMetricsService *metrics.MockMetricsService)
		expectedStatus  int
		expectedMessage string
	}{
		{
			name: "🔴missing_authorization_header",
			setupRequest: func() *http.Request {
				req, err := http.NewRequest("GET", "https://test.com/authenticated", nil)
				require.NoError(t, err)
				return req
			},
			expectedStatus:  http.StatusUnauthorized,
			expectedMessage: `{"error":"Not authorized."}`,
		},
		{
			name: "🔴missing_Bearer_prefix",
			setupRequest: func() *http.Request {
				req, err := http.NewRequest("GET", "https://test.com/authenticated", nil)
				require.NoError(t, err)
				req.Header.Set("Authorization", "token")
				return req
			},
			expectedStatus:  http.StatusUnauthorized,
			expectedMessage: `{"error":"Not authorized."}`,
		},
		{
			name: "🔴invalid_token",
			setupRequest: func() *http.Request {
				req, err := http.NewRequest("GET", "https://test.com/authenticated", nil)
				require.NoError(t, err)
				req.Header.Set("Authorization", "Bearer invalid-token")
				return req
			},
			setupMocks: func(t *testing.T, mJWTokenParser *auth.MockJWTTokenParser, mAppTracker *apptracker.MockAppTracker, mMetricsService *metrics.MockMetricsService) {
				mJWTokenParser.EXPECT().
					ParseJWT("invalid-token", "test.com", []byte(nil)).
					Return(nil, nil, errors.New("invalid token")).
					Once()
			},
			expectedStatus:  http.StatusUnauthorized,
			expectedMessage: `{"error":"Not authorized."}`,
		},
		{
			name: "🔴expired_token",
			setupRequest: func() *http.Request {
				req, err := http.NewRequest("GET", "https://test.com/authenticated", nil)
				require.NoError(t, err)
				req.Header.Set("Authorization", "Bearer invalid-token")
				return req
			},
			setupMocks: func(t *testing.T, mJWTokenParser *auth.MockJWTTokenParser, mAppTracker *apptracker.MockAppTracker, mMetricsService *metrics.MockMetricsService) {
				mJWTokenParser.EXPECT().
					ParseJWT("invalid-token", "test.com", []byte(nil)).
					Return(nil, nil, &jwt.ValidationError{
						Errors: jwt.ValidationErrorExpired,
						Inner:  errors.New("token is expired by 1s"),
					}).
					Once()
				mMetricsService.
					On("IncSignatureVerificationExpired", 1.0).
					Once()
			},
			expectedStatus:  http.StatusUnauthorized,
			expectedMessage: `{"error":"Not authorized."}`,
		},
		{
			name: "🔴invalid_hostname",
			setupRequest: func() *http.Request {
				req, err := http.NewRequest("GET", "https://invalid.test.com/authenticated", nil)
				require.NoError(t, err)
				req.Header.Set("Authorization", "Bearer valid-token")
				return req
			},
			setupMocks: func(t *testing.T, mJWTokenParser *auth.MockJWTTokenParser, mAppTracker *apptracker.MockAppTracker, mMetricsService *metrics.MockMetricsService) {
				mJWTokenParser.EXPECT().
					ParseJWT("valid-token", "test.com", []byte(nil)).
					Return(nil, nil, errors.New("the token audience [invalid.test.com] does not match the expected audience [test.com]")).
					Once()
			},
			expectedStatus:  http.StatusUnauthorized,
			expectedMessage: `{"error":"Not authorized."}`,
		},
		{
			name: "🟢valid_token_with_body",
			setupRequest: func() *http.Request {
				req, err := http.NewRequest("GET", "https://test.com/authenticated", io.NopCloser(bytes.NewReader([]byte(`{"foo": "bar"}`))))
				require.NoError(t, err)
				req.Header.Set("Authorization", "Bearer valid-token")
				return req
			},
			setupMocks: func(t *testing.T, mJWTokenParser *auth.MockJWTTokenParser, mAppTracker *apptracker.MockAppTracker, mMetricsService *metrics.MockMetricsService) {
				mJWTokenParser.EXPECT().
					ParseJWT("valid-token", "test.com", []byte(`{"foo": "bar"}`)).
					Return(nil, nil, nil).
					Once()
			},
			expectedStatus:  http.StatusOK,
			expectedMessage: `{"status":"ok"}`,
		},
		{
			name: "🟢valid_token_without_body",
			setupRequest: func() *http.Request {
				req, err := http.NewRequest("GET", "https://test.com/authenticated", nil)
				require.NoError(t, err)
				req.Header.Set("Authorization", "Bearer valid-token")
				return req
			},
			setupMocks: func(t *testing.T, mJWTokenParser *auth.MockJWTTokenParser, mAppTracker *apptracker.MockAppTracker, mMetricsService *metrics.MockMetricsService) {
				mJWTokenParser.EXPECT().
					ParseJWT("valid-token", "test.com", []byte(nil)).
					Return(nil, nil, nil).
					Once()
			},
			expectedStatus:  http.StatusOK,
			expectedMessage: `{"status":"ok"}`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mJWTokenParser := auth.NewMockJWTTokenParser(t)
			mAppTracker := apptracker.NewMockAppTracker(t)
			mMetricsService := metrics.NewMockMetricsService()
			t.Cleanup(func() {
				mMetricsService.AssertExpectations(t)
			})
			if tc.setupMocks != nil {
				tc.setupMocks(t, mJWTokenParser, mAppTracker, mMetricsService)
			}
			authMiddleware := AuthenticationMiddleware("test.com", mJWTokenParser, mAppTracker, mMetricsService)

			r := chi.NewRouter()
			r.Get("/unauthenticated", func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				_, err := w.Write(json.RawMessage(`{"status":"ok"}`))
				require.NoError(t, err)
			})

			r.Group(func(r chi.Router) {
				r.Use(authMiddleware)

				r.Route("/authenticated", func(r chi.Router) {
					r.Get("/", func(w http.ResponseWriter, r *http.Request) {
						w.WriteHeader(http.StatusOK)
						_, err := w.Write(json.RawMessage(`{"status":"ok"}`))
						require.NoError(t, err)
					})

					r.Post("/", func(w http.ResponseWriter, r *http.Request) {
						w.WriteHeader(http.StatusOK)
						_, err := w.Write(json.RawMessage(`{"status":"ok"}`))
						require.NoError(t, err)
					})
				})
			})

			req := tc.setupRequest()
			rr := httptest.NewRecorder()
			r.ServeHTTP(rr, req)

			assert.Equal(t, tc.expectedStatus, rr.Code)
			assert.JSONEq(t, tc.expectedMessage, rr.Body.String())
		})
	}
}

func TestRecoverHandler(t *testing.T) {
	getEntries := log.DefaultLogger.StartTest(log.ErrorLevel)
	appTrackerMock := apptracker.MockAppTracker{}

	// setup
	r := chi.NewRouter()
	errString := "test panic"
	r.Use(RecoverHandler(&appTrackerMock))
	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		panic(errString)
	})

	// test
	req, err := http.NewRequest("GET", "/", nil)
	require.NoError(t, err)
	rr := httptest.NewRecorder()
	appTrackerMock.On("CaptureException", errors.New("panic: "+errString))
	r.ServeHTTP(rr, req)

	// assert response
	assert.Equal(t, http.StatusInternalServerError, rr.Code)
	wantJSON := `{
		"error": "An error occurred while processing this request."
	}`
	assert.JSONEq(t, wantJSON, rr.Body.String())

	entries := getEntries()
	require.Len(t, entries, 2)
	assert.Contains(t, entries[0].Message, "panic: test panic", "should log the panic message")
}
