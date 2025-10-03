package middleware

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-chi/chi"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/support/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/apptracker"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/pkg/wbclient/auth"
)

func TestAuthenticationMiddleware(t *testing.T) {
	// GCT4SHMV6WIRE7G3RNHOMG5XTFOAVH4HKLGXDRASQDDNTYLSROUATLWN
	kpValid := keypair.MustParseFull("SCPXLP2FDRNQXBFOXVELL2WOZRIY6JD65X7XFNRJZYP254DYBANSCKD5")

	// GDQ2UMOTKA4RHL4ZH7QPOKPVF4DI3EATLL3GBNKULDH4ERXJ32E252VH
	kpInvalid := keypair.MustParseFull("SDDAY7FIDYF4ROE6X2CJV5KHXBOF4R6QFLD4QT6DZ52UUJNUO6NECF3K")

	jwtParser, err := auth.NewJWTTokenParser(10*time.Second, kpValid.Address())
	require.NoError(t, err)
	reqJWTVerifier := auth.NewHTTPRequestVerifier(jwtParser, auth.DefaultMaxBodySizeBytes)

	validJWTGenerator, err := auth.NewJWTTokenGenerator(kpValid.Seed())
	require.NoError(t, err)
	validSigner := auth.NewHTTPRequestSigner(validJWTGenerator)
	invalidJWTGenerator, err := auth.NewJWTTokenGenerator(kpInvalid.Seed())
	require.NoError(t, err)
	invalidSigner := auth.NewHTTPRequestSigner(invalidJWTGenerator)

	testCases := []struct {
		name            string
		setupRequest    func() *http.Request
		setupMocks      func(t *testing.T, mAppTracker *apptracker.MockAppTracker, mMetricsService *metrics.MockMetricsService)
		expectedStatus  int
		expectedMessage string
	}{
		{
			name: "🔴missing_authorization_header",
			setupRequest: func() *http.Request {
				req := httptest.NewRequest("GET", "https://test.com/authenticated", nil)
				return req
			},
			expectedStatus:  http.StatusUnauthorized,
			expectedMessage: `{"error":"Not authorized."}`,
		},
		{
			name: "🔴missing_Bearer_prefix",
			setupRequest: func() *http.Request {
				req := httptest.NewRequest("GET", "https://test.com/authenticated", nil)
				req.Header.Set("Authorization", "token")
				return req
			},
			expectedStatus:  http.StatusUnauthorized,
			expectedMessage: `{"error":"Not authorized."}`,
		},
		{
			name: "🔴invalid_token",
			setupRequest: func() *http.Request {
				req := httptest.NewRequest("GET", "https://test.com/authenticated", nil)
				req.Header.Set("Authorization", "Bearer invalid-token")
				return req
			},
			expectedStatus:  http.StatusUnauthorized,
			expectedMessage: `{"error":"Not authorized."}`,
		},
		{
			name: "🔴invalid_signer",
			setupRequest: func() *http.Request {
				req := httptest.NewRequest("GET", "https://test.com/authenticated", nil)
				err := invalidSigner.SignHTTPRequest(req, time.Second*5)
				require.NoError(t, err)
				return req
			},
			expectedStatus:  http.StatusUnauthorized,
			expectedMessage: `{"error":"Not authorized."}`,
		},
		{
			name: "🔴expired_token",
			setupRequest: func() *http.Request {
				req := httptest.NewRequest("GET", "https://test.com/authenticated", nil)
				err := validSigner.SignHTTPRequest(req, -time.Nanosecond*1)
				require.NoError(t, err)
				return req
			},
			setupMocks: func(t *testing.T, mAppTracker *apptracker.MockAppTracker, mMetricsService *metrics.MockMetricsService) {
				mMetricsService.
					On("IncSignatureVerificationExpired", mock.AnythingOfType("float64")).
					Once()
			},
			expectedStatus:  http.StatusUnauthorized,
			expectedMessage: `{"error":"Not authorized."}`,
		},
		{
			name: "🔴body_too_big",
			setupRequest: func() *http.Request {
				req := httptest.NewRequest("GET", "https://test.com/authenticated", bytes.NewBuffer(make([]byte, auth.DefaultMaxBodySizeBytes+1)))
				err := validSigner.SignHTTPRequest(req, time.Second*5)
				require.NoError(t, err)
				return req
			},
			expectedStatus:  http.StatusUnauthorized,
			expectedMessage: `{"error":"Not authorized."}`,
		},
		{
			name: "🟢valid_token_with_body",
			setupRequest: func() *http.Request {
				req := httptest.NewRequest("GET", "https://test.com/authenticated", io.NopCloser(bytes.NewReader([]byte(`{"foo": "bar"}`))))
				err := validSigner.SignHTTPRequest(req, time.Second*5)
				require.NoError(t, err)
				return req
			},
			expectedStatus:  http.StatusOK,
			expectedMessage: `{"status":"ok"}`,
		},
		{
			name: "🟢valid_token_without_body",
			setupRequest: func() *http.Request {
				req := httptest.NewRequest("GET", "https://test.com/authenticated", nil)
				err := validSigner.SignHTTPRequest(req, time.Second*5)
				require.NoError(t, err)
				return req
			},
			expectedStatus:  http.StatusOK,
			expectedMessage: `{"status":"ok"}`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mAppTracker := apptracker.NewMockAppTracker(t)
			mMetricsService := metrics.NewMockMetricsService()
			t.Cleanup(func() {
				mMetricsService.AssertExpectations(t)
			})
			if tc.setupMocks != nil {
				tc.setupMocks(t, mAppTracker, mMetricsService)
			}
			authMiddleware := AuthenticationMiddleware(reqJWTVerifier, mAppTracker, mMetricsService)

			r := chi.NewRouter()
			r.Use(authMiddleware)
			r.Get("/authenticated", func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				_, err := w.Write(json.RawMessage(`{"status":"ok"}`))
				require.NoError(t, err)
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
