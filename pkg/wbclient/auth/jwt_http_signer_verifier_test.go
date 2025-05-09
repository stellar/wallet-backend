package auth

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_JWTHTTPSignerVerifier_Integration(t *testing.T) {
	reqBodyTooBig := make([]byte, DefaultMaxBodySize*2)

	jwtParser, err := NewJWTTokenParser(10*time.Second, testKP1.Address())
	require.NoError(t, err)
	reqJWTVerifier := NewHTTPRequestVerifier(jwtParser, DefaultMaxBodySize)

	validJWTGenerator, err := NewJWTTokenGenerator(testKP1.Seed())
	require.NoError(t, err)
	validSigner := NewHTTPRequestSigner(validJWTGenerator)
	invalidJWTGenerator, err := NewJWTTokenGenerator(testKP2.Seed())
	require.NoError(t, err)
	invalidSigner := NewHTTPRequestSigner(invalidJWTGenerator)

	testCases := []struct {
		name            string
		setupRequest    func(t *testing.T) *http.Request
		expectedStatus  int
		wantErrContains string
	}{
		{
			name: "🔴missing_authorization_header",
			setupRequest: func(t *testing.T) *http.Request {
				return httptest.NewRequest("GET", "http://example.com/authenticated", nil)
			},
			expectedStatus:  http.StatusUnauthorized,
			wantErrContains: "missing Authorization header",
		},
		{
			name: "🔴invalid_authorization_header",
			setupRequest: func(t *testing.T) *http.Request {
				req := httptest.NewRequest("GET", "http://example.com/authenticated", nil)
				req.Header.Set("Authorization", "invalid-token")
				return req
			},
			expectedStatus:  http.StatusUnauthorized,
			wantErrContains: "the Authorization header is invalid, expected 'Bearer <token>'",
		},
		{
			name: "🔴invalid_bearer_token",
			setupRequest: func(t *testing.T) *http.Request {
				req := httptest.NewRequest("GET", "http://example.com/authenticated", nil)
				req.Header.Set("Authorization", "Bearer invalid-bearer")
				return req
			},
			expectedStatus:  http.StatusUnauthorized,
			wantErrContains: "verifying JWT",
		},
		{
			name: "🔴wrong_signer",
			setupRequest: func(t *testing.T) *http.Request {
				req := httptest.NewRequest("GET", "http://example.com/authenticated", nil)
				err := invalidSigner.SignHTTPRequest(req, time.Second*5)
				require.NoError(t, err)
				return req
			},
			expectedStatus:  http.StatusUnauthorized,
			wantErrContains: "verifying JWT",
		},
		{
			name: "🔴body_is_too_big",
			setupRequest: func(t *testing.T) *http.Request {
				req := httptest.NewRequest("GET", "http://example.com/authenticated", bytes.NewBuffer(reqBodyTooBig))
				err := validSigner.SignHTTPRequest(req, time.Second*5)
				require.NoError(t, err)
				return req
			},
			expectedStatus:  http.StatusUnauthorized,
			wantErrContains: "verifying JWT: pre-validating JWT token claims: the JWT hashed body does not match the expected value",
		},
		{
			name: "🟢valid_authenticated_request_no_body",
			setupRequest: func(t *testing.T) *http.Request {
				req := httptest.NewRequest("GET", "http://example.com/authenticated", nil)
				err := validSigner.SignHTTPRequest(req, time.Second*5)
				require.NoError(t, err)
				return req
			},
			expectedStatus:  http.StatusOK,
			wantErrContains: `{"message": "ok"}`,
		},
		{
			name: "🟢valid_authenticated_request_with_body_1",
			setupRequest: func(t *testing.T) *http.Request {
				req := httptest.NewRequest("GET", "http://example.com/authenticated", bytes.NewBuffer([]byte(`{"foo": "bar"}`)))
				err := validSigner.SignHTTPRequest(req, time.Second*5)
				require.NoError(t, err)
				return req
			},
			expectedStatus:  http.StatusOK,
			wantErrContains: `{"message": "ok"}`,
		},
		{
			name: "🟢valid_authenticated_request_with_body_2",
			setupRequest: func(t *testing.T) *http.Request {
				req := httptest.NewRequest("GET", "/authenticated", bytes.NewBuffer([]byte(`{"foo": "bar"}`)))
				err := validSigner.SignHTTPRequest(req, time.Second*5)
				require.NoError(t, err)
				return req
			},
			expectedStatus:  http.StatusOK,
			wantErrContains: `{"message": "ok"}`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req := tc.setupRequest(t)
			w := httptest.NewRecorder()

			ts := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				err := reqJWTVerifier.VerifyHTTPRequest(r, "example.com")
				if err != nil {
					http.Error(w, err.Error(), http.StatusUnauthorized)
					return
				}
				w.WriteHeader(http.StatusOK)
				_, err = w.Write([]byte(`{"message": "ok"}`))
				require.NoError(t, err)
			})

			ts.ServeHTTP(w, req)

			resp := w.Result()
			respBody, err := io.ReadAll(resp.Body)
			require.NoError(t, err)

			assert.Equal(t, tc.expectedStatus, resp.StatusCode)
			assert.Contains(t, string(respBody), tc.wantErrContains)
		})
	}
}
