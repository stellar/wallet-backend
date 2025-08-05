package httphandler

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/stellar/wallet-backend/internal/utils"
)

type testCase struct {
	name            string
	requestBody     string
	contentType     string
	prepareMocks    func(t *testing.T, mHTTPClient *utils.MockHTTPClient)
	wantErrContains string
	wantStatusCode  int
	wantResponse    string
}

func TestRPCHandler_ForwardRPCRequest(t *testing.T) {
	testCases := []testCase{
		{
			name:        "🟢successful_proxy",
			requestBody: `{"jsonrpc":"2.0","id":2,"method":"getTransaction","params":{"hash":"abc123"}}`,
			contentType: "application/json",
			prepareMocks: func(t *testing.T, mHTTPClient *utils.MockHTTPClient) {
				mHTTPClient.On("Post", "http://localhost:8000", "application/json", mock.Anything).
					Return(&http.Response{
						StatusCode: http.StatusOK,
						Header:     http.Header{"Content-Type": []string{"application/json"}},
						Body:       io.NopCloser(strings.NewReader(`{"jsonrpc":"2.0","id":2,"result":{"hash":"abc123"}}`)),
					}, nil).Once()
			},
			wantStatusCode: http.StatusOK,
			wantResponse:   `{"jsonrpc":"2.0","id":2,"result":{"hash":"abc123"}}`,
		},
		{
			name:        "🔴http_client_error",
			requestBody: `{"jsonrpc":"2.0","id":1,"method":"getHealth"}`,
			contentType: "application/json",
			prepareMocks: func(t *testing.T, mHTTPClient *utils.MockHTTPClient) {
				mHTTPClient.On("Post", "http://localhost:8000", "application/json", mock.Anything).
					Return(nil, assert.AnError).Once()
			},
			wantErrContains: "Failed to forward request",
			wantStatusCode:  http.StatusInternalServerError,
		},
		{
			name:        "🟡rpc_error_response",
			requestBody: `{"jsonrpc":"2.0","id":1,"method":"getHealth"}`,
			contentType: "application/json",
			prepareMocks: func(t *testing.T, mHTTPClient *utils.MockHTTPClient) {
				mHTTPClient.On("Post", "http://localhost:8000", "application/json", mock.Anything).
					Return(&http.Response{
						StatusCode: http.StatusBadRequest,
						Header:     http.Header{"Content-Type": []string{"application/json"}},
						Body:       io.NopCloser(strings.NewReader(`{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"Method not found"}}`)),
					}, nil).Once()
			},
			wantStatusCode: http.StatusBadRequest,
			wantResponse:   `{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"Method not found"}}`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Fresh mocks for each test case
			mHTTPClient := &utils.MockHTTPClient{}

			// Prepare mocks
			tc.prepareMocks(t, mHTTPClient)

			// Create handler
			handler := &RPCHandler{
				RPCURL:     "http://localhost:8000",
				HTTPClient: mHTTPClient,
			}

			// Create request
			req := httptest.NewRequest(http.MethodPost, "/rpc", bytes.NewReader([]byte(tc.requestBody)))
			req.Header.Set("Content-Type", tc.contentType)
			w := httptest.NewRecorder()

			// Execute
			handler.ForwardRPCRequest(w, req)

			// Assert
			if tc.wantErrContains != "" {
				assert.Contains(t, w.Body.String(), tc.wantErrContains)
			} else {
				assert.Equal(t, tc.wantResponse, strings.TrimSpace(w.Body.String()))
			}

			assert.Equal(t, tc.wantStatusCode, w.Code)

			// Verify mocks
			mHTTPClient.AssertExpectations(t)
		})
	}
}
