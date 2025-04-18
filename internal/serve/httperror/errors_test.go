package httperror

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/apptracker"
)

func TestErrorResponseRender(t *testing.T) {
	appTrackerMock := apptracker.MockAppTracker{}
	appTrackerMock.On("CaptureException", errors.New("error"))
	defer appTrackerMock.AssertExpectations(t)
	testCases := []struct {
		in                   ErrorResponse
		want                 ErrorResponse
		expectedResponseBody string
	}{
		{
			in:                   *InternalServerError(context.Background(), "", errors.New("error"), nil, &appTrackerMock),
			want:                 ErrorResponse{Status: http.StatusInternalServerError, Error: "An error occurred while processing this request."},
			expectedResponseBody: `{"error": "An error occurred while processing this request."}`,
		},
		{
			in:                   NotFound,
			want:                 ErrorResponse{Status: http.StatusNotFound, Error: "The resource at the url requested was not found."},
			expectedResponseBody: `{"error": "The resource at the url requested was not found."}`,
		},
		{
			in:                   MethodNotAllowed,
			want:                 ErrorResponse{Status: http.StatusMethodNotAllowed, Error: "The method is not allowed for resource at the url requested."},
			expectedResponseBody: `{"error": "The method is not allowed for resource at the url requested."}`,
		},
		{
			in:                   *BadRequest("Validation error.", map[string]interface{}{"field": "field error"}),
			want:                 ErrorResponse{Status: http.StatusBadRequest, Error: "Validation error."},
			expectedResponseBody: `{"error": "Validation error.", "extras": {"field": "field error"}}`,
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			w := httptest.NewRecorder()
			tc.in.Render(w)
			resp := w.Result()
			assert.Equal(t, tc.want.Status, resp.StatusCode)
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			assert.JSONEq(t, tc.expectedResponseBody, string(body))
		})
	}
}

func TestErrorHandler(t *testing.T) {
	appTrackerMock := apptracker.MockAppTracker{}
	appTrackerMock.On("CaptureException", errors.New("error"))
	defer appTrackerMock.AssertExpectations(t)
	testCases := []struct {
		in   ErrorHandler
		want ErrorResponse
	}{
		{
			in:   ErrorHandler{*InternalServerError(context.Background(), "", errors.New("error"), nil, &appTrackerMock)},
			want: ErrorResponse{Status: http.StatusInternalServerError, Error: "An error occurred while processing this request."},
		},
		{
			in:   ErrorHandler{NotFound},
			want: ErrorResponse{Status: http.StatusNotFound, Error: "The resource at the url requested was not found."},
		},
		{
			in:   ErrorHandler{MethodNotAllowed},
			want: ErrorResponse{Status: http.StatusMethodNotAllowed, Error: "The method is not allowed for resource at the url requested."},
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			r := httptest.NewRequest("GET", "/", nil)
			w := httptest.NewRecorder()
			tc.in.ServeHTTP(w, r)
			resp := w.Result()
			assert.Equal(t, tc.want.Status, resp.StatusCode)
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			assert.JSONEq(t, fmt.Sprintf(`{"error":%q}`, tc.want.Error), string(body))
		})
	}
}
