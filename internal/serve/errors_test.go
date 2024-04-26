package serve

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestErrorResponseRender(t *testing.T) {
	testCases := []struct {
		in   errorResponse
		want errorResponse
	}{
		{
			in:   serverError,
			want: errorResponse{Status: http.StatusInternalServerError, Error: "An error occurred while processing this request."},
		},
		{
			in:   notFound,
			want: errorResponse{Status: http.StatusNotFound, Error: "The resource at the url requested was not found."},
		},
		{
			in:   methodNotAllowed,
			want: errorResponse{Status: http.StatusMethodNotAllowed, Error: "The method is not allowed for resource at the url requested."},
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			w := httptest.NewRecorder()
			tc.in.Render(w)
			resp := w.Result()
			assert.Equal(t, tc.want.Status, resp.StatusCode)
			body, err := ioutil.ReadAll(resp.Body)
			require.NoError(t, err)
			assert.JSONEq(t, fmt.Sprintf(`{"error":%q}`, tc.want.Error), string(body))
		})
	}
}

func TestErrorHandler(t *testing.T) {
	testCases := []struct {
		in   errorHandler
		want errorResponse
	}{
		{
			in:   errorHandler{serverError},
			want: errorResponse{Status: http.StatusInternalServerError, Error: "An error occurred while processing this request."},
		},
		{
			in:   errorHandler{notFound},
			want: errorResponse{Status: http.StatusNotFound, Error: "The resource at the url requested was not found."},
		},
		{
			in:   errorHandler{methodNotAllowed},
			want: errorResponse{Status: http.StatusMethodNotAllowed, Error: "The method is not allowed for resource at the url requested."},
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			r := httptest.NewRequest("GET", "/", nil)
			w := httptest.NewRecorder()
			tc.in.ServeHTTP(w, r)
			resp := w.Result()
			assert.Equal(t, tc.want.Status, resp.StatusCode)
			body, err := ioutil.ReadAll(resp.Body)
			require.NoError(t, err)
			assert.JSONEq(t, fmt.Sprintf(`{"error":%q}`, tc.want.Error), string(body))
		})
	}
}
