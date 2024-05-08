package httperror

import (
	"net/http"

	"github.com/stellar/go/support/render/httpjson"
)

type errorResponse struct {
	Status int    `json:"-"`
	Error  string `json:"error"`
}

func (e errorResponse) Render(w http.ResponseWriter) {
	httpjson.RenderStatus(w, e.Status, e, httpjson.JSON)
}

type ErrorHandler struct {
	Error errorResponse
}

func (h ErrorHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.Error.Render(w)
}

var InternalServerError = errorResponse{
	Status: http.StatusInternalServerError,
	Error:  "An error occurred while processing this request.",
}

var NotFound = errorResponse{
	Status: http.StatusNotFound,
	Error:  "The resource at the url requested was not found.",
}

var MethodNotAllowed = errorResponse{
	Status: http.StatusMethodNotAllowed,
	Error:  "The method is not allowed for resource at the url requested.",
}

func BadRequest(message string) errorResponse {
	if message == "" {
		message = "Invalid request"
	}

	return errorResponse{
		Status: http.StatusBadRequest,
		Error:  message,
	}
}
