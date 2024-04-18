package serve

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

type errorHandler struct {
	Error errorResponse
}

func (h errorHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.Error.Render(w)
}

var serverError = errorResponse{
	Status: http.StatusInternalServerError,
	Error:  "An error occurred while processing this request.",
}

var notFound = errorResponse{
	Status: http.StatusNotFound,
	Error:  "The resource at the url requested was not found.",
}

var methodNotAllowed = errorResponse{
	Status: http.StatusMethodNotAllowed,
	Error:  "The method is not allowed for resource at the url requested.",
}
