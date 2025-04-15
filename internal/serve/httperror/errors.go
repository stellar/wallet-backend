package httperror

import (
	"context"
	"net/http"

	"github.com/stellar/go/support/log"
	"github.com/stellar/go/support/render/httpjson"

	"github.com/stellar/wallet-backend/internal/apptracker"
)

type ErrorResponse struct {
	Status int                    `json:"-"`
	Error  string                 `json:"error"`
	Extras map[string]interface{} `json:"extras,omitempty"`
}

func (e ErrorResponse) Render(w http.ResponseWriter) {
	httpjson.RenderStatus(w, e.Status, e, httpjson.JSON)
}

type ErrorHandler struct {
	Error ErrorResponse
}

func (h ErrorHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.Error.Render(w)
}

var NotFound = ErrorResponse{
	Status: http.StatusNotFound,
	Error:  "The resource at the url requested was not found.",
}

var MethodNotAllowed = ErrorResponse{
	Status: http.StatusMethodNotAllowed,
	Error:  "The method is not allowed for resource at the url requested.",
}

func BadRequest(message string, extras map[string]interface{}) *ErrorResponse {
	if message == "" {
		message = "Invalid request"
	}

	return &ErrorResponse{
		Status: http.StatusBadRequest,
		Error:  message,
		Extras: extras,
	}
}

func Unauthorized(message string, extras map[string]interface{}) *ErrorResponse {
	if message == "" {
		message = "Not authorized."
	}

	return &ErrorResponse{
		Status: http.StatusUnauthorized,
		Error:  message,
		Extras: extras,
	}
}

func InternalServerError(ctx context.Context, message string, err error, extras map[string]interface{}, appTracker apptracker.AppTracker) *ErrorResponse {
	log.Ctx(ctx).Error(err)
	if appTracker != nil {
		appTracker.CaptureException(err)
	} else {
		log.Warn("App Tracker is nil")
	}

	return &ErrorResponse{
		Status: http.StatusInternalServerError,
		Error:  "An error occurred while processing this request.",
		Extras: extras,
	}
}
