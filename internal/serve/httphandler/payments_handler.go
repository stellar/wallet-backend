package httphandler

import (
	"net/http"
	"wallet-backend/internal/data"
	"wallet-backend/internal/serve/httperror"

	"github.com/stellar/go/support/http/httpdecode"
)

type PaymentsHandler struct {
	*data.Models
}

type PaymentsSubscribeRequest struct {
	Address string `json:"address"`
}

func (h PaymentsHandler) SubscribeAddress(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var reqBody PaymentsSubscribeRequest
	err := httpdecode.DecodeJSON(r, &reqBody)
	if err != nil {
		httperror.BadRequest("Invalid request body").Render(w)
		return
	}

	err = h.Models.Payments.SubscribeAddress(ctx, reqBody.Address)
	if err != nil {
		httperror.InternalServerError.Render(w)
		return
	}
}

func (h PaymentsHandler) UnsubscribeAddress(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var reqBody PaymentsSubscribeRequest
	err := httpdecode.DecodeJSON(r, &reqBody)
	if err != nil {
		httperror.BadRequest("Invalid request body").Render(w)
		return
	}

	err = h.Models.Payments.UnsubscribeAddress(ctx, reqBody.Address)
	if err != nil {
		httperror.InternalServerError.Render(w)
		return
	}
}
