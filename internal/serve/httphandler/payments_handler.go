package httphandler

import (
	"net/http"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/serve/httperror"
)

type PaymentsHandler struct {
	Models *data.Models
}

type PaymentsSubscribeRequest struct {
	Address string `json:"address" validate:"required,public_key"`
}

func (h PaymentsHandler) SubscribeAddress(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var reqBody PaymentsSubscribeRequest
	httpErr := DecodeJSONAndValidate(ctx, r, &reqBody)
	if httpErr != nil {
		httpErr.Render(w)
		return
	}

	err := h.Models.Account.Insert(ctx, reqBody.Address)
	if err != nil {
		httperror.InternalServerError(ctx, "", err, nil).Render(w)
		return
	}
}

func (h PaymentsHandler) UnsubscribeAddress(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var reqBody PaymentsSubscribeRequest
	httpErr := DecodeJSONAndValidate(ctx, r, &reqBody)
	if httpErr != nil {
		httpErr.Render(w)
		return
	}

	err := h.Models.Account.Delete(ctx, reqBody.Address)
	if err != nil {
		httperror.InternalServerError(ctx, "", err, nil).Render(w)
		return
	}
}
