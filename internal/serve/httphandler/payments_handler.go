package httphandler

import (
	"net/http"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/serve/httperror"
)

type PaymentsHandler struct {
	*data.PaymentModel
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

	err := h.PaymentModel.SubscribeAddress(ctx, reqBody.Address)
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

	err := h.PaymentModel.UnsubscribeAddress(ctx, reqBody.Address)
	if err != nil {
		httperror.InternalServerError(ctx, "", err, nil).Render(w)
		return
	}
}
