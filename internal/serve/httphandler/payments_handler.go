package httphandler

import (
	"net/http"

	"github.com/stellar/go/support/render/httpjson"
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

type PaymentsRequest struct {
	Address string         `query:"address" validate:"public_key"`
	AfterID int64          `query:"afterId"`
	Sort    data.SortOrder `query:"sort" validate:"oneof=ASC DESC"`
	Limit   int            `query:"limit" validate:"gt=0"`
}

type PaymentsResponse struct {
	Payments []data.Payment `json:"payments"`
}

func (h PaymentsHandler) GetPayments(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	reqQuery := PaymentsRequest{Sort: data.DESC, Limit: 50}
	httpErr := DecodeQueryAndValidate(ctx, r, &reqQuery)
	if httpErr != nil {
		httpErr.Render(w)
		return
	}

	payments, err := h.Models.Payments.GetPayments(ctx, reqQuery.Address, reqQuery.AfterID, reqQuery.Sort, reqQuery.Limit)
	if err != nil {
		httperror.InternalServerError(ctx, "", err, nil).Render(w)
		return
	}

	httpjson.Render(w, PaymentsResponse{
		Payments: payments,
	}, httpjson.JSON)
}
