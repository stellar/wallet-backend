package httphandler

import (
	"net/http"

	"github.com/stellar/go/support/render/httpjson"
	"github.com/stellar/wallet-backend/internal/apptracker"
	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/serve/httperror"
	"github.com/stellar/wallet-backend/internal/services"
)

type PaymentHandler struct {
	Models     *data.Models
	Service    *services.PaymentService
	AppTracker apptracker.AppTracker
}

type PaymentsSubscribeRequest struct {
	Address string `json:"address" validate:"required,public_key"`
}

func (h PaymentHandler) SubscribeAddress(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var reqBody PaymentsSubscribeRequest
	httpErr := DecodeJSONAndValidate(ctx, r, &reqBody, h.AppTracker)
	if httpErr != nil {
		httpErr.Render(w)
		return
	}

	err := h.Models.Account.Insert(ctx, reqBody.Address)
	if err != nil {
		httperror.InternalServerError(ctx, "", err, nil, h.AppTracker).Render(w)
		return
	}
}

func (h PaymentHandler) UnsubscribeAddress(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var reqBody PaymentsSubscribeRequest
	httpErr := DecodeJSONAndValidate(ctx, r, &reqBody, h.AppTracker)
	if httpErr != nil {
		httpErr.Render(w)
		return
	}

	err := h.Models.Account.Delete(ctx, reqBody.Address)
	if err != nil {
		httperror.InternalServerError(ctx, "", err, nil, h.AppTracker).Render(w)
		return
	}
}

type PaymentsRequest struct {
	Address  string         `query:"address" validate:"public_key"`
	AfterID  string         `query:"afterId"`
	BeforeID string         `query:"beforeId"`
	Sort     data.SortOrder `query:"sort" validate:"oneof=ASC DESC"`
	Limit    int            `query:"limit" validate:"gt=0"`
}

type PaymentsResponse struct {
	Payments []data.Payment `json:"payments"`
	entities.Pagination
}

func (h PaymentHandler) GetPayments(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	reqQuery := PaymentsRequest{Sort: data.DESC, Limit: 50}
	httpErr := DecodeQueryAndValidate(ctx, r, &reqQuery, h.AppTracker)
	if httpErr != nil {
		httpErr.Render(w)
		return
	}

	payments, pagination, err := h.Service.GetPaymentsPaginated(ctx, reqQuery.Address, reqQuery.BeforeID, reqQuery.AfterID, reqQuery.Sort, reqQuery.Limit)
	if err != nil {
		httperror.InternalServerError(ctx, "", err, nil, h.AppTracker).Render(w)
		return
	}

	httpjson.Render(w, PaymentsResponse{
		Payments:   payments,
		Pagination: pagination,
	}, httpjson.JSON)
}
