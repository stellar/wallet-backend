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
	PaymentService services.PaymentService
	AppTracker     apptracker.AppTracker
}

type PaymentsRequest struct {
	Address  string         `query:"address" validate:"public_key"`
	AfterID  string         `query:"afterId"`
	BeforeID string         `query:"beforeId"`
	Sort     data.SortOrder `query:"sort" validate:"oneof=ASC DESC"`
	Limit    int            `query:"limit" validate:"gt=0,lte=200"`
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

	payments, pagination, err := h.PaymentService.GetPaymentsPaginated(ctx, reqQuery.Address, reqQuery.BeforeID, reqQuery.AfterID, reqQuery.Sort, reqQuery.Limit)
	if err != nil {
		httperror.InternalServerError(ctx, "", err, nil, h.AppTracker).Render(w)
		return
	}

	httpjson.Render(w, PaymentsResponse{
		Payments:   payments,
		Pagination: pagination,
	}, httpjson.JSON)
}
