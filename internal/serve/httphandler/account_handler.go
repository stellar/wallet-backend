package httphandler

import (
	"errors"
	"net/http"

	"github.com/stellar/go/support/render/httpjson"
	"github.com/stellar/go/txnbuild"

	"github.com/stellar/wallet-backend/internal/apptracker"
	"github.com/stellar/wallet-backend/internal/serve/httperror"
	"github.com/stellar/wallet-backend/internal/services"
	"github.com/stellar/wallet-backend/pkg/wbclient/types"
)

type AccountHandler struct {
	FeeBumpService services.FeeBumpService
	AppTracker     apptracker.AppTracker
}

func (h AccountHandler) CreateFeeBumpTransaction(rw http.ResponseWriter, req *http.Request) {
	ctx := req.Context()

	var reqBody types.CreateFeeBumpTransactionRequest
	httpErr := DecodeJSONAndValidate(ctx, req, &reqBody, h.AppTracker)
	if httpErr != nil {
		httpErr.Render(rw)
		return
	}

	genericTx, err := txnbuild.TransactionFromXDR(reqBody.Transaction)
	if err != nil {
		httperror.BadRequest("Could not parse transaction envelope.", nil).Render(rw)
		return
	}

	tx, ok := genericTx.Transaction()
	if !ok {
		httperror.BadRequest("Cannot accept a fee-bump transaction.", nil).Render(rw)
		return
	}

	feeBumpTxe, networkPassphrase, err := h.FeeBumpService.WrapTransaction(ctx, tx)
	if err != nil {
		var opNotAllowedErr *services.OperationNotAllowedError
		switch {
		case errors.Is(err, services.ErrAccountNotEligibleForBeingSponsored), errors.Is(err, services.ErrFeeExceedsMaximumBaseFee),
			errors.Is(err, services.ErrNoSignaturesProvided), errors.As(err, &opNotAllowedErr):
			httperror.BadRequest(err.Error(), nil).Render(rw)
			return
		default:
			httperror.InternalServerError(ctx, "", err, nil, h.AppTracker).Render(rw)
			return
		}
	}

	respBody := types.TransactionEnvelopeResponse{
		Transaction:       feeBumpTxe,
		NetworkPassphrase: networkPassphrase,
	}
	httpjson.Render(rw, respBody, httpjson.JSON)
}
