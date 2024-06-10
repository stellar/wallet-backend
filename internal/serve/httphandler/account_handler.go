package httphandler

import (
	"errors"
	"net/http"

	"github.com/stellar/go/support/http/httpdecode"
	"github.com/stellar/go/support/render/httpjson"
	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/serve/httperror"
	"github.com/stellar/wallet-backend/internal/services"
)

type AccountHandler struct {
	AccountSponsorshipService services.AccountSponsorshipService
	SupportedAssets           []entities.Asset
}

type SponsorAccountCreationRequest struct {
	Address string            `json:"address" validate:"required,public_key"`
	Signers []entities.Signer `json:"signers" validate:"required,gt=0,dive"`
}

type TransactionEnvelopeResponse struct {
	Transaction       string `json:"transaction"`
	NetworkPassphrase string `json:"networkPassphrase"`
}

func (h AccountHandler) SponsorAccountCreation(rw http.ResponseWriter, req *http.Request) {
	ctx := req.Context()

	var reqBody SponsorAccountCreationRequest
	if err := httpdecode.DecodeJSON(req, &reqBody); err != nil {
		httperror.BadRequest("", nil).Render(rw)
		return
	}

	httpErr := ValidateRequestBody(ctx, reqBody)
	if httpErr != nil {
		httpErr.Render(rw)
		return
	}

	_, err := entities.ValidateSignersWeights(reqBody.Signers)
	if err != nil {
		httperror.BadRequest("Validation error.", map[string]interface{}{"signers": err.Error()}).Render(rw)
		return
	}

	// TODO: Store the sponsored account on the database.
	txe, networkPassphrase, err := h.AccountSponsorshipService.SponsorAccountCreationTransaction(ctx, reqBody.Address, reqBody.Signers, h.SupportedAssets)
	if err != nil {
		if errors.Is(err, services.ErrSponsorshipLimitExceed) {
			httperror.BadRequest("Sponsorship limit exceed.", nil).Render(rw)
			return
		}

		if errors.Is(err, services.ErrAccountAlreadyExists) {
			httperror.BadRequest("Account already exists.", nil).Render(rw)
			return
		}

		httperror.InternalServerError(ctx, "", err, nil).Render(rw)
		return
	}

	respBody := TransactionEnvelopeResponse{
		Transaction:       txe,
		NetworkPassphrase: networkPassphrase,
	}
	httpjson.Render(rw, respBody, httpjson.JSON)
}
