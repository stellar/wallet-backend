package httphandler

import (
	"errors"
	"net/http"

	"github.com/stellar/go/support/render/httpjson"
	"github.com/stellar/go/txnbuild"
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

type CreateFeeBumpTransactionRequest struct {
	Transaction string `json:"transaction" validate:"required"`
}

type TransactionEnvelopeResponse struct {
	Transaction       string `json:"transaction"`
	NetworkPassphrase string `json:"networkPassphrase"`
}

func (h AccountHandler) SponsorAccountCreation(rw http.ResponseWriter, req *http.Request) {
	ctx := req.Context()

	var reqBody SponsorAccountCreationRequest
	httpErr := DecodeJSONAndValidate(ctx, req, &reqBody)
	if httpErr != nil {
		httpErr.Render(rw)
		return
	}

	_, err := entities.ValidateSignersWeights(reqBody.Signers)
	if err != nil {
		httperror.BadRequest("Validation error.", map[string]interface{}{"signers": err.Error()}).Render(rw)
		return
	}

	txe, networkPassphrase, err := h.AccountSponsorshipService.SponsorAccountCreationTransaction(ctx, reqBody.Address, reqBody.Signers, h.SupportedAssets)
	if err != nil {
		if errors.Is(err, services.ErrSponsorshipLimitExceeded) {
			httperror.BadRequest("Sponsorship limit exceeded.", nil).Render(rw)
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

func (h AccountHandler) CreateFeeBumpTransaction(rw http.ResponseWriter, req *http.Request) {
	ctx := req.Context()

	var reqBody CreateFeeBumpTransactionRequest
	httpErr := DecodeJSONAndValidate(ctx, req, &reqBody)
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

	feeBumpTxe, networkPassphrase, err := h.AccountSponsorshipService.WrapTransaction(ctx, tx)
	if err != nil {
		var errOperationNotAllowed *services.ErrOperationNotAllowed
		switch {
		case errors.Is(err, services.ErrAccountNotEligibleForBeingSponsored), errors.Is(err, services.ErrFeeExceedsMaximumBaseFee),
			errors.Is(err, services.ErrNoSignaturesProvided), errors.As(err, &errOperationNotAllowed):
			httperror.BadRequest(err.Error(), nil).Render(rw)
			return
		default:
			httperror.InternalServerError(ctx, "", err, nil).Render(rw)
			return
		}
	}

	respBody := TransactionEnvelopeResponse{
		Transaction:       feeBumpTxe,
		NetworkPassphrase: networkPassphrase,
	}
	httpjson.Render(rw, respBody, httpjson.JSON)
}
