package httphandler

import (
	"errors"
	"net/http"

	"github.com/stellar/go/support/render/httpjson"
	"github.com/stellar/go/txnbuild"

	"github.com/stellar/wallet-backend/internal/apptracker"
	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/serve/httperror"
	"github.com/stellar/wallet-backend/internal/services"
	"github.com/stellar/wallet-backend/pkg/wbclient/types"
)

type AccountHandler struct {
	AccountService            services.AccountService
	AccountSponsorshipService services.AccountSponsorshipService
	AppTracker                apptracker.AppTracker
	NetworkPassphrase         string
}

type AccountRegistrationRequest struct {
	Address string `json:"address" validate:"required,public_key"`
}

func (h AccountHandler) RegisterAccount(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var reqParams AccountRegistrationRequest
	httpErr := DecodePathAndValidate(ctx, r, &reqParams, h.AppTracker)
	if httpErr != nil {
		httpErr.Render(w)
		return
	}

	err := h.AccountService.RegisterAccount(ctx, reqParams.Address)
	if err != nil {
		httperror.InternalServerError(ctx, "", err, nil, h.AppTracker).Render(w)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (h AccountHandler) DeregisterAccount(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var reqParams AccountRegistrationRequest
	httpErr := DecodePathAndValidate(ctx, r, &reqParams, h.AppTracker)
	if httpErr != nil {
		httpErr.Render(w)
		return
	}

	err := h.AccountService.DeregisterAccount(ctx, reqParams.Address)
	if err != nil {
		httperror.InternalServerError(ctx, "", err, nil, h.AppTracker).Render(w)
		return
	}

	w.WriteHeader(http.StatusOK)
}

type SponsorAccountCreationOptions struct {
	Address            string            `json:"address"                        validate:"required,public_key"`
	Signers            []entities.Signer `json:"signers"                        validate:"required,gt=0,dive"`
	MasterSignerWeight *int              `json:"master_signer_weight,omitempty" validate:"omitempty,gt=0"`
	Assets             []entities.Asset  `json:"assets"                         validate:"dive"`
}

func (h AccountHandler) SponsorAccountCreation(rw http.ResponseWriter, req *http.Request) {
	ctx := req.Context()

	var reqBody SponsorAccountCreationOptions
	httpErr := DecodeJSONAndValidate(ctx, req, &reqBody, h.AppTracker)
	if httpErr != nil {
		httpErr.Render(rw)
		return
	}

	_, err := entities.ValidateSignersWeights(reqBody.Signers)
	if err != nil {
		httperror.BadRequest("Validation error.", map[string]any{"signers": err.Error()}).Render(rw)
		return
	}

	txe, err := h.AccountSponsorshipService.SponsorAccountCreationTransaction(ctx, services.SponsorAccountCreationOptions{
		Address:            reqBody.Address,
		Signers:            reqBody.Signers,
		Assets:             reqBody.Assets,
		MasterSignerWeight: reqBody.MasterSignerWeight,
	})
	if err != nil {
		if errors.Is(err, services.ErrSponsorshipLimitExceeded) {
			httperror.BadRequest("Sponsorship limit exceeded.", nil).Render(rw)
			return
		}

		if errors.Is(err, services.ErrAccountAlreadyExists) {
			httperror.Conflict("Account already exists on the Stellar network.", nil).Render(rw)
			return
		}

		httperror.InternalServerError(ctx, "", err, nil, h.AppTracker).Render(rw)
		return
	}

	respBody := types.TransactionEnvelopeResponse{
		Transaction:       txe,
		NetworkPassphrase: h.NetworkPassphrase,
	}
	httpjson.Render(rw, respBody, httpjson.JSON)
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

	feeBumpTxe, networkPassphrase, err := h.AccountSponsorshipService.WrapTransaction(ctx, tx)
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
