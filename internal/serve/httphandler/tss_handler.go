package httphandler

import (
	"net/http"

	"github.com/stellar/go/support/log"
	"github.com/stellar/go/support/render/httpjson"
	"github.com/stellar/wallet-backend/internal/apptracker"
	"github.com/stellar/wallet-backend/internal/serve/httperror"
	"github.com/stellar/wallet-backend/internal/tss"
	"github.com/stellar/wallet-backend/internal/tss/router"
	"github.com/stellar/wallet-backend/internal/tss/store"
	"github.com/stellar/wallet-backend/internal/tss/utils"
)

type TSSHandler struct {
	Router            router.Router
	Store             store.Store
	AppTracker        apptracker.AppTracker
	NetworkPassphrase string
}

type Transaction struct {
	Operations []string `json:"operations" validate:"required"`
}

type TransactionSubmissionRequest struct {
	WebhookURL   string        `json:"webhook" validate:"required"`
	Transactions []Transaction `json:"transactions" validate:"required,gt=0"`
}

type TransactionSubmissionResponse struct {
	TransactionHashes []string `json:"transactionhashes"`
}

func (t *TSSHandler) SubmitTransactions(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var reqParams TransactionSubmissionRequest
	httpErr := DecodeJSONAndValidate(ctx, r, &reqParams, t.AppTracker)
	if httpErr != nil {
		httpErr.Render(w)
		return
	}
	var transactionHashes []string
	var payloads []tss.Payload
	for _, transaction := range reqParams.Transactions {
		tx, err := utils.BuildOriginalTransaction(transaction.Operations)
		if err != nil {
			httperror.BadRequest("bad operation xdr", nil).Render(w)
			return
		}
		txHash, err := tx.HashHex(t.NetworkPassphrase)
		if err != nil {
			httperror.InternalServerError(ctx, "unable to hashhex transaction", err, nil, t.AppTracker).Render(w)
			return
		}

		txXDR, err := tx.Base64()
		if err != nil {
			httperror.InternalServerError(ctx, "unable to base64 transaction", err, nil, t.AppTracker).Render(w)
			return
		}

		payload := tss.Payload{
			TransactionHash: txHash,
			TransactionXDR:  txXDR,
			WebhookURL:      reqParams.WebhookURL,
		}

		payloads = append(payloads, payload)
		transactionHashes = append(transactionHashes, txHash)
	}
	httpjson.Render(w, TransactionSubmissionResponse{
		TransactionHashes: transactionHashes,
	}, httpjson.JSON)

	for _, payload := range payloads {
		err := t.Router.Route(payload)
		if err != nil {
			log.Errorf("unable to route payload: %v", err)
		}

	}

}

type GetTransactionRequest struct {
	TransactionHash string `json:"transactionhash" validate:"required"`
}

type GetTransactionResponse struct {
	Hash   string `json:"transactionhash"`
	XDR    string `json:"transactionxdr"`
	Status string `json:"status"`
}

func (t *TSSHandler) GetTransaction(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var reqParams GetTransactionRequest
	httpErr := DecodePathAndValidate(ctx, r, &reqParams, t.AppTracker)
	if httpErr != nil {
		httpErr.Render(w)
		return
	}
	tx, err := t.Store.GetTransaction(ctx, reqParams.TransactionHash)
	if err != nil {
		httperror.InternalServerError(ctx, "unable to get transaction "+reqParams.TransactionHash, err, nil, t.AppTracker).Render(w)
		return
	}

	if tx == (store.Transaction{}) {
		httperror.NotFound.Render(w)
	}

	httpjson.Render(w, GetTransactionResponse{
		Hash:   tx.Hash,
		XDR:    tx.XDR,
		Status: tx.Status,
	}, httpjson.JSON)
}
