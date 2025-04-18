package httphandler

import (
	"fmt"
	"net/http"

	"github.com/stellar/go/support/log"
	"github.com/stellar/go/support/render/httpjson"
	"github.com/stellar/go/txnbuild"

	"github.com/stellar/wallet-backend/internal/apptracker"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/serve/httperror"
	"github.com/stellar/wallet-backend/internal/tss"
	"github.com/stellar/wallet-backend/internal/tss/router"
	tssservices "github.com/stellar/wallet-backend/internal/tss/services"
	"github.com/stellar/wallet-backend/internal/tss/store"
	tssUtils "github.com/stellar/wallet-backend/internal/tss/utils"
	"github.com/stellar/wallet-backend/internal/utils"
)

type TSSHandler struct {
	Router             router.Router
	Store              store.Store
	AppTracker         apptracker.AppTracker
	NetworkPassphrase  string
	TransactionService tssservices.TransactionService
	MetricsService     metrics.MetricsService
}

type Transaction struct {
	Operations []string `json:"operations" validate:"required"`
	TimeBounds int64    `json:"timebounds" validate:"required"`
}

type BuildTransactionsRequest struct {
	Transactions []Transaction `json:"transactions" validate:"required,gt=0"`
}

type BuildTransactionsResponse struct {
	TransactionXDRs []string `json:"transactionxdrs"`
}

type TransactionSubmissionRequest struct {
	WebhookURL   string   `json:"webhook" validate:"required"`
	Transactions []string `json:"transactions" validate:"required,gt=0"`
	FeeBump      bool     `json:"feebump"`
}

type TransactionSubmissionResponse struct {
	TransactionHashes []string `json:"transactionhashes"`
}

func (t *TSSHandler) BuildTransactions(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var reqParams BuildTransactionsRequest
	httpErr := DecodeJSONAndValidate(ctx, r, &reqParams, t.AppTracker)
	if httpErr != nil {
		httpErr.Render(w)
		return
	}
	var transactionXDRs []string
	for _, transaction := range reqParams.Transactions {
		ops, err := tssUtils.BuildOperations(transaction.Operations)
		if err != nil {
			httperror.BadRequest("bad operation xdr", nil).Render(w)
			return
		}
		tx, err := t.TransactionService.BuildAndSignTransactionWithChannelAccount(ctx, ops, transaction.TimeBounds)
		if err != nil {
			httperror.InternalServerError(ctx, "unable to build transaction", err, nil, t.AppTracker).Render(w)
			return
		}
		txXdrStr, err := tx.Base64()
		if err != nil {
			httperror.InternalServerError(ctx, "unable to base64 transaction", err, nil, t.AppTracker).Render(w)
			return
		}
		transactionXDRs = append(transactionXDRs, txXdrStr)
	}
	httpjson.Render(w, BuildTransactionsResponse{
		TransactionXDRs: transactionXDRs,
	}, httpjson.JSON)
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
	for _, txXDR := range reqParams.Transactions {
		genericTx, err := txnbuild.TransactionFromXDR(txXDR)
		if err != nil {
			httperror.BadRequest("bad transaction xdr", nil).Render(w)
			return
		}
		tx, txEmpty := genericTx.Transaction()
		if !txEmpty {
			httperror.BadRequest("bad transaction xdr", nil).Render(w)
			return
		}
		txHash, err := tx.HashHex(t.NetworkPassphrase)
		if err != nil {
			httperror.InternalServerError(ctx, "unable to hashhex transaction", err, nil, t.AppTracker).Render(w)
			return
		}
		payload := tss.Payload{
			TransactionHash: txHash,
			TransactionXDR:  txXDR,
			WebhookURL:      reqParams.WebhookURL,
			FeeBump:         reqParams.FeeBump,
		}

		payloads = append(payloads, payload)
		transactionHashes = append(transactionHashes, txHash)
		if t.MetricsService != nil {
			t.MetricsService.IncNumTSSTransactionsSubmitted()
		}
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

	if utils.IsEmpty(tx) {
		httperror.NotFound.Render(w)
		return
	}

	tssTry, err := t.Store.GetLatestTry(ctx, tx.Hash)
	if err != nil {
		httperror.InternalServerError(ctx, "unable to get tx try "+tx.Hash, err, nil, t.AppTracker).Render(w)
		return
	}

	httpjson.Render(w, tss.TSSResponse{
		TransactionHash:       tx.Hash,
		TransactionResultCode: fmt.Sprint(tssTry.Code),
		Status:                tx.Status,
		CreatedAt:             tssTry.CreatedAt.Unix(),
		EnvelopeXDR:           tssTry.XDR,
		ResultXDR:             tssTry.ResultXDR,
	}, httpjson.JSON)
}
