package httphandler

import (
	"errors"
	"net/http"

	"github.com/stellar/go/support/render/httpjson"

	"github.com/stellar/wallet-backend/internal/apptracker"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/serve/httperror"
	"github.com/stellar/wallet-backend/internal/signing"
	"github.com/stellar/wallet-backend/internal/signing/store"
	transactionservices "github.com/stellar/wallet-backend/internal/transactions/services"
	"github.com/stellar/wallet-backend/pkg/sorobanauth"
	"github.com/stellar/wallet-backend/pkg/wbclient/types"
)

type TransactionsHandler struct {
	AppTracker         apptracker.AppTracker
	NetworkPassphrase  string
	TransactionService transactionservices.TransactionService
	MetricsService     metrics.MetricsService
}

func (t *TransactionsHandler) BuildTransactions(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var reqParams types.BuildTransactionsRequest
	if httpErr := DecodeJSONAndValidate(ctx, r, &reqParams, t.AppTracker); httpErr != nil {
		httpErr.Render(w)
		return
	}

	txXDRs, err := t.TransactionService.BuildAndSignTransactionsWithChannelAccounts(ctx, reqParams.Transactions...)
	if err != nil {
		if errors.Is(err, transactionservices.ErrInvalidArguments) ||
			errors.Is(err, signing.ErrUnavailableChannelAccounts) ||
			errors.Is(err, sorobanauth.ErrForbiddenSigner) {
			httperror.BadRequest(err.Error(), nil).Render(w)
			return
		}
		if errors.Is(err, store.ErrNoIdleChannelAccountAvailable) {
			httperror.InternalServerError(ctx, err.Error(), err, nil, t.AppTracker).Render(w)
			return
		}
		httperror.InternalServerError(ctx, "unable to build transaction", err, nil, t.AppTracker).Render(w)
		return
	}

	httpjson.Render(w, types.BuildTransactionsResponse{TransactionXDRs: txXDRs}, httpjson.JSON)
}
