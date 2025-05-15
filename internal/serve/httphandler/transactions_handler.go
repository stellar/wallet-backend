package httphandler

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/stellar/go/support/render/httpjson"

	"github.com/stellar/wallet-backend/internal/apptracker"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/serve/httperror"
	"github.com/stellar/wallet-backend/internal/signing"
	"github.com/stellar/wallet-backend/internal/signing/store"
	transactionservices "github.com/stellar/wallet-backend/internal/transactions/services"
	transactionsUtils "github.com/stellar/wallet-backend/internal/transactions/utils"
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
	httpErr := DecodeJSONAndValidate(ctx, r, &reqParams, t.AppTracker)
	if httpErr != nil {
		httpErr.Render(w)
		return
	}
	var transactionXDRs []string
	for i, transaction := range reqParams.Transactions {
		ops, err := transactionsUtils.BuildOperations(transaction.Operations)
		if err != nil {
			httperror.BadRequest("", map[string]any{
				fmt.Sprintf("transactions[%d].operations", i): err.Error(),
			}).Render(w)
			return
		}
		tx, err := t.TransactionService.BuildAndSignTransactionWithChannelAccount(ctx, ops, transaction.Timeout, transaction.SimulationResult)
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
		txXdrStr, err := tx.Base64()
		if err != nil {
			httperror.InternalServerError(ctx, "unable to base64 transaction", err, nil, t.AppTracker).Render(w)
			return
		}
		transactionXDRs = append(transactionXDRs, txXdrStr)
	}
	httpjson.Render(w, types.BuildTransactionsResponse{
		TransactionXDRs: transactionXDRs,
	}, httpjson.JSON)
}
