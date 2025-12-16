package httphandler

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/stellar/go-stellar-sdk/support/render/httpjson"

	"github.com/stellar/wallet-backend/internal/apptracker"
	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/serve/httperror"
	"github.com/stellar/wallet-backend/internal/services"
)

type HealthHandler struct {
	Models     *data.Models
	RPCService services.RPCService
	AppTracker apptracker.AppTracker
}

const (
	ledgerCursorName      = "live_ingest_cursor"
	ledgerHealthThreshold = uint32(50)
)

func (h HealthHandler) GetHealth(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	rpcHealth, err := h.RPCService.GetHealth()
	if err != nil {
		err = fmt.Errorf("failed to get RPC health: %w", err)
		httperror.InternalServerError(ctx, err.Error(), err, nil, h.AppTracker).Render(w)
		return
	}
	if rpcHealth.Status != "healthy" {
		err = errors.New("rpc is not healthy")
		httperror.ServiceUnavailable(ctx, err.Error(), err, nil, h.AppTracker).Render(w)
		return
	}

	backendLatestLedger, err := h.Models.IngestStore.Get(ctx, ledgerCursorName)
	if err != nil {
		err = fmt.Errorf("failed to get backend latest ledger: %w", err)
		httperror.InternalServerError(ctx, err.Error(), err, nil, h.AppTracker).Render(w)
		return
	}
	if rpcHealth.LatestLedger-backendLatestLedger > ledgerHealthThreshold {
		err = errors.New("wallet backend is not in sync with the RPC")
		httperror.ServiceUnavailable(ctx, err.Error(), err, map[string]interface{}{
			"rpc_latest_ledger":     rpcHealth.LatestLedger,
			"backend_latest_ledger": backendLatestLedger,
		}, h.AppTracker).Render(w)
		return
	}

	httpjson.Render(w, map[string]any{
		"status":                "ok",
		"backend_latest_ledger": backendLatestLedger,
	}, httpjson.JSON)
}
