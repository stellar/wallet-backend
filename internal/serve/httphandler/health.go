package httphandler

import (
	"net/http"

	"github.com/stellar/go/support/render/httpjson"

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

var (
	ledgerCursorName      = "live_ingest_cursor"
	ledgerHealthThreshold = uint32(50)
)

func (h HealthHandler) GetHealth(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	rpcHealth, err := h.RPCService.GetHealth()
	if err != nil {
		httperror.InternalServerError(ctx, "", err, nil, h.AppTracker).Render(w)
		return
	}
	if rpcHealth.Status != "healthy" {
		httperror.ServiceUnavailable(ctx, "rpc is not healthy", nil, nil, h.AppTracker).Render(w)
		return
	}

	backendLatestLedger, err := h.Models.IngestStore.GetLatestLedgerSynced(ctx, ledgerCursorName)
	if err != nil {
		httperror.InternalServerError(ctx, "", err, nil, h.AppTracker).Render(w)
		return
	}
	if rpcHealth.LatestLedger-backendLatestLedger > ledgerHealthThreshold {
		httperror.ServiceUnavailable(ctx, "wallet backend is not in sync with the RPC", nil, nil, h.AppTracker).Render(w)
		return
	}

	httpjson.Render(w, map[string]interface{}{
		"status":                "ok",
		"rpc_latest_ledger":     rpcHealth.LatestLedger,
		"backend_latest_ledger": backendLatestLedger,
	}, httpjson.JSON)
}
