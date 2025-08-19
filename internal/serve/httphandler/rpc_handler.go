package httphandler

import (
	"io"
	"net/http"

	"github.com/stellar/wallet-backend/internal/utils"
)

type RPCHandler struct {
	RPCURL     string
	HTTPClient utils.HTTPClient
}

func (h RPCHandler) ForwardRPCRequest(w http.ResponseWriter, r *http.Request) {
	// Forward the request body directly to the RPC service
	resp, err := h.HTTPClient.Post(h.RPCURL, r.Header.Get("Content-Type"), r.Body)
	if err != nil {
		http.Error(w, "Failed to forward request", http.StatusInternalServerError)
		return
	}
	//nolint:errcheck
	defer resp.Body.Close()

	// Copy response headers
	for key, values := range resp.Header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}

	// Set status code
	w.WriteHeader(resp.StatusCode)

	// Copy response body
	_, err = io.Copy(w, resp.Body)
	if err != nil {
		http.Error(w, "Failed to copy response body", http.StatusInternalServerError)
		return
	}
}
