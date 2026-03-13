package infrastructure

import (
	"context"
	"net/http"
	"testing"

	"github.com/stellar/wallet-backend/internal/entities"
)

// Exported constants for use by integration tests in the parent package.
const (
	NetworkPassphraseStandalone = networkPassphrase
	DefaultProtocolVersionConst = DefaultProtocolVersion
)

// SimulateTransactionRPC is an exported wrapper around simulateTransactionRPC.
func SimulateTransactionRPC(client *http.Client, rpcURL, txXDR string) (*entities.RPCSimulateTransactionResult, error) {
	return simulateTransactionRPC(client, rpcURL, txXDR)
}

// SubmitTransactionToRPC is an exported wrapper around submitTransactionToRPC.
func SubmitTransactionToRPC(client *http.Client, rpcURL, txXDR string) (*entities.RPCSendTransactionResult, error) {
	return submitTransactionToRPC(client, rpcURL, txXDR)
}

// WaitForTransactionConfirmationHTTP is an exported wrapper around waitForTransactionConfirmation
// (the HTTP-based variant that polls RPC directly without an RPCService).
func WaitForTransactionConfirmationHTTP(ctx context.Context, t *testing.T, client *http.Client, rpcURL, hash string, retries int) error {
	return waitForTransactionConfirmation(ctx, t, client, rpcURL, hash, retries)
}
