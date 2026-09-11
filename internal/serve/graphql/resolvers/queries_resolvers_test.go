package resolvers

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stellar/go-stellar-sdk/toid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vektah/gqlparser/v2/gqlerror"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/services"
)

// stubSimulationService is a minimal TransactionSimulationService for resolver tests.
type stubSimulationService struct {
	result *services.SimulatedStateChanges
	err    error
}

func (s stubSimulationService) SimulateStateChanges(_ context.Context, _ string) (*services.SimulatedStateChanges, error) {
	return s.result, s.err
}

// testOpXDR returns the expected base64-encoded XDR for test operation N
func testOpXDR(n int) string {
	return base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("opxdr%d", n)))
}

func TestQueryResolver_TransactionByHash(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := metrics.NewMetrics(reg)

	resolver := &queryResolver{
		&Resolver{
			models: &data.Models{
				Transactions: &data.TransactionModel{
					DB:      testDBConnectionPool,
					Metrics: m.DB,
				},
			},
		},
	}

	t.Run("success", func(t *testing.T) {
		ctx := getTestCtx("transactions", []string{"hash", "toId", "feeCharged", "resultCode", "ledgerNumber", "ledgerCreatedAt", "isFeeBump"})
		tx, err := resolver.TransactionByHash(ctx, testTxHash1)

		require.NoError(t, err)
		assert.Equal(t, testTxHash1, tx.Hash.String())
		assert.Equal(t, toid.New(1000, 1, 0).ToInt64(), tx.ToID)
		assert.Equal(t, int64(100), tx.FeeCharged)
		assert.Equal(t, "TransactionResultCodeTxSuccess", tx.ResultCode)
		assert.Equal(t, uint32(1), tx.LedgerNumber)
	})

	t.Run("non-existent hash", func(t *testing.T) {
		ctx := getTestCtx("transactions", []string{"hash"})
		tx, err := resolver.TransactionByHash(ctx, "0000000000000000000000000000000000000000000000000000000000000000")

		require.Error(t, err)
		assert.Nil(t, tx)
	})

	t.Run("invalid hash format", func(t *testing.T) {
		ctx := getTestCtx("transactions", []string{"hash"})
		tx, err := resolver.TransactionByHash(ctx, "not-a-valid-hash")

		require.Error(t, err)
		assert.Nil(t, tx)
		var gqlErr *gqlerror.Error
		require.ErrorAs(t, err, &gqlErr)
		assert.Equal(t, ErrMsgInvalidTransactionHash, gqlErr.Message)
	})

	t.Run("empty hash", func(t *testing.T) {
		ctx := getTestCtx("transactions", []string{"hash"})
		tx, err := resolver.TransactionByHash(ctx, "")

		require.Error(t, err)
		assert.Nil(t, tx)
	})
}

func TestQueryResolver_Account(t *testing.T) {
	resolver := &queryResolver{&Resolver{}}

	t.Run("success", func(t *testing.T) {
		acc, err := resolver.AccountByAddress(testCtx, sharedTestAccountAddress)
		require.NoError(t, err)
		assert.Equal(t, sharedTestAccountAddress, string(acc.StellarAddress))
	})

	t.Run("any valid address returns account", func(t *testing.T) {
		acc, err := resolver.AccountByAddress(testCtx, sharedNonExistentAccountAddress)
		require.NoError(t, err)
		assert.NotNil(t, acc)
		assert.Equal(t, sharedNonExistentAccountAddress, string(acc.StellarAddress))
	})

	t.Run("empty address", func(t *testing.T) {
		acc, err := resolver.AccountByAddress(testCtx, "")
		require.Error(t, err)
		assert.Nil(t, acc)
	})
}

func TestQueryResolver_SimulateStateChanges_errorCodes(t *testing.T) {
	// The service wraps its sentinels (fmt.Errorf("...: %w", ...)), so wrap here too
	// to prove the resolver's errors.Is matching survives the wrap.
	tests := []struct {
		name     string
		svcErr   error
		wantCode string
	}{
		{"invalid XDR maps to INVALID_TRANSACTION_XDR", services.ErrInvalidTransactionXDR, "INVALID_TRANSACTION_XDR"},
		{"unsupported maps to UNSUPPORTED_TRANSACTION", services.ErrUnsupportedTransaction, "UNSUPPORTED_TRANSACTION"},
		{"simulation failure maps to SIMULATION_FAILED", services.ErrSimulationFailed, "SIMULATION_FAILED"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resolver := &queryResolver{&Resolver{
				simulationService: stubSimulationService{err: fmt.Errorf("simulating: %w", tt.svcErr)},
			}}

			_, err := resolver.SimulateStateChanges(context.Background(), "any-xdr")
			require.Error(t, err)

			var gqlErr *gqlerror.Error
			require.ErrorAs(t, err, &gqlErr)
			assert.Equal(t, tt.wantCode, gqlErr.Extensions["code"])
		})
	}

	t.Run("unexpected error carries no client-safe code (presenter will mask it)", func(t *testing.T) {
		resolver := &queryResolver{&Resolver{
			simulationService: stubSimulationService{err: errors.New("rpc unreachable")},
		}}

		_, err := resolver.SimulateStateChanges(context.Background(), "any-xdr")
		require.Error(t, err)

		// Falls to the default branch: a plain wrapped error, not a coded gqlerror.
		var gqlErr *gqlerror.Error
		assert.False(t, errors.As(err, &gqlErr), "unexpected errors must not be given a client-facing code")
	})
}

func TestQueryResolver_OperationByID(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := metrics.NewMetrics(reg)

	resolver := &queryResolver{
		&Resolver{
			models: &data.Models{
				Operations: &data.OperationModel{
					DB:      testDBConnectionPool,
					Metrics: m.DB,
				},
			},
		},
	}

	t.Run("success", func(t *testing.T) {
		ctx := getTestCtx("operations", []string{""})
		op, err := resolver.OperationByID(ctx, toid.New(1000, 1, 1).ToInt64())

		require.NoError(t, err)
		assert.Equal(t, toid.New(1000, 1, 1).ToInt64(), op.ID)
		assert.Equal(t, testOpXDR(1), op.OperationXDR.String())
		assert.Equal(t, uint32(1), op.LedgerNumber)
	})

	t.Run("non-existent ID", func(t *testing.T) {
		ctx := getTestCtx("operations", []string{"id"})
		op, err := resolver.OperationByID(ctx, 999)

		require.Error(t, err)
		assert.Nil(t, op)
	})

	t.Run("zero ID", func(t *testing.T) {
		ctx := getTestCtx("operations", []string{"id"})
		op, err := resolver.OperationByID(ctx, 0)

		require.Error(t, err)
		assert.Nil(t, op)
	})

	t.Run("negative ID", func(t *testing.T) {
		ctx := getTestCtx("operations", []string{"id"})
		op, err := resolver.OperationByID(ctx, -1)

		require.Error(t, err)
		assert.Nil(t, op)
	})
}
