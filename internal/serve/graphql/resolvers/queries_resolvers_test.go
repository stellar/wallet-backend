package resolvers

import (
	"encoding/base64"
	"fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stellar/go-stellar-sdk/toid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vektah/gqlparser/v2/gqlerror"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/metrics"
)

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
