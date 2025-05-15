package httphandler

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	xdr3 "github.com/stellar/go-xdr/xdr3"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/txnbuild"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/apptracker"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/transactions/services"
	"github.com/stellar/wallet-backend/internal/transactions/utils"
	"github.com/stellar/wallet-backend/pkg/wbclient/types"
)

func TestBuildTransactions(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()
	mockAppTracker := apptracker.MockAppTracker{}
	mockTxService := services.TransactionServiceMock{}

	handler := &TransactionsHandler{
		AppTracker:         &mockAppTracker,
		NetworkPassphrase:  "testnet passphrase",
		TransactionService: &mockTxService,
	}

	srcAccount := keypair.MustRandom().Address()
	p := txnbuild.Payment{
		Destination:   keypair.MustRandom().Address(),
		Amount:        "10",
		Asset:         txnbuild.NativeAsset{},
		SourceAccount: srcAccount,
	}
	op, err := p.BuildXDR()
	require.NoError(t, err)

	var buf strings.Builder
	enc := xdr3.NewEncoder(&buf)
	err = op.EncodeTo(enc)
	require.NoError(t, err)

	opXDR := buf.String()
	opXDRBase64 := base64.StdEncoding.EncodeToString([]byte(opXDR))

	const endpoint = "/transactions/build"

	t.Run("tx_signing_fails", func(t *testing.T) {
		reqBody := fmt.Sprintf(`{
			"transactions": [{"operations": [%q], "timeout": 100}]
		}`, opXDRBase64)
		rw := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, endpoint, strings.NewReader(reqBody))

		expectedOps, err := utils.BuildOperations([]string{opXDRBase64})
		require.NoError(t, err)

		err = errors.New("unable to find channel account")
		mockTxService.
			On("BuildAndSignTransactionWithChannelAccount", context.Background(), expectedOps, int64(100), entities.RPCSimulateTransactionResult{}).
			Return(nil, err).
			Once()

		mockAppTracker.
			On("CaptureException", err).
			Return().
			Once()

		http.HandlerFunc(handler.BuildTransactions).ServeHTTP(rw, req)
		resp := rw.Result()
		respBody, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		expectedRespBody := `{"error": "unable to build transaction"}`
		assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
		assert.JSONEq(t, expectedRespBody, string(respBody))
	})

	t.Run("happy_path", func(t *testing.T) {
		reqBody := fmt.Sprintf(`{
			"transactions": [{"operations": [%q], "timeout": 100}]
		}`, opXDRBase64)
		rw := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, endpoint, strings.NewReader(reqBody))

		expectedOps, err := utils.BuildOperations([]string{opXDRBase64})
		require.NoError(t, err)
		tx := utils.BuildTestTransaction(t)

		mockTxService.
			On("BuildAndSignTransactionWithChannelAccount", context.Background(), expectedOps, int64(100), entities.RPCSimulateTransactionResult{}).
			Return(tx, nil).
			Once()

		http.HandlerFunc(handler.BuildTransactions).ServeHTTP(rw, req)
		resp := rw.Result()
		respBody, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		var buildTxResp types.BuildTransactionsResponse
		err = json.Unmarshal(respBody, &buildTxResp)
		require.NoError(t, err)
		expectedTxXDR, err := tx.Base64()
		require.NoError(t, err)
		assert.Equal(t, expectedTxXDR, buildTxResp.TransactionXDRs[0])
	})
}
