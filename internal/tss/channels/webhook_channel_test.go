package channels

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/metrics"
	channelAccountStore "github.com/stellar/wallet-backend/internal/signing/store"
	"github.com/stellar/wallet-backend/internal/tss"
	"github.com/stellar/wallet-backend/internal/tss/store"
	tssutils "github.com/stellar/wallet-backend/internal/tss/utils"
	"github.com/stellar/wallet-backend/internal/utils"
)

func TestWebhookHandlerServiceChannel(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	mockMetricsService := metrics.NewMockMetricsService()
	store, err := store.NewStore(dbConnectionPool, mockMetricsService)
	require.NoError(t, err)
	channelAccountStore := channelAccountStore.ChannelAccountStoreMock{}
	mockHTTPClient := utils.MockHTTPClient{}
	cfg := WebhookChannelConfigs{
		HTTPClient:           &mockHTTPClient,
		Store:                store,
		ChannelAccountStore:  &channelAccountStore,
		MaxBufferSize:        1,
		MaxWorkers:           1,
		MaxRetries:           3,
		MinWaitBtwnRetriesMS: 5,
		NetworkPassphrase:    "networkpassphrase",
		MetricsService:       mockMetricsService,
	}

	mockMetricsService.On("RegisterPoolMetrics", WebhookChannelName, mock.AnythingOfType("*pond.WorkerPool")).Once()
	mockMetricsService.On("ObserveDBQueryDuration", "SELECT", "tss_transactions", mock.AnythingOfType("float64")).Once()
	mockMetricsService.On("IncDBQuery", "SELECT", "tss_transactions").Once()
	mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "tss_transactions", mock.AnythingOfType("float64")).Once()
	mockMetricsService.On("IncDBQuery", "INSERT", "tss_transactions").Once()
	defer mockMetricsService.AssertExpectations(t)

	channel := NewWebhookChannel(cfg)

	payload := tss.Payload{}
	payload.TransactionHash = "hash"
	payload.TransactionXDR = "xdr"
	payload.WebhookURL = "www.stellar.org"
	jsonData, err := json.Marshal(tssutils.PayloadTOTSSResponse(payload))
	require.NoError(t, err)
	httpResponse1 := &http.Response{
		StatusCode: http.StatusBadGateway,
		Body:       io.NopCloser(strings.NewReader(`{"result": {"status": "OK"}}`)),
	}

	httpResponse2 := &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(`{"result": {"status": "OK"}}`)),
	}

	mockHTTPClient.
		On("Post", payload.WebhookURL, "application/json", bytes.NewBuffer(jsonData)).
		Return(httpResponse1, nil).
		Once()

	mockHTTPClient.
		On("Post", payload.WebhookURL, "application/json", bytes.NewBuffer(jsonData)).
		Return(httpResponse2, nil).
		Once()

	channel.Send(payload)
	channel.Stop()

	mockHTTPClient.AssertNumberOfCalls(t, "Post", 2)

	tx, err := store.GetTransaction(context.Background(), payload.TransactionHash)
	assert.Equal(t, string(tss.SentStatus), tx.Status)
	assert.NoError(t, err)
}
