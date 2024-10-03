package channels

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/tss"
	"github.com/stellar/wallet-backend/internal/tss/store"
	tssutils "github.com/stellar/wallet-backend/internal/tss/utils"
	"github.com/stellar/wallet-backend/internal/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWebhookHandlerServiceChannel(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()
	store, _ := store.NewStore(dbConnectionPool)
	mockHTTPClient := utils.MockHTTPClient{}
	cfg := WebhookChannelConfigs{
		HTTPClient:           &mockHTTPClient,
		Store:                store,
		MaxBufferSize:        1,
		MaxWorkers:           1,
		MaxRetries:           3,
		MinWaitBtwnRetriesMS: 5,
	}
	channel := NewWebhookChannel(cfg)

	payload := tss.Payload{}
	payload.TransactionHash = "hash"
	payload.TransactionXDR = "xdr"
	payload.WebhookURL = "www.stellar.org"
	jsonData, _ := json.Marshal(tssutils.PayloadTOTSSResponse(payload))

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

	tx, _ := store.GetTransaction(context.Background(), payload.TransactionHash)
	assert.Equal(t, string(tss.SentStatus), tx.Status)
}
