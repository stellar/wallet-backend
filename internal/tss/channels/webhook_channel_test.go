package channels

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stellar/wallet-backend/internal/tss"
	tssutils "github.com/stellar/wallet-backend/internal/tss/utils"
	"github.com/stellar/wallet-backend/internal/utils"
)

func TestWebhookHandlerServiceChannel(t *testing.T) {
	mockHTTPClient := utils.MockHTTPClient{}
	cfg := WebhookChannelConfigs{
		HTTPClient:           &mockHTTPClient,
		MaxBufferSize:        1,
		MaxWorkers:           1,
		MaxRetries:           3,
		MinWaitBtwnRetriesMS: 5,
	}
	channel := NewWebhookChannel(cfg)

	payload := tss.Payload{}
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
}
