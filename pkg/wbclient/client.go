package wbclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/stellar/wallet-backend/internal/utils"
	"github.com/stellar/wallet-backend/pkg/wbclient/types"
)

const (
	buildTransactionsPath = "/tss/transactions/build"
)

type Client struct {
	HTTPClient    *http.Client
	BaseURL       string
	RequestSigner RequestSigner
}

func NewClient(baseURL string, requestSigner RequestSigner) *Client {
	return &Client{
		HTTPClient:    &http.Client{Timeout: time.Duration(30 * time.Second)},
		BaseURL:       baseURL,
		RequestSigner: requestSigner,
	}
}

func (c *Client) BuildTransactions(ctx context.Context, transactions ...types.Transaction) (*types.BuildTransactionsResponse, error) {
	buildTxRequest := types.BuildTransactionsRequest{Transactions: transactions}
	reqBody, err := json.Marshal(buildTxRequest)
	if err != nil {
		return nil, fmt.Errorf("marshalling request: %w", err)
	}

	u, err := url.JoinPath(c.BaseURL, buildTransactionsPath)
	if err != nil {
		return nil, fmt.Errorf("joining path: %w", err)
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewBuffer(reqBody))
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	err = c.RequestSigner.SignHTTPRequest(request, time.Now().Unix())
	if err != nil {
		return nil, fmt.Errorf("signing request: %w", err)
	}

	request.Header.Set("Content-Type", "application/json")

	resp, err := c.HTTPClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("sending request: %w", err)
	}

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response body: %w", err)
	}
	defer utils.DeferredClose(ctx, resp.Body, "closing response body")

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("unexpected statusCode=%d, body=%s", resp.StatusCode, string(respBody))
	}

	var buildTxResponse types.BuildTransactionsResponse
	err = json.Unmarshal(respBody, &buildTxResponse)
	if err != nil {
		return nil, fmt.Errorf("unmarshalling response body: %w", err)
	}

	return &buildTxResponse, nil
}
