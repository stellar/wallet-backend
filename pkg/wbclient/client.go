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
	buildTransactionsPath        = "/tss/transactions/build"
	createFeeBumpTransactionPath = "/tx/create-fee-bump"
	getTransactionPath           = "/tss/transactions"
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

	resp, err := c.request(ctx, http.MethodPost, buildTransactionsPath, buildTxRequest)
	if err != nil {
		return nil, fmt.Errorf("calling client request: %w", err)
	}

	if c.isHTTPError(resp) {
		return nil, c.logHTTPError(ctx, resp)
	}

	buildTxResponse, err := parseResponseBody[types.BuildTransactionsResponse](ctx, resp.Body)
	if err != nil {
		return nil, fmt.Errorf("parsing response body: %w", err)
	}

	return buildTxResponse, nil
}

func parseResponseBody[T any](ctx context.Context, respBody io.ReadCloser) (*T, error) {
	respBodyBytes, err := io.ReadAll(respBody)
	if err != nil {
		return nil, fmt.Errorf("reading response body: %w", err)
	}
	defer utils.DeferredClose(ctx, respBody, "closing response body")

	var response T
	err = json.Unmarshal(respBodyBytes, &response)
	if err != nil {
		return nil, fmt.Errorf("unmarshalling response body: %w", err)
	}

	return &response, nil
}

func (c *Client) FeeBumpTransaction(ctx context.Context, transactionXDR string) (*types.TransactionEnvelopeResponse, error) {
	buildTxRequest := types.CreateFeeBumpTransactionRequest{Transaction: transactionXDR}

	resp, err := c.request(ctx, http.MethodPost, createFeeBumpTransactionPath, buildTxRequest)
	if err != nil {
		return nil, fmt.Errorf("calling client request: %w", err)
	}

	if c.isHTTPError(resp) {
		return nil, c.logHTTPError(ctx, resp)
	}

	feeBumpTxResponse, err := parseResponseBody[types.TransactionEnvelopeResponse](ctx, resp.Body)
	if err != nil {
		return nil, fmt.Errorf("parsing response body: %w", err)
	}

	return feeBumpTxResponse, nil
}

func (c *Client) request(ctx context.Context, method, path string, bodyObj any) (*http.Response, error) {
	reqBody, err := json.Marshal(bodyObj)
	if err != nil {
		return nil, fmt.Errorf("marshalling request body: %w", err)
	}

	u, err := url.JoinPath(c.BaseURL, path)
	if err != nil {
		return nil, fmt.Errorf("joining path: %w", err)
	}

	request, err := http.NewRequestWithContext(ctx, method, u, bytes.NewBuffer(reqBody))
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

	return resp, nil
}

func (c *Client) isHTTPError(resp *http.Response) bool {
	return resp.StatusCode >= 400
}

func (c *Client) logHTTPError(ctx context.Context, resp *http.Response) error {
	if c.isHTTPError(resp) {
		respBody, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("reading response body to log error when statusCode=%d: %w", resp.StatusCode, err)
		}
		defer utils.DeferredClose(ctx, resp.Body, "closing response body")

		return fmt.Errorf("unexpected statusCode=%d, body=%v", resp.StatusCode, string(respBody))
	}

	return nil
}
