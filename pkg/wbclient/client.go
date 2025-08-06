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

	"github.com/stellar/go/xdr"

	"github.com/stellar/wallet-backend/internal/utils"
	"github.com/stellar/wallet-backend/pkg/wbclient/auth"
	"github.com/stellar/wallet-backend/pkg/wbclient/types"
)

const (
	graphqlPath                  = "/graphql/query"
	createFeeBumpTransactionPath = "/tx/create-fee-bump"
)

type GraphQLRequest struct {
	Query     string                 `json:"query"`
	Variables map[string]interface{} `json:"variables,omitempty"`
}

type GraphQLResponse struct {
	Data   json.RawMessage `json:"data,omitempty"`
	Errors []GraphQLError  `json:"errors,omitempty"`
}

type GraphQLError struct {
	Message    string                 `json:"message"`
	Extensions map[string]interface{} `json:"extensions,omitempty"`
}

type BuildTransactionPayload struct {
	Success        bool   `json:"success"`
	TransactionXdr string `json:"transactionXdr"`
}

type BuildTransactionData struct {
	BuildTransaction BuildTransactionPayload `json:"buildTransaction"`
}

type Client struct {
	HTTPClient    *http.Client
	BaseURL       string
	RequestSigner auth.HTTPRequestSigner
}

func NewClient(baseURL string, requestSigner auth.HTTPRequestSigner) *Client {
	return &Client{
		HTTPClient:    &http.Client{Timeout: 30 * time.Second},
		BaseURL:       baseURL,
		RequestSigner: requestSigner,
	}
}

func (c *Client) BuildTransactions(ctx context.Context, transactions ...types.Transaction) (*types.BuildTransactionsResponse, error) {
	if len(transactions) == 0 {
		return nil, fmt.Errorf("at least one transaction is required")
	}

	// We only support building one transaction at a time
	if len(transactions) > 1 {
		return nil, fmt.Errorf("GraphQL buildTransaction mutation supports only one transaction at a time")
	}

	transaction := transactions[0]

	query := `
		mutation BuildTransaction($input: BuildTransactionInput!) {
			buildTransaction(input: $input) {
				success
				transactionXdr
			}
		}
	`

	variables := map[string]interface{}{
		"input": map[string]interface{}{
			"transaction": map[string]interface{}{
				"operations": transaction.Operations,
				"timeout":    transaction.Timeout,
			},
		},
	}

	// Add simulation result if provided
	if !utils.IsEmpty(transaction.SimulationResult.TransactionData) ||
		len(transaction.SimulationResult.Events) > 0 ||
		transaction.SimulationResult.MinResourceFee != "" ||
		len(transaction.SimulationResult.Results) > 0 ||
		transaction.SimulationResult.LatestLedger != 0 ||
		transaction.SimulationResult.Error != "" {

		simulationResult := map[string]interface{}{}

		if !utils.IsEmpty(transaction.SimulationResult.TransactionData) {
			txDataStr, err := xdr.MarshalBase64(transaction.SimulationResult.TransactionData)
			if err != nil {
				return nil, fmt.Errorf("marshaling transaction data: %w", err)
			}
			simulationResult["transactionData"] = txDataStr
		}

		if len(transaction.SimulationResult.Events) > 0 {
			simulationResult["events"] = transaction.SimulationResult.Events
		}

		if transaction.SimulationResult.MinResourceFee != "" {
			simulationResult["minResourceFee"] = transaction.SimulationResult.MinResourceFee
		}

		if len(transaction.SimulationResult.Results) > 0 {
			// Convert RPCSimulateHostFunctionResult as GraphQL expects JSON
			results := make([]string, len(transaction.SimulationResult.Results))
			for i, result := range transaction.SimulationResult.Results {
				resultJSON, err := json.Marshal(result)
				if err != nil {
					return nil, fmt.Errorf("marshaling simulation result %d: %w", i, err)
				}
				results[i] = string(resultJSON)
			}
			simulationResult["results"] = results
		}

		if transaction.SimulationResult.LatestLedger != 0 {
			simulationResult["latestLedger"] = transaction.SimulationResult.LatestLedger
		}

		if transaction.SimulationResult.Error != "" {
			simulationResult["error"] = transaction.SimulationResult.Error
		}

		variables["input"].(map[string]interface{})["transaction"].(map[string]interface{})["simulationResult"] = simulationResult
	}

	gqlRequest := GraphQLRequest{
		Query:     query,
		Variables: variables,
	}

	resp, err := c.request(ctx, http.MethodPost, graphqlPath, gqlRequest)
	if err != nil {
		return nil, fmt.Errorf("calling client request: %w", err)
	}

	if c.isHTTPError(resp) {
		return nil, c.logHTTPError(ctx, resp)
	}

	gqlResponse, err := parseResponseBody[GraphQLResponse](ctx, resp.Body)
	if err != nil {
		return nil, fmt.Errorf("parsing GraphQL response body: %w", err)
	}

	if len(gqlResponse.Errors) > 0 {
		return nil, fmt.Errorf("GraphQL error: %s", gqlResponse.Errors[0].Message)
	}

	var data BuildTransactionData
	if err := json.Unmarshal(gqlResponse.Data, &data); err != nil {
		return nil, fmt.Errorf("unmarshaling GraphQL data: %w", err)
	}

	// Convert to the expected response format
	return &types.BuildTransactionsResponse{
		TransactionXDRs: []string{data.BuildTransaction.TransactionXdr},
	}, nil
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

	err = c.RequestSigner.SignHTTPRequest(request, 5*time.Second)
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
