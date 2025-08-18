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

	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/utils"
	"github.com/stellar/wallet-backend/pkg/wbclient/auth"
	"github.com/stellar/wallet-backend/pkg/wbclient/types"
)

const (
	graphqlPath                  = "/graphql/query"
	createFeeBumpTransactionPath = "/tx/create-fee-bump"
	buildTransactionQuery        = `
		mutation BuildTransaction($input: BuildTransactionInput!) {
			buildTransaction(input: $input) {
				success
				transactionXdr
			}
		}
	`
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

func buildSimulationResultMap(simResult entities.RPCSimulateTransactionResult) (map[string]interface{}, error) {
	result := make(map[string]interface{})

	if !utils.IsEmpty(simResult.TransactionData) {
		if txDataStr, err := xdr.MarshalBase64(simResult.TransactionData); err != nil {
			return nil, fmt.Errorf("marshaling transaction data: %w", err)
		} else {
			result["transactionData"] = txDataStr
		}
	}

	if len(simResult.Events) > 0 {
		result["events"] = simResult.Events
	}

	if simResult.MinResourceFee != "" {
		result["minResourceFee"] = simResult.MinResourceFee
	}

	if len(simResult.Results) > 0 {
		// Convert RPCSimulateHostFunctionResult as GraphQL expects JSON
		results := make([]string, len(simResult.Results))
		for i, result := range simResult.Results {
			if resultJSON, err := json.Marshal(result); err != nil {
				return nil, fmt.Errorf("marshaling simulation result %d: %w", i, err)
			} else {
				results[i] = string(resultJSON)
			}
		}
		result["results"] = results
	}

	if simResult.LatestLedger != 0 {
		result["latestLedger"] = simResult.LatestLedger
	}

	if simResult.Error != "" {
		result["error"] = simResult.Error
	}

	return result, nil
}

func (c *Client) BuildTransaction(ctx context.Context, transaction types.Transaction) (*types.BuildTransactionResponse, error) {
	simulationResult, err := buildSimulationResultMap(transaction.SimulationResult)
	if err != nil {
		return nil, err
	}

	variables := map[string]interface{}{
		"input": map[string]interface{}{
			"transaction": map[string]interface{}{
				"operations":       transaction.Operations,
				"timeout":          transaction.Timeout,
				"simulationResult": simulationResult,
			},
		},
	}

	gqlRequest := GraphQLRequest{
		Query:     buildTransactionQuery,
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

	return &types.BuildTransactionResponse{
		TransactionXDR: data.BuildTransaction.TransactionXdr,
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

	if c.RequestSigner != nil {
		err = c.RequestSigner.SignHTTPRequest(request, 5*time.Second)
		if err != nil {
			return nil, fmt.Errorf("signing request: %w", err)
		}
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
