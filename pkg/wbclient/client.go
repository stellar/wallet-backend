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

type CreateFeeBumpTransactionPayload struct {
	Success           bool   `json:"success"`
	Transaction       string `json:"transaction"`
	NetworkPassphrase string `json:"networkPassphrase"`
}

type CreateFeeBumpTransactionData struct {
	CreateFeeBumpTransaction CreateFeeBumpTransactionPayload `json:"createFeeBumpTransaction"`
}

type RegisterAccountData struct {
	RegisterAccount types.RegisterAccountPayload `json:"registerAccount"`
}

type DeregisterAccountData struct {
	DeregisterAccount types.DeregisterAccountPayload `json:"deregisterAccount"`
}

type TransactionByHashData struct {
	TransactionByHash *types.GraphQLTransaction `json:"transactionByHash"`
}

type TransactionsData struct {
	Transactions *types.TransactionConnection `json:"transactions"`
}

type AccountByAddressData struct {
	AccountByAddress *types.Account `json:"accountByAddress"`
}

type OperationsData struct {
	Operations *types.OperationConnection `json:"operations"`
}

type OperationByIDData struct {
	OperationByID *types.Operation `json:"operationById"`
}

type StateChangesData struct {
	StateChanges *types.StateChangeConnection `json:"stateChanges"`
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
			"transactionXdr":   transaction.TransactionXdr,
			"simulationResult": simulationResult,
		},
	}

	gqlRequest := GraphQLRequest{
		Query:     buildTransactionQuery(),
		Variables: variables,
	}

	resp, err := c.request(ctx, gqlRequest)
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
	variables := map[string]interface{}{
		"input": map[string]interface{}{
			"transactionXDR": transactionXDR,
		},
	}

	gqlRequest := GraphQLRequest{
		Query:     createFeeBumpTransactionQuery(),
		Variables: variables,
	}

	resp, err := c.request(ctx, gqlRequest)
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

	var data CreateFeeBumpTransactionData
	if err := json.Unmarshal(gqlResponse.Data, &data); err != nil {
		return nil, fmt.Errorf("unmarshaling GraphQL data: %w", err)
	}

	return &types.TransactionEnvelopeResponse{
		Transaction:       data.CreateFeeBumpTransaction.Transaction,
		NetworkPassphrase: data.CreateFeeBumpTransaction.NetworkPassphrase,
	}, nil
}

func (c *Client) RegisterAccount(ctx context.Context, address string) (*types.RegisterAccountPayload, error) {
	variables := map[string]interface{}{
		"input": map[string]interface{}{
			"address": address,
		},
	}

	gqlRequest := GraphQLRequest{
		Query:     registerAccountQuery(),
		Variables: variables,
	}

	resp, err := c.request(ctx, gqlRequest)
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

	var data RegisterAccountData
	if err := json.Unmarshal(gqlResponse.Data, &data); err != nil {
		return nil, fmt.Errorf("unmarshaling GraphQL data: %w", err)
	}

	return &data.RegisterAccount, nil
}

func (c *Client) DeregisterAccount(ctx context.Context, address string) (*types.DeregisterAccountPayload, error) {
	variables := map[string]interface{}{
		"input": map[string]interface{}{
			"address": address,
		},
	}

	gqlRequest := GraphQLRequest{
		Query:     deregisterAccountQuery(),
		Variables: variables,
	}

	resp, err := c.request(ctx, gqlRequest)
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

	var data DeregisterAccountData
	if err := json.Unmarshal(gqlResponse.Data, &data); err != nil {
		return nil, fmt.Errorf("unmarshaling GraphQL data: %w", err)
	}

	return &data.DeregisterAccount, nil
}

func (c *Client) GetTransactionByHash(ctx context.Context, hash string, opts ...*QueryOptions) (*types.GraphQLTransaction, error) {
	var fields []string
	if len(opts) > 0 && opts[0] != nil {
		fields = opts[0].TransactionFields
	}

	variables := map[string]interface{}{
		"hash": hash,
	}

	gqlRequest := GraphQLRequest{
		Query:     buildTransactionByHashQuery(fields),
		Variables: variables,
	}

	resp, err := c.request(ctx, gqlRequest)
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

	var data TransactionByHashData
	if err := json.Unmarshal(gqlResponse.Data, &data); err != nil {
		return nil, fmt.Errorf("unmarshaling GraphQL data: %w", err)
	}

	return data.TransactionByHash, nil
}

func (c *Client) GetTransactions(ctx context.Context, first, last *int32, after, before *string, opts ...*QueryOptions) (*types.TransactionConnection, error) {
	var fields []string
	if len(opts) > 0 && opts[0] != nil {
		fields = opts[0].TransactionFields
	}

	variables := map[string]interface{}{}
	if first != nil {
		variables["first"] = *first
	}
	if after != nil {
		variables["after"] = *after
	}
	if last != nil {
		variables["last"] = *last
	}
	if before != nil {
		variables["before"] = *before
	}

	gqlRequest := GraphQLRequest{
		Query:     buildTransactionsQuery(fields),
		Variables: variables,
	}

	resp, err := c.request(ctx, gqlRequest)
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

	var data TransactionsData
	if err := json.Unmarshal(gqlResponse.Data, &data); err != nil {
		return nil, fmt.Errorf("unmarshaling GraphQL data: %w", err)
	}

	return data.Transactions, nil
}

func (c *Client) GetAccountByAddress(ctx context.Context, address string, opts ...*QueryOptions) (*types.Account, error) {
	var fields []string
	if len(opts) > 0 && opts[0] != nil {
		fields = opts[0].AccountFields
	}

	variables := map[string]interface{}{
		"address": address,
	}

	gqlRequest := GraphQLRequest{
		Query:     buildAccountByAddressQuery(fields),
		Variables: variables,
	}

	resp, err := c.request(ctx, gqlRequest)
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

	var data AccountByAddressData
	if err := json.Unmarshal(gqlResponse.Data, &data); err != nil {
		return nil, fmt.Errorf("unmarshaling GraphQL data: %w", err)
	}

	return data.AccountByAddress, nil
}

func (c *Client) GetOperations(ctx context.Context, first, last *int32, after, before *string, opts ...*QueryOptions) (*types.OperationConnection, error) {
	var fields []string
	if len(opts) > 0 && opts[0] != nil {
		fields = opts[0].OperationFields
	}

	variables := map[string]interface{}{}
	if first != nil {
		variables["first"] = *first
	}
	if after != nil {
		variables["after"] = *after
	}
	if last != nil {
		variables["last"] = *last
	}
	if before != nil {
		variables["before"] = *before
	}

	gqlRequest := GraphQLRequest{
		Query:     buildOperationsQuery(fields),
		Variables: variables,
	}

	resp, err := c.request(ctx, gqlRequest)
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

	var data OperationsData
	if err := json.Unmarshal(gqlResponse.Data, &data); err != nil {
		return nil, fmt.Errorf("unmarshaling GraphQL data: %w", err)
	}

	return data.Operations, nil
}

func (c *Client) GetOperationByID(ctx context.Context, id int64, opts ...*QueryOptions) (*types.Operation, error) {
	var fields []string
	if len(opts) > 0 && opts[0] != nil {
		fields = opts[0].OperationFields
	}

	variables := map[string]interface{}{
		"id": id,
	}

	gqlRequest := GraphQLRequest{
		Query:     buildOperationByIDQuery(fields),
		Variables: variables,
	}

	resp, err := c.request(ctx, gqlRequest)
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

	var data OperationByIDData
	if err := json.Unmarshal(gqlResponse.Data, &data); err != nil {
		return nil, fmt.Errorf("unmarshaling GraphQL data: %w", err)
	}

	return data.OperationByID, nil
}

func (c *Client) GetStateChanges(ctx context.Context, first, last *int32, after, before *string) (*types.StateChangeConnection, error) {
	variables := map[string]interface{}{}
	if first != nil {
		variables["first"] = *first
	}
	if after != nil {
		variables["after"] = *after
	}
	if last != nil {
		variables["last"] = *last
	}
	if before != nil {
		variables["before"] = *before
	}

	gqlRequest := GraphQLRequest{
		Query:     buildStateChangesQuery(),
		Variables: variables,
	}

	resp, err := c.request(ctx, gqlRequest)
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

	var data StateChangesData
	if err := json.Unmarshal(gqlResponse.Data, &data); err != nil {
		return nil, fmt.Errorf("unmarshaling GraphQL data: %w", err)
	}

	return data.StateChanges, nil
}

func (c *Client) request(ctx context.Context, bodyObj any) (*http.Response, error) {
	reqBody, err := json.Marshal(bodyObj)
	if err != nil {
		return nil, fmt.Errorf("marshalling request body: %w", err)
	}

	u, err := url.JoinPath(c.BaseURL, graphqlPath)
	if err != nil {
		return nil, fmt.Errorf("joining path: %w", err)
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewBuffer(reqBody))
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
