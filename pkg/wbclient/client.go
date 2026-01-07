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

	"github.com/stellar/go-stellar-sdk/xdr"

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

type AccountTransactionsData struct {
	AccountByAddress struct {
		Transactions *types.TransactionConnection `json:"transactions"`
	} `json:"accountByAddress"`
}

type AccountOperationsData struct {
	AccountByAddress struct {
		Operations *types.OperationConnection `json:"operations"`
	} `json:"accountByAddress"`
}

type AccountStateChangesData struct {
	AccountByAddress struct {
		StateChanges *types.StateChangeConnection `json:"stateChanges"`
	} `json:"accountByAddress"`
}

type TransactionOperationsData struct {
	TransactionByHash struct {
		Operations *types.OperationConnection `json:"operations"`
	} `json:"transactionByHash"`
}

type TransactionStateChangesData struct {
	TransactionByHash struct {
		StateChanges *types.StateChangeConnection `json:"stateChanges"`
	} `json:"transactionByHash"`
}

type OperationStateChangesData struct {
	OperationByID struct {
		StateChanges *types.StateChangeConnection `json:"stateChanges"`
	} `json:"operationById"`
}

type BalancesByAccountAddressData struct {
	BalancesByAccountAddress []json.RawMessage `json:"balancesByAccountAddress"`
}

type BalancesByAccountAddressesData struct {
	BalancesByAccountAddresses []*types.AccountBalances `json:"balancesByAccountAddresses"`
}

// QueryOptions allows clients to specify which fields to fetch for each entity type
type QueryOptions struct {
	// TransactionFields specifies which transaction fields to fetch
	// If nil or empty, all default fields are fetched
	TransactionFields []string

	// OperationFields specifies which operation fields to fetch
	// If nil or empty, all default fields are fetched
	OperationFields []string

	// AccountFields specifies which account fields to fetch
	// If nil or empty, all default fields are fetched
	AccountFields []string
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

	data, err := executeGraphQL[BuildTransactionData](c, ctx, buildTransactionQuery(), variables)
	if err != nil {
		return nil, err
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

// executeGraphQL executes a GraphQL query and returns the unmarshaled response data
func executeGraphQL[T any](c *Client, ctx context.Context, query string, variables map[string]interface{}) (*T, error) {
	gqlRequest := GraphQLRequest{
		Query:     query,
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

	var data T
	if err := json.Unmarshal(gqlResponse.Data, &data); err != nil {
		return nil, fmt.Errorf("unmarshaling GraphQL data: %w", err)
	}

	return &data, nil
}

// buildPaginationVars builds a variables map from pagination parameters
func buildPaginationVars(first, last *int32, after, before *string) (map[string]interface{}, error) {
	vars := make(map[string]interface{})
	err := validatePaginationParams(first, after, last, before)
	if err != nil {
		return nil, fmt.Errorf("validating pagination params: %w", err)
	}
	if first != nil {
		vars["first"] = *first
	}
	if after != nil {
		vars["after"] = *after
	}
	if last != nil {
		vars["last"] = *last
	}
	if before != nil {
		vars["before"] = *before
	}
	return vars, nil
}

func validatePaginationParams(first *int32, after *string, last *int32, before *string) error {
	if first != nil && last != nil {
		return fmt.Errorf("first and last cannot be used together")
	}

	if after != nil && before != nil {
		return fmt.Errorf("after and before cannot be used together")
	}

	if first != nil && *first <= 0 {
		return fmt.Errorf("first must be greater than 0")
	}

	if last != nil && *last <= 0 {
		return fmt.Errorf("last must be greater than 0")
	}

	if first != nil && before != nil {
		return fmt.Errorf("first and before cannot be used together")
	}

	if last != nil && after != nil {
		return fmt.Errorf("last and after cannot be used together")
	}

	return nil
}

// mergeVariables merges multiple variable maps into one
func mergeVariables(maps ...map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	for _, m := range maps {
		for k, v := range m {
			result[k] = v
		}
	}
	return result
}

func (c *Client) FeeBumpTransaction(ctx context.Context, transactionXDR string) (*types.TransactionEnvelopeResponse, error) {
	variables := map[string]interface{}{
		"input": map[string]interface{}{
			"transactionXDR": transactionXDR,
		},
	}

	data, err := executeGraphQL[CreateFeeBumpTransactionData](c, ctx, createFeeBumpTransactionQuery(), variables)
	if err != nil {
		return nil, err
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

	data, err := executeGraphQL[RegisterAccountData](c, ctx, registerAccountQuery(), variables)
	if err != nil {
		return nil, err
	}

	return &data.RegisterAccount, nil
}

func (c *Client) DeregisterAccount(ctx context.Context, address string) (*types.DeregisterAccountPayload, error) {
	variables := map[string]interface{}{
		"input": map[string]interface{}{
			"address": address,
		},
	}

	data, err := executeGraphQL[DeregisterAccountData](c, ctx, deregisterAccountQuery(), variables)
	if err != nil {
		return nil, err
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

	data, err := executeGraphQL[TransactionByHashData](c, ctx, buildTransactionByHashQuery(fields), variables)
	if err != nil {
		return nil, err
	}

	return data.TransactionByHash, nil
}

func (c *Client) GetTransactions(ctx context.Context, first, last *int32, after, before *string, opts ...*QueryOptions) (*types.TransactionConnection, error) {
	var fields []string
	if len(opts) > 0 && opts[0] != nil {
		fields = opts[0].TransactionFields
	}

	variables, err := buildPaginationVars(first, last, after, before)
	if err != nil {
		return nil, fmt.Errorf("building pagination variables: %w", err)
	}

	data, err := executeGraphQL[TransactionsData](c, ctx, buildTransactionsQuery(fields), variables)
	if err != nil {
		return nil, err
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

	data, err := executeGraphQL[AccountByAddressData](c, ctx, buildAccountByAddressQuery(fields), variables)
	if err != nil {
		return nil, err
	}

	return data.AccountByAddress, nil
}

func (c *Client) GetOperations(ctx context.Context, first, last *int32, after, before *string, opts ...*QueryOptions) (*types.OperationConnection, error) {
	var fields []string
	if len(opts) > 0 && opts[0] != nil {
		fields = opts[0].OperationFields
	}

	variables, err := buildPaginationVars(first, last, after, before)
	if err != nil {
		return nil, fmt.Errorf("building pagination variables: %w", err)
	}

	data, err := executeGraphQL[OperationsData](c, ctx, buildOperationsQuery(fields), variables)
	if err != nil {
		return nil, err
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

	data, err := executeGraphQL[OperationByIDData](c, ctx, buildOperationByIDQuery(fields), variables)
	if err != nil {
		return nil, err
	}

	return data.OperationByID, nil
}

func (c *Client) GetStateChanges(ctx context.Context, first, last *int32, after, before *string) (*types.StateChangeConnection, error) {
	variables, err := buildPaginationVars(first, last, after, before)
	if err != nil {
		return nil, fmt.Errorf("building pagination variables: %w", err)
	}

	data, err := executeGraphQL[StateChangesData](c, ctx, buildStateChangesQuery(), variables)
	if err != nil {
		return nil, err
	}

	return data.StateChanges, nil
}

func (c *Client) GetAccountTransactions(ctx context.Context, address string, first, last *int32, after, before *string, opts ...*QueryOptions) (*types.TransactionConnection, error) {
	var fields []string
	if len(opts) > 0 && opts[0] != nil {
		fields = opts[0].TransactionFields
	}

	paginationVars, err := buildPaginationVars(first, last, after, before)
	if err != nil {
		return nil, fmt.Errorf("building pagination variables: %w", err)
	}

	variables := mergeVariables(
		map[string]interface{}{"address": address},
		paginationVars,
	)

	data, err := executeGraphQL[AccountTransactionsData](c, ctx, buildAccountTransactionsQuery(fields), variables)
	if err != nil {
		return nil, err
	}

	return data.AccountByAddress.Transactions, nil
}

func (c *Client) GetAccountOperations(ctx context.Context, address string, first, last *int32, after, before *string, opts ...*QueryOptions) (*types.OperationConnection, error) {
	var fields []string
	if len(opts) > 0 && opts[0] != nil {
		fields = opts[0].OperationFields
	}

	paginationVars, err := buildPaginationVars(first, last, after, before)
	if err != nil {
		return nil, fmt.Errorf("building pagination variables: %w", err)
	}

	variables := mergeVariables(
		map[string]interface{}{"address": address},
		paginationVars,
	)

	data, err := executeGraphQL[AccountOperationsData](c, ctx, buildAccountOperationsQuery(fields), variables)
	if err != nil {
		return nil, err
	}

	return data.AccountByAddress.Operations, nil
}

func (c *Client) GetAccountStateChanges(ctx context.Context, address string, transactionHash *string, operationID *int64, category *string, reason *string, first, last *int32, after, before *string) (*types.StateChangeConnection, error) {
	paginationVars, err := buildPaginationVars(first, last, after, before)
	if err != nil {
		return nil, fmt.Errorf("building pagination variables: %w", err)
	}

	variables := map[string]interface{}{
		"address": address,
	}

	// Build filter object if any filter parameters are provided
	if transactionHash != nil || operationID != nil || category != nil || reason != nil {
		filter := make(map[string]interface{})
		if transactionHash != nil {
			filter["transactionHash"] = *transactionHash
		}
		if operationID != nil {
			filter["operationId"] = *operationID
		}
		if category != nil {
			filter["category"] = *category
		}
		if reason != nil {
			filter["reason"] = *reason
		}
		variables["filter"] = filter
	}

	variables = mergeVariables(variables, paginationVars)

	data, err := executeGraphQL[AccountStateChangesData](c, ctx, buildAccountStateChangesQuery(), variables)
	if err != nil {
		return nil, err
	}

	return data.AccountByAddress.StateChanges, nil
}

func (c *Client) GetTransactionOperations(ctx context.Context, hash string, first, last *int32, after, before *string, opts ...*QueryOptions) (*types.OperationConnection, error) {
	var fields []string
	if len(opts) > 0 && opts[0] != nil {
		fields = opts[0].OperationFields
	}

	paginationVars, err := buildPaginationVars(first, last, after, before)
	if err != nil {
		return nil, fmt.Errorf("building pagination variables: %w", err)
	}

	variables := mergeVariables(
		map[string]interface{}{"hash": hash},
		paginationVars,
	)

	data, err := executeGraphQL[TransactionOperationsData](c, ctx, buildTransactionOperationsQuery(fields), variables)
	if err != nil {
		return nil, err
	}

	return data.TransactionByHash.Operations, nil
}

func (c *Client) GetTransactionStateChanges(ctx context.Context, hash string, first, last *int32, after, before *string) (*types.StateChangeConnection, error) {
	paginationVars, err := buildPaginationVars(first, last, after, before)
	if err != nil {
		return nil, fmt.Errorf("building pagination variables: %w", err)
	}

	variables := mergeVariables(
		map[string]interface{}{"hash": hash},
		paginationVars,
	)

	data, err := executeGraphQL[TransactionStateChangesData](c, ctx, buildTransactionStateChangesQuery(), variables)
	if err != nil {
		return nil, err
	}

	return data.TransactionByHash.StateChanges, nil
}

func (c *Client) GetOperationStateChanges(ctx context.Context, id int64, first, last *int32, after, before *string) (*types.StateChangeConnection, error) {
	paginationVars, err := buildPaginationVars(first, last, after, before)
	if err != nil {
		return nil, fmt.Errorf("building pagination variables: %w", err)
	}

	variables := mergeVariables(
		map[string]interface{}{"id": id},
		paginationVars,
	)

	data, err := executeGraphQL[OperationStateChangesData](c, ctx, buildOperationStateChangesQuery(), variables)
	if err != nil {
		return nil, err
	}

	return data.OperationByID.StateChanges, nil
}

func (c *Client) GetBalancesByAccountAddress(ctx context.Context, address string) ([]types.Balance, error) {
	variables := map[string]any{
		"address": address,
	}

	data, err := executeGraphQL[BalancesByAccountAddressData](c, ctx, buildBalancesByAccountAddressQuery(), variables)
	if err != nil {
		return nil, err
	}

	// Unmarshal each raw balance into the appropriate concrete type
	balances := make([]types.Balance, 0, len(data.BalancesByAccountAddress))
	for i, rawBalance := range data.BalancesByAccountAddress {
		balance, err := types.UnmarshalBalance(rawBalance)
		if err != nil {
			return nil, fmt.Errorf("unmarshaling balance %d: %w", i, err)
		}
		balances = append(balances, balance)
	}

	return balances, nil
}

func (c *Client) GetBalancesByAccountAddresses(ctx context.Context, addresses []string) ([]*types.AccountBalances, error) {
	variables := map[string]any{
		"addresses": addresses,
	}

	data, err := executeGraphQL[BalancesByAccountAddressesData](c, ctx, buildBalancesByAccountAddressesQuery(), variables)
	if err != nil {
		return nil, err
	}

	return data.BalancesByAccountAddresses, nil
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
