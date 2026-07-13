package wbclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/stellar/wallet-backend/internal/utils"
	"github.com/stellar/wallet-backend/pkg/wbclient/auth"
	"github.com/stellar/wallet-backend/pkg/wbclient/types"
)

// ErrAccountNotFound is returned by account-scoped queries when the
// GraphQL server reports the account does not exist (accountByAddress
// returned null). Distinct from schema/pagination failures so callers
// can classify it as an address-scoped error rather than a systemic
// upstream failure. Use errors.Is(err, wbclient.ErrAccountNotFound).
var ErrAccountNotFound = errors.New("account not found")

// ErrTransactionNotFound is returned by transaction-scoped queries when the
// GraphQL server reports the transaction does not exist (transactionByHash
// returned null). Distinct from schema/pagination failures so callers can
// classify it as a hash-scoped error rather than a systemic upstream failure.
// Use errors.Is(err, wbclient.ErrTransactionNotFound).
var ErrTransactionNotFound = errors.New("transaction not found")

// ErrOperationNotFound is returned by operation-scoped queries when the
// GraphQL server reports the operation does not exist (operationById returned
// null). Distinct from schema/pagination failures so callers can classify it
// as an id-scoped error rather than a systemic upstream failure. Use
// errors.Is(err, wbclient.ErrOperationNotFound).
var ErrOperationNotFound = errors.New("operation not found")

// GraphQLRequest is the JSON body of a GraphQL POST: an operation document
// and its variables.
type GraphQLRequest struct {
	Query     string         `json:"query"`
	Variables map[string]any `json:"variables,omitempty"`
}

// GraphQLResponse is the top-level GraphQL response envelope. Data holds the
// raw result object and Errors any errors the server reported for the operation.
type GraphQLResponse struct {
	Data   json.RawMessage `json:"data,omitempty"`
	Errors []GraphQLError  `json:"errors,omitempty"`
}

// GraphQLError is a single error entry from a GraphQL response. Extensions
// carries server-defined metadata; Extensions["code"] (e.g. "BAD_USER_INPUT")
// classifies the error when present.
type GraphQLError struct {
	Message    string         `json:"message"`
	Extensions map[string]any `json:"extensions,omitempty"`
}

// GraphQLErrors is the non-empty set of errors returned in a GraphQL response
// body. It implements error; callers can errors.As it and inspect each error's
// Extensions["code"] (e.g. "BAD_USER_INPUT") for classification.
type GraphQLErrors []GraphQLError

// Error joins every error's message into one string, prefixing each with its
// Extensions["code"] when present (e.g. "BAD_USER_INPUT: first must be...; ...").
func (e GraphQLErrors) Error() string {
	msgs := make([]string, len(e))
	for i, ge := range e {
		if code, ok := ge.Extensions["code"].(string); ok && code != "" {
			msgs[i] = code + ": " + ge.Message
		} else {
			msgs[i] = ge.Message
		}
	}
	return strings.Join(msgs, "; ")
}

// TransactionByHashData is the result shape of the transactionByHash query.
type TransactionByHashData struct {
	TransactionByHash *types.GraphQLTransaction `json:"transactionByHash"`
}

// AccountByAddressData is the result shape of the accountByAddress query.
type AccountByAddressData struct {
	AccountByAddress *types.Account `json:"accountByAddress"`
}

// OperationByIDData is the result shape of the operationById query.
type OperationByIDData struct {
	OperationByID *types.Operation `json:"operationById"`
}

// AccountTransactionsData is the result shape of the accountByAddress.transactions query.
type AccountTransactionsData struct {
	AccountByAddress *struct {
		Transactions *types.TransactionConnection `json:"transactions"`
	} `json:"accountByAddress"`
}

// AccountOperationsData is the result shape of the accountByAddress.operations query.
type AccountOperationsData struct {
	AccountByAddress *struct {
		Operations *types.OperationConnection `json:"operations"`
	} `json:"accountByAddress"`
}

// AccountStateChangesData is the result shape of the accountByAddress.stateChanges query.
type AccountStateChangesData struct {
	AccountByAddress *struct {
		StateChanges *types.StateChangeConnection `json:"stateChanges"`
	} `json:"accountByAddress"`
}

// TransactionOperationsData is the result shape of the transactionByHash.operations query.
type TransactionOperationsData struct {
	TransactionByHash *struct {
		Operations *types.OperationConnection `json:"operations"`
	} `json:"transactionByHash"`
}

// TransactionStateChangesData is the result shape of the transactionByHash.stateChanges query.
type TransactionStateChangesData struct {
	TransactionByHash *struct {
		StateChanges *types.StateChangeConnection `json:"stateChanges"`
	} `json:"transactionByHash"`
}

// OperationStateChangesData is the result shape of the operationById.stateChanges query.
type OperationStateChangesData struct {
	OperationByID *struct {
		StateChanges *types.StateChangeConnection `json:"stateChanges"`
	} `json:"operationById"`
}

// AccountBalancesData is the result shape of the accountByAddress.balances query.
type AccountBalancesData struct {
	AccountByAddress *struct {
		Balances *types.BalanceConnection `json:"balances"`
	} `json:"accountByAddress"`
}

// AccountTransactionsWithOpsAndStateChangesData is the result shape of the
// accountByAddress.transactions query that embeds per-transaction operations
// and state changes.
type AccountTransactionsWithOpsAndStateChangesData struct {
	AccountByAddress *struct {
		Transactions *types.AccountTransactionConnection `json:"transactions"`
	} `json:"accountByAddress"`
}

type BlendPoolsData struct {
	BlendPools []types.BlendPool `json:"blendPools"`
}

type BlendPoolData struct {
	BlendPool *types.BlendPool `json:"blendPool"`
}

type BlendEarnOptionsData struct {
	BlendEarnOptions []types.BlendEarnOption `json:"blendEarnOptions"`
}

type AccountBlendPositionsData struct {
	AccountByAddress *struct {
		BlendPositions *types.BlendAccountPositions `json:"blendPositions"`
	} `json:"accountByAddress"`
}

// QueryOptions allows clients to specify which fields to fetch for each entity type.
type QueryOptions struct {
	// TransactionFields specifies which transaction fields to fetch.
	// If nil or empty, all default fields are fetched.
	TransactionFields []string

	// OperationFields specifies which operation fields to fetch.
	// If nil or empty, all default fields are fetched.
	OperationFields []string

	// AccountFields specifies which account fields to fetch.
	// If nil or empty, all default fields are fetched.
	AccountFields []string
}

// Client is a GraphQL client for the wallet-backend API. It signs each request
// with RequestSigner when one is set and targets the GraphQL endpoint at BaseURL.
type Client struct {
	HTTPClient    *http.Client
	BaseURL       string
	RequestSigner auth.HTTPRequestSigner
}

// NewClient returns a Client that talks to the wallet-backend GraphQL API at
// baseURL, signing each request with requestSigner (may be nil for unauthenticated
// use). It uses an HTTP client with a 30-second timeout.
func NewClient(baseURL string, requestSigner auth.HTTPRequestSigner) *Client {
	return &Client{
		HTTPClient:    &http.Client{Timeout: 30 * time.Second},
		BaseURL:       baseURL,
		RequestSigner: requestSigner,
	}
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

// executeGraphQL executes a GraphQL query and returns the unmarshaled response data.
func executeGraphQL[T any](c *Client, ctx context.Context, query string, variables map[string]any) (*T, error) {
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
		return nil, fmt.Errorf("GraphQL request returned errors: %w", GraphQLErrors(gqlResponse.Errors))
	}

	var data T
	if err := json.Unmarshal(gqlResponse.Data, &data); err != nil {
		return nil, fmt.Errorf("unmarshaling GraphQL data: %w", err)
	}

	return &data, nil
}

// mergeVariables merges multiple variable maps into one.
func mergeVariables(sources ...map[string]any) map[string]any {
	result := make(map[string]any)
	for _, m := range sources {
		maps.Copy(result, m)
	}
	return result
}

// GetTransactionByHash fetches a single transaction by its hash. Pass a
// *QueryOptions to restrict the transaction fields fetched; omit it (or pass nil)
// for the default field set. Returns ErrTransactionNotFound if the transaction
// does not exist.
func (c *Client) GetTransactionByHash(ctx context.Context, hash string, opts ...*QueryOptions) (*types.GraphQLTransaction, error) {
	var fields []string
	if len(opts) > 0 && opts[0] != nil {
		fields = opts[0].TransactionFields
	}

	variables := map[string]any{
		"hash": hash,
	}

	data, err := executeGraphQL[TransactionByHashData](c, ctx, buildTransactionByHashQuery(fields), variables)
	if err != nil {
		return nil, err
	}

	if data.TransactionByHash == nil {
		return nil, fmt.Errorf("%w: %s", ErrTransactionNotFound, hash)
	}

	return data.TransactionByHash, nil
}

// GetAccountByAddress fetches a single account by its address. Pass a
// *QueryOptions to restrict the account fields fetched; omit it (or pass nil)
// for the default field set. Returns ErrAccountNotFound if the account does not
// exist.
func (c *Client) GetAccountByAddress(ctx context.Context, address string, opts ...*QueryOptions) (*types.Account, error) {
	var fields []string
	if len(opts) > 0 && opts[0] != nil {
		fields = opts[0].AccountFields
	}

	variables := map[string]any{
		"address": address,
	}

	data, err := executeGraphQL[AccountByAddressData](c, ctx, buildAccountByAddressQuery(fields), variables)
	if err != nil {
		return nil, err
	}

	if data.AccountByAddress == nil {
		return nil, fmt.Errorf("%w: %s", ErrAccountNotFound, address)
	}

	return data.AccountByAddress, nil
}

// GetOperationByID fetches a single operation by its ID. Pass a *QueryOptions
// to restrict the operation fields fetched; omit it (or pass nil) for the default
// field set. Returns ErrOperationNotFound if the operation does not exist.
func (c *Client) GetOperationByID(ctx context.Context, id int64, opts ...*QueryOptions) (*types.Operation, error) {
	var fields []string
	if len(opts) > 0 && opts[0] != nil {
		fields = opts[0].OperationFields
	}

	variables := map[string]any{
		"id": id,
	}

	data, err := executeGraphQL[OperationByIDData](c, ctx, buildOperationByIDQuery(fields), variables)
	if err != nil {
		return nil, err
	}

	if data.OperationByID == nil {
		return nil, fmt.Errorf("%w: %d", ErrOperationNotFound, id)
	}

	return data.OperationByID, nil
}

// GetAccountTransactions fetches a page of an account's transactions. A nil
// timeRange applies no time bounds and a nil page requests the server's default
// page. Pass a *QueryOptions to restrict the transaction fields fetched. Returns
// ErrAccountNotFound if the account does not exist.
func (c *Client) GetAccountTransactions(ctx context.Context, address string, timeRange *TimeRange, page *Page, opts ...*QueryOptions) (*types.TransactionConnection, error) {
	var fields []string
	if len(opts) > 0 && opts[0] != nil {
		fields = opts[0].TransactionFields
	}

	paginationVars, err := buildPaginationVars(page)
	if err != nil {
		return nil, fmt.Errorf("building pagination variables: %w", err)
	}

	variables := mergeVariables(
		map[string]any{"address": address},
		paginationVars,
		buildTimeRangeVars(timeRange),
	)

	data, err := executeGraphQL[AccountTransactionsData](c, ctx, buildAccountTransactionsQuery(fields), variables)
	if err != nil {
		return nil, err
	}

	if data.AccountByAddress == nil {
		return nil, fmt.Errorf("%w: %s", ErrAccountNotFound, address)
	}
	if data.AccountByAddress.Transactions == nil {
		return nil, fmt.Errorf("account transactions response missing required transactions field for address %s", address)
	}

	return data.AccountByAddress.Transactions, nil
}

// GetAccountTransactionsWithOpsAndStateChanges fetches a page of an account's
// transactions with that account's operations and state changes embedded per
// transaction, in a single GraphQL call. A nil timeRange applies no time bounds
// and a nil page requests the server's default page. Returns ErrAccountNotFound
// if the account does not exist.
func (c *Client) GetAccountTransactionsWithOpsAndStateChanges(ctx context.Context, address string, timeRange *TimeRange, page *Page) (*types.AccountTransactionConnection, error) {
	paginationVars, err := buildPaginationVars(page)
	if err != nil {
		return nil, fmt.Errorf("building pagination variables: %w", err)
	}

	variables := mergeVariables(
		map[string]any{"address": address},
		paginationVars,
		buildTimeRangeVars(timeRange),
	)

	data, err := executeGraphQL[AccountTransactionsWithOpsAndStateChangesData](c, ctx, buildAccountTransactionsWithOpsAndStateChangesQuery(), variables)
	if err != nil {
		return nil, err
	}

	if data.AccountByAddress == nil {
		return nil, fmt.Errorf("%w: %s", ErrAccountNotFound, address)
	}
	if data.AccountByAddress.Transactions == nil {
		return nil, fmt.Errorf("account transactions response missing required transactions field for address %s", address)
	}

	return data.AccountByAddress.Transactions, nil
}

// GetAccountOperations fetches a page of an account's operations. A nil timeRange
// applies no time bounds and a nil page requests the server's default page. Pass
// a *QueryOptions to restrict the operation fields fetched. Returns
// ErrAccountNotFound if the account does not exist.
func (c *Client) GetAccountOperations(ctx context.Context, address string, timeRange *TimeRange, page *Page, opts ...*QueryOptions) (*types.OperationConnection, error) {
	var fields []string
	if len(opts) > 0 && opts[0] != nil {
		fields = opts[0].OperationFields
	}

	paginationVars, err := buildPaginationVars(page)
	if err != nil {
		return nil, fmt.Errorf("building pagination variables: %w", err)
	}

	variables := mergeVariables(
		map[string]any{"address": address},
		paginationVars,
		buildTimeRangeVars(timeRange),
	)

	data, err := executeGraphQL[AccountOperationsData](c, ctx, buildAccountOperationsQuery(fields), variables)
	if err != nil {
		return nil, err
	}

	if data.AccountByAddress == nil {
		return nil, fmt.Errorf("%w: %s", ErrAccountNotFound, address)
	}
	if data.AccountByAddress.Operations == nil {
		return nil, fmt.Errorf("account operations response missing required operations field for address %s", address)
	}

	return data.AccountByAddress.Operations, nil
}

// GetAccountStateChanges fetches a page of an account's state changes. A nil
// filter applies no filtering, a nil timeRange applies no time bounds, and a nil
// page requests the server's default page. Returns ErrAccountNotFound if the
// account does not exist.
func (c *Client) GetAccountStateChanges(ctx context.Context, address string, filter *StateChangeFilter, timeRange *TimeRange, page *Page) (*types.StateChangeConnection, error) {
	paginationVars, err := buildPaginationVars(page)
	if err != nil {
		return nil, fmt.Errorf("building pagination variables: %w", err)
	}

	variables := mergeVariables(
		map[string]any{"address": address},
		buildStateChangeFilterVars(filter),
		buildTimeRangeVars(timeRange),
		paginationVars,
	)

	data, err := executeGraphQL[AccountStateChangesData](c, ctx, buildAccountStateChangesQuery(), variables)
	if err != nil {
		return nil, err
	}

	if data.AccountByAddress == nil {
		return nil, fmt.Errorf("%w: %s", ErrAccountNotFound, address)
	}
	if data.AccountByAddress.StateChanges == nil {
		return nil, fmt.Errorf("account state changes response missing required stateChanges field for address %s", address)
	}

	return data.AccountByAddress.StateChanges, nil
}

// GetTransactionOperations fetches a page of a transaction's operations. A nil
// page requests the server's default page. Pass a *QueryOptions to restrict the
// operation fields fetched. Returns ErrTransactionNotFound if the transaction
// does not exist.
func (c *Client) GetTransactionOperations(ctx context.Context, hash string, page *Page, opts ...*QueryOptions) (*types.OperationConnection, error) {
	var fields []string
	if len(opts) > 0 && opts[0] != nil {
		fields = opts[0].OperationFields
	}

	paginationVars, err := buildPaginationVars(page)
	if err != nil {
		return nil, fmt.Errorf("building pagination variables: %w", err)
	}

	variables := mergeVariables(
		map[string]any{"hash": hash},
		paginationVars,
	)

	data, err := executeGraphQL[TransactionOperationsData](c, ctx, buildTransactionOperationsQuery(fields), variables)
	if err != nil {
		return nil, err
	}

	if data.TransactionByHash == nil {
		return nil, fmt.Errorf("%w: %s", ErrTransactionNotFound, hash)
	}
	if data.TransactionByHash.Operations == nil {
		return nil, fmt.Errorf("transaction operations response missing required operations field for hash %s", hash)
	}

	return data.TransactionByHash.Operations, nil
}

// GetTransactionStateChanges fetches a page of a transaction's state changes. A
// nil page requests the server's default page. Returns ErrTransactionNotFound if
// the transaction does not exist.
func (c *Client) GetTransactionStateChanges(ctx context.Context, hash string, page *Page) (*types.StateChangeConnection, error) {
	paginationVars, err := buildPaginationVars(page)
	if err != nil {
		return nil, fmt.Errorf("building pagination variables: %w", err)
	}

	variables := mergeVariables(
		map[string]any{"hash": hash},
		paginationVars,
	)

	data, err := executeGraphQL[TransactionStateChangesData](c, ctx, buildTransactionStateChangesQuery(), variables)
	if err != nil {
		return nil, err
	}

	if data.TransactionByHash == nil {
		return nil, fmt.Errorf("%w: %s", ErrTransactionNotFound, hash)
	}
	if data.TransactionByHash.StateChanges == nil {
		return nil, fmt.Errorf("transaction state changes response missing required stateChanges field for hash %s", hash)
	}

	return data.TransactionByHash.StateChanges, nil
}

// GetOperationStateChanges fetches a page of an operation's state changes. A nil
// page requests the server's default page. Returns ErrOperationNotFound if the
// operation does not exist.
func (c *Client) GetOperationStateChanges(ctx context.Context, id int64, page *Page) (*types.StateChangeConnection, error) {
	paginationVars, err := buildPaginationVars(page)
	if err != nil {
		return nil, fmt.Errorf("building pagination variables: %w", err)
	}

	variables := mergeVariables(
		map[string]any{"id": id},
		paginationVars,
	)

	data, err := executeGraphQL[OperationStateChangesData](c, ctx, buildOperationStateChangesQuery(), variables)
	if err != nil {
		return nil, err
	}

	if data.OperationByID == nil {
		return nil, fmt.Errorf("%w: %d", ErrOperationNotFound, id)
	}
	if data.OperationByID.StateChanges == nil {
		return nil, fmt.Errorf("operation state changes response missing required stateChanges field for id %d", id)
	}

	return data.OperationByID.StateChanges, nil
}

// GetAccountBalances fetches a page of an account's balances. A nil page requests
// the server's default page. Returns ErrAccountNotFound if the account does not
// exist. Use GetAllAccountBalances to retrieve every balance without managing
// pagination yourself.
func (c *Client) GetAccountBalances(ctx context.Context, address string, page *Page) (*types.BalanceConnection, error) {
	paginationVars, err := buildPaginationVars(page)
	if err != nil {
		return nil, fmt.Errorf("building pagination variables: %w", err)
	}

	variables := mergeVariables(
		map[string]any{"address": address},
		paginationVars,
	)

	data, err := executeGraphQL[AccountBalancesData](c, ctx, buildAccountBalancesQuery(), variables)
	if err != nil {
		return nil, err
	}

	if data.AccountByAddress == nil {
		return nil, fmt.Errorf("%w: %s", ErrAccountNotFound, address)
	}
	if data.AccountByAddress.Balances == nil {
		return nil, fmt.Errorf("account balances response missing required balances field for address %s", address)
	}

	return data.AccountByAddress.Balances, nil
}

// GetAllAccountBalances returns every balance for the given address by
// driving GetAccountBalances forward through the cursor sequence in
// fixed-size pages. Returns a non-nil empty slice when the account has
// no balances. Use this when you want a flat list of balances for an
// account; use GetAccountBalances directly when you need explicit
// control over page size or position.
//
// Returns an error if the server's pagination response is internally
// inconsistent — HasNextPage=true with a missing EndCursor, or the same
// EndCursor returned on two consecutive pages (which would otherwise loop
// forever). Both indicate a server-side pagination bug.
func (c *Client) GetAllAccountBalances(ctx context.Context, address string) ([]types.Balance, error) {
	first := int32(100)

	var after *string
	balances := make([]types.Balance, 0)

	for {
		connection, err := c.GetAccountBalances(ctx, address, &Page{First: &first, After: after})
		if err != nil {
			return nil, fmt.Errorf("getting account balances page: %w", err)
		}
		if connection == nil {
			return nil, fmt.Errorf("getting account balances page: missing required balances connection")
		}
		if connection.PageInfo == nil {
			return nil, fmt.Errorf("getting account balances page: missing required pageInfo")
		}

		balances = append(balances, connection.Balances()...)

		if !connection.PageInfo.HasNextPage {
			break
		}

		if connection.PageInfo.EndCursor == nil {
			return nil, fmt.Errorf("paginating account balances: server reported HasNextPage=true but did not return an EndCursor")
		}

		if after != nil && *after == *connection.PageInfo.EndCursor {
			return nil, fmt.Errorf("paginating account balances: server returned the same EndCursor (%q) on two consecutive pages; pagination is not advancing", *connection.PageInfo.EndCursor)
		}

		after = connection.PageInfo.EndCursor
	}

	return balances, nil
}

// GetBlendPools returns the pool-wide catalog view of every Blend v2 pool.
func (c *Client) GetBlendPools(ctx context.Context) ([]types.BlendPool, error) {
	data, err := executeGraphQL[BlendPoolsData](c, ctx, buildBlendPoolsQuery(), nil)
	if err != nil {
		return nil, err
	}

	return data.BlendPools, nil
}

// GetBlendPool returns one Blend v2 pool's catalog view, or nil if the pool is unknown to the server.
func (c *Client) GetBlendPool(ctx context.Context, address string) (*types.BlendPool, error) {
	variables := map[string]interface{}{
		"address": address,
	}

	data, err := executeGraphQL[BlendPoolData](c, ctx, buildBlendPoolQuery(), variables)
	if err != nil {
		return nil, err
	}

	return data.BlendPool, nil
}

// GetBlendEarnOptions returns the "where can I earn this asset" catalog view across all Blend v2 pools.
func (c *Client) GetBlendEarnOptions(ctx context.Context) ([]types.BlendEarnOption, error) {
	data, err := executeGraphQL[BlendEarnOptionsData](c, ctx, buildBlendEarnOptionsQuery(), nil)
	if err != nil {
		return nil, err
	}

	return data.BlendEarnOptions, nil
}

// GetAccountBlendPositions returns an account's Blend v2 lending, collateral, and backstop positions.
func (c *Client) GetAccountBlendPositions(ctx context.Context, address string) (*types.BlendAccountPositions, error) {
	variables := map[string]interface{}{
		"address": address,
	}

	data, err := executeGraphQL[AccountBlendPositionsData](c, ctx, buildAccountBlendPositionsQuery(), variables)
	if err != nil {
		return nil, err
	}

	if data.AccountByAddress == nil {
		return nil, fmt.Errorf("%w: %s", ErrAccountNotFound, address)
	}

	return data.AccountByAddress.BlendPositions, nil
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
