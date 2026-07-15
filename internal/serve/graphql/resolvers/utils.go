package resolvers

import (
	"context"
	"encoding/base64"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/99designs/gqlgen/graphql"
	"github.com/vektah/gqlparser/v2/gqlerror"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	graphqlutils "github.com/stellar/wallet-backend/internal/serve/graphql"
	generated "github.com/stellar/wallet-backend/internal/serve/graphql/generated"
)

// GenericEdge is a generic wrapper for a GraphQL edge.
type GenericEdge[T any] struct {
	Node   T
	Cursor string
}

// GenericConnection is a generic wrapper for a GraphQL connection.
type GenericConnection[T any] struct {
	Edges    []*GenericEdge[T]
	PageInfo *generated.PageInfo
}

// CursorType determines how pagination cursors are parsed and interpreted.
type CursorType int

const (
	// CursorTypeInt64 is used for within-transaction nested resolvers (e.g., operations by ToID)
	CursorTypeInt64 CursorType = iota
	// CursorTypeComposite is used for account-level and root tx/ops queries (ledger_created_at:id format)
	CursorTypeComposite
	// CursorTypeStateChange is used for state change queries (ledger_created_at:to_id:op_id:sc_order format)
	CursorTypeStateChange
	// CursorTypeString is used for opaque string-based cursors.
	CursorTypeString
)

type PaginationParams struct {
	Limit             *int32
	Cursor            *int64
	StringCursor      *string
	CompositeCursor   *types.CompositeCursor
	StateChangeCursor *types.StateChangeCursor
	ForwardPagination bool
	SortOrder         data.SortOrder
}

type baseStateChangeWithCursor struct {
	stateChange generated.BaseStateChange
	cursor      types.StateChangeCursor
}

// NewConnectionWithRelayPagination builds a connection supporting both forward and backward pagination.
func NewConnectionWithRelayPagination[T any, C int64 | string](nodes []T, params PaginationParams, getCursorID func(T) C) *GenericConnection[T] {
	hasNextPage := false
	hasPreviousPage := false

	hasCursor := params.Cursor != nil || params.StringCursor != nil || params.CompositeCursor != nil || params.StateChangeCursor != nil

	if params.ForwardPagination {
		if int32(len(nodes)) > *params.Limit {
			hasNextPage = true
			nodes = nodes[:*params.Limit]
		}
		hasPreviousPage = hasCursor
	} else {
		if int32(len(nodes)) > *params.Limit {
			hasPreviousPage = true
			nodes = nodes[1:]
		}
		// In backward pagination, presence of a before-cursor implies there may be newer items (a "next page")
		hasNextPage = hasCursor
	}

	edges := make([]*GenericEdge[T], len(nodes))
	for i, node := range nodes {
		edges[i] = &GenericEdge[T]{
			Node:   node,
			Cursor: encodeCursor(getCursorID(node)),
		}
	}

	var startCursor, endCursor *string
	if len(edges) > 0 {
		startCursor = &edges[0].Cursor
		endCursor = &edges[len(edges)-1].Cursor
	}

	pageInfo := &generated.PageInfo{
		StartCursor:     startCursor,
		EndCursor:       endCursor,
		HasNextPage:     hasNextPage,
		HasPreviousPage: hasPreviousPage,
	}

	return &GenericConnection[T]{
		Edges:    edges,
		PageInfo: pageInfo,
	}
}

func convertStateChangeToBaseStateChange(stateChanges []*types.StateChangeWithCursor) []*baseStateChangeWithCursor {
	convertedStateChanges := make([]*baseStateChangeWithCursor, len(stateChanges))
	for i, stateChange := range stateChanges {
		convertedStateChanges[i] = &baseStateChangeWithCursor{
			stateChange: convertStateChangeTypes(stateChange.StateChange),
			cursor:      stateChange.StateChangeCursor,
		}
	}

	return convertedStateChanges
}

// convertStateChangeTypes is the resolver for BaseStateChange interface type resolution
// This method determines which concrete GraphQL type to return based on StateChangeCategory
func convertStateChangeTypes(stateChange types.StateChange) generated.BaseStateChange {
	switch stateChange.StateChangeCategory {
	case types.StateChangeCategoryBalance:
		return &types.StandardBalanceStateChangeModel{
			StateChange: stateChange,
		}
	case types.StateChangeCategoryAccount:
		return &types.AccountStateChangeModel{
			StateChange: stateChange,
		}
	case types.StateChangeCategoryReserves:
		return &types.ReservesStateChangeModel{
			StateChange: stateChange,
		}
	case types.StateChangeCategorySigner:
		return &types.SignerStateChangeModel{
			StateChange: stateChange,
		}
	case types.StateChangeCategorySignatureThreshold:
		return &types.SignerThresholdsStateChangeModel{
			StateChange: stateChange,
		}
	case types.StateChangeCategoryFlags:
		return &types.FlagsStateChangeModel{
			StateChange: stateChange,
		}
	case types.StateChangeCategoryMetadata:
		return &types.MetadataStateChangeModel{
			StateChange: stateChange,
		}
	case types.StateChangeCategoryTrustline:
		return &types.TrustlineStateChangeModel{
			StateChange: stateChange,
		}
	case types.StateChangeCategoryBalanceAuthorization:
		return &types.BalanceAuthorizationStateChangeModel{
			StateChange: stateChange,
		}
	default:
		return nil
	}
}

func GetDBColumnsForFields(ctx context.Context, model any) []string {
	opCtx := graphql.GetOperationContext(ctx)
	fields := graphql.CollectFieldsCtx(ctx, nil)

	for _, field := range fields {
		if field.Name == "edges" {
			edgeFields := graphql.CollectFields(opCtx, field.Selections, nil)
			for _, edgeField := range edgeFields {
				if edgeField.Name == "node" {
					nodeFields := graphql.CollectFields(opCtx, edgeField.Selections, nil)
					return getDBColumns(model, nodeFields)
				}
			}
		}
	}

	return getDBColumns(model, fields)
}

func encodeCursor[T int64 | string](i T) string {
	switch v := any(i).(type) {
	case int64:
		return base64.StdEncoding.EncodeToString([]byte(strconv.FormatInt(v, 10)))
	case string:
		return base64.StdEncoding.EncodeToString([]byte(v))
	default:
		panic(fmt.Sprintf("unsupported type: %T", i))
	}
}

func decodeInt64Cursor(s *string) (*int64, error) {
	if s == nil {
		return nil, nil
	}

	decoded, err := base64.StdEncoding.DecodeString(*s)
	if err != nil {
		return nil, badUserInputError(fmt.Sprintf("decoding cursor string %s: %s", *s, err))
	}

	id, err := strconv.ParseInt(string(decoded), 10, 64)
	if err != nil {
		return nil, badUserInputError(fmt.Sprintf("parsing cursor %s: %s", string(decoded), err))
	}

	return &id, nil
}

func decodeStringCursor(s *string) (*string, error) {
	if s == nil {
		return nil, nil
	}

	decoded, err := base64.StdEncoding.DecodeString(*s)
	if err != nil {
		return nil, badUserInputError(fmt.Sprintf("decoding cursor string %s: %s", *s, err))
	}
	decodedStr := string(decoded)

	return &decodedStr, nil
}

func getDBColumns(model any, fields []graphql.CollectedField) []string {
	fieldToColumnMap := getColumnMap(model)
	dbColumns := make([]string, 0, len(fields))
	for _, field := range fields {
		fieldName := field.Name
		// In our graphql schema, the following fields do not match the Go json tags. For e.g. sponsoredAddress in graphql vs sponsoredAccountId in Go struct.
		// So in order to have them resolve to the db column, we need to manually change the field name here.
		// Some GraphQL fields map to multiple database columns (old/new pairs).
		switch fieldName {
		case "type":
			fieldName = "stateChangeCategory"
		case "reason":
			fieldName = "stateChangeReason"
		case "sponsoredAddress":
			fieldName = "sponsoredAccountId"
		case "sponsorAddress":
			fieldName = "sponsorAccountId"
		case "signerAddress":
			fieldName = "signerAccountId"
		case "funderAddress":
			fieldName = "funderAccountId"
		case "deployerAddress":
			fieldName = "deployerAccountId"
		case "destinationAddress":
			fieldName = "destinationAccountId"
		case "limit":
			// GraphQL "limit" field requires both old and new trustline limit columns
			dbColumns = append(dbColumns, "trustline_limit_old", "trustline_limit_new")
			continue
		case "signerWeights":
			// GraphQL "signerWeights" field requires both old and new signer weight columns
			dbColumns = append(dbColumns, "signer_weight_old", "signer_weight_new")
			continue
		case "thresholds":
			// GraphQL "thresholds" field requires both old and new threshold columns
			dbColumns = append(dbColumns, "threshold_old", "threshold_new")
			continue
		}
		if colName, ok := fieldToColumnMap[fieldName]; ok {
			dbColumns = append(dbColumns, colName)
		}
	}
	return dbColumns
}

func getColumnMap(model any) map[string]string {
	modelType := reflect.TypeOf(model)
	fieldToColumnMap := make(map[string]string)
	for i := 0; i < modelType.NumField(); i++ {
		field := modelType.Field(i)
		jsonTag := field.Tag.Get("json")
		dbTag := field.Tag.Get("db")

		// Not all fields have a db tag for e.g. the relationship fields in the indexer model structs
		// dont have a db tag. So we need to check for both jsonTag and dbTag.
		if jsonTag != "" && dbTag != "" && dbTag != "-" {
			jsonFieldName := strings.Split(jsonTag, ",")[0]
			fieldToColumnMap[jsonFieldName] = dbTag
		}
	}
	return fieldToColumnMap
}

// maxAccountPageLimit bounds page size on the account history connections
// (transactions/operations/stateChanges). Those fields fan out per-transaction
// into nested operations and state-change queries, so an uncapped `first` would
// multiply that fan-out across the whole page.
const maxAccountPageLimit int32 = 100

// badUserInputError normalizes client-correctable validation failures to the
// GraphQL error code used elsewhere in the API for bad input.
func badUserInputError(message string) error {
	return &gqlerror.Error{
		Message:    message,
		Extensions: map[string]interface{}{"code": "BAD_USER_INPUT"},
	}
}

// capPaginationLimit returns a BAD_USER_INPUT error if first or last exceeds maxLimit. Shared by
// every capped connection's parseXPaginationParams wrapper below (reject, not clamp, so a client
// gets a clear error instead of a silently-shrunk page).
func capPaginationLimit(first *int32, last *int32, maxLimit int32) error {
	if first != nil && *first > maxLimit {
		return badUserInputError(fmt.Sprintf("first must be less than or equal to %d", maxLimit))
	}
	if last != nil && *last > maxLimit {
		return badUserInputError(fmt.Sprintf("last must be less than or equal to %d", maxLimit))
	}
	return nil
}

// parseAccountPaginationParams caps page size for the account history connections
// before delegating to parsePaginationParams. It mirrors parseBalancePaginationParams'
// cap policy for the multi-source balances connection.
func parseAccountPaginationParams(first *int32, after *string, last *int32, before *string, cursorType CursorType) (PaginationParams, error) {
	if err := capPaginationLimit(first, last, maxAccountPageLimit); err != nil {
		return PaginationParams{}, err
	}
	return parsePaginationParams(first, after, last, before, cursorType)
}

// maxNestedPageLimit bounds page size on nested relation connections (Transaction.operations,
// Transaction.stateChanges, Operation.stateChanges). Their dataloader keys carry the requested
// limit as part of their batch shape, so an uncapped first/last would send an unbounded query
// once a batch's keys share that shape.
const maxNestedPageLimit int32 = 100

// parseNestedPaginationParams caps page size for nested relation connections before delegating to
// parsePaginationParams. It mirrors parseAccountPaginationParams' cap policy (reject, not clamp).
func parseNestedPaginationParams(first *int32, after *string, last *int32, before *string, cursorType CursorType) (PaginationParams, error) {
	if err := capPaginationLimit(first, last, maxNestedPageLimit); err != nil {
		return PaginationParams{}, err
	}
	return parsePaginationParams(first, after, last, before, cursorType)
}

// maxRootPageLimit bounds page size on the root connections (Query.transactions/operations/
// stateChanges). Unlike the account-scoped connections, these have no other bound (no single
// account to scope the scan to), so an uncapped first is the most expensive case: it both sizes
// the compressed-chunk scan/sort and, combined with D3's time-bounded windowing, determines how
// much of the window has to be materialized before paging.
const maxRootPageLimit int32 = 100

// parseRootPaginationParams caps page size for the root transactions/operations/stateChanges
// connections before delegating to parsePaginationParams (SQL-04). It mirrors
// parseAccountPaginationParams' cap policy (reject, not clamp).
func parseRootPaginationParams(first *int32, after *string, last *int32, before *string, cursorType CursorType) (PaginationParams, error) {
	if err := capPaginationLimit(first, last, maxRootPageLimit); err != nil {
		return PaginationParams{}, err
	}
	return parsePaginationParams(first, after, last, before, cursorType)
}

func parsePaginationParams(first *int32, after *string, last *int32, before *string, cursorType CursorType) (PaginationParams, error) {
	// validatePaginationParams and the cursor decoders below already return a self-describing,
	// client-safe *gqlerror.Error (BAD_USER_INPUT); returning it unwrapped avoids stuttering,
	// noisy prefixes (fmt.Errorf's %w formatting would otherwise sit alongside gqlerror.Error's own
	// "input: " location prefix) while errors.As still finds it fine through the call chain.
	err := validatePaginationParams(first, after, last, before)
	if err != nil {
		return PaginationParams{}, err
	}

	var cursor *string
	limit := graphqlutils.DefaultPageLimit
	forwardPagination := true
	sortOrder := data.ASC
	if first != nil {
		cursor = after
		limit = *first
	} else if last != nil {
		cursor = before
		limit = *last
		forwardPagination = false
		sortOrder = data.DESC
	}

	paginationParams := PaginationParams{
		Limit:             &limit,
		SortOrder:         sortOrder,
		ForwardPagination: forwardPagination,
	}

	switch cursorType {
	case CursorTypeStateChange:
		stateChangeCursor, err := parseStateChangeCursor(cursor)
		if err != nil {
			return PaginationParams{}, err
		}
		paginationParams.StateChangeCursor = stateChangeCursor
	case CursorTypeComposite:
		compositeCursor, err := parseCompositeCursor(cursor)
		if err != nil {
			return PaginationParams{}, err
		}
		paginationParams.CompositeCursor = compositeCursor
	case CursorTypeString:
		decodedCursor, err := decodeStringCursor(cursor)
		if err != nil {
			return PaginationParams{}, err
		}
		paginationParams.StringCursor = decodedCursor
	default:
		decodedCursor, err := decodeInt64Cursor(cursor)
		if err != nil {
			return PaginationParams{}, err
		}
		paginationParams.Cursor = decodedCursor
	}

	return paginationParams, nil
}

func parseStateChangeCursor(s *string) (*types.StateChangeCursor, error) {
	if s == nil {
		return nil, nil
	}

	decodedCursor, err := decodeStringCursor(s)
	if err != nil {
		return nil, err
	}

	parts := strings.Split(*decodedCursor, ":")
	if len(parts) != 4 {
		return nil, badUserInputError(fmt.Sprintf("invalid cursor format: %s (expected format: ledger_created_at_nano:to_id:operation_id:state_change_id)", *s))
	}

	nanos, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return nil, badUserInputError(fmt.Sprintf("parsing ledger_created_at: %s", err))
	}

	toID, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return nil, badUserInputError(fmt.Sprintf("parsing to_id: %s", err))
	}

	operationID, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return nil, badUserInputError(fmt.Sprintf("parsing operation_id: %s", err))
	}

	stateChangeID, err := strconv.ParseInt(parts[3], 10, 64)
	if err != nil {
		return nil, badUserInputError(fmt.Sprintf("parsing state_change_id: %s", err))
	}

	return &types.StateChangeCursor{
		LedgerCreatedAt: time.Unix(0, nanos),
		ToID:            toID,
		OperationID:     operationID,
		StateChangeID:   stateChangeID,
	}, nil
}

func parseCompositeCursor(s *string) (*types.CompositeCursor, error) {
	if s == nil {
		return nil, nil
	}

	decodedCursor, err := decodeStringCursor(s)
	if err != nil {
		return nil, err
	}

	parts := strings.Split(*decodedCursor, ":")
	if len(parts) != 2 {
		return nil, badUserInputError(fmt.Sprintf("invalid cursor format: %s (expected format: ledger_created_at_nano:id)", *s))
	}

	nanos, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return nil, badUserInputError(fmt.Sprintf("parsing ledger_created_at: %s", err))
	}

	id, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return nil, badUserInputError(fmt.Sprintf("parsing id: %s", err))
	}

	return &types.CompositeCursor{
		LedgerCreatedAt: time.Unix(0, nanos),
		ID:              id,
	}, nil
}

// buildTimeRange constructs a *data.TimeRange from optional since/until params.
// Returns an error if both are provided and until is before since.
// Returns nil if both are nil (no time range filtering).
func buildTimeRange(since *time.Time, until *time.Time) (*data.TimeRange, error) {
	if since == nil && until == nil {
		return nil, nil
	}
	if since != nil && until != nil && until.Before(*since) {
		return nil, badUserInputError("until must not be before since")
	}
	return &data.TimeRange{Since: since, Until: until}, nil
}

// defaultRootQueryWindow is the time window applied to the root transactions/operations/
// stateChanges connections when neither since nor until is given (D3). Root connections have no
// other bound (an account-scoped connection is inherently bounded to one address; a root
// connection is not), so an unbounded first page would scan/sort the full history. Defaulting to
// a recent window keeps that bounded while still letting a client reach older history via an
// explicit since.
const defaultRootQueryWindow = 7 * 24 * time.Hour

// buildRootTimeRange builds the *data.TimeRange for a root transactions/operations/stateChanges
// connection. Unlike buildTimeRange, it defaults to [now-defaultRootQueryWindow, now] when both
// since and until are omitted; when either is given, it defers to buildTimeRange and uses exactly
// what was given (open-ended on the unset side).
func buildRootTimeRange(since *time.Time, until *time.Time) (*data.TimeRange, error) {
	if since == nil && until == nil {
		now := time.Now()
		windowStart := now.Add(-defaultRootQueryWindow)
		return &data.TimeRange{Since: &windowStart, Until: &now}, nil
	}
	return buildTimeRange(since, until)
}

func validatePaginationParams(first *int32, after *string, last *int32, before *string) error {
	if first != nil && last != nil {
		return badUserInputError("first and last cannot be used together")
	}

	if after != nil && before != nil {
		return badUserInputError("after and before cannot be used together")
	}

	if first != nil && *first <= 0 {
		return badUserInputError("first must be greater than 0")
	}

	if last != nil && *last <= 0 {
		return badUserInputError("last must be greater than 0")
	}

	if first != nil && before != nil {
		return badUserInputError("first and before cannot be used together")
	}

	if last != nil && after != nil {
		return badUserInputError("last and after cannot be used together")
	}

	return nil
}
