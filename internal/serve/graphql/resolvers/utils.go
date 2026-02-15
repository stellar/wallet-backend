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

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	generated "github.com/stellar/wallet-backend/internal/serve/graphql/generated"
)

const (
	DefaultLimit = int32(50)
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
)

type PaginationParams struct {
	Limit             *int32
	Cursor            *int64
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

	hasCursor := params.Cursor != nil || params.CompositeCursor != nil || params.StateChangeCursor != nil

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
		if params.ForwardPagination {
			endCursor = &edges[len(edges)-1].Cursor
		} else {
			endCursor = &edges[0].Cursor
		}
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
			cursor:      stateChange.Cursor,
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
		return nil, fmt.Errorf("decoding cursor string %s: %w", *s, err)
	}

	id, err := strconv.ParseInt(string(decoded), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parsing cursor %s: %w", string(decoded), err)
	}

	return &id, nil
}

func decodeStringCursor(s *string) (*string, error) {
	if s == nil {
		return nil, nil
	}

	decoded, err := base64.StdEncoding.DecodeString(*s)
	if err != nil {
		return nil, fmt.Errorf("decoding cursor string %s: %w", *s, err)
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

func parsePaginationParams(first *int32, after *string, last *int32, before *string, cursorType CursorType) (PaginationParams, error) {
	err := validatePaginationParams(first, after, last, before)
	if err != nil {
		return PaginationParams{}, fmt.Errorf("validating pagination params: %w", err)
	}

	var cursor *string
	limit := DefaultLimit
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
			return PaginationParams{}, fmt.Errorf("parsing state change cursor: %w", err)
		}
		paginationParams.StateChangeCursor = stateChangeCursor
	case CursorTypeComposite:
		compositeCursor, err := parseCompositeCursor(cursor)
		if err != nil {
			return PaginationParams{}, fmt.Errorf("parsing composite cursor: %w", err)
		}
		paginationParams.CompositeCursor = compositeCursor
	default:
		decodedCursor, err := decodeInt64Cursor(cursor)
		if err != nil {
			return PaginationParams{}, fmt.Errorf("decoding cursor: %w", err)
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
		return nil, fmt.Errorf("decoding cursor: %w", err)
	}

	parts := strings.Split(*decodedCursor, ":")
	if len(parts) != 4 {
		return nil, fmt.Errorf("invalid cursor format: %s (expected format: ledger_created_at_nano:to_id:operation_id:state_change_order)", *s)
	}

	nanos, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parsing ledger_created_at: %w", err)
	}

	toID, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parsing to_id: %w", err)
	}

	operationID, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parsing operation_id: %w", err)
	}

	stateChangeOrder, err := strconv.ParseInt(parts[3], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parsing state_change_order: %w", err)
	}

	return &types.StateChangeCursor{
		LedgerCreatedAt:  time.Unix(0, nanos),
		ToID:             toID,
		OperationID:      operationID,
		StateChangeOrder: stateChangeOrder,
	}, nil
}

func parseCompositeCursor(s *string) (*types.CompositeCursor, error) {
	if s == nil {
		return nil, nil
	}

	decodedCursor, err := decodeStringCursor(s)
	if err != nil {
		return nil, fmt.Errorf("decoding cursor: %w", err)
	}

	parts := strings.Split(*decodedCursor, ":")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid cursor format: %s (expected format: ledger_created_at_nano:id)", *s)
	}

	nanos, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parsing ledger_created_at: %w", err)
	}

	id, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parsing id: %w", err)
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
		return nil, fmt.Errorf("until must not be before since")
	}
	return &data.TimeRange{Since: since, Until: until}, nil
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
