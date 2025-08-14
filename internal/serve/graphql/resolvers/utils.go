package resolvers

import (
	"context"
	"encoding/base64"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/99designs/gqlgen/graphql"

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

type PaginationParams struct {
	Limit     *int32
	Cursor    *int64
	IsDescending bool
}

// NewConnectionWithRelayPagination builds a connection supporting both forward and backward pagination.
func NewConnectionWithRelayPagination[T any, C int64 | string](nodes []T, params PaginationParams, getCursorID func(T) C) *GenericConnection[T] {
	hasNextPage := false
	hasPreviousPage := false

	if !params.IsDescending {
		if int32(len(nodes)) > *params.Limit {
			hasNextPage = true
			nodes = nodes[:*params.Limit]
		}
		hasPreviousPage = params.Cursor != nil
	} else {
		if int32(len(nodes)) > *params.Limit {
			hasPreviousPage = true
			nodes = nodes[1:]
		}
		// In backward pagination, presence of a before-cursor implies there may be newer items (a "next page")
		hasNextPage = params.Cursor != nil
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
		if !params.IsDescending {
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

func GetDBColumnsForFields(ctx context.Context, model any, prefix string) []string {
	opCtx := graphql.GetOperationContext(ctx)
	fields := graphql.CollectFieldsCtx(ctx, nil)

	for _, field := range fields {
		if field.Name == "edges" {
			edgeFields := graphql.CollectFields(opCtx, field.Selections, nil)
			for _, edgeField := range edgeFields {
				if edgeField.Name == "node" {
					nodeFields := graphql.CollectFields(opCtx, edgeField.Selections, nil)
					return prefixDBColumns(prefix, getDBColumns(model, nodeFields))
				}
			}
		}
	}

	return prefixDBColumns(prefix, getDBColumns(model, fields))
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

func getDBColumns(model any, fields []graphql.CollectedField) []string {
	fieldToColumnMap := getColumnMap(model)
	dbColumns := make([]string, 0, len(fields))
	for _, field := range fields {
		if colName, ok := fieldToColumnMap[field.Name]; ok {
			dbColumns = append(dbColumns, colName)
		}
	}
	return dbColumns
}

func prefixDBColumns(prefix string, cols []string) []string {
	if prefix == "" {
		return cols
	}
	prefixedCols := make([]string, len(cols))
	for i, col := range cols {
		prefixedCols[i] = prefix + "." + col
	}
	return prefixedCols
}

func getColumnMap(model any) map[string]string {
	modelType := reflect.TypeOf(model)
	fieldToColumnMap := make(map[string]string)
	for i := 0; i < modelType.NumField(); i++ {
		field := modelType.Field(i)
		jsonTag := field.Tag.Get("json")
		dbTag := field.Tag.Get("db")

		if jsonTag != "" && dbTag != "" && dbTag != "-" {
			jsonFieldName := strings.Split(jsonTag, ",")[0]
			fieldToColumnMap[jsonFieldName] = dbTag
		}
	}
	return fieldToColumnMap
}

func parsePaginationParams(first *int32, after *string, last *int32, before *string, defaultLimit int32) (PaginationParams, error) {
	if first != nil && last != nil {
		return PaginationParams{}, fmt.Errorf("first and last cannot be used together")
	}

	if after != nil && before != nil {
		return PaginationParams{}, fmt.Errorf("after and before cannot be used together")
	}

	var cursor *string
	limit := defaultLimit
	isDescending := false
	if first != nil {
		cursor = after
		limit = *first
	} else if last != nil {
		cursor = before
		limit = *last
		isDescending = true
	}

	decodedCursor, err := decodeInt64Cursor(cursor)
	if err != nil {
		return PaginationParams{}, fmt.Errorf("decoding cursor: %w", err)
	}

	return PaginationParams{
		Cursor:    decodedCursor,
		Limit:     &limit,
		IsDescending: isDescending,
	}, nil
}
