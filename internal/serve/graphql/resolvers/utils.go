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

// NewConnection builds a generic GraphQL connection from a slice of nodes.
func NewConnection[T any, C int64 | string](nodes []T, limit int32, after *string, getCursorID func(T) C) *GenericConnection[T] {
	hasNextPage := false
	if int32(len(nodes)) > limit {
		hasNextPage = true
		nodes = nodes[:limit]
	}

	edges := make([]*GenericEdge[T], len(nodes))
	for i, node := range nodes {
		edges[i] = &GenericEdge[T]{
			Node:   node,
			Cursor: EncodeCursor(getCursorID(node)),
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
		HasPreviousPage: after != nil,
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

func EncodeCursor[T int64 | string](i T) string {
	switch v := any(i).(type) {
	case int64:
		return base64.StdEncoding.EncodeToString([]byte(strconv.FormatInt(v, 10)))
	case string:
		return base64.StdEncoding.EncodeToString([]byte(v))
	default:
		panic(fmt.Sprintf("unsupported type: %T", i))
	}
}

func DecodeInt64Cursor(s *string) (*int64, error) {
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
