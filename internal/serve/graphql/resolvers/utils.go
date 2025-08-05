package resolvers

import (
	"context"
	"encoding/base64"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/99designs/gqlgen/graphql"
)

func GetDBColumnsForFields(ctx context.Context, model any, prefix string) []string {
	fields := graphql.CollectFieldsCtx(ctx, nil)
	return prefixDBColumns(prefix, getDBColumns(model, fields))
}

func encodeCursor(i int64) string {
	return base64.StdEncoding.EncodeToString([]byte(strconv.FormatInt(i, 10)))
}

func decodeCursor(s *string) (*int64, error) {
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
