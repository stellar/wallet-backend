package resolvers

import (
	"context"
	"reflect"
	"strings"

	"github.com/99designs/gqlgen/graphql"
)

func GetDBColumnsForFields(ctx context.Context, model any, prefix string) []string {
	fields := graphql.CollectFieldsCtx(ctx, nil)
	return prefixDBColumns(prefix, getDBColumns(model, fields))
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
