package resolvers

import (
	"context"
	"reflect"
	"strings"

	"github.com/99designs/gqlgen/graphql"
)

func GetDBColumnsForQuery(ctx context.Context, model any) []string {
	fieldNames := getFieldNames(ctx)
	modelType := reflect.TypeOf(model)
	dbColumns := make([]string, 0, len(fieldNames))
	// Create a map of GraphQL field names (camelCase from json tag) to DB column names (snake_case from db tag)
	fieldToColumnMap := make(map[string]string)
	for i := 0; i < modelType.NumField(); i++ {
		field := modelType.Field(i)
		jsonTag := field.Tag.Get("json")
		dbTag := field.Tag.Get("db")

		if jsonTag != "" && dbTag != "" && dbTag != "-" {
			// Assumes the JSON tag is "fieldName,omitempty"
			jsonFieldName := strings.Split(jsonTag, ",")[0]
			fieldToColumnMap[jsonFieldName] = dbTag
		}
	}
	for _, fieldName := range fieldNames {
		dbColumns = append(dbColumns, fieldToColumnMap[fieldName])
	}
	return dbColumns
}

func getFieldNames(ctx context.Context) []string {
	fields := graphql.CollectFieldsCtx(ctx, nil)
	fieldNames := make([]string, 0, len(fields))
	for _, field := range fields {
		fieldNames = append(fieldNames, field.Name)
	}
	return fieldNames
}

