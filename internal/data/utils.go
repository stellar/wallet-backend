package data

import (
	"reflect"
)

func getDBColumns(model any) []string {
	modelType := reflect.TypeOf(model)
	dbColumns := make([]string, 0)
	for i := 0; i < modelType.NumField(); i++ {
		field := modelType.Field(i)
		dbTag := field.Tag.Get("db")

		if dbTag != "" && dbTag != "-" {
			dbColumns = append(dbColumns, dbTag)
		}
	}
	return dbColumns
}
