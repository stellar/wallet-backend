package data

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	set "github.com/deckarep/golang-set/v2"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/stellar/wallet-backend/internal/indexer/types"
)

type SortOrder string

const (
	ASC  SortOrder = "ASC"
	DESC SortOrder = "DESC"
)

// pgtypeTextFromNullString converts sql.NullString to pgtype.Text for efficient binary COPY.
func pgtypeTextFromNullString(ns sql.NullString) pgtype.Text {
	return pgtype.Text{String: ns.String, Valid: ns.Valid}
}

// pgtypeTextFromReasonPtr converts *types.StateChangeReason to pgtype.Text for efficient binary COPY.
func pgtypeTextFromReasonPtr(r *types.StateChangeReason) pgtype.Text {
	if r == nil {
		return pgtype.Text{Valid: false}
	}
	return pgtype.Text{String: string(*r), Valid: true}
}

// jsonbFromMap converts types.NullableJSONB to any for pgx CopyFrom.
// pgx automatically handles map[string]any → JSONB conversion.
func jsonbFromMap(m types.NullableJSONB) any {
	if m == nil {
		return nil
	}
	// Return the map directly; pgx handles JSON marshaling automatically
	return map[string]any(m)
}

// pgtypeInt2FromNullInt16 converts sql.NullInt16 to pgtype.Int2 for efficient binary COPY.
func pgtypeInt2FromNullInt16(ni sql.NullInt16) pgtype.Int2 {
	return pgtype.Int2{Int16: ni.Int16, Valid: ni.Valid}
}

// pgtypeBytesFromNullAddressBytea converts NullAddressBytea to bytes for BYTEA insert.
func pgtypeBytesFromNullAddressBytea(na types.NullAddressBytea) ([]byte, error) {
	if !na.Valid {
		return nil, nil
	}
	val, err := na.Value()
	if err != nil {
		return nil, fmt.Errorf("converting address to bytes: %w", err)
	}
	if val == nil {
		return nil, nil
	}
	return val.([]byte), nil
}

func getDBColumns(model any) set.Set[string] {
	modelType := reflect.TypeOf(model)
	dbColumns := set.NewSet[string]()
	for i := 0; i < modelType.NumField(); i++ {
		field := modelType.Field(i)
		dbTag := field.Tag.Get("db")

		if dbTag != "" && dbTag != "-" {
			dbColumns.Add(dbTag)
		}
	}
	return dbColumns
}

// PrepareColumnsWithID ensures that the specified ID column is always included in the column list
func prepareColumnsWithID(columns string, model any, prefix string, idColumns ...string) string {
	var dbColumns set.Set[string]
	if columns == "" {
		dbColumns = getDBColumns(model)
	} else {
		dbColumns = set.NewSet[string]()
		for _, col := range strings.Split(columns, ",") {
			col = strings.TrimSpace(col)
			if col != "" {
				dbColumns.Add(col)
			}
		}
	}

	if prefix != "" {
		dbColumns = addPrefixToColumns(dbColumns, prefix)
	}
	// State changes has both to_id and state_change_order as id columns
	for _, idColumn := range idColumns {
		dbColumns = addIDColumn(dbColumns, prefix, idColumn)
	}
	return strings.Join(dbColumns.ToSlice(), ", ")
}

func addPrefixToColumns(columns set.Set[string], prefix string) set.Set[string] {
	result := set.NewSet[string]()
	for _, column := range columns.ToSlice() {
		result.Add(fmt.Sprintf("%s.%s", prefix, column))
	}
	return result
}

func addIDColumn(columns set.Set[string], prefix string, idColumn string) set.Set[string] {
	var columnToAdd string
	if prefix == "" {
		columnToAdd = idColumn
	} else {
		columnToAdd = fmt.Sprintf("%s.%s", prefix, idColumn)
	}
	if !columns.Contains(columnToAdd) {
		columns.Add(columnToAdd)
	}
	return columns
}
