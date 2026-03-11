package data

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"

	set "github.com/deckarep/golang-set/v2"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/stellar/wallet-backend/internal/indexer/types"
)

// TimeRange represents an optional time window for filtering queries by ledger_created_at.
// Both fields are optional: omit both for all data, use Since alone for "from this point",
// use Until alone for "up to this point", or both for a bounded window.
type TimeRange struct {
	Since *time.Time
	Until *time.Time
}

// appendTimeRangeConditions appends ledger_created_at >= and/or <= conditions to the query builder.
// Returns the updated args slice and next arg index. The column parameter allows specifying
// a table-qualified column name (e.g., "ta.ledger_created_at" for CTEs).
func appendTimeRangeConditions(qb *strings.Builder, column string, timeRange *TimeRange, args []interface{}, argIndex int) ([]interface{}, int) {
	if timeRange == nil {
		return args, argIndex
	}
	if timeRange.Since != nil {
		fmt.Fprintf(qb, " AND %s >= $%d", column, argIndex)
		args = append(args, *timeRange.Since)
		argIndex++
	}
	if timeRange.Until != nil {
		fmt.Fprintf(qb, " AND %s <= $%d", column, argIndex)
		args = append(args, *timeRange.Until)
		argIndex++
	}
	return args, argIndex
}

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

// CursorColumn represents a column name and its cursor value for decomposed pagination.
type CursorColumn struct {
	Name  string
	Value interface{}
}

// buildDecomposedCursorCondition decomposes a ROW() tuple comparison into an equivalent
// OR clause that TimescaleDB's ColumnarScan can push into vectorized filters.
//
// For example, (a, b, c) < ($1, $2, $3) becomes:
//
//	(a < $1 OR (a = $1 AND b < $2) OR (a = $1 AND b = $2 AND c < $3))
//
// DESC uses "<", ASC uses ">". Returns the clause string, args slice, and next arg index.
func buildDecomposedCursorCondition(columns []CursorColumn, sortOrder SortOrder, startArgIndex int) (string, []interface{}, int) {
	if len(columns) == 0 {
		return "", nil, startArgIndex
	}

	op := "<"
	if sortOrder == ASC {
		op = ">"
	}

	argIdx := startArgIndex
	var args []interface{}
	var orParts []string

	for i := range columns {
		var parts []string
		// Add equality conditions for all preceding columns
		for j := 0; j < i; j++ {
			parts = append(parts, fmt.Sprintf("%s = $%d", columns[j].Name, argIdx))
			args = append(args, columns[j].Value)
			argIdx++
		}
		// Add the comparison condition for the current column
		parts = append(parts, fmt.Sprintf("%s %s $%d", columns[i].Name, op, argIdx))
		args = append(args, columns[i].Value)
		argIdx++

		orParts = append(orParts, strings.Join(parts, " AND "))
	}

	// Wrap each OR branch in parens if it has multiple conditions
	for i, part := range orParts {
		if i > 0 {
			orParts[i] = "(" + part + ")"
		}
	}

	clause := "(" + strings.Join(orParts, " OR ") + ")"
	return clause, args, argIdx
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
