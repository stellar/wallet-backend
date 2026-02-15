// Tests for query utility functions: time range conditions and decomposed cursor conditions.
package data

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_buildDecomposedCursorCondition(t *testing.T) {
	testTime := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)

	tests := []struct {
		name          string
		columns       []CursorColumn
		sortOrder     SortOrder
		startArgIndex int
		wantClause    string
		wantArgs      []interface{}
		wantNextIndex int
	}{
		{
			name:          "empty columns returns empty",
			columns:       []CursorColumn{},
			sortOrder:     DESC,
			startArgIndex: 1,
			wantClause:    "",
			wantArgs:      nil,
			wantNextIndex: 1,
		},
		{
			name: "single column DESC",
			columns: []CursorColumn{
				{Name: "ledger_created_at", Value: testTime},
			},
			sortOrder:     DESC,
			startArgIndex: 1,
			wantClause:    "(ledger_created_at < $1)",
			wantArgs:      []interface{}{testTime},
			wantNextIndex: 2,
		},
		{
			name: "single column ASC",
			columns: []CursorColumn{
				{Name: "id", Value: int64(42)},
			},
			sortOrder:     ASC,
			startArgIndex: 3,
			wantClause:    "(id > $3)",
			wantArgs:      []interface{}{int64(42)},
			wantNextIndex: 4,
		},
		{
			name: "two columns DESC",
			columns: []CursorColumn{
				{Name: "ledger_created_at", Value: testTime},
				{Name: "to_id", Value: int64(100)},
			},
			sortOrder:     DESC,
			startArgIndex: 1,
			wantClause:    "(ledger_created_at < $1 OR (ledger_created_at = $2 AND to_id < $3))",
			wantArgs:      []interface{}{testTime, testTime, int64(100)},
			wantNextIndex: 4,
		},
		{
			name: "two columns ASC",
			columns: []CursorColumn{
				{Name: "ledger_created_at", Value: testTime},
				{Name: "id", Value: int64(50)},
			},
			sortOrder:     ASC,
			startArgIndex: 2,
			wantClause:    "(ledger_created_at > $2 OR (ledger_created_at = $3 AND id > $4))",
			wantArgs:      []interface{}{testTime, testTime, int64(50)},
			wantNextIndex: 5,
		},
		{
			name: "three columns DESC",
			columns: []CursorColumn{
				{Name: "to_id", Value: int64(10)},
				{Name: "operation_id", Value: int64(20)},
				{Name: "state_change_order", Value: int64(30)},
			},
			sortOrder:     DESC,
			startArgIndex: 2,
			wantClause:    "(to_id < $2 OR (to_id = $3 AND operation_id < $4) OR (to_id = $5 AND operation_id = $6 AND state_change_order < $7))",
			wantArgs:      []interface{}{int64(10), int64(10), int64(20), int64(10), int64(20), int64(30)},
			wantNextIndex: 8,
		},
		{
			name: "four columns DESC",
			columns: []CursorColumn{
				{Name: "ledger_created_at", Value: testTime},
				{Name: "to_id", Value: int64(10)},
				{Name: "operation_id", Value: int64(20)},
				{Name: "state_change_order", Value: int64(30)},
			},
			sortOrder:     DESC,
			startArgIndex: 1,
			wantClause:    "(ledger_created_at < $1 OR (ledger_created_at = $2 AND to_id < $3) OR (ledger_created_at = $4 AND to_id = $5 AND operation_id < $6) OR (ledger_created_at = $7 AND to_id = $8 AND operation_id = $9 AND state_change_order < $10))",
			wantArgs:      []interface{}{testTime, testTime, int64(10), testTime, int64(10), int64(20), testTime, int64(10), int64(20), int64(30)},
			wantNextIndex: 11,
		},
		{
			name: "four columns ASC",
			columns: []CursorColumn{
				{Name: "ledger_created_at", Value: testTime},
				{Name: "to_id", Value: int64(10)},
				{Name: "operation_id", Value: int64(20)},
				{Name: "state_change_order", Value: int64(30)},
			},
			sortOrder:     ASC,
			startArgIndex: 5,
			wantClause:    "(ledger_created_at > $5 OR (ledger_created_at = $6 AND to_id > $7) OR (ledger_created_at = $8 AND to_id = $9 AND operation_id > $10) OR (ledger_created_at = $11 AND to_id = $12 AND operation_id = $13 AND state_change_order > $14))",
			wantArgs:      []interface{}{testTime, testTime, int64(10), testTime, int64(10), int64(20), testTime, int64(10), int64(20), int64(30)},
			wantNextIndex: 15,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotClause, gotArgs, gotNextIndex := buildDecomposedCursorCondition(tt.columns, tt.sortOrder, tt.startArgIndex)
			assert.Equal(t, tt.wantClause, gotClause)
			assert.Equal(t, tt.wantArgs, gotArgs)
			assert.Equal(t, tt.wantNextIndex, gotNextIndex)
		})
	}
}

func Test_appendTimeRangeConditions(t *testing.T) {
	since := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	until := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name          string
		timeRange     *TimeRange
		column        string
		startArgs     []interface{}
		startArgIndex int
		wantSQL       string
		wantArgs      []interface{}
		wantNextIndex int
	}{
		{
			name:          "nil time range appends nothing",
			timeRange:     nil,
			column:        "ledger_created_at",
			startArgs:     []interface{}{"existing"},
			startArgIndex: 2,
			wantSQL:       "",
			wantArgs:      []interface{}{"existing"},
			wantNextIndex: 2,
		},
		{
			name:          "since only",
			timeRange:     &TimeRange{Since: &since},
			column:        "ledger_created_at",
			startArgs:     []interface{}{"existing"},
			startArgIndex: 2,
			wantSQL:       " AND ledger_created_at >= $2",
			wantArgs:      []interface{}{"existing", since},
			wantNextIndex: 3,
		},
		{
			name:          "until only",
			timeRange:     &TimeRange{Until: &until},
			column:        "ledger_created_at",
			startArgs:     []interface{}{"existing"},
			startArgIndex: 2,
			wantSQL:       " AND ledger_created_at <= $2",
			wantArgs:      []interface{}{"existing", until},
			wantNextIndex: 3,
		},
		{
			name:          "both since and until",
			timeRange:     &TimeRange{Since: &since, Until: &until},
			column:        "ledger_created_at",
			startArgs:     []interface{}{"existing"},
			startArgIndex: 2,
			wantSQL:       " AND ledger_created_at >= $2 AND ledger_created_at <= $3",
			wantArgs:      []interface{}{"existing", since, until},
			wantNextIndex: 4,
		},
		{
			name:          "table-qualified column name",
			timeRange:     &TimeRange{Since: &since},
			column:        "ta.ledger_created_at",
			startArgs:     nil,
			startArgIndex: 1,
			wantSQL:       " AND ta.ledger_created_at >= $1",
			wantArgs:      []interface{}{since},
			wantNextIndex: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var qb strings.Builder
			gotArgs, gotNextIndex := appendTimeRangeConditions(&qb, tt.column, tt.timeRange, tt.startArgs, tt.startArgIndex)
			assert.Equal(t, tt.wantSQL, qb.String())
			assert.Equal(t, tt.wantArgs, gotArgs)
			assert.Equal(t, tt.wantNextIndex, gotNextIndex)
		})
	}
}
