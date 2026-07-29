package wbclient

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/pkg/wbclient/types"
)

func TestBuildPaginationVars(t *testing.T) {
	first := int32(10)
	last := int32(5)
	after := "cursorA"
	before := "cursorB"

	t.Run("nil Page yields no variables", func(t *testing.T) {
		vars, err := buildPaginationVars(nil)
		require.NoError(t, err)
		assert.Empty(t, vars)
	})

	t.Run("empty Page yields no variables", func(t *testing.T) {
		vars, err := buildPaginationVars(&Page{})
		require.NoError(t, err)
		assert.Empty(t, vars)
	})

	t.Run("forward page sets first and after", func(t *testing.T) {
		vars, err := buildPaginationVars(&Page{First: &first, After: &after})
		require.NoError(t, err)
		assert.Equal(t, map[string]any{"first": first, "after": after}, vars)
	})

	t.Run("backward page sets last and before", func(t *testing.T) {
		vars, err := buildPaginationVars(&Page{Last: &last, Before: &before})
		require.NoError(t, err)
		assert.Equal(t, map[string]any{"last": last, "before": before}, vars)
	})

	t.Run("validation errors still fire", func(t *testing.T) {
		zero := int32(0)
		testCases := []struct {
			name string
			page *Page
		}{
			{"first and last together", &Page{First: &first, Last: &last}},
			{"after and before together", &Page{After: &after, Before: &before}},
			{"first not positive", &Page{First: &zero}},
			{"last not positive", &Page{Last: &zero}},
			{"first and before together", &Page{First: &first, Before: &before}},
			{"last and after together", &Page{Last: &last, After: &after}},
		}
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				vars, err := buildPaginationVars(tc.page)
				require.Error(t, err)
				assert.Nil(t, vars)
			})
		}
	})
}

func TestBuildTimeRangeVars(t *testing.T) {
	since := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	until := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)

	t.Run("nil TimeRange yields no variables", func(t *testing.T) {
		assert.Empty(t, buildTimeRangeVars(nil))
	})

	t.Run("empty TimeRange yields no variables", func(t *testing.T) {
		assert.Empty(t, buildTimeRangeVars(&TimeRange{}))
	})

	t.Run("both bounds set", func(t *testing.T) {
		vars := buildTimeRangeVars(&TimeRange{Since: &since, Until: &until})
		assert.Equal(t, map[string]any{"since": since, "until": until}, vars)
	})

	t.Run("only one bound set", func(t *testing.T) {
		vars := buildTimeRangeVars(&TimeRange{Since: &since})
		assert.Equal(t, map[string]any{"since": since}, vars)
	})
}

func TestBuildStateChangeFilterVars(t *testing.T) {
	txHash := "deadbeef"
	opID := int64(42)
	category := types.StateChangeCategoryBalance
	reason := types.StateChangeReasonCredit

	t.Run("nil filter yields no variables", func(t *testing.T) {
		assert.Empty(t, buildStateChangeFilterVars(nil))
	})

	t.Run("empty filter yields no variables", func(t *testing.T) {
		assert.Empty(t, buildStateChangeFilterVars(&StateChangeFilter{}))
	})

	t.Run("all fields build the $filter object", func(t *testing.T) {
		vars := buildStateChangeFilterVars(&StateChangeFilter{
			TransactionHash: &txHash,
			OperationID:     &opID,
			Category:        &category,
			Reason:          &reason,
		})
		require.Contains(t, vars, "filter")
		filter, ok := vars["filter"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, map[string]any{
			"transactionHash": txHash,
			"operationId":     opID,
			"category":        category,
			"reason":          reason,
		}, filter)
	})

	t.Run("only set fields appear in the $filter object", func(t *testing.T) {
		vars := buildStateChangeFilterVars(&StateChangeFilter{TransactionHash: &txHash, Category: &category})
		filter, ok := vars["filter"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, map[string]any{"transactionHash": txHash, "category": category}, filter)
	})
}
