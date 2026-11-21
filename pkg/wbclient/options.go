package wbclient

import (
	"fmt"
	"time"

	"github.com/stellar/wallet-backend/pkg/wbclient/types"
)

// Page selects a Relay-style page: First/After paginate forward, Last/Before backward.
// The zero value (or a nil *Page) requests the server's default page.
type Page struct {
	First  *int32
	Last   *int32
	After  *string
	Before *string
}

// TimeRange bounds a query by ledger close time; nil bounds are open-ended.
// A nil *TimeRange applies no bounds.
type TimeRange struct {
	Since *time.Time
	Until *time.Time
}

// StateChangeFilter narrows an account's state changes; nil fields are ignored, conditions are ANDed.
type StateChangeFilter struct {
	TransactionHash *string
	OperationID     *int64
	Category        *types.StateChangeCategory
	Reason          *types.StateChangeReason
}

// MemoFilter restricts an account's transactions to those whose memo matches
// exactly. Value uses the same encoding as GraphQLTransaction.Memo: TEXT as
// UTF-8, ID as a decimal string, HASH and RETURN as 64 hex characters. The
// server rejects MemoTypeNone.
type MemoFilter struct {
	Type  types.MemoType
	Value string
}

// buildPaginationVars turns a Page into GraphQL variables, enforcing the
// forward/backward mutual-exclusion rules. A nil Page yields an empty map and
// no error, requesting the server's default page.
func buildPaginationVars(page *Page) (map[string]any, error) {
	vars := make(map[string]any)
	if page == nil {
		return vars, nil
	}

	if err := validatePaginationParams(page.First, page.After, page.Last, page.Before); err != nil {
		return nil, err
	}

	if page.First != nil {
		vars["first"] = *page.First
	}
	if page.After != nil {
		vars["after"] = *page.After
	}
	if page.Last != nil {
		vars["last"] = *page.Last
	}
	if page.Before != nil {
		vars["before"] = *page.Before
	}
	return vars, nil
}

// buildTimeRangeVars turns a TimeRange into the since/until GraphQL variables.
// A nil TimeRange, or nil bounds within it, contribute no variables.
func buildTimeRangeVars(tr *TimeRange) map[string]any {
	vars := make(map[string]any)
	if tr == nil {
		return vars
	}
	if tr.Since != nil {
		vars["since"] = *tr.Since
	}
	if tr.Until != nil {
		vars["until"] = *tr.Until
	}
	return vars
}

// buildStateChangeFilterVars turns a StateChangeFilter into the $filter GraphQL
// variable. It returns an empty map when the filter is nil or has no set fields,
// so no $filter variable is sent in that case.
func buildStateChangeFilterVars(filter *StateChangeFilter) map[string]any {
	vars := make(map[string]any)
	if filter == nil {
		return vars
	}

	filterObj := make(map[string]any)
	if filter.TransactionHash != nil {
		filterObj["transactionHash"] = *filter.TransactionHash
	}
	if filter.OperationID != nil {
		filterObj["operationId"] = *filter.OperationID
	}
	if filter.Category != nil {
		filterObj["category"] = *filter.Category
	}
	if filter.Reason != nil {
		filterObj["reason"] = *filter.Reason
	}

	if len(filterObj) > 0 {
		vars["filter"] = filterObj
	}
	return vars
}

// buildMemoFilterVars turns a MemoFilter into the $memo GraphQL variable. A nil
// filter yields an empty map, so no $memo variable is sent.
func buildMemoFilterVars(filter *MemoFilter) map[string]any {
	vars := make(map[string]any)
	if filter == nil {
		return vars
	}
	vars["memo"] = map[string]any{
		"type":  filter.Type,
		"value": filter.Value,
	}
	return vars
}

func validatePaginationParams(first *int32, after *string, last *int32, before *string) error {
	if first != nil && last != nil {
		return fmt.Errorf("first and last cannot be used together")
	}

	if after != nil && before != nil {
		return fmt.Errorf("after and before cannot be used together")
	}

	if first != nil && *first <= 0 {
		return fmt.Errorf("first must be greater than 0")
	}

	if last != nil && *last <= 0 {
		return fmt.Errorf("last must be greater than 0")
	}

	if first != nil && before != nil {
		return fmt.Errorf("first and before cannot be used together")
	}

	if last != nil && after != nil {
		return fmt.Errorf("last and after cannot be used together")
	}

	return nil
}
