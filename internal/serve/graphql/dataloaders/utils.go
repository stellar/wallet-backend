// Shared utilities for the data package
package dataloaders

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/vektah/gqlparser/v2/gqlerror"
)

// badUserInputError normalizes client-correctable validation failures to the GraphQL error code
// used elsewhere in the API for bad input, so the custom error presenter's internal-error masking
// (GQL-05) does not swallow these behind a generic message.
func badUserInputError(message string) error {
	return &gqlerror.Error{
		Message:    message,
		Extensions: map[string]interface{}{"code": "BAD_USER_INPUT"},
	}
}

// parseStateChangeIDs parses composite state change IDs (format: "to_id-operation_id-state_change_id")
// into separate slices of to_id, operation_id, and state_change_id values.
func parseStateChangeIDs(stateChangeIDs []string) ([]int64, []int64, []int64, error) {
	toIDs := make([]int64, len(stateChangeIDs))
	opIDs := make([]int64, len(stateChangeIDs))
	stateChangeIDValues := make([]int64, len(stateChangeIDs))

	for i, id := range stateChangeIDs {
		parts := strings.Split(id, "-")
		if len(parts) != 3 {
			return nil, nil, nil, badUserInputError(fmt.Sprintf("invalid state change ID format: %s (expected format: to_id-operation_id-state_change_id)", id))
		}

		toID, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			return nil, nil, nil, badUserInputError(fmt.Sprintf("invalid to_id in state change ID %s: %s", id, err))
		}

		opID, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return nil, nil, nil, badUserInputError(fmt.Sprintf("invalid operation_id in state change ID %s: %s", id, err))
		}

		stateChangeID, err := strconv.ParseInt(parts[2], 10, 64)
		if err != nil {
			return nil, nil, nil, badUserInputError(fmt.Sprintf("invalid state_change_id in state change ID %s: %s", id, err))
		}

		toIDs[i] = toID
		opIDs[i] = opID
		stateChangeIDValues[i] = stateChangeID
	}

	return toIDs, opIDs, stateChangeIDValues, nil
}
