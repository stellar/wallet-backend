// Shared utilities for the data package
package dataloaders

import (
	"fmt"
	"strconv"
	"strings"
)

// parseStateChangeIDs parses composite state change IDs (format: "to_id-state_change_order")
// into separate slices of to_id and state_change_order values.
func parseStateChangeIDs(stateChangeIDs []string) ([]int64, []int64, error) {
	toIDs := make([]int64, len(stateChangeIDs))
	orders := make([]int64, len(stateChangeIDs))

	for i, id := range stateChangeIDs {
		parts := strings.Split(id, "-")
		if len(parts) != 2 {
			return nil, nil, fmt.Errorf("invalid state change ID format: %s (expected format: to_id-state_change_order)", id)
		}

		toID, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid to_id in state change ID %s: %w", id, err)
		}

		order, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid state_change_order in state change ID %s: %w", id, err)
		}

		toIDs[i] = toID
		orders[i] = order
	}

	return toIDs, orders, nil
}
