// Utility functions for token transfer processing
// Contains pure functions that don't depend on processor state and can be reused
package processors

import (
	"fmt"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/xdr"
)

// IsLiquidityPool checks if the given account ID is a liquidity pool
func IsLiquidityPool(accountID string) bool {
	// Try to decode the account ID as a strkey
	versionByte, _, err := strkey.DecodeAny(accountID)
	if err != nil {
		return false
	}
	// Check if it's a liquidity pool strkey
	return versionByte == strkey.VersionByteLiquidityPool
}

// OperationSourceAccount returns the source account for an operation,
// falling back to the transaction source account if the operation doesn't have one
func OperationSourceAccount(tx ingest.LedgerTransaction, op xdr.Operation) string {
	acc := op.SourceAccount
	if acc != nil {
		return acc.ToAccountId().Address()
	}
	res := tx.Envelope.SourceAccount()
	return res.ToAccountId().Address()
}

// ConvertToInt32 safely converts values to int32
func ConvertToInt32(value any) (int32, error) {
	if value == nil {
		return 0, nil
	}
	
	switch v := value.(type) {
	case int:
		return int32(v), nil
	case int32:
		return v, nil
	case int64:
		return int32(v), nil
	default:
		return 0, fmt.Errorf("unexpected weight type: %T", value)
	}
}

// SafeStringFromDetails safely extracts a string value from effect details
func SafeStringFromDetails(details map[string]any, key string) (string, error) {
	if value, ok := details[key].(string); ok {
		return value, nil
	}
	return "", fmt.Errorf("invalid %s value", key)
}
