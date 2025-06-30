// Utility functions for token transfer processing
// Contains pure functions that don't depend on processor state and can be reused
package processors

import (
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
