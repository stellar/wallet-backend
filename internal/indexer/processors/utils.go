// Utility functions for token transfer processing
// Contains pure functions that don't depend on processor state and can be reused
package processors

import (
	"fmt"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/toid"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/xdr"

	"github.com/stellar/wallet-backend/internal/indexer/types"
)

func getContractIDFromAssetDetails(networkPassphrase string, assetType, assetCode, assetIssuer string) (string, error) {
	var asset xdr.Asset

	switch assetType {
	case "native":
		asset = xdr.Asset{
			Type: xdr.AssetTypeAssetTypeNative,
		}
	case "credit_alphanum4", "credit_alphanum12":
		asset = xdr.MustNewCreditAsset(assetCode, assetIssuer)
	default:
		return "", fmt.Errorf("invalid asset type: %s", assetType)
	}

	contractID, err := asset.ContractID(networkPassphrase)
	if err != nil {
		return "", fmt.Errorf("getting asset contract ID: %w", err)
	}

	return strkey.MustEncode(strkey.VersionByteContract, contractID[:]), nil
}

// isLiquidityPool checks if the given account ID is a liquidity pool
func isLiquidityPool(accountID string) bool {
	// Try to decode the account ID as a strkey
	versionByte, _, err := strkey.DecodeAny(accountID)
	if err != nil {
		return false
	}
	// Check if it's a liquidity pool strkey
	return versionByte == strkey.VersionByteLiquidityPool
}

// operationSourceAccount returns the source account for an operation,
// falling back to the transaction source account if the operation doesn't have one
func operationSourceAccount(tx ingest.LedgerTransaction, op xdr.Operation) string {
	acc := op.SourceAccount
	if acc != nil {
		return acc.ToAccountId().Address()
	}
	res := tx.Envelope.SourceAccount()
	return res.ToAccountId().Address()
}

// convertToInt32 safely converts values to int32
func convertToInt32(value any) (int32, error) {
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

// safeStringFromDetails safely extracts a string value from effect details
func safeStringFromDetails(details map[string]any, key string) (string, error) {
	if value, ok := details[key].(string); ok {
		return value, nil
	}
	return "", fmt.Errorf("invalid %s value", key)
}

func ConvertTransaction(transaction *ingest.LedgerTransaction) (*types.Transaction, error) {
	envelopeXDR, err := xdr.MarshalBase64(transaction.Envelope)
	if err != nil {
		return nil, fmt.Errorf("marshalling transaction envelope: %w", err)
	}

	resultXDR, err := xdr.MarshalBase64(transaction.Result)
	if err != nil {
		return nil, fmt.Errorf("marshalling transaction result: %w", err)
	}

	metaXDR, err := xdr.MarshalBase64(transaction.UnsafeMeta)
	if err != nil {
		return nil, fmt.Errorf("marshalling transaction meta: %w", err)
	}

	ledgerSequence := transaction.Ledger.LedgerSequence()
	transactionID := toid.New(int32(ledgerSequence), int32(transaction.Index), 0).ToInt64()

	return &types.Transaction{
		ToID:            transactionID,
		Hash:            transaction.Hash.HexString(),
		LedgerCreatedAt: transaction.Ledger.ClosedAt(),
		EnvelopeXDR:     envelopeXDR,
		ResultXDR:       resultXDR,
		MetaXDR:         metaXDR,
		LedgerNumber:    ledgerSequence,
	}, nil
}

func ConvertOperation(transaction *ingest.LedgerTransaction, op *xdr.Operation, opID int64) (*types.Operation, error) {
	xdrOpStr, err := xdr.MarshalBase64(op)
	if err != nil {
		return nil, fmt.Errorf("marshalling operation %d: %w", opID, err)
	}

	return &types.Operation{
		ID:              opID,
		OperationType:   types.OperationTypeFromXDR(op.Body.Type),
		OperationXDR:    xdrOpStr,
		LedgerCreatedAt: transaction.Ledger.ClosedAt(),
		TxHash:          transaction.Hash.HexString(),
	}, nil
}
