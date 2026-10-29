// Utility functions for token transfer processing
// Contains pure functions that don't depend on processor state and can be reused
package processors

import (
	"fmt"

	"github.com/pkg/errors"
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/toid"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/wallet-backend/internal/indexer/types"
)

// PoolIDToString converts a pool ID to its string representation
func PoolIDToString(id xdr.PoolId) string {
	return xdr.Hash(id).HexString()
}

// AddLiquidityPoolAssetDetails adds liquidity pool asset details to the result map
func AddLiquidityPoolAssetDetails(result map[string]interface{}, lpp xdr.LiquidityPoolParameters) error {
	result["asset_type"] = "liquidity_pool_shares"
	if lpp.Type != xdr.LiquidityPoolTypeLiquidityPoolConstantProduct {
		return fmt.Errorf("unknown liquidity pool type %d", lpp.Type)
	}
	cp := lpp.ConstantProduct
	poolID, err := xdr.NewPoolId(cp.AssetA, cp.AssetB, cp.Fee)
	if err != nil {
		return fmt.Errorf("creating pool ID: %w", err)
	}
	result["liquidity_pool_id"] = PoolIDToString(poolID)
	return nil
}

// AddAssetDetails adds asset details to the result map with an optional prefix
func AddAssetDetails(result map[string]interface{}, a xdr.Asset, prefix string) error {
	var (
		assetType string
		code      string
		issuer    string
	)
	err := a.Extract(&assetType, &code, &issuer)
	if err != nil {
		err = errors.Wrap(err, "xdr.Asset.Extract error")
		return err
	}
	result[prefix+"asset_type"] = assetType

	if a.Type == xdr.AssetTypeAssetTypeNative {
		return nil
	}

	result[prefix+"asset_code"] = code
	result[prefix+"asset_issuer"] = issuer
	return nil
}

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

// isClaimableBalance checks if the given ID is a claimable balance
func isClaimableBalance(id string) bool {
	versionByte, _, err := strkey.DecodeAny(id)
	if err != nil {
		return false
	}
	return versionByte == strkey.VersionByteClaimableBalance
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
	feeCharged, _ := transaction.FeeCharged()

	innerTxHash, ok := transaction.InnerTransactionHash()
	if !ok {
		innerTxHash = transaction.Hash.HexString()
	}

	ledgerSequence := transaction.Ledger.LedgerSequence()
	transactionID := toid.New(int32(ledgerSequence), int32(transaction.Index), 0).ToInt64()

	return &types.Transaction{
		ToID:                 transactionID,
		Hash:                 types.HashBytea(transaction.Hash.HexString()),
		LedgerCreatedAt:      transaction.Ledger.ClosedAt(),
		FeeCharged:           feeCharged,
		ResultCode:           transaction.ResultCode(),
		LedgerNumber:         ledgerSequence,
		IsFeeBump:            transaction.Envelope.IsFeeBump(),
		InnerTransactionHash: innerTxHash,
	}, nil
}

func ConvertOperation(
	transaction *ingest.LedgerTransaction,
	op *xdr.Operation,
	opID int64,
	opIndex uint32,
	opResults []xdr.OperationResult,
) (*types.Operation, error) {
	xdrBytes, err := op.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("marshalling operation %d: %w", opID, err)
	}

	// Extract result code and success status
	var resultCode string
	var successful bool
	if int(opIndex) < len(opResults) {
		resultCode, successful, err = forOperationResult(opResults[opIndex])
		if err != nil {
			return nil, fmt.Errorf("getting result code for operation %d: %w", opID, err)
		}
	} else {
		// If no results available (shouldn't happen in normal circumstances), mark as failed
		resultCode = "op_unknown"
		successful = false
	}

	return &types.Operation{
		ID:              opID,
		OperationType:   types.OperationTypeFromXDR(op.Body.Type),
		OperationXDR:    types.XDRBytea(xdrBytes),
		ResultCode:      resultCode,
		Successful:      successful,
		LedgerCreatedAt: transaction.Ledger.ClosedAt(),
		LedgerNumber:    transaction.Ledger.LedgerSequence(),
	}, nil
}

// deployerAddressString converts a create-contract preimage's deployer address to its strkey
// form, reporting ok=false when it is a kind types.AddressBytea cannot store. Muxed deployers
// reduce to their base account, mirroring the classic path's MuxedAccount.ToAccountId(), so
// one account cannot produce duplicate rows that violate the operations_accounts primary key.
func deployerAddressString(addr xdr.ScAddress) (string, bool, error) {
	return types.StorableAddressString(addr) //nolint:wrapcheck // thin delegate; the callee wraps its errors
}
