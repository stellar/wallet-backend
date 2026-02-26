// Utility functions for token transfer processing
// Contains pure functions that don't depend on processor state and can be reused
package processors

import (
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/stellar/go-stellar-sdk/hash"
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/toid"
	"github.com/stellar/go-stellar-sdk/txnbuild"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/wallet-backend/internal/indexer/types"
)

// PoolIDToString converts a pool ID to its string representation
func PoolIDToString(id xdr.PoolId) string {
	return xdr.Hash(id).HexString()
}

// formatPrefix adds an underscore suffix to a prefix if it's not empty
func formatPrefix(p string) string {
	if p != "" {
		p += "_"
	}
	return p
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

// AddAccountAndMuxedAccountDetails adds account and muxed account details to the result map
func AddAccountAndMuxedAccountDetails(result map[string]interface{}, a xdr.MuxedAccount, prefix string) error {
	accountID := a.ToAccountId()
	result[prefix] = accountID.Address()
	prefix = formatPrefix(prefix)
	if a.Type == xdr.CryptoKeyTypeKeyTypeMuxedEd25519 {
		muxedAccountAddress, err := a.GetAddress()
		if err != nil {
			return fmt.Errorf("getting muxed account address: %w", err)
		}
		result[prefix+"muxed"] = muxedAccountAddress
		muxedAccountID, err := a.GetId()
		if err != nil {
			return fmt.Errorf("getting muxed account ID: %w", err)
		}
		result[prefix+"muxed_id"] = muxedAccountID
	}
	return nil
}

// LedgerKeyToLedgerKeyHash converts a ledger key to its hash representation
func LedgerKeyToLedgerKeyHash(ledgerKey xdr.LedgerKey) string {
	ledgerKeyByte, err := ledgerKey.MarshalBinary()
	if err != nil {
		return ""
	}
	hashedLedgerKeyByte := hash.Hash(ledgerKeyByte)
	ledgerKeyHash := hex.EncodeToString(hashedLedgerKeyByte[:])

	return ledgerKeyHash
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

// addAuthFlagDetails adds account flag details to the result map
func addAuthFlagDetails(result map[string]interface{}, f xdr.AccountFlags, prefix string) {
	var (
		n []int32
		s []string
	)

	if f.IsAuthRequired() {
		n = append(n, int32(xdr.AccountFlagsAuthRequiredFlag))
		s = append(s, "auth_required")
	}

	if f.IsAuthRevocable() {
		n = append(n, int32(xdr.AccountFlagsAuthRevocableFlag))
		s = append(s, "auth_revocable")
	}

	if f.IsAuthImmutable() {
		n = append(n, int32(xdr.AccountFlagsAuthImmutableFlag))
		s = append(s, "auth_immutable")
	}

	if f.IsAuthClawbackEnabled() {
		n = append(n, int32(xdr.AccountFlagsAuthClawbackEnabledFlag))
		s = append(s, "auth_clawback_enabled")
	}

	result[prefix+"_flags"] = n
	result[prefix+"_flags_s"] = s
}

// addTrustLineFlagDetails adds trustline flag details to the result map
func addTrustLineFlagDetails(result map[string]interface{}, f xdr.TrustLineFlags, prefix string) {
	var (
		n []int32
		s []string
	)

	if f.IsAuthorized() {
		n = append(n, int32(xdr.TrustLineFlagsAuthorizedFlag))
		s = append(s, "authorized")
	}

	if f.IsAuthorizedToMaintainLiabilitiesFlag() {
		n = append(n, int32(xdr.TrustLineFlagsAuthorizedToMaintainLiabilitiesFlag))
		s = append(s, "authorized_to_maintain_liabilites")
	}

	if f.IsClawbackEnabledFlag() {
		n = append(n, int32(xdr.TrustLineFlagsTrustlineClawbackEnabledFlag))
		s = append(s, "clawback_enabled")
	}

	result[prefix+"_flags"] = n
	result[prefix+"_flags_s"] = s
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

// getContractIDFromAssetString converts an asset string in "CODE:ISSUER" format to a contract ID.
// For native assets, use "native" as the input.
func getContractIDFromAssetString(networkPassphrase string, assetStr string) (string, error) {
	if assetStr == "native" {
		return getContractIDFromAssetDetails(networkPassphrase, "native", "", "")
	}

	// Parse CODE:ISSUER format
	parts := strings.Split(assetStr, ":")
	if len(parts) != 2 {
		return "", fmt.Errorf("invalid asset format, expected CODE:ISSUER: %s", assetStr)
	}

	assetCode := parts[0]
	assetIssuer := parts[1]

	// Determine asset type based on code length
	assetType := "credit_alphanum4"
	if len(assetCode) > 4 {
		assetType = "credit_alphanum12"
	}

	return getContractIDFromAssetDetails(networkPassphrase, assetType, assetCode, assetIssuer)
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

func ConvertTransaction(transaction *ingest.LedgerTransaction, skipTxMeta bool, skipTxEnvelope bool, networkPassphrase string) (*types.Transaction, error) {
	var envelopeXDR *string
	envelopeXDRStr, err := xdr.MarshalBase64(transaction.Envelope)
	if err != nil {
		return nil, fmt.Errorf("marshalling transaction envelope: %w", err)
	}

	if !skipTxEnvelope {
		envelopeXDR = &envelopeXDRStr
	}

	feeCharged, _ := transaction.FeeCharged()

	var metaXDR *string
	if !skipTxMeta {
		metaXDRStr, marshalErr := xdr.MarshalBase64(transaction.UnsafeMeta)
		if marshalErr != nil {
			return nil, fmt.Errorf("marshalling transaction meta: %w", marshalErr)
		}
		metaXDR = &metaXDRStr
	}

	// Calculate inner transaction hash
	genericTx, err := txnbuild.TransactionFromXDR(envelopeXDRStr)
	if err != nil {
		return nil, fmt.Errorf("deserializing envelope xdr: %w", err)
	}

	var innerTx *txnbuild.Transaction
	if feeBumpTx, ok := genericTx.FeeBump(); ok {
		innerTx = feeBumpTx.InnerTransaction()
	} else if tx, ok := genericTx.Transaction(); ok {
		innerTx = tx
	} else {
		return nil, fmt.Errorf("transaction is neither fee bump nor inner transaction")
	}

	innerTxHash, err := innerTx.HashHex(networkPassphrase)
	if err != nil {
		return nil, fmt.Errorf("generating inner hash hex: %w", err)
	}

	ledgerSequence := transaction.Ledger.LedgerSequence()
	transactionID := toid.New(int32(ledgerSequence), int32(transaction.Index), 0).ToInt64()

	return &types.Transaction{
		ToID:                 transactionID,
		Hash:                 types.HashBytea(transaction.Hash.HexString()),
		LedgerCreatedAt:      transaction.Ledger.ClosedAt(),
		EnvelopeXDR:          envelopeXDR,
		FeeCharged:           feeCharged,
		ResultCode:           transaction.ResultCode(),
		MetaXDR:              metaXDR,
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
