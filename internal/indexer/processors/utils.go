// Utility functions for token transfer processing
// Contains pure functions that don't depend on processor state and can be reused
package processors

import (
	"encoding/hex"
	"fmt"
	"sync"

	"github.com/pkg/errors"
	"github.com/stellar/go-stellar-sdk/hash"
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

// AssetContractIDMemo caches asset→contract-ID derivations — each one a
// SHA-256 over the asset's contract-ID preimage plus a strkey encode —
// which processors otherwise recompute for the same few assets on every
// event. Results are content-derived (the processor's network passphrase is
// fixed at construction), so entries never invalidate and the memo is
// bounded by the distinct assets a process sees. sync.Map because one
// processor instance serves every indexer pool worker concurrently, and the
// workload is read-mostly once warm.
//
// Entries are keyed by "<type>\x00<code>\x00<issuer>", which is the complete
// identity of a Stellar asset: native is the only type with an empty code and
// issuer, and the two credit widths are separated both by their type and by the
// code length that implies it. The NUL delimiter is unambiguous because an
// asset code is alphanumeric with its padding trimmed and an issuer is base32
// strkey, so neither component can contain a NUL. No two distinct assets can
// therefore share a key. Every accessor normalizes to the same three
// components — the type spelled exactly as xdr.Asset.Extract spells it — so an
// asset reaching the memo through different accessors lands on one entry.
//
// The zero value is ready to use.
type AssetContractIDMemo struct {
	m sync.Map // "<type>\x00<code>\x00<issuer>" → strkey-encoded contract ID
}

// fromDetails returns the contract ID of the asset described by the extracted
// asset detail strings.
func (memo *AssetContractIDMemo) fromDetails(networkPassphrase string, assetType, assetCode, assetIssuer string) (string, error) {
	key := assetType + "\x00" + assetCode + "\x00" + assetIssuer
	if id, ok := memo.m.Load(key); ok {
		return id.(string), nil
	}
	id, err := getContractIDFromAssetDetails(networkPassphrase, assetType, assetCode, assetIssuer)
	if err != nil {
		return "", err
	}
	memo.m.Store(key, id)
	return id, nil
}

// FromAsset returns the contract ID of an asset held as XDR. Extracting the
// key components costs a strkey encode of the issuer on every call, hit or
// miss, which is why callers already holding the code and issuer as strings go
// through fromCreditAsset instead.
func (memo *AssetContractIDMemo) FromAsset(networkPassphrase string, asset xdr.Asset) (string, error) {
	var assetType, assetCode, assetIssuer string
	if err := asset.Extract(&assetType, &assetCode, &assetIssuer); err != nil {
		return "", fmt.Errorf("extracting asset details: %w", err)
	}
	return memo.fromDetails(networkPassphrase, assetType, assetCode, assetIssuer)
}

// fromCreditAsset returns the contract ID of an issued asset held as its code
// and issuer, the shape token transfer events carry. The code length fixes the
// asset type, the same way xdr.MustNewCreditAsset derives it from the code.
func (memo *AssetContractIDMemo) fromCreditAsset(networkPassphrase string, assetCode, assetIssuer string) (string, error) {
	assetType := "credit_alphanum4"
	if len(assetCode) > 4 {
		assetType = "credit_alphanum12"
	}
	return memo.fromDetails(networkPassphrase, assetType, assetCode, assetIssuer)
}

// operationXDRBuffers recycles XDR encoding buffers across ConvertOperation
// calls, which run on every indexer pool worker concurrently.
var operationXDRBuffers = sync.Pool{New: func() any { return xdr.NewEncodingBuffer() }}

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

// Every strkey version byte is a multiple of 8, so its top five bits — which
// are exactly what the first base32 character encodes — determine it. A strkey
// that does not start with these characters cannot decode to the corresponding
// version byte, which lets the checks below skip a base32 decode and a CRC
// validation for the account and contract addresses that make up nearly every
// transfer endpoint.
const (
	liquidityPoolStrkeyPrefix    = 'L' // strkey.VersionByteLiquidityPool
	claimableBalanceStrkeyPrefix = 'B' // strkey.VersionByteClaimableBalance
)

// isLiquidityPool checks if the given account ID is a liquidity pool
func isLiquidityPool(accountID string) bool {
	if len(accountID) == 0 || accountID[0] != liquidityPoolStrkeyPrefix {
		return false
	}
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
	if len(id) == 0 || id[0] != claimableBalanceStrkeyPrefix {
		return false
	}
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
	// The operation's XDR bytes are retained by the returned row, so one
	// exact-size allocation is unavoidable — but the encoder and its growth
	// copies are not: a pooled EncodingBuffer reuses the scratch across calls
	// and pool workers, and MarshalBinary returns the owned copy.
	buf := operationXDRBuffers.Get().(*xdr.EncodingBuffer)
	xdrBytes, err := buf.MarshalBinary(op)
	operationXDRBuffers.Put(buf)
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
