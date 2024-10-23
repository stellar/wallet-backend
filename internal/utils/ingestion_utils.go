package utils

import (
	"strconv"

	"github.com/stellar/go/toid"
	"github.com/stellar/go/xdr"
)

func OperationID(ledgerNumber, txNumber, opNumber int32) string {
	return toid.New(ledgerNumber, txNumber, opNumber).String()
}

func OperationResult(txResult xdr.TransactionResult, opNumber int) *xdr.OperationResultTr {
	results, _ := txResult.OperationResults()
	tr := results[opNumber-1].MustTr()
	return &tr
}

func TransactionID(ledgerNumber, txNumber int32) string {
	return toid.New(int32(ledgerNumber), int32(txNumber), 0).String()
}

// Memo returns the memo value parsed to string and its type.
func Memo(memo xdr.Memo, txHash string) (*string, string) {
	memoType := memo.Type
	switch memoType {
	case xdr.MemoTypeMemoNone:
		return nil, memoType.String()
	case xdr.MemoTypeMemoText:
		if text, ok := memo.GetText(); ok {
			return &text, memoType.String()
		}
	case xdr.MemoTypeMemoId:
		if id, ok := memo.GetId(); ok {
			idStr := strconv.FormatUint(uint64(id), 10)
			return &idStr, memoType.String()
		}
	case xdr.MemoTypeMemoHash:
		if hash, ok := memo.GetHash(); ok {
			hashStr := hash.HexString()
			return &hashStr, memoType.String()
		}
	case xdr.MemoTypeMemoReturn:
		if retHash, ok := memo.GetRetHash(); ok {
			retHashStr := retHash.HexString()
			return &retHashStr, memoType.String()
		}
	default:
		// TODO: track in Sentry
		// sentry.CaptureException(fmt.Errorf("unknown memo type %q for transaction %s", memoType.String(), txHash))
		return nil, ""
	}

	// TODO: track in Sentry
	// sentry.CaptureException(fmt.Errorf("failed to parse memo for type %q and transaction %s", memoType.String(), txHash))
	return nil, memoType.String()
}

func SourceAccount(op xdr.Operation, txEnvelope xdr.TransactionEnvelope) string {
	account := op.SourceAccount
	if account != nil {
		return account.ToAccountId().Address()
	}
	return txEnvelope.SourceAccount().ToAccountId().Address()
}

func AssetCode(asset xdr.Asset) string {
	if asset.Type == xdr.AssetTypeAssetTypeNative {
		return "XLM"
	}
	return SanitizeUTF8(asset.GetCode())
}
