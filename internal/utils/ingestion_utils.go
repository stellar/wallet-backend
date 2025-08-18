package utils

import (
	"strconv"

	"github.com/stellar/go/xdr"
)

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
