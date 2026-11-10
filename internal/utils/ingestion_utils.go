package utils

import (
	"fmt"
	"strconv"

	"github.com/stellar/go-stellar-sdk/xdr"
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
		return nil, ""
	}

	// TODO: track in Sentry
	return nil, memoType.String()
}

// ProtocolHistoryCursorName returns the ingest_store key for a protocol's history migration cursor.
func ProtocolHistoryCursorName(protocolID string) string {
	return fmt.Sprintf("protocol_%s_history_cursor", protocolID)
}

// ProtocolCurrentStateCursorName returns the ingest_store key for a protocol's current state cursor.
func ProtocolCurrentStateCursorName(protocolID string) string {
	return fmt.Sprintf("protocol_%s_current_state_cursor", protocolID)
}
