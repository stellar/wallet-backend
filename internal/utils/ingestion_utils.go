package utils

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"github.com/stellar/go-stellar-sdk/xdr"
)

// Memo type names as the GraphQL MemoType enum spells them.
const (
	MemoTypeNone   = "NONE"
	MemoTypeText   = "TEXT"
	MemoTypeID     = "ID"
	MemoTypeHash   = "HASH"
	MemoTypeReturn = "RETURN"
)

// EncodeMemo maps a memo to its storage form: the XDR MemoType discriminant and the raw
// value bytes (text bytes as-is, id as 8-byte big-endian, hash/return as 32 bytes, nil for
// none). Raw bytes keep every memo lossless; a text memo need not be valid UTF-8.
func EncodeMemo(memo xdr.Memo) (int16, []byte) {
	switch memo.Type {
	case xdr.MemoTypeMemoText:
		return int16(memo.Type), []byte(memo.MustText())
	case xdr.MemoTypeMemoId:
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], uint64(memo.MustId()))
		return int16(memo.Type), b[:]
	case xdr.MemoTypeMemoHash:
		h := memo.MustHash()
		return int16(memo.Type), h[:]
	case xdr.MemoTypeMemoReturn:
		h := memo.MustRetHash()
		return int16(memo.Type), h[:]
	case xdr.MemoTypeMemoNone:
		return int16(memo.Type), nil
	default:
		return int16(memo.Type), nil
	}
}

// DecodeMemo maps the storage form back to the API form: the MemoType enum name and the
// value rendered as text (TEXT as UTF-8 with invalid sequences replaced, ID as a decimal
// string, HASH/RETURN as hex), nil for NONE.
func DecodeMemo(memoType int16, value []byte) (string, *string, error) {
	switch xdr.MemoType(memoType) {
	case xdr.MemoTypeMemoNone:
		return MemoTypeNone, nil, nil
	case xdr.MemoTypeMemoText:
		s := strings.ToValidUTF8(string(value), "�")
		return MemoTypeText, &s, nil
	case xdr.MemoTypeMemoId:
		if len(value) != 8 {
			return "", nil, fmt.Errorf("memo id value has %d bytes, want 8", len(value))
		}
		s := strconv.FormatUint(binary.BigEndian.Uint64(value), 10)
		return MemoTypeID, &s, nil
	case xdr.MemoTypeMemoHash, xdr.MemoTypeMemoReturn:
		if len(value) != 32 {
			return "", nil, fmt.Errorf("memo hash value has %d bytes, want 32", len(value))
		}
		s := hex.EncodeToString(value)
		if xdr.MemoType(memoType) == xdr.MemoTypeMemoHash {
			return MemoTypeHash, &s, nil
		}
		return MemoTypeReturn, &s, nil
	default:
		return "", nil, fmt.Errorf("unknown memo type %d", memoType)
	}
}

// ProtocolHistoryCursorName returns the ingest_store key for a protocol's history migration cursor.
func ProtocolHistoryCursorName(protocolID string) string {
	return fmt.Sprintf("protocol_%s_history_cursor", protocolID)
}

// ProtocolCurrentStateCursorName returns the ingest_store key for a protocol's current state cursor.
func ProtocolCurrentStateCursorName(protocolID string) string {
	return fmt.Sprintf("protocol_%s_current_state_cursor", protocolID)
}
