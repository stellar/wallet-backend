package utils

import (
	"crypto/sha256"
	"testing"

	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
)

func TestMemo(t *testing.T) {
	t.Run("type_none", func(t *testing.T) {
		memo := xdr.Memo{
			Type: xdr.MemoTypeMemoNone,
		}

		memo_value, memo_type := Memo(memo, "")
		assert.Equal(t, (*string)(nil), memo_value)
		assert.Equal(t, xdr.MemoTypeMemoNone.String(), memo_type)
	})

	t.Run("type_text", func(t *testing.T) {
		memo := xdr.Memo{
			Type: xdr.MemoTypeMemoText,
			Text: PointOf("test"),
		}

		memo_value, memo_type := Memo(memo, "")
		assert.Equal(t, "test", *memo_value)
		assert.Equal(t, xdr.MemoTypeMemoText.String(), memo_type)
	})

	t.Run("type_id", func(t *testing.T) {
		memo := xdr.Memo{
			Type: xdr.MemoTypeMemoId,
			Id:   PointOf(xdr.Uint64(12345)),
		}

		memo_value, memo_type := Memo(memo, "")
		assert.Equal(t, "12345", *memo_value)
		assert.Equal(t, xdr.MemoTypeMemoId.String(), memo_type)
	})

	t.Run("type_hash", func(t *testing.T) {
		hash := sha256.New()
		hash.Write([]byte("test"))
		value := xdr.Hash(hash.Sum(nil))
		memo := xdr.Memo{
			Type: xdr.MemoTypeMemoHash,
			Hash: &value,
		}

		memo_value, memo_type := Memo(memo, "")
		assert.Equal(t, value.HexString(), *memo_value)
		assert.Equal(t, xdr.MemoTypeMemoHash.String(), memo_type)
	})

	t.Run("type_return", func(t *testing.T) {
		hash := sha256.New()
		hash.Write([]byte("test"))
		value := xdr.Hash(hash.Sum(nil))
		memo := xdr.Memo{
			Type:    xdr.MemoTypeMemoReturn,
			RetHash: &value,
		}

		memo_value, memo_type := Memo(memo, "")
		assert.Equal(t, value.HexString(), *memo_value)
		assert.Equal(t, xdr.MemoTypeMemoReturn.String(), memo_type)
	})
}
