package utils

import (
	"crypto/sha256"
	"testing"

	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
)

func TestMemo(t *testing.T) {
	t.Run("type_none", func(t *testing.T) {
		memo := xdr.Memo{
			Type: xdr.MemoTypeMemoNone,
		}

		memoValue, memoType := Memo(memo, "")
		assert.Equal(t, (*string)(nil), memoValue)
		assert.Equal(t, xdr.MemoTypeMemoNone.String(), memoType)
	})

	t.Run("type_text", func(t *testing.T) {
		memo := xdr.Memo{
			Type: xdr.MemoTypeMemoText,
			Text: PointOf("test"),
		}

		memoValue, memoType := Memo(memo, "")
		assert.Equal(t, "test", *memoValue)
		assert.Equal(t, xdr.MemoTypeMemoText.String(), memoType)
	})

	t.Run("type_id", func(t *testing.T) {
		memo := xdr.Memo{
			Type: xdr.MemoTypeMemoId,
			Id:   PointOf(xdr.Uint64(12345)),
		}

		memoValue, memoType := Memo(memo, "")
		assert.Equal(t, "12345", *memoValue)
		assert.Equal(t, xdr.MemoTypeMemoId.String(), memoType)
	})

	t.Run("type_hash", func(t *testing.T) {
		hash := sha256.New()
		hash.Write([]byte("test"))
		value := xdr.Hash(hash.Sum(nil))
		memo := xdr.Memo{
			Type: xdr.MemoTypeMemoHash,
			Hash: &value,
		}

		memoValue, memoType := Memo(memo, "")
		assert.Equal(t, value.HexString(), *memoValue)
		assert.Equal(t, xdr.MemoTypeMemoHash.String(), memoType)
	})

	t.Run("type_return", func(t *testing.T) {
		hash := sha256.New()
		hash.Write([]byte("test"))
		value := xdr.Hash(hash.Sum(nil))
		memo := xdr.Memo{
			Type:    xdr.MemoTypeMemoReturn,
			RetHash: &value,
		}

		memoValue, memoType := Memo(memo, "")
		assert.Equal(t, value.HexString(), *memoValue)
		assert.Equal(t, xdr.MemoTypeMemoReturn.String(), memoType)
	})
}
