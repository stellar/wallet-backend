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

		result := Memo(memo, "")
		assert.Equal(t, (*string)(nil), result)
	})

	t.Run("type_text", func(t *testing.T) {
		value := "test"
		memo := xdr.Memo{
			Type: xdr.MemoTypeMemoText,
			Text: &value,
		}

		result := Memo(memo, "")
		assert.Equal(t, "test", *result)
	})

	t.Run("type_id", func(t *testing.T) {
		value := xdr.Uint64(12345)
		memo := xdr.Memo{
			Type: xdr.MemoTypeMemoId,
			Id:   &value,
		}

		result := Memo(memo, "")
		assert.Equal(t, "12345", *result)
	})

	t.Run("type_hash", func(t *testing.T) {
		hash := sha256.New()
		hash.Write([]byte("test"))
		value := xdr.Hash(hash.Sum(nil))
		memo := xdr.Memo{
			Type: xdr.MemoTypeMemoHash,
			Hash: &value,
		}

		result := Memo(memo, "")
		assert.Equal(t, value.HexString(), *result)
	})

	t.Run("type_return", func(t *testing.T) {
		hash := sha256.New()
		hash.Write([]byte("test"))
		value := xdr.Hash(hash.Sum(nil))
		memo := xdr.Memo{
			Type:    xdr.MemoTypeMemoReturn,
			RetHash: &value,
		}

		result := Memo(memo, "")
		assert.Equal(t, value.HexString(), *result)
	})
}
