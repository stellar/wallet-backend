package utils

import (
	"encoding/hex"
	"strings"
	"testing"

	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncodeDecodeMemo(t *testing.T) {
	var hash xdr.Hash
	for i := range hash {
		hash[i] = byte(i)
	}
	hashHex := hex.EncodeToString(hash[:])

	testCases := []struct {
		name      string
		memo      xdr.Memo
		wantType  int16
		wantBytes []byte
		wantName  string
		wantValue *string
	}{
		{
			name:     "none",
			memo:     xdr.Memo{Type: xdr.MemoTypeMemoNone},
			wantType: 0, wantBytes: nil, wantName: MemoTypeNone, wantValue: nil,
		},
		{
			name:     "text",
			memo:     xdr.MemoText("hello memo"),
			wantType: 1, wantBytes: []byte("hello memo"), wantName: MemoTypeText, wantValue: ptr("hello memo"),
		},
		{
			name:     "text_invalid_utf8_is_stored_raw_and_rendered_with_replacement",
			memo:     xdr.MemoText("ab\xffcd"),
			wantType: 1, wantBytes: []byte("ab\xffcd"), wantName: MemoTypeText, wantValue: ptr("ab�cd"),
		},
		{
			name:     "id_above_int64",
			memo:     xdr.MemoID(18446744073709551615),
			wantType: 2, wantBytes: []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, wantName: MemoTypeID, wantValue: ptr("18446744073709551615"),
		},
		{
			name:     "id_zero",
			memo:     xdr.MemoID(0),
			wantType: 2, wantBytes: make([]byte, 8), wantName: MemoTypeID, wantValue: ptr("0"),
		},
		{
			name:     "hash",
			memo:     xdr.MemoHash(hash),
			wantType: 3, wantBytes: hash[:], wantName: MemoTypeHash, wantValue: ptr(hashHex),
		},
		{
			name:     "return",
			memo:     xdr.MemoRetHash(hash),
			wantType: 4, wantBytes: hash[:], wantName: MemoTypeReturn, wantValue: ptr(hashHex),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gotType, gotBytes := EncodeMemo(tc.memo)
			assert.Equal(t, tc.wantType, gotType)
			assert.Equal(t, tc.wantBytes, gotBytes)

			gotName, gotValue, err := DecodeMemo(gotType, gotBytes)
			require.NoError(t, err)
			assert.Equal(t, tc.wantName, gotName)
			assert.Equal(t, tc.wantValue, gotValue)
		})
	}
}

func TestDecodeMemo_malformed(t *testing.T) {
	_, _, err := DecodeMemo(2, []byte{1, 2, 3})
	require.ErrorContains(t, err, "memo id value has 3 bytes")
	_, _, err = DecodeMemo(3, []byte(strings.Repeat("x", 31)))
	require.ErrorContains(t, err, "memo hash value has 31 bytes")
	_, _, err = DecodeMemo(9, nil)
	require.ErrorContains(t, err, "unknown memo type 9")
}

func ptr(s string) *string { return &s }
