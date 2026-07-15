package scalars

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUnmarshalUInt32(t *testing.T) {
	testCases := []struct {
		name      string
		input     any
		want      uint32
		wantError bool
	}{
		{name: "int", input: int(42), want: 42},
		{name: "int32", input: int32(42), want: 42},
		{name: "int64", input: int64(42), want: 42},
		{name: "json.Number integral", input: json.Number("42"), want: 42},
		{name: "float64 integral", input: float64(42), want: 42},
		{name: "string numeric", input: "42", want: 42},
		{name: "zero", input: int(0), want: 0},
		{name: "max uint32", input: int64(math.MaxUint32), want: math.MaxUint32},

		{name: "negative int rejected", input: int(-1), wantError: true},
		{name: "negative int64 rejected", input: int64(-1), wantError: true},
		{name: "negative string rejected", input: "-1", wantError: true},
		{name: "int64 above max rejected", input: int64(math.MaxUint32) + 1, wantError: true},
		{name: "json.Number above max rejected", input: json.Number("4294967296"), wantError: true},
		{name: "float above max rejected", input: float64(math.MaxUint32) + 1, wantError: true},
		{name: "non-integral float rejected", input: float64(42.5), wantError: true},
		{name: "non-numeric string rejected", input: "not-a-number", wantError: true},
		{name: "unsupported type rejected", input: true, wantError: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := UnmarshalUInt32(tc.input)
			if tc.wantError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}
