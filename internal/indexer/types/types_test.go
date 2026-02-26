package types

import (
	"database/sql/driver"
	"testing"

	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNullableJSONB_Scan(t *testing.T) {
	testCases := []struct {
		name            string
		input           any
		want            NullableJSONB
		wantErrContains string
	}{
		{
			name:  "🟢nil value",
			input: nil,
			want:  nil,
		},
		{
			name:  "🟢empty JSON",
			input: []byte("{}"),
			want:  NullableJSONB{},
		},
		{
			name:  "🟢valid JSON",
			input: []byte(`{"key": "value"}`),
			want:  NullableJSONB{"key": "value"},
		},
		{
			name:            "🔴invalid JSON",
			input:           []byte(`{"key": "value"`), // missing closing brace
			want:            nil,
			wantErrContains: "unmarshalling value []byte",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var n NullableJSONB
			err := n.Scan(tc.input)
			if tc.wantErrContains != "" {
				assert.ErrorContains(t, err, tc.wantErrContains)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.want, n)
			}
		})
	}
}

func TestAddressBytea_Scan(t *testing.T) {
	// Generate a valid G... address for testing
	kp := keypair.MustRandom()
	validAddress := kp.Address()

	// Build expected 33-byte representation
	_, rawBytes, err := strkey.DecodeAny(validAddress)
	require.NoError(t, err)
	validBytes := make([]byte, 33)
	validBytes[0] = byte(strkey.VersionByteAccountID)
	copy(validBytes[1:], rawBytes)

	testCases := []struct {
		name            string
		input           any
		want            AddressBytea
		wantErrContains string
	}{
		{
			name:  "🟢nil value",
			input: nil,
			want:  "",
		},
		{
			name:  "🟢valid 33-byte address",
			input: validBytes,
			want:  AddressBytea(validAddress),
		},
		{
			name:            "🔴wrong type",
			input:           12345,
			wantErrContains: "expected []byte",
		},
		{
			name:            "🔴wrong length",
			input:           []byte{1, 2, 3},
			wantErrContains: "expected 33 bytes",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var s AddressBytea
			err := s.Scan(tc.input)
			if tc.wantErrContains != "" {
				assert.ErrorContains(t, err, tc.wantErrContains)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.want, s)
			}
		})
	}
}

func TestAddressBytea_Value(t *testing.T) {
	// Generate a valid G... address for testing
	kp := keypair.MustRandom()
	validAddress := kp.Address()

	// Build expected 33-byte representation
	_, rawBytes, err := strkey.DecodeAny(validAddress)
	require.NoError(t, err)
	expectedBytes := make([]byte, 33)
	expectedBytes[0] = byte(strkey.VersionByteAccountID)
	copy(expectedBytes[1:], rawBytes)

	testCases := []struct {
		name            string
		input           AddressBytea
		want            driver.Value
		wantErrContains string
	}{
		{
			name:  "🟢empty string",
			input: "",
			want:  nil,
		},
		{
			name:  "🟢valid address",
			input: AddressBytea(validAddress),
			want:  expectedBytes,
		},
		{
			name:            "🔴invalid address",
			input:           "not-a-valid-address",
			wantErrContains: "decoding stellar address",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.input.Value()
			if tc.wantErrContains != "" {
				assert.ErrorContains(t, err, tc.wantErrContains)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.want, got)
			}
		})
	}
}

func TestAddressBytea_Roundtrip(t *testing.T) {
	// Test that Value -> Scan produces the original address
	kp := keypair.MustRandom()
	original := AddressBytea(kp.Address())

	// Convert to bytes
	bytes, err := original.Value()
	require.NoError(t, err)

	// Convert back to address
	var restored AddressBytea
	err = restored.Scan(bytes)
	require.NoError(t, err)

	assert.Equal(t, original, restored)
}

func TestAddressBytea_String(t *testing.T) {
	testCases := []struct {
		name  string
		input AddressBytea
		want  string
	}{
		{
			name:  "🟢empty string",
			input: "",
			want:  "",
		},
		{
			name:  "🟢valid G address",
			input: AddressBytea("GBRPYHIL2CI3FNQ4BXLFMNDLFJUNPU2HY3ZMFSHONUCEOASW7QC7OX2H"),
			want:  "GBRPYHIL2CI3FNQ4BXLFMNDLFJUNPU2HY3ZMFSHONUCEOASW7QC7OX2H",
		},
		{
			name:  "🟢valid C address",
			input: AddressBytea("CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"),
			want:  "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.input.String()
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestNullableJSONB_Value(t *testing.T) {
	testCases := []struct {
		name            string
		input           NullableJSONB
		want            driver.Value
		wantErrContains string
	}{
		{
			name:  "🟢nil map",
			input: nil,
			want:  []byte("null"),
		},
		{
			name:  "🟢empty map",
			input: NullableJSONB{},
			want:  []byte("{}"),
		},
		{
			name:            "🟢non-empty map",
			input:           NullableJSONB{"key": "value"},
			want:            []byte(`{"key":"value"}`),
			wantErrContains: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.input.Value()
			if tc.wantErrContains != "" {
				assert.ErrorContains(t, err, tc.wantErrContains)
			} else {
				assert.NoError(t, err)
				assert.JSONEq(t, string(tc.want.([]byte)), string(got.([]byte)))
			}
		})
	}
}
