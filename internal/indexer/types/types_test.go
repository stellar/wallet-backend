package types

import (
	"database/sql/driver"
	"testing"

	"github.com/stretchr/testify/assert"
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
			wantErrContains: "unmarshalling JSONB",
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
