package types

import (
	"database/sql/driver"
	"encoding/hex"
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

func TestHashBytea_Scan(t *testing.T) {
	// Valid 32-byte hash
	validHex := "e76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48760"
	validBytes, err := hex.DecodeString(validHex)
	require.NoError(t, err)

	testCases := []struct {
		name            string
		input           any
		want            HashBytea
		wantErrContains string
	}{
		{
			name:  "🟢nil value",
			input: nil,
			want:  "",
		},
		{
			name:  "🟢valid 32-byte hash",
			input: validBytes,
			want:  HashBytea(validHex),
		},
		{
			name:            "🔴wrong type",
			input:           12345,
			wantErrContains: "expected []byte",
		},
		{
			name:            "🔴wrong length",
			input:           []byte{1, 2, 3},
			wantErrContains: "expected 32 bytes",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var h HashBytea
			err := h.Scan(tc.input)
			if tc.wantErrContains != "" {
				assert.ErrorContains(t, err, tc.wantErrContains)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.want, h)
			}
		})
	}
}

func TestHashBytea_Value(t *testing.T) {
	// Valid 32-byte hash
	validHex := "e76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48760"
	expectedBytes, err := hex.DecodeString(validHex)
	require.NoError(t, err)

	testCases := []struct {
		name            string
		input           HashBytea
		want            driver.Value
		wantErrContains string
	}{
		{
			name:  "🟢empty string",
			input: "",
			want:  nil,
		},
		{
			name:  "🟢valid hex hash",
			input: HashBytea(validHex),
			want:  expectedBytes,
		},
		{
			name:            "🔴invalid hex",
			input:           "not-a-valid-hex",
			wantErrContains: "decoding hex hash",
		},
		{
			name:            "🔴wrong length (short)",
			input:           "abcd",
			wantErrContains: "invalid hash length",
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

func TestHashBytea_Roundtrip(t *testing.T) {
	// Test that Value -> Scan produces the original hash
	original := HashBytea("e76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48760")

	// Convert to bytes
	bytes, err := original.Value()
	require.NoError(t, err)

	// Convert back to hash
	var restored HashBytea
	err = restored.Scan(bytes)
	require.NoError(t, err)

	assert.Equal(t, original, restored)
}

func TestHashBytea_String(t *testing.T) {
	testCases := []struct {
		name  string
		input HashBytea
		want  string
	}{
		{
			name:  "🟢empty string",
			input: "",
			want:  "",
		},
		{
			name:  "🟢valid hex hash",
			input: HashBytea("e76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48760"),
			want:  "e76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48760",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.input.String()
			assert.Equal(t, tc.want, got)
		})
	}
}

// buildOrdinalTestInput returns a fixed slice of state changes spanning two
// operations, interleaved in emission order: op 10 gets three, op 20 gets two.
func buildOrdinalTestInput() []StateChange {
	return []StateChange{
		{ToID: 1, OperationID: 10, StateChangeCategory: StateChangeCategoryBalance},
		{ToID: 1, OperationID: 20, StateChangeCategory: StateChangeCategoryBalance},
		{ToID: 1, OperationID: 10, StateChangeCategory: StateChangeCategoryBalance},
		{ToID: 1, OperationID: 10, StateChangeCategory: StateChangeCategoryBalance},
		{ToID: 1, OperationID: 20, StateChangeCategory: StateChangeCategoryBalance},
	}
}

func TestAssignStateChangeOrdinals_OrdinalsPerOperationGroup(t *testing.T) {
	changes := buildOrdinalTestInput()
	AssignStateChangeOrdinals(changes, StateChangeOrdinalBaseIndexer)

	// op 10's three changes are numbered 1..3 in the order they appear; op 20's
	// two changes are numbered 1..2 independently, interleaved with op 10's.
	assert.Equal(t, []int64{1, 1, 2, 3, 2}, []int64{
		changes[0].StateChangeID, changes[1].StateChangeID, changes[2].StateChangeID,
		changes[3].StateChangeID, changes[4].StateChangeID,
	})
}

func TestAssignStateChangeOrdinals_NamespaceBaseIsAdded(t *testing.T) {
	changes := buildOrdinalTestInput()
	AssignStateChangeOrdinals(changes, StateChangeOrdinalBaseBlend)

	for _, sc := range changes {
		assert.GreaterOrEqual(t, sc.StateChangeID, StateChangeOrdinalBaseBlend)
		assert.Less(t, sc.StateChangeID, StateChangeOrdinalBaseBlend+1000)
	}
	// Same relative ordinals as the unnamespaced case, offset by the base.
	assert.Equal(t, StateChangeOrdinalBaseBlend+1, changes[0].StateChangeID)
	assert.Equal(t, StateChangeOrdinalBaseBlend+3, changes[3].StateChangeID)
}

func TestAssignStateChangeOrdinals_Deterministic(t *testing.T) {
	first := buildOrdinalTestInput()
	second := buildOrdinalTestInput()

	AssignStateChangeOrdinals(first, StateChangeOrdinalBaseSEP41)
	AssignStateChangeOrdinals(second, StateChangeOrdinalBaseSEP41)

	// Processing the same input twice (e.g. a re-ingested ledger) must produce
	// byte-identical state_change_ids, not just equal-length results.
	require.Len(t, second, len(first))
	for i := range first {
		assert.Equal(t, first[i].StateChangeID, second[i].StateChangeID, "index %d", i)
	}
}

func TestAssignStateChangeOrdinals_DifferentEmittersNeverCollide(t *testing.T) {
	indexerChanges := []StateChange{{ToID: 1, OperationID: 42}}
	sep41Changes := []StateChange{{ToID: 1, OperationID: 42}}
	blendChanges := []StateChange{{ToID: 1, OperationID: 42}}

	AssignStateChangeOrdinals(indexerChanges, StateChangeOrdinalBaseIndexer)
	AssignStateChangeOrdinals(sep41Changes, StateChangeOrdinalBaseSEP41)
	AssignStateChangeOrdinals(blendChanges, StateChangeOrdinalBaseBlend)

	// Same (to_id, operation_id) from three independent emitters: without
	// namespacing these would all be state_change_id=1 and collide on the
	// state_changes primary key.
	ids := map[int64]bool{
		indexerChanges[0].StateChangeID: true,
		sep41Changes[0].StateChangeID:   true,
		blendChanges[0].StateChangeID:   true,
	}
	assert.Len(t, ids, 3, "each emitter's ID for the same operation must be distinct")
}

func TestAssignStateChangeOrdinals_GroupsByToIDAndOperationID(t *testing.T) {
	// A window-scoped emitter can stage transaction-level changes with
	// OperationID=0 for several transactions (distinct ToIDs) in one call.
	// Ordinals restart per (to_id, operation_id) pair, not per operation_id
	// alone: otherwise a row's ID would depend on which other transactions
	// happen to share the window, and re-running the same ledger under
	// different window bounds would produce different IDs for the same logical
	// row — silently bypassing the state_changes primary key on re-ingest.
	changes := []StateChange{
		{ToID: 100, OperationID: 0, StateChangeCategory: StateChangeCategoryBalance},
		{ToID: 100, OperationID: 0, StateChangeCategory: StateChangeCategoryBalance},
		{ToID: 200, OperationID: 0, StateChangeCategory: StateChangeCategoryBalance},
		{ToID: 200, OperationID: 0, StateChangeCategory: StateChangeCategoryBalance},
	}
	AssignStateChangeOrdinals(changes, StateChangeOrdinalBaseIndexer)

	// Keying on operation_id alone would yield 1,2,3,4 (one shared bucket).
	assert.Equal(t, []int64{1, 2, 1, 2}, []int64{
		changes[0].StateChangeID, changes[1].StateChangeID,
		changes[2].StateChangeID, changes[3].StateChangeID,
	})
}
