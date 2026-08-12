package processors

import (
	"testing"

	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// dedupeParticipantsReference is the map-based implementation dedupeParticipants
// replaced, kept as a differential oracle: both must always keep the same set of
// account IDs, whatever order each returns them in.
func dedupeParticipantsReference(in []xdr.AccountId) (out []xdr.AccountId) {
	set := map[string]xdr.AccountId{}
	for _, id := range in {
		set[id.Address()] = id
	}

	for _, id := range set {
		out = append(out, id)
	}
	return
}

// testAccountID builds a deterministic AccountId whose ed25519 key starts with seed.
func testAccountID(seed byte) xdr.AccountId {
	key := xdr.Uint256{seed}
	return xdr.AccountId{Type: xdr.PublicKeyTypePublicKeyTypeEd25519, Ed25519: &key}
}

func Test_dedupeParticipants(t *testing.T) {
	a, b, c := testAccountID(1), testAccountID(2), testAccountID(3)
	// aClone shares a's key bytes through a distinct pointer: duplicates in real
	// input are separate xdr decodes of the same account, never shared pointers.
	aKey := *a.Ed25519
	aClone := xdr.AccountId{Type: xdr.PublicKeyTypePublicKeyTypeEd25519, Ed25519: &aKey}

	testCases := []struct {
		name string
		in   []xdr.AccountId
		want []xdr.AccountId
	}{
		{name: "🟢empty", in: []xdr.AccountId{}, want: []xdr.AccountId{}},
		{name: "🟢single", in: []xdr.AccountId{a}, want: []xdr.AccountId{a}},
		{name: "🟢no duplicates", in: []xdr.AccountId{a, b, c}, want: []xdr.AccountId{a, b, c}},
		{name: "🟢adjacent duplicate", in: []xdr.AccountId{a, aClone, b}, want: []xdr.AccountId{a, b}},
		{name: "🟢non-adjacent duplicate keeps first", in: []xdr.AccountId{b, a, c, aClone}, want: []xdr.AccountId{b, a, c}},
		{name: "🟢all the same", in: []xdr.AccountId{a, aClone, a}, want: []xdr.AccountId{a}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// The oracle runs on its own copy: dedupeParticipants may reuse its
			// input's backing array.
			oracle := dedupeParticipantsReference(append([]xdr.AccountId{}, tc.in...))

			got := dedupeParticipants(tc.in)

			require.ElementsMatch(t, oracle, got, "diverged from the reference implementation")
			assert.Equal(t, tc.want, got, "expected first-seen order")
		})
	}
}

func Test_dedupeParticipants_zeroAllocationsOnTypicalInput(t *testing.T) {
	a, b := testAccountID(1), testAccountID(2)
	in := []xdr.AccountId{a, b, a}

	allocs := testing.AllocsPerRun(100, func() {
		dedupeParticipants(in)
	})
	assert.Zero(t, allocs)
}
