package resolvers

import (
	"encoding/base64"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBalanceSourcesForAddress_IncludesSEP41(t *testing.T) {
	g := keypair.MustRandom().Address()
	gSources := balanceSourcesForAddress(g)
	assert.Contains(t, gSources, balanceSourceSEP41, "G-addresses must advertise SEP-41 as a balance source")
	assert.Equal(t, []balanceSource{balanceSourceNative, balanceSourceClassic, balanceSourceSEP41}, gSources)

	// A syntactically valid contract address for the sake of the test.
	c := "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA"
	cSources := balanceSourcesForAddress(c)
	assert.Contains(t, cSources, balanceSourceSEP41, "C-addresses must also advertise SEP-41")
	assert.Equal(t, []balanceSource{balanceSourceSAC, balanceSourceSEP41}, cSources)
}

func TestParseBalanceCursor_AcceptsSEP41CursorWithUUID(t *testing.T) {
	cid := uuid.New()
	// parseBalanceCursor takes the inner payload, not the base64-wrapped cursor.
	inner := fmt.Sprintf("%s:%s:%s", balanceCursorPrefix, balanceSourceSEP41, cid.String())
	got, err := parseBalanceCursor(&inner, []balanceSource{balanceSourceNative, balanceSourceClassic, balanceSourceSEP41})
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, balanceSourceSEP41, got.Source)
	assert.Equal(t, cid.String(), got.ID)

	// And its .uuid() helper should round-trip.
	parsed, uerr := got.uuid()
	require.NoError(t, uerr)
	require.NotNil(t, parsed)
	assert.Equal(t, cid, *parsed)
}

func TestParseBalanceCursor_RejectsSEP41CursorWithNonUUIDID(t *testing.T) {
	inner := fmt.Sprintf("%s:%s:not-a-uuid", balanceCursorPrefix, balanceSourceSEP41)
	_, err := parseBalanceCursor(&inner, []balanceSource{balanceSourceSEP41})
	require.Error(t, err)
}

func TestParseBalanceCursor_UnknownSourceStillRejected(t *testing.T) {
	// A client that tampers with the cursor source should still get rejected; this
	// guards against replaying e.g. a classic cursor against an address that can't hold classic.
	inner := base64.StdEncoding.EncodeToString([]byte("v1:classic:" + uuid.New().String()))
	_, err := parseBalanceCursor(&inner, []balanceSource{balanceSourceSEP41})
	require.Error(t, err)
}
