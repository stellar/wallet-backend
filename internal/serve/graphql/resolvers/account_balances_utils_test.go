package resolvers

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
	graphql1 "github.com/stellar/wallet-backend/internal/serve/graphql/generated"
)

func TestBuildNativeBalanceFromDB(t *testing.T) {
	nb := &data.NativeBalance{
		Balance:            1000000000,
		MinimumBalance:     30000000, // pure base reserve for 4 subentries: (2 + 4) * 5_000_000
		BuyingLiabilities:  100,
		SellingLiabilities: 200,
		NumSubEntries:      4,
		LedgerNumber:       12345,
	}

	got, err := buildNativeBalanceFromDB(nb, "Test SDF Network ; September 2015")
	require.NoError(t, err)
	require.NotNil(t, got)

	assert.Equal(t, graphql1.TokenTypeNative, got.TokenType)
	assert.Equal(t, uint32(4), got.NumSubentries)
	assert.Equal(t, uint32(12345), got.LastModifiedLedger)
}
