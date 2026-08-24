package processors

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"testing"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/toid"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/indexer/types"
)

const (
	ledgerCloseMetaXDR = "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"
	opXDRStr           = "AAAAAAAAABgAAAADAAAAAAAAAAAAAAAAbAP8Dg5wDC5/9n7IDHGKRCJGNYyEnhgENXUu25/mVkfDszyD0xmggy2XywsLdKboM1yzTNxvYPZIRMN/whOT/AAAAABZPFPHak6AnPiuLp9sXpV+KSwSVllBH+VkL93LXY2EqAAAAAEAAAAQAAAAAQAAAAUAAAAPAAAAB0VkMjU1MTkAAAAADQAAACCx6kbMzSf2zoYoDWEaBAMVOOg4C9qyQjO+Rktf48yb8gAAABAAAAABAAAAAQAAAAEAAAAQAAAAAQAAAAEAAAABAAAAEAAAAAEAAAABAAAADwAAAApQZXJzaXN0ZW50AAAAAAABAAAAAAAAAAIAAAAAAAAAAAAAAABsA/wODnAMLn/2fsgMcYpEIkY1jISeGAQ1dS7bn+ZWR8OzPIPTGaCDLZfLCwt0pugzXLNM3G9g9khEw3/CE5P8AAAAAFk8U8dqToCc+K4un2xelX4pLBJWWUEf5WQv3ctdjYSoAAAAAQAAABAAAAABAAAABQAAAA8AAAAHRWQyNTUxOQAAAAANAAAAILHqRszNJ/bOhigNYRoEAxU46DgL2rJCM75GS1/jzJvyAAAAEAAAAAEAAAABAAAAAQAAABAAAAABAAAAAQAAAAEAAAAQAAAAAQAAAAEAAAAPAAAAClBlcnNpc3RlbnQAAAAAAAA="
)

func Test_ConvertTransaction(t *testing.T) {
	var lcm xdr.LedgerCloseMeta
	err := xdr.SafeUnmarshalBase64(ledgerCloseMetaXDR, &lcm)
	require.NoError(t, err)

	ledgerTxReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(network.TestNetworkPassphrase, lcm)
	require.NoError(t, err)
	ingestTx, err := ledgerTxReader.Read()
	require.NoError(t, err)

	gotDataTx, err := ConvertTransaction(&ingestTx)
	require.NoError(t, err)

	wantDataTx := &types.Transaction{
		Hash:                 "64eb94acc50eefc323cea80387fdceefc31466cc3a69eb8d2b312e0b5c3c62f0",
		ToID:                 20929375637504,
		FeeCharged:           4595918,
		ResultCode:           "TransactionResultCodeTxFeeBumpInnerSuccess",
		LedgerNumber:         4873,
		LedgerCreatedAt:      time.Date(2025, time.June, 19, 0, 3, 16, 0, time.UTC),
		IsFeeBump:            true,
		InnerTransactionHash: "afaef8a1b657ad5d2360cc001eb31b763bfd3430cba20273d49ff44be2a2152e",
	}
	assert.Equal(t, wantDataTx, gotDataTx)
}

func Test_ConvertOperation(t *testing.T) {
	var lcm xdr.LedgerCloseMeta
	err := xdr.SafeUnmarshalBase64(ledgerCloseMetaXDR, &lcm)
	require.NoError(t, err)

	ledgerTxReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(network.TestNetworkPassphrase, lcm)
	require.NoError(t, err)
	ingestTx, err := ledgerTxReader.Read()
	require.NoError(t, err)

	opIndex := uint32(0)
	op := ingestTx.Envelope.Operations()[opIndex]
	opID := toid.New(int32(ingestTx.Ledger.LedgerSequence()), int32(ingestTx.Index), int32(opIndex+1)).ToInt64()
	opResults, _ := ingestTx.Result.OperationResults()

	gotDataOp, err := ConvertOperation(&ingestTx, &op, opID, opIndex, opResults)
	require.NoError(t, err)

	// Decode expected base64 XDR to raw bytes for comparison
	expectedXDRBytes, err := base64.StdEncoding.DecodeString(opXDRStr)
	require.NoError(t, err)

	wantDataOp := &types.Operation{
		ID:              opID,
		OperationType:   types.OperationTypeFromXDR(op.Body.Type),
		OperationXDR:    types.XDRBytea(expectedXDRBytes),
		ResultCode:      OpSuccess,
		Successful:      true,
		LedgerCreatedAt: time.Date(2025, time.June, 19, 0, 3, 16, 0, time.UTC),
		LedgerNumber:    4873,
	}
	assert.Equal(t, wantDataOp, gotDataOp)
}

func Test_isClaimableBalance(t *testing.T) {
	tests := []struct {
		name     string
		id       string
		expected bool
	}{
		{
			name:     "valid claimable balance ID",
			id:       "BAAFK3PZYCD4YKOLFNOCJVG2JIHWOBE5NHU5FHY3ESAHMAO3C5RIYGTBDI",
			expected: true,
		},
		{
			name:     "regular account ID starting with G",
			id:       "GBRPYHIL2CI3FNQ4BXLFMNDLFJUNPU2HY3ZMFSHONUCEOASW7QC7OX2H",
			expected: false,
		},
		{
			name:     "liquidity pool ID starting with P",
			id:       "PAQKWTDZ3PQLV6OB5HZRLJ6BPZAXUZWBQNC6FDZF3EOJF5LNXKN7C5TJ",
			expected: false,
		},
		{
			name:     "invalid string",
			id:       "invalid-id",
			expected: false,
		},
		{
			name:     "empty string",
			id:       "",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isClaimableBalance(tt.id)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func Test_isLiquidityPool(t *testing.T) {
	poolID, err := strkey.Encode(strkey.VersionByteLiquidityPool, make([]byte, 32))
	require.NoError(t, err)

	tests := []struct {
		name     string
		id       string
		expected bool
	}{
		{name: "valid liquidity pool ID", id: poolID, expected: true},
		{name: "regular account ID starting with G", id: "GBRPYHIL2CI3FNQ4BXLFMNDLFJUNPU2HY3ZMFSHONUCEOASW7QC7OX2H", expected: false},
		{name: "claimable balance ID starting with B", id: "BAAFK3PZYCD4YKOLFNOCJVG2JIHWOBE5NHU5FHY3ESAHMAO3C5RIYGTBDI", expected: false},
		{name: "L prefix but not a valid strkey", id: "L" + poolID[1:len(poolID)-1] + "A", expected: false},
		{name: "invalid string", id: "invalid-id", expected: false},
		{name: "empty string", id: "", expected: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, isLiquidityPool(tt.id))
		})
	}
}

// Test_strkeyPrefixMatchesVersionByte pins the invariant the prefix gates in
// isLiquidityPool and isClaimableBalance rest on: a strkey's first character is
// fixed by its version byte and never by its payload, so a string that does not
// start with that character cannot decode to that version byte. Both an
// all-zero and an all-ones payload are encoded to show the payload does not
// reach the leading character.
func Test_strkeyPrefixMatchesVersionByte(t *testing.T) {
	payloads := map[string][]byte{
		"zero": make([]byte, 64),
		"ones": bytes.Repeat([]byte{0xFF}, 64),
	}
	versionBytes := map[byte]struct {
		versionByte strkey.VersionByte
		payloadLen  int
	}{
		liquidityPoolStrkeyPrefix:    {strkey.VersionByteLiquidityPool, 32},
		claimableBalanceStrkeyPrefix: {strkey.VersionByteClaimableBalance, 33},
	}

	for name, payload := range payloads {
		for wantPrefix, v := range versionBytes {
			t.Run(fmt.Sprintf("%s/%c", name, wantPrefix), func(t *testing.T) {
				encoded, err := strkey.Encode(v.versionByte, payload[:v.payloadLen])
				require.NoError(t, err)
				assert.Equal(t, wantPrefix, encoded[0])
			})
		}
	}
}

// memoAssetIssuerAlt is a second issuer, used to show the issuer participates in
// the memo key.
const memoAssetIssuerAlt = "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN"

// memoEntryCount reports how many distinct assets the memo has cached.
func memoEntryCount(memo *AssetContractIDMemo) int {
	count := 0
	memo.m.Range(func(_, _ any) bool {
		count++
		return true
	})
	return count
}

// Test_AssetContractIDMemo pins the property the memo rests on: a cached lookup
// returns exactly what an uncached derivation returns, for every asset shape,
// and each distinct asset gets its own entry. The asset set separates the two
// credit widths under one issuer (including the 4-vs-5 character boundary where
// the width flips), one code under two issuers, and the native asset.
func Test_AssetContractIDMemo(t *testing.T) {
	assets := []struct {
		name  string
		asset xdr.Asset
	}{
		{name: "native", asset: xdr.MustNewNativeAsset()},
		{name: "alphanum4", asset: xdr.MustNewCreditAsset(testAssetCode, testAssetIssuer)},
		{name: "alphanum12 at the width boundary", asset: xdr.MustNewCreditAsset(testAssetCode+"X", testAssetIssuer)},
		{name: "alphanum12 at full width", asset: xdr.MustNewCreditAsset("USDCOIN12345", testAssetIssuer)},
		{name: "alphanum4 under another issuer", asset: xdr.MustNewCreditAsset(testAssetCode, memoAssetIssuerAlt)},
	}

	var memo AssetContractIDMemo
	for _, tc := range assets {
		t.Run(tc.name, func(t *testing.T) {
			rawContractID, err := tc.asset.ContractID(networkPassphrase)
			require.NoError(t, err)
			want := strkey.MustEncode(strkey.VersionByteContract, rawContractID[:])

			// The miss and the hit must both agree with the uncached derivation.
			// A key that collided with an asset cached by an earlier subtest
			// would surface here as the other asset's contract ID.
			missed, err := memo.FromAsset(networkPassphrase, tc.asset)
			require.NoError(t, err)
			assert.Equal(t, want, missed)

			hit, err := memo.FromAsset(networkPassphrase, tc.asset)
			require.NoError(t, err)
			assert.Equal(t, want, hit)
		})
	}
	assert.Equal(t, len(assets), memoEntryCount(&memo), "each distinct asset must occupy its own entry")
}

// Test_AssetContractIDMemoAccessorsAgree pins that the accessors are one
// mechanism and not three: the same asset arriving as XDR, as extracted detail
// strings, or as a bare code and issuer resolves to one contract ID and shares
// one entry.
func Test_AssetContractIDMemoAccessorsAgree(t *testing.T) {
	tests := []struct {
		name      string
		assetType string
		code      string
	}{
		{name: "alphanum4", assetType: "credit_alphanum4", code: testAssetCode},
		{name: "alphanum12", assetType: "credit_alphanum12", code: testAssetCode + "X"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var memo AssetContractIDMemo

			fromAsset, err := memo.FromAsset(networkPassphrase, xdr.MustNewCreditAsset(tc.code, testAssetIssuer))
			require.NoError(t, err)

			fromDetails, err := memo.fromDetails(networkPassphrase, tc.assetType, tc.code, testAssetIssuer)
			require.NoError(t, err)
			assert.Equal(t, fromAsset, fromDetails)

			fromCredit, err := memo.fromCreditAsset(networkPassphrase, tc.code, testAssetIssuer)
			require.NoError(t, err)
			assert.Equal(t, fromAsset, fromCredit)

			assert.Equal(t, 1, memoEntryCount(&memo), "all three accessors must key the asset identically")
		})
	}
}
