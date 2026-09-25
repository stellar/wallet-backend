package contracts

import (
	"testing"

	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtractAddressFromScVal(t *testing.T) {
	const (
		accountAddr  = "GCYNTH5HDQRNIQ3BSSYPWFO5AHH5ERVZ32C37QRXT6TXK3OJFFOIVXDE"
		contractAddr = "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA"
	)
	accountID := xdr.MustAddress(accountAddr)
	var contractID xdr.ContractId
	copy(contractID[:], strkey.MustDecode(strkey.VersionByteContract, contractAddr))
	var ed xdr.Uint256
	copy(ed[:], strkey.MustDecode(strkey.VersionByteAccountID, accountAddr))
	cbHash := xdr.Hash{0xcb}
	poolID := xdr.PoolId{0x1f}

	testCases := []struct {
		name    string
		addr    xdr.ScAddress
		want    string
		wantErr string
	}{
		{
			name: "account",
			addr: xdr.ScAddress{Type: xdr.ScAddressTypeScAddressTypeAccount, AccountId: &accountID},
			want: accountAddr,
		},
		{
			name: "contract",
			addr: xdr.ScAddress{Type: xdr.ScAddressTypeScAddressTypeContract, ContractId: &contractID},
			want: contractAddr,
		},
		{
			name: "muxed_reduces_to_base",
			addr: xdr.ScAddress{
				Type:         xdr.ScAddressTypeScAddressTypeMuxedAccount,
				MuxedAccount: &xdr.MuxedEd25519Account{Id: 9, Ed25519: ed},
			},
			want: accountAddr,
		},
		{
			name: "claimable_balance_rejected",
			addr: xdr.ScAddress{
				Type: xdr.ScAddressTypeScAddressTypeClaimableBalance,
				ClaimableBalanceId: &xdr.ClaimableBalanceId{
					Type: xdr.ClaimableBalanceIdTypeClaimableBalanceIdTypeV0,
					V0:   &cbHash,
				},
			},
			wantErr: "ScAddressTypeScAddressTypeClaimableBalance",
		},
		{
			name:    "liquidity_pool_rejected",
			addr:    xdr.ScAddress{Type: xdr.ScAddressTypeScAddressTypeLiquidityPool, LiquidityPoolId: &poolID},
			wantErr: "ScAddressTypeScAddressTypeLiquidityPool",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := extractAddressFromScVal(xdr.ScVal{Type: xdr.ScValTypeScvAddress, Address: &tc.addr})
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				assert.Empty(t, got)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}
