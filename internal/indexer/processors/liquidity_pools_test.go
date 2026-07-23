// Unit tests for the liquidity pool processors.
// Tests pool-share balance extraction from pool_share trustlines and pool reserve extraction
// from LiquidityPoolEntry ledger entries.
package processors

import (
	"context"
	"testing"

	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/indexer/types"
)

// poolShareEntry creates a pool_share trustline entry with the given shares.
func poolShareEntry(accountID xdr.AccountId, poolID xdr.PoolId, shares int64) *xdr.LedgerEntry {
	return &xdr.LedgerEntry{
		LastModifiedLedgerSeq: 12345,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeTrustline,
			TrustLine: &xdr.TrustLineEntry{
				AccountId: accountID,
				Asset: xdr.TrustLineAsset{
					Type:            xdr.AssetTypeAssetTypePoolShare,
					LiquidityPoolId: &poolID,
				},
				Balance: xdr.Int64(shares),
				Limit:   xdr.Int64(10000000),
			},
		},
	}
}

func TestLiquidityPoolSharesProcessor_Name(t *testing.T) {
	assert.Equal(t, "liquidity_pool_shares", NewLiquidityPoolSharesProcessor(nil).Name())
}

func TestLiquidityPoolSharesProcessor_ProcessOperation(t *testing.T) {
	processor := NewLiquidityPoolSharesProcessor(nil)
	testAccount := accountA.ToAccountId()
	poolID := lpBtcEthID

	tests := []struct {
		name           string
		changes        xdr.LedgerEntryChanges
		expectedCount  int
		expectedOp     types.LiquidityPoolShareOp
		expectedShares int64
	}{
		{
			name: "pool share created",
			changes: xdr.LedgerEntryChanges{
				{Type: xdr.LedgerEntryChangeTypeLedgerEntryCreated, Created: poolShareEntry(testAccount, poolID, 5000)},
			},
			expectedCount:  1,
			expectedOp:     types.LiquidityPoolShareOpAdd,
			expectedShares: 5000,
		},
		{
			name: "pool share updated",
			changes: xdr.LedgerEntryChanges{
				{Type: xdr.LedgerEntryChangeTypeLedgerEntryState, State: poolShareEntry(testAccount, poolID, 5000)},
				{Type: xdr.LedgerEntryChangeTypeLedgerEntryUpdated, Updated: poolShareEntry(testAccount, poolID, 8000)},
			},
			expectedCount:  1,
			expectedOp:     types.LiquidityPoolShareOpUpdate,
			expectedShares: 8000,
		},
		{
			name: "pool share removed",
			changes: xdr.LedgerEntryChanges{
				{Type: xdr.LedgerEntryChangeTypeLedgerEntryState, State: poolShareEntry(testAccount, poolID, 5000)},
				{
					Type: xdr.LedgerEntryChangeTypeLedgerEntryRemoved,
					Removed: &xdr.LedgerKey{
						Type: xdr.LedgerEntryTypeTrustline,
						TrustLine: &xdr.LedgerKeyTrustLine{
							AccountId: testAccount,
							Asset:     xdr.TrustLineAsset{Type: xdr.AssetTypeAssetTypePoolShare, LiquidityPoolId: &poolID},
						},
					},
				},
			},
			expectedCount:  1,
			expectedOp:     types.LiquidityPoolShareOpRemove,
			expectedShares: 5000, // removal reports the pre-state shares
		},
		{
			name: "regular asset trustline skipped",
			changes: xdr.LedgerEntryChanges{
				{Type: xdr.LedgerEntryChangeTypeLedgerEntryCreated, Created: trustlineLedgerEntry(testAccount, usdcAsset, 5000000)},
			},
			expectedCount: 0,
		},
		{
			name: "non-trustline change skipped",
			changes: xdr.LedgerEntryChanges{
				{
					Type: xdr.LedgerEntryChangeTypeLedgerEntryCreated,
					Created: &xdr.LedgerEntry{Data: xdr.LedgerEntryData{
						Type:    xdr.LedgerEntryTypeAccount,
						Account: accountEntry(accountA, 10000000),
					}},
				},
			},
			expectedCount: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			op := xdr.Operation{
				SourceAccount: &accountA,
				Body:          xdr.OperationBody{Type: xdr.OperationTypeChangeTrust, ChangeTrustOp: &xdr.ChangeTrustOp{}},
			}
			tx := createTx(op, tc.changes, nil, false)
			wrapper := &TransactionOperationWrapper{
				Index:          0,
				Transaction:    tx,
				Operation:      op,
				LedgerSequence: 12345,
				Network:        networkPassphrase,
			}

			changes, err := processor.ProcessOperation(context.Background(), wrapper)
			require.NoError(t, err)
			require.Len(t, changes, tc.expectedCount)

			if tc.expectedCount > 0 {
				change := changes[0]
				assert.Equal(t, tc.expectedOp, change.Operation)
				assert.Equal(t, testAccount.Address(), change.AccountID)
				assert.Equal(t, PoolIDToString(poolID), change.PoolID)
				assert.Equal(t, tc.expectedShares, change.Shares)
				assert.Equal(t, uint32(12345), change.LedgerNumber)
				assert.NotZero(t, change.OperationID)
			}
		})
	}
}

func TestLiquidityPoolsProcessor_Name(t *testing.T) {
	assert.Equal(t, "liquidity_pools", NewLiquidityPoolsProcessor(nil).Name())
}

func TestLiquidityPoolsProcessor_ProcessOperation(t *testing.T) {
	processor := NewLiquidityPoolsProcessor(nil)
	poolID := lpBtcEthID

	tests := []struct {
		name             string
		changes          xdr.LedgerEntryChanges
		expectedCount    int
		expectedOp       types.LiquidityPoolOp
		expectedAssetA   string
		expectedReserveA int64
		expectedAssetB   string
		expectedReserveB int64
	}{
		{
			name: "pool created",
			changes: xdr.LedgerEntryChanges{
				generateLpEntryCreatedChange(lpLedgerEntry(poolID, btcAsset, ethAsset, 100, 200)),
			},
			expectedCount:    1,
			expectedOp:       types.LiquidityPoolOpAdd,
			expectedAssetA:   btcAsset.StringCanonical(),
			expectedReserveA: 100,
			expectedAssetB:   ethAsset.StringCanonical(),
			expectedReserveB: 200,
		},
		{
			name: "pool updated with native reserve",
			changes: xdr.LedgerEntryChanges{
				generateLpEntryChangeState(lpLedgerEntry(poolID, xlmAsset, ethAsset, 100, 200)),
				{Type: xdr.LedgerEntryChangeTypeLedgerEntryUpdated, Updated: ptrLpEntry(lpLedgerEntry(poolID, xlmAsset, ethAsset, 500, 900))},
			},
			expectedCount:    1,
			expectedOp:       types.LiquidityPoolOpUpdate,
			expectedAssetA:   "native",
			expectedReserveA: 500,
			expectedAssetB:   ethAsset.StringCanonical(),
			expectedReserveB: 900,
		},
		{
			name: "pool removed",
			changes: xdr.LedgerEntryChanges{
				generateLpEntryChangeState(lpLedgerEntry(poolID, btcAsset, ethAsset, 100, 200)),
				generateLpEntryRemovedChange(poolID),
			},
			expectedCount:    1,
			expectedOp:       types.LiquidityPoolOpRemove,
			expectedAssetA:   btcAsset.StringCanonical(),
			expectedReserveA: 100,
			expectedAssetB:   ethAsset.StringCanonical(),
			expectedReserveB: 200,
		},
		{
			name: "non-pool change skipped",
			changes: xdr.LedgerEntryChanges{
				{Type: xdr.LedgerEntryChangeTypeLedgerEntryCreated, Created: trustlineLedgerEntry(accountA.ToAccountId(), usdcAsset, 5000000)},
			},
			expectedCount: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			op := lpDepositOp(poolID, 100, 200, 1, 2, &accountA)
			tx := createTx(op, tc.changes, nil, false)
			wrapper := &TransactionOperationWrapper{
				Index:          0,
				Transaction:    tx,
				Operation:      op,
				LedgerSequence: 12345,
				Network:        networkPassphrase,
			}

			changes, err := processor.ProcessOperation(context.Background(), wrapper)
			require.NoError(t, err)
			require.Len(t, changes, tc.expectedCount)

			if tc.expectedCount > 0 {
				change := changes[0]
				assert.Equal(t, tc.expectedOp, change.Operation)
				assert.Equal(t, PoolIDToString(poolID), change.PoolID)
				assert.Equal(t, tc.expectedAssetA, change.AssetA)
				assert.Equal(t, tc.expectedReserveA, change.ReserveA)
				assert.Equal(t, tc.expectedAssetB, change.AssetB)
				assert.Equal(t, tc.expectedReserveB, change.ReserveB)
				assert.Equal(t, uint32(12345), change.LedgerNumber)
				assert.NotZero(t, change.OperationID)
			}
		})
	}
}

// ptrLpEntry returns a pointer to a liquidity pool ledger entry (for building Updated changes).
func ptrLpEntry(entry xdr.LedgerEntry) *xdr.LedgerEntry {
	return &entry
}
