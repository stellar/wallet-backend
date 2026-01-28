// Unit tests for TrustlinesProcessor.
// Tests trustline change extraction from ledger entries.
package processors

import (
	"context"
	"testing"

	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/indexer/types"
)

// trustlineLedgerEntry creates a trustline ledger entry for testing.
func trustlineLedgerEntry(accountID xdr.AccountId, asset xdr.Asset, balance int64) *xdr.LedgerEntry {
	tlAsset := asset.ToTrustLineAsset()
	return &xdr.LedgerEntry{
		LastModifiedLedgerSeq: 12345,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeTrustline,
			TrustLine: &xdr.TrustLineEntry{
				AccountId: accountID,
				Asset:     tlAsset,
				Balance:   xdr.Int64(balance),
				Limit:     xdr.Int64(10000000),
				Flags:     xdr.Uint32(1),
				Ext: xdr.TrustLineEntryExt{
					V: 1,
					V1: &xdr.TrustLineEntryV1{
						Liabilities: xdr.Liabilities{
							Buying:  100,
							Selling: 200,
						},
					},
				},
			},
		},
	}
}

// poolShareLedgerEntry creates a liquidity pool share trustline entry for testing.
func poolShareLedgerEntry(accountID xdr.AccountId) *xdr.LedgerEntry {
	return &xdr.LedgerEntry{
		LastModifiedLedgerSeq: 12345,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeTrustline,
			TrustLine: &xdr.TrustLineEntry{
				AccountId: accountID,
				Asset: xdr.TrustLineAsset{
					Type:            xdr.AssetTypeAssetTypePoolShare,
					LiquidityPoolId: &lpBtcEthID,
				},
				Balance: 1000,
				Limit:   10000,
			},
		},
	}
}

func TestTrustlinesProcessor_Name(t *testing.T) {
	processor := NewTrustlinesProcessor(nil)

	assert.Equal(t, "trustlines", processor.Name())
}

func TestTrustlinesProcessor_ProcessOperation(t *testing.T) {
	processor := NewTrustlinesProcessor(nil)
	testAccount := accountA.ToAccountId()
	testAsset := usdcAsset

	tests := []struct {
		name          string
		changes       xdr.LedgerEntryChanges
		expectedCount int
		expectedOp    types.TrustlineOpType
		checkFields   bool
		expectedAsset string
		expectedAcct  string
	}{
		{
			name: "trustline created",
			changes: xdr.LedgerEntryChanges{
				{
					Type:    xdr.LedgerEntryChangeTypeLedgerEntryCreated,
					Created: trustlineLedgerEntry(testAccount, testAsset, 5000000),
				},
			},
			expectedCount: 1,
			expectedOp:    types.TrustlineOpAdd,
			checkFields:   true,
			expectedAsset: "USDC:" + usdcIssuer,
			expectedAcct:  testAccount.Address(),
		},
		{
			name: "trustline updated",
			changes: xdr.LedgerEntryChanges{
				{
					Type:  xdr.LedgerEntryChangeTypeLedgerEntryState,
					State: trustlineLedgerEntry(testAccount, testAsset, 1000000),
				},
				{
					Type:    xdr.LedgerEntryChangeTypeLedgerEntryUpdated,
					Updated: trustlineLedgerEntry(testAccount, testAsset, 5000000),
				},
			},
			expectedCount: 1,
			expectedOp:    types.TrustlineOpUpdate,
			checkFields:   true,
			expectedAsset: "USDC:" + usdcIssuer,
			expectedAcct:  testAccount.Address(),
		},
		{
			name: "trustline removed",
			changes: xdr.LedgerEntryChanges{
				{
					Type:  xdr.LedgerEntryChangeTypeLedgerEntryState,
					State: trustlineLedgerEntry(testAccount, testAsset, 0),
				},
				{
					Type: xdr.LedgerEntryChangeTypeLedgerEntryRemoved,
					Removed: &xdr.LedgerKey{
						Type: xdr.LedgerEntryTypeTrustline,
						TrustLine: &xdr.LedgerKeyTrustLine{
							AccountId: testAccount,
							Asset:     testAsset.ToTrustLineAsset(),
						},
					},
				},
			},
			expectedCount: 1,
			expectedOp:    types.TrustlineOpRemove,
			checkFields:   true,
			expectedAsset: "USDC:" + usdcIssuer,
			expectedAcct:  testAccount.Address(),
		},
		{
			name: "liquidity pool share skipped",
			changes: xdr.LedgerEntryChanges{
				{
					Type:    xdr.LedgerEntryChangeTypeLedgerEntryCreated,
					Created: poolShareLedgerEntry(testAccount),
				},
			},
			expectedCount: 0,
		},
		{
			name: "non-trustline change skipped",
			changes: xdr.LedgerEntryChanges{
				{
					Type: xdr.LedgerEntryChangeTypeLedgerEntryCreated,
					Created: &xdr.LedgerEntry{
						Data: xdr.LedgerEntryData{
							Type:    xdr.LedgerEntryTypeAccount,
							Account: accountEntry(accountA, 10000000),
						},
					},
				},
			},
			expectedCount: 0,
		},
		{
			name:          "empty changes",
			changes:       xdr.LedgerEntryChanges{},
			expectedCount: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			op := xdr.Operation{
				SourceAccount: &accountA,
				Body: xdr.OperationBody{
					Type:          xdr.OperationTypeChangeTrust,
					ChangeTrustOp: &xdr.ChangeTrustOp{},
				},
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

			if tc.expectedCount > 0 && tc.checkFields {
				change := changes[0]
				assert.Equal(t, tc.expectedOp, change.Operation)
				assert.Equal(t, tc.expectedAcct, change.AccountID)
				assert.Equal(t, tc.expectedAsset, change.Asset)
				assert.Equal(t, uint32(12345), change.LedgerNumber)
				assert.NotZero(t, change.OperationID)

				// Verify balance/limit for created/updated
				if tc.expectedOp != types.TrustlineOpRemove {
					assert.Equal(t, int64(5000000), change.Balance)
					assert.Equal(t, int64(10000000), change.Limit)
					assert.Equal(t, int64(100), change.BuyingLiabilities)
					assert.Equal(t, int64(200), change.SellingLiabilities)
					assert.Equal(t, uint32(1), change.Flags)
				}
			}
		})
	}
}
