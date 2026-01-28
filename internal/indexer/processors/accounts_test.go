// Unit tests for AccountsProcessor.
// Tests account balance change extraction from ledger entries.
package processors

import (
	"context"
	"testing"

	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/indexer/types"
)

// accountLedgerEntry creates an account ledger entry for testing.
func accountLedgerEntry(accountID xdr.AccountId, balance int64) *xdr.LedgerEntry {
	return &xdr.LedgerEntry{
		LastModifiedLedgerSeq: 12345,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeAccount,
			Account: &xdr.AccountEntry{
				AccountId:  accountID,
				Balance:    xdr.Int64(balance),
				SeqNum:     xdr.SequenceNumber(12345),
				Thresholds: xdr.Thresholds{1, 0, 0, 0},
				Ext: xdr.AccountEntryExt{
					V: 1,
					V1: &xdr.AccountEntryExtensionV1{
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

// accountLedgerEntrySignersOnly creates an account entry with only signer changes.
func accountLedgerEntrySignersOnly(accountID xdr.AccountId, balance int64, numSigners int) *xdr.LedgerEntry {
	signers := make([]xdr.Signer, numSigners)
	for i := 0; i < numSigners; i++ {
		signers[i] = xdr.Signer{
			Key:    xdr.SignerKey{Type: xdr.SignerKeyTypeSignerKeyTypeEd25519},
			Weight: xdr.Uint32(1),
		}
	}
	return &xdr.LedgerEntry{
		LastModifiedLedgerSeq: 12345,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeAccount,
			Account: &xdr.AccountEntry{
				AccountId:  accountID,
				Balance:    xdr.Int64(balance),
				SeqNum:     xdr.SequenceNumber(12345),
				Thresholds: xdr.Thresholds{1, 0, 0, 0},
				Signers:    signers,
			},
		},
	}
}

func TestAccountsProcessor_Name(t *testing.T) {
	processor := NewAccountsProcessor(nil)
	assert.Equal(t, "accounts", processor.Name())
}

func TestAccountsProcessor_ProcessOperation(t *testing.T) {
	processor := NewAccountsProcessor(nil)
	testAccount := accountA.ToAccountId()

	tests := []struct {
		name          string
		changes       xdr.LedgerEntryChanges
		expectedCount int
		expectedOp    types.AccountOpType
		checkFields   bool
	}{
		{
			name: "account created",
			changes: xdr.LedgerEntryChanges{
				{
					Type:    xdr.LedgerEntryChangeTypeLedgerEntryCreated,
					Created: accountLedgerEntry(testAccount, 10000000),
				},
			},
			expectedCount: 1,
			expectedOp:    types.AccountOpCreate,
			checkFields:   true,
		},
		{
			name: "account updated",
			changes: xdr.LedgerEntryChanges{
				{
					Type:  xdr.LedgerEntryChangeTypeLedgerEntryState,
					State: accountLedgerEntry(testAccount, 5000000),
				},
				{
					Type:    xdr.LedgerEntryChangeTypeLedgerEntryUpdated,
					Updated: accountLedgerEntry(testAccount, 10000000),
				},
			},
			expectedCount: 1,
			expectedOp:    types.AccountOpUpdate,
			checkFields:   true,
		},
		{
			name: "account removed (merge)",
			changes: xdr.LedgerEntryChanges{
				{
					Type:  xdr.LedgerEntryChangeTypeLedgerEntryState,
					State: accountLedgerEntry(testAccount, 10000000),
				},
				{
					Type: xdr.LedgerEntryChangeTypeLedgerEntryRemoved,
					Removed: &xdr.LedgerKey{
						Type: xdr.LedgerEntryTypeAccount,
						Account: &xdr.LedgerKeyAccount{
							AccountId: testAccount,
						},
					},
				},
			},
			expectedCount: 1,
			expectedOp:    types.AccountOpRemove,
			checkFields:   true,
		},
		{
			name: "signers only change skipped",
			changes: xdr.LedgerEntryChanges{
				{
					Type:  xdr.LedgerEntryChangeTypeLedgerEntryState,
					State: accountLedgerEntrySignersOnly(testAccount, 10000000, 1),
				},
				{
					Type:    xdr.LedgerEntryChangeTypeLedgerEntryUpdated,
					Updated: accountLedgerEntrySignersOnly(testAccount, 10000000, 2),
				},
			},
			expectedCount: 0,
		},
		{
			name: "non-account change skipped",
			changes: xdr.LedgerEntryChanges{
				{
					Type:    xdr.LedgerEntryChangeTypeLedgerEntryCreated,
					Created: trustlineLedgerEntry(testAccount, usdcAsset, 5000000),
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
					Type:            xdr.OperationTypeCreateAccount,
					CreateAccountOp: &xdr.CreateAccountOp{},
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
				assert.Equal(t, testAccount.Address(), change.AccountID)
				assert.Equal(t, uint32(12345), change.LedgerNumber)
				assert.NotZero(t, change.OperationID)

				// Verify balance/liabilities for created/updated
				if tc.expectedOp != types.AccountOpRemove {
					assert.Equal(t, int64(10000000), change.Balance)
					assert.Equal(t, int64(100), change.BuyingLiabilities)
					assert.Equal(t, int64(200), change.SellingLiabilities)
				}
			}
		})
	}
}
