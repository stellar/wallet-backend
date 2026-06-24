// Unit tests for AccountsProcessor.
// Tests account balance change extraction from ledger entries.
package processors

import (
	"context"
	"testing"

	"github.com/stellar/go-stellar-sdk/toid"
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
					// MinimumBalance = (2 + 0 + 0 - 0) * 5_000_000 + 200 = 10_000_200
					assert.Equal(t, int64(10000200), change.MinimumBalance)
				}
			}
		})
	}
}

func TestAccountsProcessor_ProcessTransactionFees(t *testing.T) {
	processor := NewAccountsProcessor(nil)
	testAccount := accountA.ToAccountId()

	// someTx is on ledger 12345. Fee debits sort below every operation in the ledger
	// (toid op 0); Soroban refunds sort above every operation (one below the next
	// ledger's floor).
	const ledgerSeq = uint32(12345)
	feeOpID := toid.New(int32(ledgerSeq), 0, 0).ToInt64()
	refundOpID := toid.New(int32(ledgerSeq)+1, 0, 0).ToInt64() - 1

	// accountBalanceChange builds a State→Updated pair for testAccount. Two separate
	// accountLedgerEntry calls (fresh entries) avoid aliasing the pre/post balance.
	accountBalanceChange := func(stateBalance, updatedBalance int64) xdr.LedgerEntryChanges {
		return xdr.LedgerEntryChanges{
			{Type: xdr.LedgerEntryChangeTypeLedgerEntryState, State: accountLedgerEntry(testAccount, stateBalance)},
			{Type: xdr.LedgerEntryChangeTypeLedgerEntryUpdated, Updated: accountLedgerEntry(testAccount, updatedBalance)},
		}
	}

	// expectedChange is the (OperationID, Balance) pair we expect for a returned change.
	type expectedChange struct {
		operationID int64
		balance     int64
	}

	tests := []struct {
		name       string
		feeChanges xdr.LedgerEntryChanges
		postApply  xdr.LedgerEntryChanges
		isFailed   bool
		expected   []expectedChange
	}{
		{
			// The #637 bug: an account whose balance moves only in the fee phase.
			name:       "fee debit captured",
			feeChanges: accountBalanceChange(100_000_000, 99_999_900),
			expected:   []expectedChange{{feeOpID, 99_999_900}},
		},
		{
			// Fees are charged even when the transaction fails.
			name:       "fee debit on failed tx still captured",
			feeChanges: accountBalanceChange(100_000_000, 99_999_900),
			isFailed:   true,
			expected:   []expectedChange{{feeOpID, 99_999_900}},
		},
		{
			name:      "soroban fee refund captured",
			postApply: accountBalanceChange(99_999_900, 99_999_950),
			expected:  []expectedChange{{refundOpID, 99_999_950}},
		},
		{
			// Fee then refund for the same account: both returned (refund outranks
			// fee during buffer dedup, but extraction yields both).
			name:       "fee debit and refund both captured",
			feeChanges: accountBalanceChange(100_000_000, 99_999_900),
			postApply:  accountBalanceChange(99_999_900, 99_999_950),
			expected:   []expectedChange{{feeOpID, 99_999_900}, {refundOpID, 99_999_950}},
		},
		{
			name: "signers-only fee change skipped",
			feeChanges: xdr.LedgerEntryChanges{
				{Type: xdr.LedgerEntryChangeTypeLedgerEntryState, State: accountLedgerEntrySignersOnly(testAccount, 10000000, 1)},
				{Type: xdr.LedgerEntryChangeTypeLedgerEntryUpdated, Updated: accountLedgerEntrySignersOnly(testAccount, 10000000, 2)},
			},
			expected: nil,
		},
		{
			name: "non-account fee change skipped",
			feeChanges: xdr.LedgerEntryChanges{
				{Type: xdr.LedgerEntryChangeTypeLedgerEntryCreated, Created: trustlineLedgerEntry(testAccount, usdcAsset, 5000000)},
			},
			expected: nil,
		},
		{
			name:     "no fee or refund changes",
			expected: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tx := createFeeAndRefundTx(tc.feeChanges, tc.postApply, tc.isFailed)

			changes, err := processor.ProcessTransactionFees(context.Background(), tx)
			require.NoError(t, err)
			require.Len(t, changes, len(tc.expected))

			for i, want := range tc.expected {
				got := changes[i]
				assert.Equal(t, testAccount.Address(), got.AccountID)
				assert.Equal(t, types.AccountOpUpdate, got.Operation)
				assert.Equal(t, ledgerSeq, got.LedgerNumber)
				assert.Equal(t, want.operationID, got.OperationID)
				assert.Equal(t, want.balance, got.Balance)
				// Reserve calc still runs on fee-phase entries: (2 + 0 + 0 - 0) * 5_000_000 + 200.
				assert.Equal(t, int64(10000200), got.MinimumBalance)
			}
		})
	}
}
