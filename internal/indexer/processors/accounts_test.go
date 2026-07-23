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
// The balance is fixed so a State→Updated pair built from it differs only in signers.
func accountLedgerEntrySignersOnly(accountID xdr.AccountId, numSigners int) *xdr.LedgerEntry {
	signers := make([]xdr.Signer, numSigners)
	for i := range signers {
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
				Balance:    xdr.Int64(10000000),
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
					State: accountLedgerEntrySignersOnly(testAccount, 1),
				},
				{
					Type:    xdr.LedgerEntryChangeTypeLedgerEntryUpdated,
					Updated: accountLedgerEntrySignersOnly(testAccount, 2),
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
				assert.NotZero(t, change.SortKey)

				// Verify balance/liabilities for created/updated
				if tc.expectedOp != types.AccountOpRemove {
					assert.Equal(t, int64(10000000), change.Balance)
					assert.Equal(t, int64(100), change.BuyingLiabilities)
					assert.Equal(t, int64(200), change.SellingLiabilities)
					// MinimumBalance is the pure base reserve, excluding the 200 selling liabilities: (2 + 0 + 0 - 0) * 5_000_000.
					assert.Equal(t, int64(10000000), change.MinimumBalance)
				}
			}
		})
	}
}

func TestAccountsProcessor_ProcessOperation_PersistsNumSubEntries(t *testing.T) {
	processor := NewAccountsProcessor(nil)
	testAccount := accountA.ToAccountId()

	// accountLedgerEntry defaults NumSubEntries to 0; set 3 to prove the count is carried through.
	entry := accountLedgerEntry(testAccount, 10000000)
	entry.Data.Account.NumSubEntries = 3

	op := xdr.Operation{
		SourceAccount: &accountA,
		Body: xdr.OperationBody{
			Type:            xdr.OperationTypeCreateAccount,
			CreateAccountOp: &xdr.CreateAccountOp{},
		},
	}
	changes := xdr.LedgerEntryChanges{
		{Type: xdr.LedgerEntryChangeTypeLedgerEntryCreated, Created: entry},
	}
	tx := createTx(op, changes, nil, false)
	wrapper := &TransactionOperationWrapper{
		Index:          0,
		Transaction:    tx,
		Operation:      op,
		LedgerSequence: 12345,
		Network:        networkPassphrase,
	}

	got, err := processor.ProcessOperation(context.Background(), wrapper)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, uint32(3), got[0].NumSubEntries)
	// MinimumBalance folds the 3 subentries into the base reserve (excludes liabilities): (2 + 3) * 5_000_000.
	assert.Equal(t, int64(25000000), got[0].MinimumBalance)
}

func TestAccountsProcessor_ProcessTransactionFees(t *testing.T) {
	processor := NewAccountsProcessor(nil)
	testAccount := accountA.ToAccountId()

	// someTx is on ledger 12345 with Index 1. Fee debits get phaseFee (sorts below every
	// operation in the ledger); Soroban refunds get phaseRefund (sorts above every operation).
	const ledgerSeq = uint32(12345)
	feeOpID := accountSortKey(phaseFee, someTx.Index, 0)
	refundOpID := accountSortKey(phaseRefund, someTx.Index, 0)

	// accountBalanceChange builds a State→Updated pair for testAccount. Two separate
	// accountLedgerEntry calls (fresh entries) avoid aliasing the pre/post balance.
	accountBalanceChange := func(stateBalance, updatedBalance int64) xdr.LedgerEntryChanges {
		return xdr.LedgerEntryChanges{
			{Type: xdr.LedgerEntryChangeTypeLedgerEntryState, State: accountLedgerEntry(testAccount, stateBalance)},
			{Type: xdr.LedgerEntryChangeTypeLedgerEntryUpdated, Updated: accountLedgerEntry(testAccount, updatedBalance)},
		}
	}

	// expectedChange is the (SortKey, Balance) pair we expect for a returned change.
	type expectedChange struct {
		sortKey int64
		balance int64
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
				{Type: xdr.LedgerEntryChangeTypeLedgerEntryState, State: accountLedgerEntrySignersOnly(testAccount, 1)},
				{Type: xdr.LedgerEntryChangeTypeLedgerEntryUpdated, Updated: accountLedgerEntrySignersOnly(testAccount, 2)},
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
				assert.Equal(t, want.sortKey, got.SortKey)
				assert.Equal(t, want.balance, got.Balance)
				// Reserve calc still runs on fee-phase entries, excluding liabilities: (2 + 0 + 0 - 0) * 5_000_000.
				assert.Equal(t, int64(10000000), got.MinimumBalance)
			}
		})
	}
}

func TestAccountSortKey(t *testing.T) {
	// Phase dominates tx/op: a fee in a late tx still sorts below an operation in an
	// early tx, which sorts below a refund in an early tx — the canonical fee < op < refund.
	assert.Less(t, accountSortKey(phaseFee, 9, 0), accountSortKey(phaseOperation, 1, 1))
	assert.Less(t, accountSortKey(phaseOperation, 9, 99), accountSortKey(phaseRefund, 1, 0))

	// Within a phase: ordered by tx, then op.
	assert.Less(t, accountSortKey(phaseFee, 3, 0), accountSortKey(phaseFee, 7, 0))
	assert.Less(t, accountSortKey(phaseRefund, 3, 0), accountSortKey(phaseRefund, 7, 0))
	assert.Less(t, accountSortKey(phaseOperation, 5, 1), accountSortKey(phaseOperation, 5, 2))
	assert.Less(t, accountSortKey(phaseOperation, 5, 2), accountSortKey(phaseOperation, 6, 1))

	// Stays positive at the SDK field maxima (tx 2^20-1, op 4095) and below the next phase.
	maxOp := accountSortKey(phaseOperation, (1<<20)-1, 4095)
	assert.Positive(t, maxOp)
	assert.Less(t, maxOp, accountSortKey(phaseRefund, 0, 0))
}
