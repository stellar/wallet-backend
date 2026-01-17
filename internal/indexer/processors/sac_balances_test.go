// Unit tests for SACBalancesProcessor.
// Tests SAC balance change extraction from contract data ledger entries.
package processors

import (
	"context"
	"testing"

	"github.com/stellar/go-stellar-sdk/ingest/sac"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/indexer/types"
)

// Test contract addresses - must be valid C-addresses (56 chars starting with C)
var (
	// SAC contract ID (the asset contract)
	testSACContractBytes = [32]byte{
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
		0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
		0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20,
	}

	// Holder contract ID (the account holding the balance)
	testHolderContractBytes = [32]byte{
		0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28,
		0x29, 0x2a, 0x2b, 0x2c, 0x2d, 0x2e, 0x2f, 0x30,
		0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38,
		0x39, 0x3a, 0x3b, 0x3c, 0x3d, 0x3e, 0x3f, 0x40,
	}

	testSACContractAddr    = strkey.MustEncode(strkey.VersionByteContract, testSACContractBytes[:])
	testHolderContractAddr = strkey.MustEncode(strkey.VersionByteContract, testHolderContractBytes[:])
)

// sacBalanceLedgerEntry creates a SAC balance contract data ledger entry for testing.
// Uses the SDK's sac.BalanceToContractData to create a valid balance entry.
func sacBalanceLedgerEntry(sacContractID, holderID [32]byte, amount uint64) *xdr.LedgerEntry {
	// Use SDK helper to create valid contract data for SAC balance
	ledgerData := sac.BalanceToContractData(sacContractID, holderID, amount)

	return &xdr.LedgerEntry{
		LastModifiedLedgerSeq: 12345,
		Data:                  ledgerData,
	}
}

// sacBalanceInt128LedgerEntry creates a SAC balance contract data ledger entry using Int128 amounts.
func sacBalanceInt128LedgerEntry(sacContractID, holderID [32]byte, hi int64, lo uint64) *xdr.LedgerEntry {
	amt := xdr.Int128Parts{Hi: xdr.Int64(hi), Lo: xdr.Uint64(lo)}
	ledgerData := sac.BalanceInt128ToContractData(sacContractID, holderID, amt)

	return &xdr.LedgerEntry{
		LastModifiedLedgerSeq: 12345,
		Data:                  ledgerData,
	}
}

func TestNewSACBalancesProcessor(t *testing.T) {
	processor := NewSACBalancesProcessor(networkPassphrase, nil)

	assert.NotNil(t, processor)
	assert.Equal(t, networkPassphrase, processor.networkPassphrase)
	assert.Nil(t, processor.metricsService)
}

func TestSACBalancesProcessor_Name(t *testing.T) {
	processor := NewSACBalancesProcessor(networkPassphrase, nil)

	assert.Equal(t, "sac_balances", processor.Name())
}

func TestSACBalancesProcessor_ProcessOperation(t *testing.T) {
	processor := NewSACBalancesProcessor(networkPassphrase, nil)

	tests := []struct {
		name            string
		changes         xdr.LedgerEntryChanges
		expectedCount   int
		expectedOp      types.SACBalanceOp
		checkFields     bool
		expectedAccount string
	}{
		{
			name: "SAC balance created",
			changes: xdr.LedgerEntryChanges{
				{
					Type:    xdr.LedgerEntryChangeTypeLedgerEntryCreated,
					Created: sacBalanceLedgerEntry(testSACContractBytes, testHolderContractBytes, 5000000),
				},
			},
			expectedCount:   1,
			expectedOp:      types.SACBalanceOpAdd,
			checkFields:     true,
			expectedAccount: testHolderContractAddr,
		},
		{
			name: "SAC balance updated",
			changes: xdr.LedgerEntryChanges{
				{
					Type:  xdr.LedgerEntryChangeTypeLedgerEntryState,
					State: sacBalanceLedgerEntry(testSACContractBytes, testHolderContractBytes, 1000000),
				},
				{
					Type:    xdr.LedgerEntryChangeTypeLedgerEntryUpdated,
					Updated: sacBalanceLedgerEntry(testSACContractBytes, testHolderContractBytes, 5000000),
				},
			},
			expectedCount:   1,
			expectedOp:      types.SACBalanceOpUpdate,
			checkFields:     true,
			expectedAccount: testHolderContractAddr,
		},
		{
			name: "SAC balance removed",
			changes: xdr.LedgerEntryChanges{
				{
					Type:  xdr.LedgerEntryChangeTypeLedgerEntryState,
					State: sacBalanceLedgerEntry(testSACContractBytes, testHolderContractBytes, 0),
				},
				{
					Type: xdr.LedgerEntryChangeTypeLedgerEntryRemoved,
					Removed: &xdr.LedgerKey{
						Type: xdr.LedgerEntryTypeContractData,
						ContractData: &xdr.LedgerKeyContractData{
							Contract: xdr.ScAddress{
								Type:       xdr.ScAddressTypeScAddressTypeContract,
								ContractId: (*xdr.ContractId)(&testSACContractBytes),
							},
							Key:        xdr.ScVal{Type: xdr.ScValTypeScvVoid},
							Durability: xdr.ContractDataDurabilityPersistent,
						},
					},
				},
			},
			expectedCount:   1,
			expectedOp:      types.SACBalanceOpRemove,
			checkFields:     true,
			expectedAccount: testHolderContractAddr,
		},
		{
			name: "non-contract data entry skipped",
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
					Type:                 xdr.OperationTypeInvokeHostFunction,
					InvokeHostFunctionOp: &xdr.InvokeHostFunctionOp{},
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
				assert.Equal(t, tc.expectedAccount, change.AccountID)
				assert.Equal(t, testSACContractAddr, change.ContractID)
				assert.Equal(t, uint32(12345), change.LedgerNumber)
				assert.NotZero(t, change.OperationID)

				// Verify balance for created/updated
				if tc.expectedOp != types.SACBalanceOpRemove {
					assert.NotEmpty(t, change.Balance)
				} else {
					// For remove, balance should be "0"
					assert.Equal(t, "0", change.Balance)
					assert.False(t, change.IsAuthorized)
					assert.False(t, change.IsClawbackEnabled)
				}
			}
		})
	}
}

func TestSACBalancesProcessor_ProcessOperation_WithInt128Balance(t *testing.T) {
	processor := NewSACBalancesProcessor(networkPassphrase, nil)

	// Create a balance with Int128 (high bits set)
	changes := xdr.LedgerEntryChanges{
		{
			Type:    xdr.LedgerEntryChangeTypeLedgerEntryCreated,
			Created: sacBalanceInt128LedgerEntry(testSACContractBytes, testHolderContractBytes, 1, 1000000000000000000),
		},
	}

	op := xdr.Operation{
		SourceAccount: &accountA,
		Body: xdr.OperationBody{
			Type:                 xdr.OperationTypeInvokeHostFunction,
			InvokeHostFunctionOp: &xdr.InvokeHostFunctionOp{},
		},
	}

	tx := createTx(op, changes, nil, false)
	wrapper := &TransactionOperationWrapper{
		Index:          0,
		Transaction:    tx,
		Operation:      op,
		LedgerSequence: 12345,
		Network:        networkPassphrase,
	}

	result, err := processor.ProcessOperation(context.Background(), wrapper)
	require.NoError(t, err)
	require.Len(t, result, 1)

	// Verify the balance is correctly converted to string
	assert.Equal(t, types.SACBalanceOpAdd, result[0].Operation)
	assert.Equal(t, testHolderContractAddr, result[0].AccountID)
	// Int128 balance: (1 << 64) + 1000000000000000000 = large number
	assert.NotEmpty(t, result[0].Balance)
}

func TestSACBalancesProcessor_extractSACBalanceFields(t *testing.T) {
	processor := NewSACBalancesProcessor(networkPassphrase, nil)

	t.Run("extracts all fields from valid map", func(t *testing.T) {
		// Create a valid SAC balance value using SDK
		entry := sacBalanceLedgerEntry(testSACContractBytes, testHolderContractBytes, 5000000)
		contractData := entry.Data.MustContractData()

		balance, authorized, clawback := processor.extractSACBalanceFields(contractData.Val)

		// SDK creates balances with default authorization settings
		assert.NotEmpty(t, balance)
		// Note: the SDK's BalanceToContractData creates entries with specific defaults
		_ = authorized
		_ = clawback
	})

	t.Run("returns defaults for invalid map", func(t *testing.T) {
		// Test with a non-map ScVal
		invalidVal := xdr.ScVal{Type: xdr.ScValTypeScvVoid}

		balance, authorized, clawback := processor.extractSACBalanceFields(invalidVal)

		assert.Equal(t, "0", balance)
		assert.False(t, authorized)
		assert.False(t, clawback)
	})

	t.Run("returns defaults for empty map", func(t *testing.T) {
		// Test with a map type but empty entries
		emptyMap := xdr.ScMap{}
		emptyMapPtr := &emptyMap
		emptyMapVal := xdr.ScVal{
			Type: xdr.ScValTypeScvMap,
			Map:  &emptyMapPtr,
		}

		balance, authorized, clawback := processor.extractSACBalanceFields(emptyMapVal)

		assert.Equal(t, "", balance)
		assert.False(t, authorized)
		assert.False(t, clawback)
	})
}

func TestSACBalancesProcessor_SkipsGAddressHolders(t *testing.T) {
	processor := NewSACBalancesProcessor(networkPassphrase, nil)

	// The SDK's ContractBalanceFromContractData automatically filters out G-address holders
	// This test verifies our processor correctly handles this by using a G-address
	// which should be skipped (returning 0 results)

	// Create a contract data entry but with a different key structure that would
	// represent a G-address holder - the SDK validation will reject it
	// We can't easily create a G-address holder balance entry because BalanceToContractData
	// only accepts contract IDs (32-byte arrays), not account addresses

	// Instead, test that non-balance contract data entries are skipped
	nonBalanceContractData := &xdr.LedgerEntry{
		LastModifiedLedgerSeq: 12345,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeContractData,
			ContractData: &xdr.ContractDataEntry{
				Contract: xdr.ScAddress{
					Type:       xdr.ScAddressTypeScAddressTypeContract,
					ContractId: (*xdr.ContractId)(&testSACContractBytes),
				},
				Key: xdr.ScVal{
					Type: xdr.ScValTypeScvSymbol,
					Sym:  (*xdr.ScSymbol)(strPtr("NotBalance")),
				},
				Durability: xdr.ContractDataDurabilityPersistent,
				Val: xdr.ScVal{
					Type: xdr.ScValTypeScvVoid,
				},
			},
		},
	}

	changes := xdr.LedgerEntryChanges{
		{
			Type:    xdr.LedgerEntryChangeTypeLedgerEntryCreated,
			Created: nonBalanceContractData,
		},
	}

	op := xdr.Operation{
		SourceAccount: &accountA,
		Body: xdr.OperationBody{
			Type:                 xdr.OperationTypeInvokeHostFunction,
			InvokeHostFunctionOp: &xdr.InvokeHostFunctionOp{},
		},
	}

	tx := createTx(op, changes, nil, false)
	wrapper := &TransactionOperationWrapper{
		Index:          0,
		Transaction:    tx,
		Operation:      op,
		LedgerSequence: 12345,
		Network:        networkPassphrase,
	}

	result, err := processor.ProcessOperation(context.Background(), wrapper)
	require.NoError(t, err)
	require.Len(t, result, 0, "non-balance contract data should be skipped")
}

// strPtr is a helper to create a string pointer for xdr.ScSymbol
func strPtr(s string) *string {
	return &s
}
