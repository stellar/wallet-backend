package processors

import (
	"context"
	"encoding/hex"
	"testing"

	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/indexer/types"
)

func TestProtocolWasmProcessor_Name(t *testing.T) {
	processor := NewProtocolWasmProcessor(nil)
	assert.Equal(t, "protocol_wasms", processor.Name())
}

func TestProtocolWasmProcessor_ProcessOperation(t *testing.T) {
	processor := NewProtocolWasmProcessor(nil)

	testWasmHash := xdr.Hash{
		0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00, 0x11,
		0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99,
		0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00, 0x11,
		0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99,
	}
	testBytecode := []byte{0x00, 0x61, 0x73, 0x6d}

	tests := []struct {
		name             string
		changes          xdr.LedgerEntryChanges
		expectedCount    int
		expectedHash     types.HashBytea
		expectedBytecode []byte
	}{
		{
			name: "ContractCode created returns observation with bytecode",
			changes: xdr.LedgerEntryChanges{
				{
					Type: xdr.LedgerEntryChangeTypeLedgerEntryCreated,
					Created: &xdr.LedgerEntry{
						Data: xdr.LedgerEntryData{
							Type: xdr.LedgerEntryTypeContractCode,
							ContractCode: &xdr.ContractCodeEntry{
								Hash: testWasmHash,
								Code: testBytecode,
							},
						},
					},
				},
			},
			expectedCount:    1,
			expectedHash:     types.HashBytea(hex.EncodeToString(testWasmHash[:])),
			expectedBytecode: testBytecode,
		},
		{
			name: "non-ContractCode entry skipped",
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
			name: "removed entry (Post nil) skipped",
			changes: xdr.LedgerEntryChanges{
				{
					Type: xdr.LedgerEntryChangeTypeLedgerEntryState,
					State: &xdr.LedgerEntry{
						Data: xdr.LedgerEntryData{
							Type: xdr.LedgerEntryTypeContractCode,
							ContractCode: &xdr.ContractCodeEntry{
								Hash: testWasmHash,
								Code: testBytecode,
							},
						},
					},
				},
				{
					Type: xdr.LedgerEntryChangeTypeLedgerEntryRemoved,
					Removed: &xdr.LedgerKey{
						Type: xdr.LedgerEntryTypeContractCode,
						ContractCode: &xdr.LedgerKeyContractCode{
							Hash: testWasmHash,
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

			obs, err := processor.ProcessOperation(context.Background(), wrapper)
			require.NoError(t, err)
			require.Len(t, obs, tc.expectedCount)

			if tc.expectedCount > 0 {
				assert.Equal(t, tc.expectedHash, obs[0].Record.WasmHash)
				assert.Nil(t, obs[0].Record.ProtocolID)
				assert.Equal(t, tc.expectedBytecode, obs[0].Bytecode)
			}
		})
	}
}
