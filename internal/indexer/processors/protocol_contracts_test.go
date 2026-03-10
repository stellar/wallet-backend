package processors

import (
	"context"
	"testing"

	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProtocolContractProcessor_Name(t *testing.T) {
	processor := NewProtocolContractProcessor()
	assert.Equal(t, "protocol_contracts", processor.Name())
}

func TestProtocolContractProcessor_ProcessOperation(t *testing.T) {
	processor := NewProtocolContractProcessor()

	testWasmHash := xdr.Hash{
		0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00, 0x11,
		0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99,
		0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00, 0x11,
		0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99,
	}

	testContractIDBytes := [32]byte{
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
		0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
		0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20,
	}
	expectedContractIDBytes := testContractIDBytes[:]

	// Helper to create a WASM instance ledger entry
	wasmInstanceEntry := func(contractID [32]byte, wasmHash *xdr.Hash) *xdr.LedgerEntry {
		contractIDVal := xdr.ContractId(contractID)
		return &xdr.LedgerEntry{
			Data: xdr.LedgerEntryData{
				Type: xdr.LedgerEntryTypeContractData,
				ContractData: &xdr.ContractDataEntry{
					Contract: xdr.ScAddress{
						Type:       xdr.ScAddressTypeScAddressTypeContract,
						ContractId: &contractIDVal,
					},
					Key: xdr.ScVal{Type: xdr.ScValTypeScvLedgerKeyContractInstance},
					Val: xdr.ScVal{
						Type: xdr.ScValTypeScvContractInstance,
						Instance: &xdr.ScContractInstance{
							Executable: xdr.ContractExecutable{
								Type:     xdr.ContractExecutableTypeContractExecutableWasm,
								WasmHash: wasmHash,
							},
						},
					},
				},
			},
		}
	}

	// Helper for SAC instance (non-WASM executable)
	sacInstanceEntry := func(contractID [32]byte) *xdr.LedgerEntry {
		contractIDVal := xdr.ContractId(contractID)
		return &xdr.LedgerEntry{
			Data: xdr.LedgerEntryData{
				Type: xdr.LedgerEntryTypeContractData,
				ContractData: &xdr.ContractDataEntry{
					Contract: xdr.ScAddress{
						Type:       xdr.ScAddressTypeScAddressTypeContract,
						ContractId: &contractIDVal,
					},
					Key: xdr.ScVal{Type: xdr.ScValTypeScvLedgerKeyContractInstance},
					Val: xdr.ScVal{
						Type: xdr.ScValTypeScvContractInstance,
						Instance: &xdr.ScContractInstance{
							Executable: xdr.ContractExecutable{
								Type: xdr.ContractExecutableTypeContractExecutableStellarAsset,
							},
						},
					},
				},
			},
		}
	}

	tests := []struct {
		name               string
		changes            xdr.LedgerEntryChanges
		expectedCount      int
		expectedContractID []byte
		expectedWasmHash   []byte
	}{
		{
			name: "WASM instance created returns ProtocolContract",
			changes: xdr.LedgerEntryChanges{
				{
					Type:    xdr.LedgerEntryChangeTypeLedgerEntryCreated,
					Created: wasmInstanceEntry(testContractIDBytes, &testWasmHash),
				},
			},
			expectedCount:      1,
			expectedContractID: expectedContractIDBytes,
			expectedWasmHash:   testWasmHash[:],
		},
		{
			name: "SAC instance (non-WASM executable) skipped",
			changes: xdr.LedgerEntryChanges{
				{
					Type:    xdr.LedgerEntryChangeTypeLedgerEntryCreated,
					Created: sacInstanceEntry(testContractIDBytes),
				},
			},
			expectedCount: 0,
		},
		{
			name: "non-instance ContractData skipped",
			changes: xdr.LedgerEntryChanges{
				{
					Type:    xdr.LedgerEntryChangeTypeLedgerEntryCreated,
					Created: sacBalanceLedgerEntry(testSACContractBytes, testHolderContractBytes, 5000000),
				},
			},
			expectedCount: 0,
		},
		{
			name: "non-ContractData entry skipped",
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
			name: "instance with nil WasmHash skipped",
			changes: xdr.LedgerEntryChanges{
				{
					Type:    xdr.LedgerEntryChangeTypeLedgerEntryCreated,
					Created: wasmInstanceEntry(testContractIDBytes, nil),
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

			contracts, err := processor.ProcessOperation(context.Background(), wrapper)
			require.NoError(t, err)
			require.Len(t, contracts, tc.expectedCount)

			if tc.expectedCount > 0 {
				assert.Equal(t, tc.expectedContractID, contracts[0].ContractID)
				assert.Equal(t, tc.expectedWasmHash, contracts[0].WasmHash)
				assert.Nil(t, contracts[0].ProtocolID)
			}
		})
	}
}
