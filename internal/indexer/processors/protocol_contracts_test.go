package processors

import (
	"context"
	"encoding/hex"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
)

func TestProtocolContractsProcessor_Name(t *testing.T) {
	processor := NewProtocolContractsProcessor(nil)
	assert.Equal(t, "protocol_contracts", processor.Name())
}

func TestProtocolContractsProcessor_ProcessOperation(t *testing.T) {
	processor := NewProtocolContractsProcessor(nil)

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
	expectedContractID := types.HashBytea(hex.EncodeToString(testContractIDBytes[:]))

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

	// Helper for a malformed entry: Key claims ContractInstance, but Val is a
	// different ScVal arm (e.g. a scalar). The XDR unions are independent, so
	// this is reachable from ledger data — the processor must not panic.
	malformedInstanceKeyEntry := func(contractID [32]byte) *xdr.LedgerEntry {
		contractIDVal := xdr.ContractId(contractID)
		u32 := xdr.Uint32(0)
		return &xdr.LedgerEntry{
			Data: xdr.LedgerEntryData{
				Type: xdr.LedgerEntryTypeContractData,
				ContractData: &xdr.ContractDataEntry{
					Contract: xdr.ScAddress{
						Type:       xdr.ScAddressTypeScAddressTypeContract,
						ContractId: &contractIDVal,
					},
					Key: xdr.ScVal{Type: xdr.ScValTypeScvLedgerKeyContractInstance},
					Val: xdr.ScVal{Type: xdr.ScValTypeScvU32, U32: &u32},
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

	// Helper for a CAP-0085 external-ref instance: the executable names an
	// (owner, tag) entry in another contract's storage, so there is no WASM
	// hash to record.
	externalRefInstanceEntry := func(contractID [32]byte) *xdr.LedgerEntry {
		contractIDVal := xdr.ContractId(contractID)
		ownerVal := xdr.ContractId(testHolderContractBytes)
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
								Type: xdr.ContractExecutableTypeContractExecutableExternalRef,
								ExternalRef: &xdr.ContractExecutableExternalRef{
									ExecutableOwner: xdr.ScAddress{
										Type:       xdr.ScAddressTypeScAddressTypeContract,
										ContractId: &ownerVal,
									},
									Tag: xdr.ScString("fleet-v1"),
								},
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
		expectedContractID types.HashBytea
		expectedWasmHash   types.HashBytea
	}{
		{
			name: "WASM instance created returns ProtocolContracts",
			changes: xdr.LedgerEntryChanges{
				{
					Type:    xdr.LedgerEntryChangeTypeLedgerEntryCreated,
					Created: wasmInstanceEntry(testContractIDBytes, &testWasmHash),
				},
			},
			expectedCount:      1,
			expectedContractID: expectedContractID,
			expectedWasmHash:   types.HashBytea(hex.EncodeToString(testWasmHash[:])),
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
			name: "external-ref instance (CAP-0085) skipped without panicking",
			changes: xdr.LedgerEntryChanges{
				{
					Type:    xdr.LedgerEntryChangeTypeLedgerEntryCreated,
					Created: externalRefInstanceEntry(testContractIDBytes),
				},
			},
			expectedCount: 0,
		},
		{
			name: "external-ref instance with nil ExternalRef pointer skipped without panicking",
			changes: xdr.LedgerEntryChanges{
				{
					Type: xdr.LedgerEntryChangeTypeLedgerEntryCreated,
					Created: func() *xdr.LedgerEntry {
						e := externalRefInstanceEntry(testContractIDBytes)
						e.Data.ContractData.Val.Instance.Executable.ExternalRef = nil
						return e
					}(),
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
			name: "malformed entry: instance Key with non-instance Val does not panic",
			changes: xdr.LedgerEntryChanges{
				{
					Type:    xdr.LedgerEntryChangeTypeLedgerEntryCreated,
					Created: malformedInstanceKeyEntry(testContractIDBytes),
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
			}
		})
	}
}

// A CAP-0085 external-ref contract records nothing, so the counter is the only
// signal that one appeared. Assert it moves, otherwise the gap is invisible.
func TestProtocolContractsProcessor_ExternalRefIncrementsMetric(t *testing.T) {
	ingestionMetrics := metrics.NewMetrics(prometheus.NewRegistry()).Ingestion
	processor := NewProtocolContractsProcessor(ingestionMetrics)

	contractIDBytes := [32]byte{0x01, 0x02, 0x03}
	contractIDVal := xdr.ContractId(contractIDBytes)
	ownerVal := xdr.ContractId([32]byte{0x09, 0x08, 0x07})

	entry := &xdr.LedgerEntry{
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
							Type: xdr.ContractExecutableTypeContractExecutableExternalRef,
							ExternalRef: &xdr.ContractExecutableExternalRef{
								ExecutableOwner: xdr.ScAddress{
									Type:       xdr.ScAddressTypeScAddressTypeContract,
									ContractId: &ownerVal,
								},
								Tag: xdr.ScString("fleet-v1"),
							},
						},
					},
				},
			},
		},
	}

	op := xdr.Operation{
		SourceAccount: &accountA,
		Body: xdr.OperationBody{
			Type:                 xdr.OperationTypeInvokeHostFunction,
			InvokeHostFunctionOp: &xdr.InvokeHostFunctionOp{},
		},
	}
	changes := xdr.LedgerEntryChanges{
		{Type: xdr.LedgerEntryChangeTypeLedgerEntryCreated, Created: entry},
	}
	wrapper := &TransactionOperationWrapper{
		Index:          0,
		Transaction:    createTx(op, changes, nil, false),
		Operation:      op,
		LedgerSequence: 12345,
		Network:        networkPassphrase,
	}

	require.Zero(t, testutil.ToFloat64(ingestionMetrics.ExternalRefContractsTotal))

	contracts, err := processor.ProcessOperation(context.Background(), wrapper)
	require.NoError(t, err)
	assert.Empty(t, contracts, "an external-ref contract has no WASM hash to record")
	assert.Equal(t, float64(1), testutil.ToFloat64(ingestionMetrics.ExternalRefContractsTotal))
}

func TestDescribeExternalRef(t *testing.T) {
	owner := xdr.ContractId([32]byte{0xab, 0xcd})
	tests := []struct {
		name       string
		executable xdr.ContractExecutable
		contains   string
	}{
		{
			name: "renders owner and tag",
			executable: xdr.ContractExecutable{
				Type: xdr.ContractExecutableTypeContractExecutableExternalRef,
				ExternalRef: &xdr.ContractExecutableExternalRef{
					ExecutableOwner: xdr.ScAddress{
						Type:       xdr.ScAddressTypeScAddressTypeContract,
						ContractId: &owner,
					},
					Tag: xdr.ScString("fleet-v1"),
				},
			},
			contains: `tag="fleet-v1"`,
		},
		{
			// GetExternalRef dereferences the arm pointer once the discriminant
			// matches, so a nil pointer here must not reach it.
			name: "nil ExternalRef pointer does not panic",
			executable: xdr.ContractExecutable{
				Type: xdr.ContractExecutableTypeContractExecutableExternalRef,
			},
			contains: "external ref missing",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Contains(t, DescribeExternalRef(tc.executable), tc.contains)
		})
	}
}
