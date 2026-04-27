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

func TestProtocolWasmProcessor_Name(t *testing.T) {
	processor := NewProtocolWasmProcessor(nil, nil)
	assert.Equal(t, "protocol_wasms", processor.Name())
}

func TestProtocolWasmProcessor_ProcessOperation(t *testing.T) {
	processor := NewProtocolWasmProcessor(nil, nil)

	testWasmHash := xdr.Hash{
		0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00, 0x11,
		0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99,
		0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00, 0x11,
		0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99,
	}

	tests := []struct {
		name          string
		changes       xdr.LedgerEntryChanges
		expectedCount int
		expectedHash  types.HashBytea
	}{
		{
			name: "ContractCode created returns ProtocolWasm",
			changes: xdr.LedgerEntryChanges{
				{
					Type: xdr.LedgerEntryChangeTypeLedgerEntryCreated,
					Created: &xdr.LedgerEntry{
						Data: xdr.LedgerEntryData{
							Type: xdr.LedgerEntryTypeContractCode,
							ContractCode: &xdr.ContractCodeEntry{
								Hash: testWasmHash,
								Code: []byte{0x00, 0x61, 0x73, 0x6d},
							},
						},
					},
				},
			},
			expectedCount: 1,
			expectedHash:  types.HashBytea(hex.EncodeToString(testWasmHash[:])),
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
								Code: []byte{0x00, 0x61, 0x73, 0x6d},
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

			wasms, err := processor.ProcessOperation(context.Background(), wrapper)
			require.NoError(t, err)
			require.Len(t, wasms, tc.expectedCount)

			if tc.expectedCount > 0 {
				assert.Equal(t, tc.expectedHash, wasms[0].WasmHash)
				assert.Nil(t, wasms[0].ProtocolID)
			}
		})
	}
}

// stubClassifier is a tiny test double for WasmClassifier that returns a
// pre-baked protocol id (or error) regardless of the bytecode.
type stubClassifier struct {
	protocolID string
	err        error
	calls      int
}

func (s *stubClassifier) Classify(ctx context.Context, wasmCode []byte) (string, error) {
	s.calls++
	return s.protocolID, s.err
}

func TestProtocolWasmProcessor_ClassifierMatch(t *testing.T) {
	classifier := &stubClassifier{protocolID: "SEP41"}
	processor := NewProtocolWasmProcessor(nil, classifier)

	hash := xdr.Hash{0x01, 0x02, 0x03}
	op := xdr.Operation{
		Body: xdr.OperationBody{
			Type:                 xdr.OperationTypeInvokeHostFunction,
			InvokeHostFunctionOp: &xdr.InvokeHostFunctionOp{},
		},
	}
	contractCodeChange := []xdr.LedgerEntryChange{
		{
			Type: xdr.LedgerEntryChangeTypeLedgerEntryCreated,
			Created: &xdr.LedgerEntry{
				Data: xdr.LedgerEntryData{
					Type: xdr.LedgerEntryTypeContractCode,
					ContractCode: &xdr.ContractCodeEntry{
						Hash: hash,
						Code: []byte{0x00, 0x61, 0x73, 0x6d}, // arbitrary 4 bytes; classifier is stubbed
					},
				},
			},
		},
	}
	tx := createTx(op, contractCodeChange, nil, false)
	wasms, err := processor.ProcessOperation(context.Background(), &TransactionOperationWrapper{
		Index: 0, Transaction: tx, Operation: op, LedgerSequence: 1, Network: networkPassphrase,
	})
	require.NoError(t, err)
	require.Len(t, wasms, 1)
	assert.Equal(t, types.HashBytea(hex.EncodeToString(hash[:])), wasms[0].WasmHash)
	require.NotNil(t, wasms[0].ProtocolID)
	assert.Equal(t, "SEP41", *wasms[0].ProtocolID)
	assert.Equal(t, 1, classifier.calls)
}

func TestProtocolWasmProcessor_ClassifierError_RecordsRaw(t *testing.T) {
	classifier := &stubClassifier{err: assert.AnError}
	m := metrics.NewMetrics(prometheus.NewRegistry())
	processor := NewProtocolWasmProcessor(m.Ingestion, classifier)

	hash := xdr.Hash{0xff}
	op := xdr.Operation{
		Body: xdr.OperationBody{
			Type:                 xdr.OperationTypeInvokeHostFunction,
			InvokeHostFunctionOp: &xdr.InvokeHostFunctionOp{},
		},
	}
	contractCodeChange := []xdr.LedgerEntryChange{
		{
			Type: xdr.LedgerEntryChangeTypeLedgerEntryCreated,
			Created: &xdr.LedgerEntry{
				Data: xdr.LedgerEntryData{
					Type: xdr.LedgerEntryTypeContractCode,
					ContractCode: &xdr.ContractCodeEntry{
						Hash: hash,
						Code: []byte{0x00},
					},
				},
			},
		},
	}
	tx := createTx(op, contractCodeChange, nil, false)
	wasms, err := processor.ProcessOperation(context.Background(), &TransactionOperationWrapper{
		Index: 0, Transaction: tx, Operation: op, LedgerSequence: 1, Network: networkPassphrase,
	})
	require.NoError(t, err)
	require.Len(t, wasms, 1)
	assert.Nil(t, wasms[0].ProtocolID, "classifier error must leave protocol_id NULL; recovery is via manual protocol-setup re-run")
	assert.Equal(t, 1.0, testutil.ToFloat64(
		m.Ingestion.WasmClassificationFailuresTotal.WithLabelValues("unknown", "classify_error"),
	), "classifier error should increment the failure counter so the NULL row is alertable")
}
