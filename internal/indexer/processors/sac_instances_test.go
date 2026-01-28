// Unit tests for SACInstanceProcessor.
// Tests SAC contract metadata extraction from contract instance ledger entries.
package processors

import (
	"context"
	"testing"

	"github.com/stellar/go-stellar-sdk/ingest/sac"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer/types"
)

// Test asset for SAC instance tests
const (
	testAssetCode   = "USDC"
	testAssetIssuer = "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5"
)

// computeSACContractID computes the contract ID for a given asset using the SDK.
func computeSACContractID(code, issuer string) [32]byte {
	asset, err := xdr.NewCreditAsset(code, issuer)
	if err != nil {
		panic(err)
	}
	contractID, err := asset.ContractID(networkPassphrase)
	if err != nil {
		panic(err)
	}
	return contractID
}

// sacInstanceLedgerEntry creates a SAC contract instance ledger entry for testing.
// Uses the SDK's sac.AssetToContractData to create a valid instance entry.
//
//nolint:unparam // issuer parameter kept for API consistency with test variations
func sacInstanceLedgerEntry(code, issuer string) *xdr.LedgerEntry {
	contractID := computeSACContractID(code, issuer)
	ledgerData, err := sac.AssetToContractData(false, code, issuer, contractID)
	if err != nil {
		panic(err)
	}

	return &xdr.LedgerEntry{
		LastModifiedLedgerSeq: 12345,
		Data:                  ledgerData,
	}
}

func TestNewSACInstanceProcessor(t *testing.T) {
	processor := NewSACInstanceProcessor(networkPassphrase)

	assert.NotNil(t, processor)
	assert.Equal(t, networkPassphrase, processor.networkPassphrase)
}

func TestSACInstanceProcessor_Name(t *testing.T) {
	processor := NewSACInstanceProcessor(networkPassphrase)

	assert.Equal(t, "sac_instances", processor.Name())
}

func TestSACInstanceProcessor_ProcessOperation(t *testing.T) {
	processor := NewSACInstanceProcessor(networkPassphrase)
	expectedContractID := computeSACContractID(testAssetCode, testAssetIssuer)
	expectedContractIDStr := strkey.MustEncode(strkey.VersionByteContract, expectedContractID[:])

	tests := []struct {
		name                 string
		changes              xdr.LedgerEntryChanges
		expectedCount        int
		checkFields          bool
		expectedCode         string
		expectedIssuer       string
		expectedContractID   string
		expectedContractType string
	}{
		{
			name: "SAC instance created",
			changes: xdr.LedgerEntryChanges{
				{
					Type:    xdr.LedgerEntryChangeTypeLedgerEntryCreated,
					Created: sacInstanceLedgerEntry(testAssetCode, testAssetIssuer),
				},
			},
			expectedCount:        1,
			checkFields:          true,
			expectedCode:         testAssetCode,
			expectedIssuer:       testAssetIssuer,
			expectedContractID:   expectedContractIDStr,
			expectedContractType: string(types.ContractTypeSAC),
		},
		{
			name: "SAC instance updated",
			changes: xdr.LedgerEntryChanges{
				{
					Type:  xdr.LedgerEntryChangeTypeLedgerEntryState,
					State: sacInstanceLedgerEntry(testAssetCode, testAssetIssuer),
				},
				{
					Type:    xdr.LedgerEntryChangeTypeLedgerEntryUpdated,
					Updated: sacInstanceLedgerEntry(testAssetCode, testAssetIssuer),
				},
			},
			expectedCount:        1,
			checkFields:          true,
			expectedCode:         testAssetCode,
			expectedIssuer:       testAssetIssuer,
			expectedContractID:   expectedContractIDStr,
			expectedContractType: string(types.ContractTypeSAC),
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
			name: "non-instance contract data skipped",
			changes: xdr.LedgerEntryChanges{
				{
					Type:    xdr.LedgerEntryChangeTypeLedgerEntryCreated,
					Created: sacBalanceLedgerEntry(testSACContractBytes, testHolderContractBytes, 5000000),
				},
			},
			expectedCount: 0,
		},
		{
			name:          "empty changes",
			changes:       xdr.LedgerEntryChanges{},
			expectedCount: 0,
		},
		{
			name: "removed entry skipped",
			changes: xdr.LedgerEntryChanges{
				{
					Type:  xdr.LedgerEntryChangeTypeLedgerEntryState,
					State: sacInstanceLedgerEntry(testAssetCode, testAssetIssuer),
				},
				{
					Type: xdr.LedgerEntryChangeTypeLedgerEntryRemoved,
					Removed: &xdr.LedgerKey{
						Type: xdr.LedgerEntryTypeContractData,
						ContractData: &xdr.LedgerKeyContractData{
							Contract: xdr.ScAddress{
								Type:       xdr.ScAddressTypeScAddressTypeContract,
								ContractId: (*xdr.ContractId)(&expectedContractID),
							},
							Key:        xdr.ScVal{Type: xdr.ScValTypeScvLedgerKeyContractInstance},
							Durability: xdr.ContractDataDurabilityPersistent,
						},
					},
				},
			},
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

			if tc.expectedCount > 0 && tc.checkFields {
				contract := contracts[0]
				assert.Equal(t, tc.expectedContractID, contract.ContractID)
				assert.Equal(t, tc.expectedContractType, contract.Type)
				assert.Equal(t, tc.expectedCode, *contract.Code)
				assert.Equal(t, tc.expectedIssuer, *contract.Issuer)
				assert.Equal(t, tc.expectedCode+":"+tc.expectedIssuer, *contract.Name)
				assert.Equal(t, tc.expectedCode, *contract.Symbol)
				assert.Equal(t, uint32(7), contract.Decimals)
				// Verify deterministic ID is set
				assert.Equal(t, data.DeterministicContractID(tc.expectedContractID), contract.ID)
			}
		})
	}
}

func TestSACInstanceProcessor_ProcessOperation_AlphaNum12(t *testing.T) {
	processor := NewSACInstanceProcessor(networkPassphrase)

	// Test with AlphaNum12 asset code (longer than 4 characters)
	longCode := "SUPERLONGCD"
	expectedContractID := computeSACContractID(longCode, testAssetIssuer)
	expectedContractIDStr := strkey.MustEncode(strkey.VersionByteContract, expectedContractID[:])

	changes := xdr.LedgerEntryChanges{
		{
			Type:    xdr.LedgerEntryChangeTypeLedgerEntryCreated,
			Created: sacInstanceLedgerEntry(longCode, testAssetIssuer),
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

	contracts, err := processor.ProcessOperation(context.Background(), wrapper)
	require.NoError(t, err)
	require.Len(t, contracts, 1)

	contract := contracts[0]
	assert.Equal(t, expectedContractIDStr, contract.ContractID)
	assert.Equal(t, longCode, *contract.Code)
	assert.Equal(t, testAssetIssuer, *contract.Issuer)
}
