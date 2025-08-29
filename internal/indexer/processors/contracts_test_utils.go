package processors

import (
	"fmt"
	"testing"
	"time"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/network"
	operation_processor "github.com/stellar/go/processors/operation"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/utils"
)

// txSourceAccount is a source account commonly used in this package's tests.
const txSourceAccount = "GAUE24B36YYY3CXTXNFE3IFXU6EE4NUOS5L744IWGTNXVXZAXFGMP6CC"

// TestSalt is a common salt used in this package's tests.
var TestSalt = xdr.Uint256{195, 179, 60, 131, 211, 25, 160, 131, 45, 151, 203, 11, 11, 116, 166, 232, 51, 92, 179, 76, 220, 111, 96, 246, 72, 68, 195, 127, 194, 19, 147, 252}

// usdcAssetTestnet is the widely known Circle USDC asset, and we use it in this package's tests.
var usdcAssetTestnet = xdr.Asset{
	Type: xdr.AssetTypeAssetTypeCreditAlphanum4,
	AlphaNum4: &xdr.AlphaNum4{
		AssetCode: [4]byte{'U', 'S', 'D', 'C'},
		Issuer:    xdr.MustAddress("GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5"),
	},
}

var closeTime = time.Date(2025, 2, 21, 12, 0, 0, 0, time.UTC)

// makeScAddress creates an xdr.ScAddress from an account ID string.
func makeScAddress(accountID string) xdr.ScAddress {
	return xdr.ScAddress{
		Type:      xdr.ScAddressTypeScAddressTypeAccount,
		AccountId: utils.PointOf(xdr.MustAddress(accountID)),
	}
}

// makeScContract creates an xdr.ScAddress from a contract ID string.
func makeScContract(contractID string) xdr.ScAddress {
	decoded := strkey.MustDecode(strkey.VersionByteContract, contractID)
	return xdr.ScAddress{
		Type:       xdr.ScAddressTypeScAddressTypeContract,
		ContractId: utils.PointOf(xdr.ContractId(decoded)),
	}
}

// makeBasicSorobanOp creates a basic Soroban operation wrapper for testing.
func makeBasicSorobanOp() *operation_processor.TransactionOperationWrapper {
	return &operation_processor.TransactionOperationWrapper{
		Network:        network.TestNetworkPassphrase,
		LedgerClosed:   closeTime,
		LedgerSequence: 12345,
		Operation:      xdr.Operation{},
		Transaction: ingest.LedgerTransaction{
			Envelope: xdr.TransactionEnvelope{
				Type: xdr.EnvelopeTypeEnvelopeTypeTx,
				V1: &xdr.TransactionV1Envelope{
					Tx: xdr.Transaction{
						SourceAccount: xdr.MustMuxedAddress(txSourceAccount), // <--- tx.SourceAccount
						Ext: xdr.TransactionExt{
							V:           1,
							SorobanData: &xdr.SorobanTransactionData{},
						},
					},
				},
			},
			Hash: xdr.Hash{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20},
			Ledger: xdr.LedgerCloseMeta{
				V: 1,
				V1: &xdr.LedgerCloseMetaV1{
					LedgerHeader: xdr.LedgerHeaderHistoryEntry{
						Header: xdr.LedgerHeader{
							LedgerSeq: 12345,
							ScpValue:  xdr.StellarValue{CloseTime: xdr.TimePoint(closeTime.Unix())},
						},
					},
				},
			},
		},
	}
}

// setFromAddress configures a Soroban operation with FromAddress contract creation.
//
//nolint:unparam
func setFromAddress(op *operation_processor.TransactionOperationWrapper, hostFnType xdr.HostFunctionType, fromSourceAccount string) {
	setFrom(op, hostFnType, xdr.ContractIdPreimage{
		Type: xdr.ContractIdPreimageTypeContractIdPreimageFromAddress,
		FromAddress: &xdr.ContractIdPreimageFromAddress{
			Address: makeScAddress(fromSourceAccount),
			Salt:    TestSalt,
		},
	})
}

// setFromAsset configures a Soroban operation with FromAsset contract creation.
func setFromAsset(op *operation_processor.TransactionOperationWrapper, hostFnType xdr.HostFunctionType, asset xdr.Asset) {
	setFrom(op, hostFnType, xdr.ContractIdPreimage{
		Type:      xdr.ContractIdPreimageTypeContractIdPreimageFromAsset,
		FromAsset: &asset,
	})
}

// setFrom configures a Soroban operation with From(Asset|Address) contract creation.
func setFrom(op *operation_processor.TransactionOperationWrapper, hostFnType xdr.HostFunctionType, preimage xdr.ContractIdPreimage) {
	op.Operation.Body = xdr.OperationBody{
		Type: xdr.OperationTypeInvokeHostFunction,
		InvokeHostFunctionOp: &xdr.InvokeHostFunctionOp{
			HostFunction: xdr.HostFunction{
				Type: hostFnType,
			},
			Auth: []xdr.SorobanAuthorizationEntry{},
		},
	}

	switch hostFnType {
	case xdr.HostFunctionTypeHostFunctionTypeCreateContract:
		op.Operation.Body.InvokeHostFunctionOp.HostFunction.CreateContract = &xdr.CreateContractArgs{
			ContractIdPreimage: preimage,
		}
	case xdr.HostFunctionTypeHostFunctionTypeCreateContractV2:
		op.Operation.Body.InvokeHostFunctionOp.HostFunction.CreateContractV2 = &xdr.CreateContractArgsV2{
			ContractIdPreimage: preimage,
		}
	default:
		require.Fail(nil, "unsupported host function type", "host function type: %s", hostFnType)
	}
}

// makeFeeBumpOp updates the envelope type to a fee bump envelope and sets the fee source account.
func makeFeeBumpOp(feeBumpSourceAccount string, baseOp *operation_processor.TransactionOperationWrapper) *operation_processor.TransactionOperationWrapper {
	op := *baseOp
	op.Transaction.Envelope.V0 = nil
	op.Transaction.Envelope.V1 = nil
	op.Transaction.Envelope.Type = xdr.EnvelopeTypeEnvelopeTypeTxFeeBump
	op.Transaction.Envelope.FeeBump = &xdr.FeeBumpTransactionEnvelope{
		Tx: xdr.FeeBumpTransaction{
			FeeSource: xdr.MustMuxedAddress(feeBumpSourceAccount),
			InnerTx: xdr.FeeBumpTransactionInnerTx{
				Type: baseOp.Transaction.Envelope.Type,
				V1:   baseOp.Transaction.Envelope.V1,
			},
		},
	}
	return &op
}

// assertStateChangeEqual compares two state changes and fails if they are not equal.
func assertStateChangeEqual(t *testing.T, want types.StateChange, got types.StateChange) {
	t.Helper()

	gotV2 := got
	gotV2.IngestedAt = want.IngestedAt
	assert.Equal(t, want, gotV2)
}

// assertStateChangesElementsMatch compares two slices of state changes and fails if they are not equal.
func assertStateChangesElementsMatch(t *testing.T, want []types.StateChange, got []types.StateChange) {
	t.Helper()

	if len(want) != len(got) {
		assert.Fail(t, "state changes length mismatch", "want %d, got %d", len(want), len(got))
	}

	wantMap := make(map[string]types.StateChange)
	for _, w := range want {
		wantMap[fmt.Sprintf("%d-%s-%s", w.ToID, w.AccountID, w.DeployerAccountID.String)] = w
	}

	for _, g := range got {
		key := fmt.Sprintf("%d-%s-%s", g.ToID, g.AccountID, g.DeployerAccountID.String)
		if _, ok := wantMap[key]; !ok {
			assert.Fail(t, "state change not found", "state change id: %s", key)
		}
		assertStateChangeEqual(t, wantMap[key], g)
	}
}
