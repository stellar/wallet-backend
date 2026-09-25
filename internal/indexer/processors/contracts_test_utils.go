package processors

import (
	"fmt"
	"testing"
	"time"

	set "github.com/deckarep/golang-set/v2"
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"
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

// makeScClaimableBalance creates a CAP-0067 SC_ADDRESS_TYPE_CLAIMABLE_BALANCE ScAddress. The
// referenced balance need not exist: nothing validates it before the indexer sees it.
func makeScClaimableBalance(hash xdr.Hash) xdr.ScAddress {
	return xdr.ScAddress{
		Type: xdr.ScAddressTypeScAddressTypeClaimableBalance,
		ClaimableBalanceId: &xdr.ClaimableBalanceId{
			Type: xdr.ClaimableBalanceIdTypeClaimableBalanceIdTypeV0,
			V0:   &hash,
		},
	}
}

// makeScLiquidityPool creates a CAP-0067 SC_ADDRESS_TYPE_LIQUIDITY_POOL ScAddress.
func makeScLiquidityPool(hash xdr.Hash) xdr.ScAddress {
	return xdr.ScAddress{
		Type:            xdr.ScAddressTypeScAddressTypeLiquidityPool,
		LiquidityPoolId: utils.PointOf(xdr.PoolId(hash)),
	}
}

// makeBasicSorobanOp creates a basic Soroban operation wrapper for testing.
func makeBasicSorobanOp() *TransactionOperationWrapper {
	return &TransactionOperationWrapper{
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
			// One empty operation meta so GetOperationChanges / GetContractEventsForOperation
			// return empty rather than "TransactionMeta.V=0 not supported".
			UnsafeMeta: xdr.TransactionMeta{
				V:  3,
				V3: &xdr.TransactionMetaV3{Operations: []xdr.OperationMeta{{}}},
			},
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
func setFromAddress(op *TransactionOperationWrapper, hostFnType xdr.HostFunctionType, fromSourceAccount string) {
	setFrom(op, hostFnType, xdr.ContractIdPreimage{
		Type: xdr.ContractIdPreimageTypeContractIdPreimageFromAddress,
		FromAddress: &xdr.ContractIdPreimageFromAddress{
			Address: makeScAddress(fromSourceAccount),
			Salt:    TestSalt,
		},
	})
}

// setFromScAddress configures a Soroban operation with FromAddress contract creation from an
// arbitrary ScAddress, including the CAP-0067 arms that setFromAddress (which takes an account ID
// string) cannot express.
func setFromScAddress(op *TransactionOperationWrapper, hostFnType xdr.HostFunctionType, addr xdr.ScAddress) {
	setFrom(op, hostFnType, xdr.ContractIdPreimage{
		Type: xdr.ContractIdPreimageTypeContractIdPreimageFromAddress,
		FromAddress: &xdr.ContractIdPreimageFromAddress{
			Address: addr,
			Salt:    TestSalt,
		},
	})
}

// setFromAsset configures a Soroban operation with FromAsset contract creation.
func setFromAsset(op *TransactionOperationWrapper, hostFnType xdr.HostFunctionType, asset xdr.Asset) {
	setFrom(op, hostFnType, xdr.ContractIdPreimage{
		Type:      xdr.ContractIdPreimageTypeContractIdPreimageFromAsset,
		FromAsset: &asset,
	})
}

// setFrom configures a Soroban operation with From(Asset|Address) contract creation.
func setFrom(op *TransactionOperationWrapper, hostFnType xdr.HostFunctionType, preimage xdr.ContractIdPreimage) {
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
func makeFeeBumpOp(feeBumpSourceAccount string, baseOp *TransactionOperationWrapper) *TransactionOperationWrapper {
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
// It normalizes IngestedAt (non-deterministic wall clock). StateChangeID is left at its
// Build()-time zero value on both sides — ProcessOperation output is compared before the
// emitter assigns ordinals (see types.AssignStateChangeOrdinals), so no normalization is needed.
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
		wantMap[fmt.Sprintf("%d-%s-%s", w.ToID, w.AccountID, w.CreatorAccountID.String())] = w
	}

	for _, g := range got {
		key := fmt.Sprintf("%d-%s-%s", g.ToID, g.AccountID, g.CreatorAccountID.String())
		if _, ok := wantMap[key]; !ok {
			assert.Fail(t, "state change not found", "state change id: %s", key)
		}
		assertStateChangeEqual(t, wantMap[key], g)
	}
}

// makeMuxedAccount returns the M… form of accountID with the given mux ID.
func makeMuxedAccount(accountID string, id uint64) xdr.MuxedAccount {
	return xdr.MuxedAccount{
		Type: xdr.CryptoKeyTypeKeyTypeMuxedEd25519,
		Med25519: &xdr.MuxedAccountMed25519{
			Id:      xdr.Uint64(id),
			Ed25519: *xdr.MustAddress(accountID).Ed25519,
		},
	}
}

// setOperationMeta attaches ledger entry changes and contract events to the op's
// (single) operation meta, the way the host records them for a Soroban transaction.
func setOperationMeta(op *TransactionOperationWrapper, changes xdr.LedgerEntryChanges, events []xdr.ContractEvent) {
	op.Transaction.UnsafeMeta = xdr.TransactionMeta{
		V: 3,
		V3: &xdr.TransactionMetaV3{
			Operations:  []xdr.OperationMeta{{Changes: changes}},
			SorobanMeta: &xdr.SorobanTransactionMeta{Events: events},
		},
	}
}

// requireNoEncodedKeyCollision fails if two participants encode to the same address bytes.
// A collision duplicates an operations_accounts primary key and aborts the ledger insert.
func requireNoEncodedKeyCollision(t *testing.T, participants set.Set[string]) {
	t.Helper()
	byKey := map[string]string{}
	for _, participant := range participants.ToSlice() {
		value, err := types.AddressBytea(participant).Value()
		require.NoError(t, err, "participant %q must encode for the operation-address COPY", participant)
		key := string(value.([]byte))
		prior, collides := byKey[key]
		require.False(t, collides, "participants %q and %q encode to the same operations_accounts row", prior, participant)
		byKey[key] = participant
	}
}

// nonceEntryCreated builds the created ContractData change the host writes under an
// authorising address once its signature verified (key type SCV_LEDGER_KEY_NONCE).
func nonceEntryCreated(authorizer xdr.ScAddress, nonce int64) xdr.LedgerEntryChange {
	return xdr.LedgerEntryChange{
		Type: xdr.LedgerEntryChangeTypeLedgerEntryCreated,
		Created: &xdr.LedgerEntry{
			LastModifiedLedgerSeq: 12345,
			Data: xdr.LedgerEntryData{
				Type: xdr.LedgerEntryTypeContractData,
				ContractData: &xdr.ContractDataEntry{
					Contract:   authorizer,
					Key:        xdr.ScVal{Type: xdr.ScValTypeScvLedgerKeyNonce, NonceKey: &xdr.ScNonceKey{Nonce: xdr.Int64(nonce)}},
					Durability: xdr.ContractDataDurabilityTemporary,
					Val:        xdr.ScVal{Type: xdr.ScValTypeScvVoid},
				},
			},
		},
	}
}

// contractEventFrom builds a minimal contract event emitted by contractID.
func contractEventFrom(contractID string) xdr.ContractEvent {
	decoded := strkey.MustDecode(strkey.VersionByteContract, contractID)
	id := xdr.ContractId(decoded)
	return xdr.ContractEvent{
		Type:       xdr.ContractEventTypeContract,
		ContractId: &id,
		Body:       xdr.ContractEventBody{V: 0, V0: &xdr.ContractEventV0{Topics: []xdr.ScVal{{Type: xdr.ScValTypeScvVoid}}, Data: xdr.ScVal{Type: xdr.ScValTypeScvVoid}}},
	}
}

// contractInstanceCreated builds the created ContractData change the host writes when a
// contract with the given C-address is deployed (key type SCV_LEDGER_KEY_CONTRACT_INSTANCE).
func contractInstanceCreated(contractID string) xdr.LedgerEntryChange {
	return xdr.LedgerEntryChange{
		Type: xdr.LedgerEntryChangeTypeLedgerEntryCreated,
		Created: &xdr.LedgerEntry{
			LastModifiedLedgerSeq: 12345,
			Data: xdr.LedgerEntryData{
				Type: xdr.LedgerEntryTypeContractData,
				ContractData: &xdr.ContractDataEntry{
					Contract:   makeScContract(contractID),
					Key:        xdr.ScVal{Type: xdr.ScValTypeScvLedgerKeyContractInstance},
					Durability: xdr.ContractDataDurabilityPersistent,
					Val:        xdr.ScVal{Type: xdr.ScValTypeScvContractInstance, Instance: &xdr.ScContractInstance{Executable: xdr.ContractExecutable{Type: xdr.ContractExecutableTypeContractExecutableWasm, WasmHash: &xdr.Hash{}}}},
				},
			},
		},
	}
}
