package indexer

import (
	"testing"

	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Protocol 28 adds arms to three XDR unions. The generated DecodeFrom returns
// "invalid Type switch value" for an arm it does not know, so on a protocol 27
// SDK these ledgers fail to decode outright rather than decoding wrongly.
//
// StellarValueEmptyTxSet (CAP-0083) is the one that matters most: it sits in
// LedgerHeader.ScpValue.Ext, so a ledger using it fails whole-LedgerCloseMeta
// decode with no contract involved at all, halting ingestion. These tests
// round-trip through the binary encoding, because a struct literal alone would
// not exercise the decoder that used to reject them.

func TestProtocol28_EmptyTxSetLedgerHeaderRoundTrips(t *testing.T) {
	const (
		ledgerSeq = xdr.Uint32(987654)
		closeTime = xdr.TimePoint(1735689600)
	)

	lcm := xdr.LedgerCloseMeta{
		V: 0,
		V0: &xdr.LedgerCloseMetaV0{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Header: xdr.LedgerHeader{
					LedgerVersion: 28,
					LedgerSeq:     ledgerSeq,
					ScpValue: xdr.StellarValue{
						CloseTime: closeTime,
						Ext: xdr.StellarValueExt{
							V: xdr.StellarValueTypeStellarValueEmptyTxSet,
							ProposedValue: &xdr.StellarValueProposedValue{
								PreviousLedgerHash:    xdr.Hash{0xde, 0xad, 0xbe, 0xef},
								PreviousLedgerVersion: 28,
								LcValueSignature: xdr.LedgerCloseValueSignature{
									NodeId: xdr.NodeId{
										Type:    xdr.PublicKeyTypePublicKeyTypeEd25519,
										Ed25519: &xdr.Uint256{0x01},
									},
									Signature: xdr.Signature{0x02, 0x03},
								},
							},
						},
					},
				},
			},
		},
	}

	encoded, err := lcm.MarshalBinary()
	require.NoError(t, err, "an empty-tx-set ledger header must encode")

	var decoded xdr.LedgerCloseMeta
	require.NoError(t, decoded.UnmarshalBinary(encoded),
		"an empty-tx-set ledger header must decode; failure here means the SDK predates protocol 28")

	// The accessors ingestion actually depends on must survive the new ext arm.
	assert.Equal(t, uint32(ledgerSeq), decoded.LedgerSequence())
	assert.Equal(t, uint(closeTime), uint(decoded.LedgerCloseTime()))
	assert.Equal(t, xdr.StellarValueTypeStellarValueEmptyTxSet,
		decoded.V0.LedgerHeader.Header.ScpValue.Ext.V)
}

func TestProtocol28_ExternalRefContractInstanceRoundTrips(t *testing.T) {
	owner := xdr.ContractId{0x11, 0x22, 0x33}

	val := xdr.ScVal{
		Type: xdr.ScValTypeScvContractInstance,
		Instance: &xdr.ScContractInstance{
			Executable: xdr.ContractExecutable{
				Type: xdr.ContractExecutableTypeContractExecutableExternalRef,
				ExternalRef: &xdr.ContractExecutableExternalRef{
					ExecutableOwner: xdr.ScAddress{
						Type:       xdr.ScAddressTypeScAddressTypeContract,
						ContractId: &owner,
					},
					Tag: xdr.ScString("fleet-v1"),
				},
			},
		},
	}

	encoded, err := val.MarshalBinary()
	require.NoError(t, err)

	var decoded xdr.ScVal
	require.NoError(t, decoded.UnmarshalBinary(encoded),
		"an external-ref executable must decode; failure here means the SDK predates protocol 28")

	ref := decoded.Instance.Executable.ExternalRef
	require.NotNil(t, ref)
	assert.Equal(t, xdr.ScString("fleet-v1"), ref.Tag)
	assert.Equal(t, owner, *ref.ExecutableOwner.ContractId)
}

// SCV_EXECUTABLE_TAG is the key discriminant of the contract-data entry that
// maps a tag to a WASM hash, so it reaches us as a ledger-entry key.
func TestProtocol28_ExecutableTagScValRoundTrips(t *testing.T) {
	tag := xdr.ScString("fleet-v1")
	val := xdr.ScVal{Type: xdr.ScValTypeScvExecutableTag, ExecutableTag: &tag}

	encoded, err := val.MarshalBinary()
	require.NoError(t, err)

	var decoded xdr.ScVal
	require.NoError(t, decoded.UnmarshalBinary(encoded),
		"an executable-tag ScVal must decode; failure here means the SDK predates protocol 28")

	require.Equal(t, xdr.ScValTypeScvExecutableTag, decoded.Type)
	require.NotNil(t, decoded.ExecutableTag)
	assert.Equal(t, tag, *decoded.ExecutableTag)
}
