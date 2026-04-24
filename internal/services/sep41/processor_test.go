package sep41

import (
	"context"
	"math/big"
	"testing"

	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer/processors"
	"github.com/stellar/wallet-backend/internal/indexer/types"
)

func newTestProcessor() *processor {
	return newProcessor(Dependencies{
		NetworkPassphrase: "Test SDF Network ; September 2015",
	})
}

func newTestProcessorWithStateChanges(sc data.StateChangeWriter) *processor {
	return newProcessor(Dependencies{
		NetworkPassphrase: "Test SDF Network ; September 2015",
		StateChanges:      sc,
	})
}

func newTestOpBuilder() *processors.StateChangeBuilder {
	return processors.NewStateChangeBuilder(42, 0, 1, nil).WithOperationID(100)
}

func TestProcessor_ProcessEvent_Transfer(t *testing.T) {
	p := newTestProcessor()
	p.resetStaged(42)

	contractID := "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA"
	p.sep41Contracts[contractID] = struct{}{}

	event := buildEventForContract(t, contractID, []xdr.ScVal{
		symScVal(EventTransfer),
		mustAddressScVal(t, testAccountA),
		mustAddressScVal(t, testAccountB),
	}, i128ScVal(500))

	require.NoError(t, p.processEvent(event, newTestOpBuilder()))

	deltaA := p.stagedBalanceDelta[balanceKey{Account: testAccountA, ContractID: contractID}]
	deltaB := p.stagedBalanceDelta[balanceKey{Account: testAccountB, ContractID: contractID}]
	require.NotNil(t, deltaA)
	require.NotNil(t, deltaB)
	assert.Equal(t, big.NewInt(-500), deltaA)
	assert.Equal(t, big.NewInt(500), deltaB)

	require.Len(t, p.stagedStateChanges, 2)
	debitFound, creditFound := false, false
	for _, sc := range p.stagedStateChanges {
		switch sc.StateChangeReason {
		case types.StateChangeReasonDebit:
			debitFound = true
		case types.StateChangeReasonCredit:
			creditFound = true
		}
	}
	assert.True(t, debitFound, "expected a DEBIT state change")
	assert.True(t, creditFound, "expected a CREDIT state change")

	_, ok := p.stagedContracts[contractID]
	assert.True(t, ok)
}

func TestProcessor_ProcessEvent_SkipsUnclassifiedContract(t *testing.T) {
	p := newTestProcessor()
	p.resetStaged(42)

	event := buildEventForContract(t, "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA",
		[]xdr.ScVal{
			symScVal(EventTransfer),
			mustAddressScVal(t, testAccountA),
			mustAddressScVal(t, testAccountB),
		},
		i128ScVal(500),
	)

	require.NoError(t, p.processEvent(event, newTestOpBuilder()))
	assert.Empty(t, p.stagedStateChanges)
	assert.Empty(t, p.stagedBalanceDelta)
}

func TestProcessor_ProcessEvent_MintAndBurn(t *testing.T) {
	p := newTestProcessor()
	p.resetStaged(42)

	contractID := "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA"
	p.sep41Contracts[contractID] = struct{}{}

	mintEvent := buildEventForContract(t, contractID, []xdr.ScVal{
		symScVal(EventMint),
		mustAddressScVal(t, testAccountB),
	}, i128ScVal(100))
	require.NoError(t, p.processEvent(mintEvent, newTestOpBuilder()))

	burnEvent := buildEventForContract(t, contractID, []xdr.ScVal{
		symScVal(EventBurn),
		mustAddressScVal(t, testAccountB),
	}, i128ScVal(30))
	require.NoError(t, p.processEvent(burnEvent, newTestOpBuilder()))

	delta := p.stagedBalanceDelta[balanceKey{Account: testAccountB, ContractID: contractID}]
	require.NotNil(t, delta)
	assert.Equal(t, big.NewInt(70), delta)
}

func TestProcessor_IndexContracts(t *testing.T) {
	p := newTestProcessor()
	contracts := []data.ProtocolContracts{
		{ContractID: types.HashBytea("0000000000000000000000000000000000000000000000000000000000000001")},
	}
	p.indexContracts(contracts)
	assert.Len(t, p.sep41Contracts, 1)
}

func TestProcessor_PersistHistory_NoOpWhenEmpty(t *testing.T) {
	scMock := data.NewStateChangeWriterMock(t)
	p := newTestProcessorWithStateChanges(scMock)
	// Fresh processor has no staged changes — BatchCopy must not be called.
	require.NoError(t, p.PersistHistory(context.Background(), nil))
}

func TestProcessor_PersistHistory_WritesStagedChanges(t *testing.T) {
	scMock := data.NewStateChangeWriterMock(t)
	p := newTestProcessorWithStateChanges(scMock)
	p.resetStaged(42)

	contractID := "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA"
	p.sep41Contracts[contractID] = struct{}{}

	event := buildEventForContract(t, contractID, []xdr.ScVal{
		symScVal(EventTransfer),
		mustAddressScVal(t, testAccountA),
		mustAddressScVal(t, testAccountB),
	}, i128ScVal(500))
	require.NoError(t, p.processEvent(event, newTestOpBuilder()))
	require.Len(t, p.stagedStateChanges, 2)

	scMock.On("BatchCopy", mock.Anything, mock.Anything, mock.MatchedBy(func(scs []types.StateChange) bool {
		return len(scs) == 2
	})).Return(2, nil).Once()

	require.NoError(t, p.PersistHistory(context.Background(), nil))
}

// ---- helpers ----

func buildEventForContract(t *testing.T, contractAddr string, topics []xdr.ScVal, data xdr.ScVal) xdr.ContractEvent {
	t.Helper()
	var cid xdr.ContractId
	raw, err := strkey.Decode(strkey.VersionByteContract, contractAddr)
	require.NoError(t, err)
	copy(cid[:], raw)
	return xdr.ContractEvent{
		Type:       xdr.ContractEventTypeContract,
		ContractId: &cid,
		Body: xdr.ContractEventBody{
			V: 0,
			V0: &xdr.ContractEventV0{Topics: topics, Data: data},
		},
	}
}
