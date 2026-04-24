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
	sep41data "github.com/stellar/wallet-backend/internal/data/sep41"
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
		if sc.StateChangeReason == types.StateChangeReasonDebit {
			debitFound = true
		}
		if sc.StateChangeReason == types.StateChangeReasonCredit {
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

func TestProcessor_ProcessEvent_Transfer_PersistsToMuxedID(t *testing.T) {
	p := newTestProcessor()
	p.resetStaged(42)

	contractID := "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA"
	p.sep41Contracts[contractID] = struct{}{}

	// CAP-67 transfer: data is a map { amount: i128, to_muxed_id: u64 }.
	const muxedID uint64 = 1234567890
	data := mapScVal(
		xdr.ScMapEntry{Key: symScVal("amount"), Val: i128ScVal(500)},
		xdr.ScMapEntry{Key: symScVal("to_muxed_id"), Val: u64ScVal(muxedID)},
	)
	event := buildEventForContract(t, contractID, []xdr.ScVal{
		symScVal(EventTransfer),
		mustAddressScVal(t, testAccountA),
		mustAddressScVal(t, testAccountB),
	}, data)
	require.NoError(t, p.processEvent(event, newTestOpBuilder()))

	require.Len(t, p.stagedStateChanges, 2)
	var creditSC *types.StateChange
	for i := range p.stagedStateChanges {
		if p.stagedStateChanges[i].StateChangeReason == types.StateChangeReasonCredit {
			creditSC = &p.stagedStateChanges[i]
		}
	}
	require.NotNil(t, creditSC, "expected a CREDIT state change")
	require.True(t, creditSC.ToMuxedID.Valid, "credit side should carry the muxed id")
	assert.Equal(t, "1234567890", creditSC.ToMuxedID.String)
}

func TestProcessor_ProcessEvent_Approve_UsesMetadataCategory(t *testing.T) {
	p := newTestProcessor()
	p.resetStaged(42)

	contractID := "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA"
	p.sep41Contracts[contractID] = struct{}{}

	event := buildEventForContract(t, contractID,
		[]xdr.ScVal{
			symScVal(EventApprove),
			mustAddressScVal(t, testAccountA),
			mustAddressScVal(t, testAccountB),
		},
		vecScVal(i128ScVal(500), u32ScVal(1234)),
	)
	require.NoError(t, p.processEvent(event, newTestOpBuilder()))

	key := allowanceKey{Owner: testAccountA, Spender: testAccountB, ContractID: contractID}
	staged, ok := p.stagedAllowances[key]
	require.True(t, ok)
	assert.Equal(t, big.NewInt(500), staged.Amount)
	assert.Equal(t, uint32(1234), staged.ExpirationLedger)

	require.Len(t, p.stagedStateChanges, 1)
	sc := p.stagedStateChanges[0]
	assert.Equal(t, types.StateChangeCategoryMetadata, sc.StateChangeCategory)
	assert.Equal(t, types.StateChangeReasonUpdate, sc.StateChangeReason)
	require.NotNil(t, sc.KeyValue)
	assert.Equal(t, EventApprove, sc.KeyValue["sep41_event"])
	assert.Equal(t, testAccountB, sc.KeyValue["spender"])
	assert.Equal(t, "500", sc.KeyValue["amount"])
	// JSON numeric decoding round-trips u32 through float64, but we never marshal→unmarshal here;
	// the builder stores the raw uint32. Cast for a stable assertion.
	assert.EqualValues(t, uint32(1234), sc.KeyValue["live_until_ledger"])
}

type metadataFetcherFunc func(ctx context.Context, ids []string) (map[string]*data.Contract, error)

func (f metadataFetcherFunc) FetchSEP41Metadata(ctx context.Context, ids []string) (map[string]*data.Contract, error) {
	return f(ctx, ids)
}

func TestProcessor_EnsureContractTokens_PopulatesMetadataFromFetcher(t *testing.T) {
	contractsMock := data.NewContractModelMock(t)
	contract := "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA"
	name, symbol := "USDC", "USDC"
	fetched := map[string]*data.Contract{
		contract: {Name: &name, Symbol: &symbol, Decimals: 7},
	}
	fetcher := metadataFetcherFunc(func(_ context.Context, ids []string) (map[string]*data.Contract, error) {
		require.Equal(t, []string{contract}, ids)
		return fetched, nil
	})

	p := newProcessor(Dependencies{
		ContractTokens:  contractsMock,
		MetadataFetcher: fetcher,
	})
	p.resetStaged(42)
	p.stagedContracts[contract] = struct{}{}

	contractsMock.On("GetExisting", mock.Anything, mock.Anything, mock.Anything).Return([]string{}, nil).Once()
	contractsMock.On("BatchInsert", mock.Anything, mock.Anything, mock.MatchedBy(func(cs []*data.Contract) bool {
		if len(cs) != 1 {
			return false
		}
		c := cs[0]
		return c.ContractID == contract &&
			c.Name != nil && *c.Name == "USDC" &&
			c.Symbol != nil && *c.Symbol == "USDC" &&
			c.Decimals == 7
	})).Return(nil).Once()

	require.NoError(t, p.ensureContractTokens(context.Background(), nil))
}

func TestProcessor_EnsureContractTokens_FallsBackOnFetcherError(t *testing.T) {
	contractsMock := data.NewContractModelMock(t)
	contract := "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA"
	fetcher := metadataFetcherFunc(func(_ context.Context, _ []string) (map[string]*data.Contract, error) {
		return nil, fakeRPCError{}
	})

	p := newProcessor(Dependencies{
		ContractTokens:  contractsMock,
		MetadataFetcher: fetcher,
	})
	p.resetStaged(42)
	p.stagedContracts[contract] = struct{}{}

	contractsMock.On("GetExisting", mock.Anything, mock.Anything, mock.Anything).Return([]string{}, nil).Once()
	contractsMock.On("BatchInsert", mock.Anything, mock.Anything, mock.MatchedBy(func(cs []*data.Contract) bool {
		// Metadata fetch failed — row should still be inserted, with defaults.
		return len(cs) == 1 && cs[0].ContractID == contract && cs[0].Name == nil && cs[0].Symbol == nil && cs[0].Decimals == 0
	})).Return(nil).Once()

	require.NoError(t, p.ensureContractTokens(context.Background(), nil))
}

type fakeRPCError struct{}

func (fakeRPCError) Error() string { return "rpc error" }

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

func TestProcessor_PersistCurrentState_PassesSignedDeltasNotAbsoluteBalances(t *testing.T) {
	// Regression test: prior behavior kept an in-memory cache and passed absolute balances
	// to the upsert (overwriting DB rows on restart). The processor must now pass raw signed
	// deltas to BatchApplyDeltas so the SQL-side add stays correct across restarts.
	balancesMock := sep41data.NewBalanceModelMock(t)
	allowancesMock := sep41data.NewAllowanceModelMock(t)
	contractsMock := data.NewContractModelMock(t)
	p := newProcessor(Dependencies{
		NetworkPassphrase: "Test SDF Network ; September 2015",
		Balances:          balancesMock,
		Allowances:        allowancesMock,
		ContractTokens:    contractsMock,
	})
	p.resetStaged(42)

	contractID := "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA"
	p.sep41Contracts[contractID] = struct{}{}

	// Transfer 500 from A to B → delta(A)=-500, delta(B)=+500.
	event := buildEventForContract(t, contractID, []xdr.ScVal{
		symScVal(EventTransfer),
		mustAddressScVal(t, testAccountA),
		mustAddressScVal(t, testAccountB),
	}, i128ScVal(500))
	require.NoError(t, p.processEvent(event, newTestOpBuilder()))

	// contract_tokens: none existing, expect BatchInsert of the one staged contract.
	contractsMock.On("GetExisting", mock.Anything, mock.Anything, mock.Anything).Return([]string{}, nil).Once()
	contractsMock.On("BatchInsert", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

	balancesMock.On("BatchApplyDeltas", mock.Anything, mock.Anything, mock.MatchedBy(func(deltas []sep41data.Balance) bool {
		if len(deltas) != 2 {
			return false
		}
		deltaByAccount := map[string]string{}
		for _, d := range deltas {
			deltaByAccount[d.AccountAddress] = d.Balance
		}
		return deltaByAccount[testAccountA] == "-500" && deltaByAccount[testAccountB] == "500"
	})).Return(nil).Once()

	// No allowances staged this ledger.
	allowancesMock.On("BatchUpsert", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

	require.NoError(t, p.PersistCurrentState(context.Background(), nil))
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

//nolint:unparam // tests currently pass the same contract, but keep the param for future varied cases
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
			V:  0,
			V0: &xdr.ContractEventV0{Topics: topics, Data: data},
		},
	}
}
