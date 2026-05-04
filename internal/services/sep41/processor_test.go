package sep41

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"testing"

	"github.com/alitto/pond/v2"
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

// newTestPool returns a pond worker pool with cleanup registered, used by
// tests that need a metadataFetcher.
func newTestPool(t *testing.T) pond.Pool {
	t.Helper()
	p := pond.NewPool(0)
	t.Cleanup(p.StopAndWait)
	return p
}

// newTestProcessor builds a bare processor for tests that don't need any
// data-layer dependencies. We construct the struct directly rather than going
// through newProcessor → ProtocolDeps because most of these tests only
// exercise event-processing logic.
func newTestProcessor() *processor {
	return &processor{
		networkPassphrase: "Test SDF Network ; September 2015",
		sep41Contracts:    map[string]struct{}{},
	}
}

func newTestProcessorWithStateChanges(sc data.StateChangeWriter) *processor {
	return &processor{
		networkPassphrase: "Test SDF Network ; September 2015",
		stateChanges:      sc,
		sep41Contracts:    map[string]struct{}{},
	}
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

	// Both sides of the transfer must be marked touched — the actual balance is
	// fetched authoritatively from the contract at PersistCurrentState time, so
	// we don't track per-event deltas here.
	_, touchedA := p.stagedTouched[balanceKey{Account: testAccountA, ContractID: contractID}]
	_, touchedB := p.stagedTouched[balanceKey{Account: testAccountB, ContractID: contractID}]
	assert.True(t, touchedA, "transfer.from must be marked touched")
	assert.True(t, touchedB, "transfer.to must be marked touched")

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
	assert.Empty(t, p.stagedTouched)
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

	// Mint and burn each touch the holder; the set keeps a single entry. The
	// authoritative balance is read from the contract, so no arithmetic.
	_, touched := p.stagedTouched[balanceKey{Account: testAccountB, ContractID: contractID}]
	assert.True(t, touched, "mint+burn target must be marked touched")
	assert.Len(t, p.stagedTouched, 1)
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

// stubMetadataRPC implements services.ContractMetadataService well enough for
// the SEP-41 metadata fetcher to call FetchSingleField against it. It only
// stores per-call answers and returns the next one on each invocation.
type stubMetadataRPC struct {
	answers []xdr.ScVal
	err     error
}

func (s *stubMetadataRPC) FetchSACMetadata(_ context.Context, _ []string) ([]*data.Contract, error) {
	return nil, nil
}

func (s *stubMetadataRPC) FetchSingleField(_ context.Context, _, _ string, _ ...xdr.ScVal) (xdr.ScVal, error) {
	if s.err != nil {
		return xdr.ScVal{}, s.err
	}
	if len(s.answers) == 0 {
		return xdr.ScVal{}, fakeRPCError{}
	}
	v := s.answers[0]
	s.answers = s.answers[1:]
	return v, nil
}

func TestProcessor_EnsureContractTokens_PopulatesMetadataFromFetcher(t *testing.T) {
	contractsMock := data.NewContractModelMock(t)
	contract := "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA"
	rpc := &stubMetadataRPC{
		answers: []xdr.ScVal{strScVal("USDC"), strScVal("USDC"), u32ScVal(7)},
	}

	pool := newTestPool(t)
	p := &processor{
		networkPassphrase: "Test SDF Network ; September 2015",
		contractTokens:    contractsMock,
		metadataFetcher:   newMetadataFetcher(rpc, pool),
		sep41Contracts:    map[string]struct{}{},
	}
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
	rpc := &stubMetadataRPC{err: fakeRPCError{}}

	pool := newTestPool(t)
	p := &processor{
		networkPassphrase: "Test SDF Network ; September 2015",
		contractTokens:    contractsMock,
		metadataFetcher:   newMetadataFetcher(rpc, pool),
		sep41Contracts:    map[string]struct{}{},
	}
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

func TestProcessor_PersistCurrentState_FetchesAuthoritativeBalances(t *testing.T) {
	// Correctness test: SEP-41 only standardizes the interface, so transfer
	// amounts are not guaranteed to equal balance changes. PersistCurrentState
	// must read `balance(addr)` from the contract and store the absolute value
	// — NOT compute it from event amounts. We simulate a fee-on-transfer
	// token: transfer of 500, but balance() reports A=499 (fee burned), B=400
	// (fee skimmed elsewhere). The stored values must match the RPC result,
	// not the event amount.
	balancesMock := sep41data.NewBalanceModelMock(t)
	allowancesMock := sep41data.NewAllowanceModelMock(t)
	contractsMock := data.NewContractModelMock(t)

	contractID := "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA"
	balanceByAccount := map[string]int64{
		testAccountA: 499,
		testAccountB: 400,
	}
	rpc := &lookupBalanceRPC{
		balanceByAccount: balanceByAccount,
	}
	pool := newTestPool(t)
	p := &processor{
		networkPassphrase: "Test SDF Network ; September 2015",
		balances:          balancesMock,
		allowances:        allowancesMock,
		contractTokens:    contractsMock,
		balanceFetcher:    newBalanceFetcher(rpc, pool),
		sep41Contracts:    map[string]struct{}{},
	}
	p.resetStaged(42)
	p.sep41Contracts[contractID] = struct{}{}

	// Transfer 500 — emitted amount lies about the actual balance change.
	event := buildEventForContract(t, contractID, []xdr.ScVal{
		symScVal(EventTransfer),
		mustAddressScVal(t, testAccountA),
		mustAddressScVal(t, testAccountB),
	}, i128ScVal(500))
	require.NoError(t, p.processEvent(event, newTestOpBuilder()))

	contractsMock.On("GetExisting", mock.Anything, mock.Anything, mock.Anything).Return([]string{}, nil).Once()
	contractsMock.On("BatchInsert", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

	balancesMock.On("BatchUpsertAbsolute", mock.Anything, mock.Anything, mock.MatchedBy(func(rows []sep41data.Balance) bool {
		if len(rows) != 2 {
			return false
		}
		got := map[string]string{}
		for _, r := range rows {
			got[r.AccountAddress] = r.Balance
		}
		return got[testAccountA] == "499" && got[testAccountB] == "400"
	})).Return(nil).Once()

	allowancesMock.On("BatchUpsert", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	allowancesMock.On("DeleteExpiredBefore", mock.Anything, mock.Anything, uint32(42)).Return(nil).Once()

	require.NoError(t, p.PersistCurrentState(context.Background(), nil))
}

func TestProcessor_PersistCurrentState_SkipsArchivedPair(t *testing.T) {
	// When balance() returns an archive/restore-required error, that pair must
	// be skipped (existing row left untouched) rather than failing the ledger.
	balancesMock := sep41data.NewBalanceModelMock(t)
	allowancesMock := sep41data.NewAllowanceModelMock(t)
	contractsMock := data.NewContractModelMock(t)

	contractID := "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA"
	rpc := &lookupBalanceRPC{
		balanceByAccount: map[string]int64{},
		errByAccount: map[string]error{
			testAccountA: errors.New("simulation failed: ledger entry archived"),
			testAccountB: errors.New("simulation failed: ledger entry archived"),
		},
	}
	pool := newTestPool(t)
	p := &processor{
		networkPassphrase: "Test SDF Network ; September 2015",
		balances:          balancesMock,
		allowances:        allowancesMock,
		contractTokens:    contractsMock,
		balanceFetcher:    newBalanceFetcher(rpc, pool),
		sep41Contracts:    map[string]struct{}{},
	}
	p.resetStaged(42)
	p.sep41Contracts[contractID] = struct{}{}

	event := buildEventForContract(t, contractID, []xdr.ScVal{
		symScVal(EventTransfer),
		mustAddressScVal(t, testAccountA),
		mustAddressScVal(t, testAccountB),
	}, i128ScVal(500))
	require.NoError(t, p.processEvent(event, newTestOpBuilder()))

	contractsMock.On("GetExisting", mock.Anything, mock.Anything, mock.Anything).Return([]string{}, nil).Once()
	contractsMock.On("BatchInsert", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	// All RPC fetches errored — BatchUpsertAbsolute must not be called.
	allowancesMock.On("BatchUpsert", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	allowancesMock.On("DeleteExpiredBefore", mock.Anything, mock.Anything, uint32(42)).Return(nil).Once()

	require.NoError(t, p.PersistCurrentState(context.Background(), nil))
}

func TestProcessor_LoadCurrentState_BootstrapsFromHistory(t *testing.T) {
	// The bootstrap (called inside the handoff transaction) enumerates
	// (account, contract) pairs from `state_changes` — the canonical output
	// of `protocol-migrate history` — and writes an authoritative balance
	// for each via RPC. After a fresh history migration `sep41_balances` is
	// empty, so this is the only path that populates pre-migration holders
	// (especially quiet accounts that won't surface a fresh event in live
	// mode). The data layer's GetAllSEP41Pairs is what reads state_changes;
	// here we mock that boundary and assert the handoff-time RPC fan-out.
	balancesMock := sep41data.NewBalanceModelMock(t)
	contractsMock := data.NewContractModelMock(t)

	contractAddr := "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA"
	contractUUID := data.DeterministicContractID(contractAddr)
	pairs := []sep41data.BalancePair{
		{AccountAddress: testAccountA, ContractID: contractUUID, ContractAddress: contractAddr},
		{AccountAddress: testAccountB, ContractID: contractUUID, ContractAddress: contractAddr},
	}
	balancesMock.On("GetAllSEP41Pairs", mock.Anything, mock.Anything).Return(pairs, nil).Once()

	rpc := &lookupBalanceRPC{
		balanceByAccount: map[string]int64{
			testAccountA: 1234,
			testAccountB: 5678,
		},
	}
	pool := newTestPool(t)
	p := &processor{
		networkPassphrase: "Test SDF Network ; September 2015",
		balances:          balancesMock,
		contractTokens:    contractsMock,
		balanceFetcher:    newBalanceFetcher(rpc, pool),
		sep41Contracts:    map[string]struct{}{},
	}
	p.resetStaged(99)

	balancesMock.On("BatchUpsertAbsolute", mock.Anything, mock.Anything, mock.MatchedBy(func(rows []sep41data.Balance) bool {
		if len(rows) != 2 {
			return false
		}
		got := map[string]string{}
		for _, r := range rows {
			if r.ContractID != contractUUID || r.LedgerNumber != 99 {
				return false
			}
			got[r.AccountAddress] = r.Balance
		}
		return got[testAccountA] == "1234" && got[testAccountB] == "5678"
	})).Return(nil).Once()

	require.NoError(t, p.LoadCurrentState(context.Background(), nil))
}

// lookupBalanceRPC is a ContractMetadataService stub that returns canned
// balance() results keyed by account address (decoded from the second arg).
// Used by PersistCurrentState/LoadCurrentState tests.
type lookupBalanceRPC struct {
	balanceByAccount map[string]int64
	errByAccount     map[string]error
}

func (s *lookupBalanceRPC) FetchSACMetadata(_ context.Context, _ []string) ([]*data.Contract, error) {
	return nil, nil
}

func (s *lookupBalanceRPC) FetchSingleField(_ context.Context, _, function string, args ...xdr.ScVal) (xdr.ScVal, error) {
	if function != "balance" {
		return xdr.ScVal{}, fmt.Errorf("lookupBalanceRPC: unexpected function %q", function)
	}
	if len(args) != 1 {
		return xdr.ScVal{}, fmt.Errorf("lookupBalanceRPC: expected 1 arg, got %d", len(args))
	}
	addr, ok := args[0].GetAddress()
	if !ok {
		return xdr.ScVal{}, fmt.Errorf("lookupBalanceRPC: arg is not an address")
	}
	addrStr, err := addr.String()
	if err != nil {
		return xdr.ScVal{}, fmt.Errorf("lookupBalanceRPC: address String: %w", err)
	}
	if e, ok := s.errByAccount[addrStr]; ok {
		return xdr.ScVal{}, e
	}
	v, ok := s.balanceByAccount[addrStr]
	if !ok {
		return xdr.ScVal{}, fmt.Errorf("lookupBalanceRPC: no balance for %s", addrStr)
	}
	return i128ScVal(v), nil
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
