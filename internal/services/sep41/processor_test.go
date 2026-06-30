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
	"github.com/stellar/wallet-backend/internal/services"
)

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

func TestProcessor_NeedsResetGuard(t *testing.T) {
	ctx := context.Background()
	p := newTestProcessor()
	p.Reset()

	// A Persist* call seals the staged sets. With nothing staged it returns early and
	// never touches the (nil) tx, but still flags that a Reset is required before folding.
	require.NoError(t, p.PersistHistory(ctx, nil))

	// Folding again without an intervening Reset must error rather than re-add the
	// already-committed deltas.
	err := p.ProcessLedger(ctx, services.ProtocolProcessorInput{LedgerSequence: 1})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Reset")

	// Reset clears the seal and folding is allowed again.
	p.Reset()
	require.NoError(t, p.ProcessLedger(ctx, services.ProtocolProcessorInput{LedgerSequence: 1}))
}

func TestProcessor_ProcessEvent(t *testing.T) {
	t.Run("stages signed balance deltas and DEBIT/CREDIT state changes for a transfer", func(t *testing.T) {
		p := newTestProcessor()
		p.Reset()

		contractID := "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA"
		p.sep41Contracts[contractID] = struct{}{}

		event := buildEventForContract(t, contractID, []xdr.ScVal{
			symScVal(EventTransfer),
			mustAddressScVal(t, testAccountA),
			mustAddressScVal(t, testAccountB),
		}, i128ScVal(500))

		require.NoError(t, p.processEvent(event, newTestOpBuilder(), services.StagingModeBoth))

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
	})

	t.Run("skips events from contracts not classified as SEP-41", func(t *testing.T) {
		p := newTestProcessor()
		p.Reset()

		event := buildEventForContract(t, "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA",
			[]xdr.ScVal{
				symScVal(EventTransfer),
				mustAddressScVal(t, testAccountA),
				mustAddressScVal(t, testAccountB),
			},
			i128ScVal(500),
		)

		require.NoError(t, p.processEvent(event, newTestOpBuilder(), services.StagingModeBoth))
		assert.Empty(t, p.stagedStateChanges)
		assert.Empty(t, p.stagedBalanceDelta)
	})

	t.Run("nets a mint and a burn on the same account into one balance delta", func(t *testing.T) {
		p := newTestProcessor()
		p.Reset()

		contractID := "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA"
		p.sep41Contracts[contractID] = struct{}{}

		mintEvent := buildEventForContract(t, contractID, []xdr.ScVal{
			symScVal(EventMint),
			mustAddressScVal(t, testAccountB),
		}, i128ScVal(100))
		require.NoError(t, p.processEvent(mintEvent, newTestOpBuilder(), services.StagingModeBoth))

		burnEvent := buildEventForContract(t, contractID, []xdr.ScVal{
			symScVal(EventBurn),
			mustAddressScVal(t, testAccountB),
		}, i128ScVal(30))
		require.NoError(t, p.processEvent(burnEvent, newTestOpBuilder(), services.StagingModeBoth))

		delta := p.stagedBalanceDelta[balanceKey{Account: testAccountB, ContractID: contractID}]
		require.NotNil(t, delta)
		assert.Equal(t, big.NewInt(70), delta)
	})

	t.Run("carries the to_muxed_id onto the credit side of a CAP-67 transfer", func(t *testing.T) {
		p := newTestProcessor()
		p.Reset()

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
		require.NoError(t, p.processEvent(event, newTestOpBuilder(), services.StagingModeBoth))

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
	})

	t.Run("history mode stages state changes but no balance deltas", func(t *testing.T) {
		p := newTestProcessor()
		p.Reset()
		contractID := "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA"
		p.sep41Contracts[contractID] = struct{}{}
		event := buildEventForContract(t, contractID, []xdr.ScVal{
			symScVal(EventTransfer),
			mustAddressScVal(t, testAccountA),
			mustAddressScVal(t, testAccountB),
		}, i128ScVal(500))

		require.NoError(t, p.processEvent(event, newTestOpBuilder(), services.StagingModeHistory))
		assert.Len(t, p.stagedStateChanges, 2)
		assert.Empty(t, p.stagedBalanceDelta)
	})

	t.Run("current-state mode stages balance deltas but no state changes", func(t *testing.T) {
		p := newTestProcessor()
		p.Reset()
		contractID := "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA"
		p.sep41Contracts[contractID] = struct{}{}
		event := buildEventForContract(t, contractID, []xdr.ScVal{
			symScVal(EventTransfer),
			mustAddressScVal(t, testAccountA),
			mustAddressScVal(t, testAccountB),
		}, i128ScVal(500))

		require.NoError(t, p.processEvent(event, newTestOpBuilder(), services.StagingModeCurrentState))
		assert.Empty(t, p.stagedStateChanges)
		assert.Len(t, p.stagedBalanceDelta, 2)
	})

	t.Run("records an approve as a single metadata-category state change", func(t *testing.T) {
		p := newTestProcessor()
		p.Reset()

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
		require.NoError(t, p.processEvent(event, newTestOpBuilder(), services.StagingModeBoth))

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
	})
}

func TestProcessor_IndexContracts(t *testing.T) {
	p := newTestProcessor()
	contracts := []data.ProtocolContracts{
		{ContractID: types.HashBytea("0000000000000000000000000000000000000000000000000000000000000001")},
	}
	p.indexContracts(contracts)
	assert.Len(t, p.sep41Contracts, 1)
}

func TestProcessor_PersistHistory(t *testing.T) {
	t.Run("is a no-op when no changes are staged", func(t *testing.T) {
		scMock := data.NewStateChangeWriterMock(t)
		p := newTestProcessorWithStateChanges(scMock)
		// Fresh processor has no staged changes — BatchCopy must not be called.
		require.NoError(t, p.PersistHistory(context.Background(), nil))
	})

	t.Run("writes the staged state changes via BatchCopy", func(t *testing.T) {
		scMock := data.NewStateChangeWriterMock(t)
		p := newTestProcessorWithStateChanges(scMock)
		p.Reset()

		contractID := "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA"
		p.sep41Contracts[contractID] = struct{}{}

		event := buildEventForContract(t, contractID, []xdr.ScVal{
			symScVal(EventTransfer),
			mustAddressScVal(t, testAccountA),
			mustAddressScVal(t, testAccountB),
		}, i128ScVal(500))
		require.NoError(t, p.processEvent(event, newTestOpBuilder(), services.StagingModeBoth))
		require.Len(t, p.stagedStateChanges, 2)

		scMock.On("BatchCopy", mock.Anything, mock.Anything, mock.MatchedBy(func(scs []types.StateChange) bool {
			return len(scs) == 2
		})).Return(2, nil).Once()

		require.NoError(t, p.PersistHistory(context.Background(), nil))
	})
}

func TestProcessor_PersistCurrentState_PassesSignedDeltasNotAbsoluteBalances(t *testing.T) {
	// Regression test: prior behavior kept an in-memory cache and passed absolute balances
	// to the upsert (overwriting DB rows on restart). The processor must now pass raw signed
	// deltas to BatchApplyDeltas so the SQL-side add stays correct across restarts.
	balancesMock := sep41data.NewBalanceModelMock(t)
	allowancesMock := sep41data.NewAllowanceModelMock(t)
	p := &processor{
		networkPassphrase: "Test SDF Network ; September 2015",
		balances:          balancesMock,
		allowances:        allowancesMock,
		sep41Contracts:    map[string]struct{}{},
	}
	p.Reset()
	p.ledgerNumber = 42

	contractID := "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA"
	p.sep41Contracts[contractID] = struct{}{}

	// Transfer 500 from A to B → delta(A)=-500, delta(B)=+500.
	event := buildEventForContract(t, contractID, []xdr.ScVal{
		symScVal(EventTransfer),
		mustAddressScVal(t, testAccountA),
		mustAddressScVal(t, testAccountB),
	}, i128ScVal(500))
	require.NoError(t, p.processEvent(event, newTestOpBuilder(), services.StagingModeBoth))

	balancesMock.On("BatchApplyDeltas", mock.Anything, mock.Anything, mock.MatchedBy(func(deltas []sep41data.Balance) bool {
		if len(deltas) != 2 {
			return false
		}
		deltaByAccount := map[string]string{}
		for _, d := range deltas {
			deltaByAccount[d.AccountID.String()] = d.Balance
		}
		return deltaByAccount[testAccountA] == "-500" && deltaByAccount[testAccountB] == "500"
	})).Return(nil).Once()

	// No allowances staged this ledger.
	allowancesMock.On("BatchUpsert", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	allowancesMock.On("DeleteExpiredBefore", mock.Anything, mock.Anything, uint32(42)).Return(nil).Once()

	require.NoError(t, p.PersistCurrentState(context.Background(), nil))
}

func TestNewProcessor_StartsWithInitializedStagedSets(t *testing.T) {
	// A freshly constructed processor must be ready to fold without an explicit
	// Reset — ProcessLedger no longer self-resets, so the staged maps must already
	// be non-nil or the first staged write would panic.
	p := newProcessor(services.ProtocolDeps{})
	contractID := "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA"
	p.sep41Contracts[contractID] = struct{}{}
	event := buildEventForContract(t, contractID, []xdr.ScVal{
		symScVal(EventTransfer),
		mustAddressScVal(t, testAccountA),
		mustAddressScVal(t, testAccountB),
	}, i128ScVal(500))
	require.NotPanics(t, func() {
		err := p.processEvent(event, newTestOpBuilder(), services.StagingModeBoth)
		require.NoError(t, err)
	})
	assert.Len(t, p.stagedBalanceDelta, 2)
}

func TestProcessor_FoldsAcrossLedgersWithoutReset(t *testing.T) {
	p := newTestProcessor()
	p.Reset()
	contractID := "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA"
	p.sep41Contracts[contractID] = struct{}{}

	// Ledger N: A -> B 500
	require.NoError(t, p.processEvent(buildEventForContract(t, contractID, []xdr.ScVal{
		symScVal(EventTransfer), mustAddressScVal(t, testAccountA), mustAddressScVal(t, testAccountB),
	}, i128ScVal(500)), newTestOpBuilder(), services.StagingModeBoth))
	// Ledger N+1 (no Reset between): B -> A 200
	require.NoError(t, p.processEvent(buildEventForContract(t, contractID, []xdr.ScVal{
		symScVal(EventTransfer), mustAddressScVal(t, testAccountB), mustAddressScVal(t, testAccountA),
	}, i128ScVal(200)), newTestOpBuilder(), services.StagingModeBoth))

	// Net deltas equal the sum: A = -500 + 200 = -300, B = +500 - 200 = +300.
	assert.Equal(t, big.NewInt(-300), p.stagedBalanceDelta[balanceKey{Account: testAccountA, ContractID: contractID}])
	assert.Equal(t, big.NewInt(300), p.stagedBalanceDelta[balanceKey{Account: testAccountB, ContractID: contractID}])
	// History rows append (4 = 2 per transfer).
	assert.Len(t, p.stagedStateChanges, 4)
}

func TestProcessor_StampsPerRowLastModifiedLedgerAcrossWindow(t *testing.T) {
	// Batch-equivalence: a coalesced window must stamp each balance/allowance with the
	// ledger that actually last touched it, not the window end — otherwise migrated rows
	// report a different last_modified_ledger than per-ledger live ingestion. Here B and the
	// allowance are touched only at ledger 100 while the window extends to 199, so they must
	// keep 100; A (re-touched at 199) must advance to 199.
	p := newTestProcessor()
	p.Reset()
	contractID := "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA"
	p.sep41Contracts[contractID] = struct{}{}

	// Ledger 100: A -> B 500 (touches A and B) plus an approve A -> B.
	p.ledgerNumber = 100
	require.NoError(t, p.processEvent(buildEventForContract(t, contractID, []xdr.ScVal{
		symScVal(EventTransfer), mustAddressScVal(t, testAccountA), mustAddressScVal(t, testAccountB),
	}, i128ScVal(500)), newTestOpBuilder(), services.StagingModeCurrentState))
	require.NoError(t, p.processEvent(buildEventForContract(t, contractID,
		[]xdr.ScVal{symScVal(EventApprove), mustAddressScVal(t, testAccountA), mustAddressScVal(t, testAccountB)},
		vecScVal(i128ScVal(500), u32ScVal(9999)),
	), newTestOpBuilder(), services.StagingModeCurrentState))

	// Ledger 199: mint 300 to A (touches A only; B and the allowance are untouched).
	p.ledgerNumber = 199
	require.NoError(t, p.processEvent(buildEventForContract(t, contractID, []xdr.ScVal{
		symScVal(EventMint), mustAddressScVal(t, testAccountA),
	}, i128ScVal(300)), newTestOpBuilder(), services.StagingModeCurrentState))

	assert.Equal(t, uint32(199), p.stagedBalanceLedger[balanceKey{Account: testAccountA, ContractID: contractID}],
		"A was last touched at ledger 199")
	assert.Equal(t, uint32(100), p.stagedBalanceLedger[balanceKey{Account: testAccountB, ContractID: contractID}],
		"B was last touched at ledger 100, not the window end 199")
	assert.Equal(t, uint32(100), p.stagedAllowances[allowanceKey{Owner: testAccountA, Spender: testAccountB, ContractID: contractID}].LedgerNumber,
		"the allowance was last modified at ledger 100, not the window end 199")
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
