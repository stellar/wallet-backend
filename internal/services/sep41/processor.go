package sep41

import (
	"context"
	"encoding/hex"
	"fmt"
	"math/big"

	"github.com/jackc/pgx/v5"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/toid"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/wallet-backend/internal/data"
	sep41data "github.com/stellar/wallet-backend/internal/data/sep41"
	"github.com/stellar/wallet-backend/internal/indexer/processors"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/services"
)

// balanceKey uniquely identifies a (account, contract) balance row.
type balanceKey struct {
	Account    types.AddressBytea
	ContractID string // C... strkey
}

// allowanceKey uniquely identifies an owner/spender/contract allowance row.
type allowanceKey struct {
	Owner      types.AddressBytea
	Spender    types.AddressBytea
	ContractID string // C... strkey
}

// stagedAllowance captures an approve() write that has not yet been persisted.
type stagedAllowance struct {
	Amount           *big.Int
	ExpirationLedger uint32
	// LedgerNumber is the ledger of the latest approve folded into this grant, recorded
	// so a coalesced window stamps the row's real last-modified ledger, not the window end.
	LedgerNumber uint32
}

// processor implements services.ProtocolProcessor for the SEP-41 token protocol.
type processor struct {
	networkPassphrase string
	balances          sep41data.BalanceModelInterface
	allowances        sep41data.AllowanceModelInterface
	stateChanges      data.StateChangeWriter

	// Contracts classified as SEP-41. Populated from input.ProtocolContracts each ledger.
	sep41Contracts map[string]struct{}

	// Staged sets that accumulate across ProcessLedger calls within a window and are
	// cleared by the caller via Reset().
	ledgerNumber       uint32
	stagedStateChanges []types.StateChange
	stagedBalanceDelta map[balanceKey]*big.Int
	// stagedBalanceLedger records the latest ledger to touch each balance key. Deltas sum
	// across a window, but last_modified_ledger must reflect the ledger of the last touch,
	// so a coalesced window matches per-ledger live ingestion rather than stamping the window end.
	stagedBalanceLedger map[balanceKey]uint32
	stagedAllowances    map[allowanceKey]stagedAllowance

	// needsReset is set by Persist* and cleared by Reset(). ProcessLedger refuses to fold
	// while it is set: folding after a Persist without an intervening Reset would re-add the
	// already-committed deltas and silently double-count. Both callers Reset() before folding
	// again, so this only fires on misuse by a future caller.
	needsReset bool
}

// newProcessor constructs a SEP-41 processor from generic ProtocolDeps. The
// data layer paths are pulled from deps.Models. Token metadata is owned by the
// SEP-41 validator (written to contract_tokens at classification time), so the
// processor needs no RPC or metadata-fetch dependencies.
func newProcessor(deps services.ProtocolDeps) *processor {
	p := &processor{
		networkPassphrase: deps.NetworkPassphrase,
		sep41Contracts:    map[string]struct{}{},
	}
	if deps.Models != nil {
		p.balances = deps.Models.SEP41.Balances
		p.allowances = deps.Models.SEP41.Allowances
		p.stateChanges = deps.Models.StateChanges
	}
	p.Reset()
	return p
}

// Compile-time interface check.
var _ services.ProtocolProcessor = (*processor)(nil)

func (p *processor) ProtocolID() string { return ProtocolID }

// StateChangeOrdinalBase returns the SEP-41 state_change_id namespace base.
func (p *processor) StateChangeOrdinalBase() int64 { return types.StateChangeOrdinalBaseSEP41 }

// ProcessLedger consumes contract events that the indexer (or
// ExtractContractEventsForLedger in the migration path) has already
// extracted into the buffer. The processor never touches LedgerCloseMeta —
// events are pre-filtered to successful transactions only by the
// indexer/migration helper, so no per-tx success check is needed here.
// See services.ProtocolProcessor for the folding/batch-equivalence contract.
//
// A Persist* call seals the staged sets; Reset() must be called before folding
// the next window. Folding while sealed returns an error rather than silently
// double-counting the already-committed deltas.
func (p *processor) ProcessLedger(_ context.Context, input services.ProtocolProcessorInput) error {
	if p.needsReset {
		return fmt.Errorf("sep41: ProcessLedger called after Persist without Reset(); deltas would double-count")
	}
	p.ledgerNumber = input.LedgerSequence
	p.indexContracts(input.ProtocolContracts)

	if len(p.sep41Contracts) == 0 {
		return nil
	}

	for key, events := range input.ContractEvents {
		// tx.ID() in stellar/go is toid.New(ledgerSeq, txIdx, 0); recompute it
		// here so PersistHistory's state-change rows carry the same txID a full
		// tx walk would have produced.
		txID := toid.New(int32(input.LedgerSequence), int32(key.TxIdx), 0).ToInt64()
		opID := toid.New(int32(input.LedgerSequence), int32(key.TxIdx), int32(key.OpIdx+1)).ToInt64()
		opBuilder := processors.NewStateChangeBuilder(input.LedgerSequence, input.LedgerCloseTime, txID, nil).
			WithOperationID(opID)

		for _, event := range events {
			if err := p.processEvent(event, opBuilder, input.StagingMode); err != nil {
				// Log and continue — a malformed event must not abort the whole ledger.
				log.Debugf("sep41: skipping malformed event at tx=%d op=%d: %v",
					key.TxIdx, key.OpIdx, err)
				continue
			}
		}
	}

	return nil
}

func (p *processor) processEvent(event xdr.ContractEvent, opBuilder *processors.StateChangeBuilder, mode services.StagingMode) error {
	if event.Type != xdr.ContractEventTypeContract || event.ContractId == nil || event.Body.V != 0 {
		return fmt.Errorf("unsupported event shape")
	}
	contractStr, err := ContractIDString(event)
	if err != nil {
		return err
	}
	if _, ok := p.sep41Contracts[contractStr]; !ok {
		return nil // not a contract we track; silently skip
	}

	topics := event.Body.V0.Topics
	if len(topics) == 0 {
		return fmt.Errorf("event has no topics")
	}
	sym, ok := topics[0].GetSym()
	if !ok {
		return fmt.Errorf("topic[0] not a symbol")
	}

	scBuilder := opBuilder.Clone().
		WithCategory(types.StateChangeCategoryBalance).
		WithToken(contractStr)

	switch string(sym) {
	case EventTransfer:
		decoded, err := ParseTransferEvent(event)
		if err != nil {
			return err
		}
		if mode.NeedsCurrentState() {
			p.applyBalanceDelta(types.AddressBytea(decoded.From), contractStr, new(big.Int).Neg(decoded.Amount))
			p.applyBalanceDelta(types.AddressBytea(decoded.To), contractStr, decoded.Amount)
		}
		if mode.NeedsHistory() {
			creditBuilder := scBuilder.Clone().WithReason(types.StateChangeReasonCredit).
				WithAccount(decoded.To).WithAmount(decoded.Amount.String())
			if decoded.ToMuxedID != nil {
				creditBuilder = creditBuilder.WithToMuxedID(*decoded.ToMuxedID)
			}
			p.stagedStateChanges = append(p.stagedStateChanges,
				scBuilder.Clone().WithReason(types.StateChangeReasonDebit).
					WithAccount(decoded.From).WithAmount(decoded.Amount.String()).Build(),
				creditBuilder.Build(),
			)
		}

	case EventMint:
		decoded, err := ParseMintEvent(event)
		if err != nil {
			return err
		}
		if mode.NeedsCurrentState() {
			p.applyBalanceDelta(types.AddressBytea(decoded.To), contractStr, decoded.Amount)
		}
		if mode.NeedsHistory() {
			mintBuilder := scBuilder.Clone().WithReason(types.StateChangeReasonMint).
				WithAccount(decoded.To).WithAmount(decoded.Amount.String())
			if decoded.ToMuxedID != nil {
				mintBuilder = mintBuilder.WithToMuxedID(*decoded.ToMuxedID)
			}
			p.stagedStateChanges = append(p.stagedStateChanges, mintBuilder.Build())
		}

	case EventBurn:
		decoded, err := ParseBurnEvent(event)
		if err != nil {
			return err
		}
		if mode.NeedsCurrentState() {
			p.applyBalanceDelta(types.AddressBytea(decoded.From), contractStr, new(big.Int).Neg(decoded.Amount))
		}
		if mode.NeedsHistory() {
			p.stagedStateChanges = append(p.stagedStateChanges,
				scBuilder.Clone().WithReason(types.StateChangeReasonBurn).
					WithAccount(decoded.From).WithAmount(decoded.Amount.String()).Build(),
			)
		}

	case EventClawback:
		decoded, err := ParseClawbackEvent(event)
		if err != nil {
			return err
		}
		if mode.NeedsCurrentState() {
			p.applyBalanceDelta(types.AddressBytea(decoded.From), contractStr, new(big.Int).Neg(decoded.Amount))
		}
		if mode.NeedsHistory() {
			// Reason=BURN reflects supply reduction — no dedicated CLAWBACK reason in the schema enum.
			p.stagedStateChanges = append(p.stagedStateChanges,
				scBuilder.Clone().WithReason(types.StateChangeReasonBurn).
					WithAccount(decoded.From).WithAmount(decoded.Amount.String()).Build(),
			)
		}

	case EventApprove:
		decoded, err := ParseApproveEvent(event)
		if err != nil {
			return err
		}
		if mode.NeedsCurrentState() {
			key := allowanceKey{Owner: types.AddressBytea(decoded.From), Spender: types.AddressBytea(decoded.Spender), ContractID: contractStr}
			p.stagedAllowances[key] = stagedAllowance{
				Amount:           new(big.Int).Set(decoded.Amount),
				ExpirationLedger: decoded.LiveUntilLedger,
				LedgerNumber:     p.ledgerNumber,
			}
		}
		if mode.NeedsHistory() {
			// Approve lives under Metadata so MetadataChange's keyValue surfaces spender/expiration
			// through GraphQL. StandardBalanceChange would drop those fields.
			p.stagedStateChanges = append(p.stagedStateChanges,
				scBuilder.Clone().
					WithCategory(types.StateChangeCategoryMetadata).
					WithReason(types.StateChangeReasonUpdate).
					WithAccount(decoded.From).
					WithAmount(decoded.Amount.String()).
					WithKeyValue(map[string]any{
						"sep41_event":       EventApprove,
						"spender":           decoded.Spender,
						"amount":            decoded.Amount.String(),
						"live_until_ledger": decoded.LiveUntilLedger,
					}).
					Build(),
			)
		}

	default:
		// Non-SEP-41 event on a SEP-41 contract (e.g., custom event) — ignore.
	}

	return nil
}

func (p *processor) applyBalanceDelta(account types.AddressBytea, contractStr string, delta *big.Int) {
	key := balanceKey{Account: account, ContractID: contractStr}
	existing, ok := p.stagedBalanceDelta[key]
	if !ok {
		existing = new(big.Int)
		p.stagedBalanceDelta[key] = existing
	}
	existing.Add(existing, delta)
	// ledgerNumber is the ledger currently being folded; ledgers fold in ascending order,
	// so this keeps the latest ledger that touched the balance.
	p.stagedBalanceLedger[key] = p.ledgerNumber
}

// Reset clears the staged sets for the next window. ledgerNumber is intentionally
// left untouched — ProcessLedger sets it each ledger.
func (p *processor) Reset() {
	p.stagedStateChanges = nil
	p.stagedBalanceDelta = map[balanceKey]*big.Int{}
	p.stagedBalanceLedger = map[balanceKey]uint32{}
	p.stagedAllowances = map[allowanceKey]stagedAllowance{}
	p.needsReset = false
}

func (p *processor) indexContracts(contracts []data.ProtocolContracts) {
	p.sep41Contracts = make(map[string]struct{}, len(contracts))
	for _, c := range contracts {
		// ContractID is a hex-encoded string of the 32 raw bytes (types.HashBytea).
		raw, err := hex.DecodeString(string(c.ContractID))
		if err != nil {
			log.Warnf("sep41: skipping contract with invalid hex ID %q: %v", c.ContractID, err)
			continue
		}
		addr, err := strkey.Encode(strkey.VersionByteContract, raw)
		if err != nil {
			log.Warnf("sep41: skipping contract with invalid ID (%d bytes): %v", len(raw), err)
			continue
		}
		p.sep41Contracts[addr] = struct{}{}
	}
}

// PersistHistory writes staged state changes inside the CAS transaction.
func (p *processor) PersistHistory(ctx context.Context, dbTx pgx.Tx) error {
	p.needsReset = true
	if len(p.stagedStateChanges) == 0 {
		return nil
	}
	// Assign deterministic state_change_ids in emission order, namespaced under
	// the SEP-41 base so they can never collide with the main indexer's or
	// Blend's IDs for the same operation (see types.AssignStateChangeOrdinals).
	types.AssignStateChangeOrdinals(p.stagedStateChanges, p.StateChangeOrdinalBase())
	if _, err := p.stateChanges.BatchCopy(ctx, dbTx, p.stagedStateChanges); err != nil {
		return fmt.Errorf("persisting %d SEP-41 state changes for ledger %d: %w",
			len(p.stagedStateChanges), p.ledgerNumber, err)
	}
	return nil
}

// PersistCurrentState applies staged balance deltas server-side (balance := existing + delta)
// and writes allowance values directly (approve semantics are a set, not an add). Runs inside
// the CAS-guarded transaction so each window's accumulated deltas are applied exactly once.
func (p *processor) PersistCurrentState(ctx context.Context, dbTx pgx.Tx) error {
	p.needsReset = true
	// Build delta rows directly from the staged deltas — the data layer sums and cleans up zeros.
	deltas := make([]sep41data.Balance, 0, len(p.stagedBalanceDelta))
	for key, delta := range p.stagedBalanceDelta {
		deltas = append(deltas, sep41data.Balance{
			AccountID:    key.Account,
			ContractID:   data.DeterministicContractID(key.ContractID),
			Balance:      delta.String(),
			LedgerNumber: p.stagedBalanceLedger[key],
		})
	}
	if err := p.balances.BatchApplyDeltas(ctx, dbTx, deltas); err != nil {
		return fmt.Errorf("applying SEP-41 balance deltas: %w", err)
	}

	// Allowances are written absolutely (approve replaces the grant). Amount=0 deletes the row.
	var allowanceUpserts []sep41data.Allowance
	var allowanceDeletes []sep41data.Allowance
	for key, staged := range p.stagedAllowances {
		row := sep41data.Allowance{
			OwnerID:          key.Owner,
			SpenderID:        key.Spender,
			ContractID:       data.DeterministicContractID(key.ContractID),
			Amount:           staged.Amount.String(),
			ExpirationLedger: staged.ExpirationLedger,
			LedgerNumber:     staged.LedgerNumber,
		}
		if staged.Amount.Sign() == 0 {
			allowanceDeletes = append(allowanceDeletes, row)
		} else {
			allowanceUpserts = append(allowanceUpserts, row)
		}
	}

	if err := p.allowances.BatchUpsert(ctx, dbTx, allowanceUpserts, allowanceDeletes); err != nil {
		return fmt.Errorf("upserting SEP-41 allowances: %w", err)
	}
	if err := p.allowances.DeleteExpiredBefore(ctx, dbTx, p.ledgerNumber); err != nil {
		return fmt.Errorf("deleting expired SEP-41 allowances: %w", err)
	}

	return nil
}
