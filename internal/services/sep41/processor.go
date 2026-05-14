package sep41

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/big"

	"github.com/alitto/pond/v2"
	"github.com/jackc/pgx/v5"
	"github.com/stellar/go-stellar-sdk/ingest"
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
	Account    string
	ContractID string // C... strkey
}

// allowanceKey uniquely identifies an owner/spender/contract allowance row.
type allowanceKey struct {
	Owner      string
	Spender    string
	ContractID string // C... strkey
}

// stagedAllowance captures an approve() write that has not yet been persisted.
type stagedAllowance struct {
	Amount           *big.Int
	ExpirationLedger uint32
}

// processor implements services.ProtocolProcessor for the SEP-41 token protocol.
type processor struct {
	networkPassphrase string
	balances          sep41data.BalanceModelInterface
	allowances        sep41data.AllowanceModelInterface
	contractTokens    data.ContractModelInterface
	stateChanges      data.StateChangeWriter
	metadataFetcher   *metadataFetcher
	ownsMetadataPool  bool

	// Contracts classified as SEP-41. Populated from input.ProtocolContracts each ledger.
	sep41Contracts map[string]struct{}

	// Per-ledger staged state (rebuilt on every ProcessLedger call for determinism).
	ledgerNumber       uint32
	stagedStateChanges []types.StateChange
	stagedBalanceDelta map[balanceKey]*big.Int
	stagedAllowances   map[allowanceKey]stagedAllowance
	stagedContracts    map[string]struct{} // C-addrs seen this ledger (for contract_tokens upsert)
}

// newProcessor constructs a SEP-41 processor from generic ProtocolDeps. The
// data layer paths are pulled from deps.Models; metadata enrichment in
// PersistCurrentState requires deps.ContractMetadataService — when nil
// (offline migration), the processor still ingests state changes and inserts
// contract_tokens with default values, leaving metadata for a subsequent run.
func newProcessor(deps services.ProtocolDeps) *processor {
	p := &processor{
		networkPassphrase: deps.NetworkPassphrase,
		sep41Contracts:    map[string]struct{}{},
	}
	if deps.Models != nil {
		p.balances = deps.Models.SEP41.Balances
		p.allowances = deps.Models.SEP41.Allowances
		p.contractTokens = deps.Models.Contract
		p.stateChanges = deps.Models.StateChanges
	}
	if deps.ContractMetadataService != nil {
		pool := pond.NewPool(0)
		p.metadataFetcher = newMetadataFetcher(deps.ContractMetadataService, pool)
		p.ownsMetadataPool = true
	}
	return p
}

// Compile-time interface check.
var _ services.ProtocolProcessor = (*processor)(nil)

func (p *processor) ProtocolID() string { return ProtocolID }

// ProcessLedger walks the ledger's transactions and extracts SEP-41 events from classified contracts.
// See services.ProtocolProcessor godoc — must be deterministic across retries.
func (p *processor) ProcessLedger(ctx context.Context, input services.ProtocolProcessorInput) error {
	p.resetStaged(input.LedgerSequence)
	p.indexContracts(input.ProtocolContracts)

	if len(p.sep41Contracts) == 0 {
		return nil
	}

	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(p.networkPassphrase, input.LedgerCloseMeta)
	if err != nil {
		return fmt.Errorf("creating ledger transaction reader for ledger %d: %w", input.LedgerSequence, err)
	}
	defer func() {
		if closeErr := txReader.Close(); closeErr != nil {
			log.Ctx(ctx).Warnf("closing ledger transaction reader: %v", closeErr)
		}
	}()

	ledgerCloseTime := input.LedgerCloseMeta.LedgerCloseTime()

	for {
		tx, readErr := txReader.Read()
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return fmt.Errorf("reading transaction in ledger %d: %w", input.LedgerSequence, readErr)
		}
		if !tx.Result.Successful() {
			continue
		}

		if err := p.processTransaction(tx, input.LedgerSequence, ledgerCloseTime); err != nil {
			return fmt.Errorf("processing transaction %s: %w",
				tx.Result.TransactionHash.HexString(), err)
		}
	}

	return nil
}

func (p *processor) processTransaction(tx ingest.LedgerTransaction, ledgerSeq uint32, ledgerCloseTime int64) error {
	txID := tx.ID()
	builder := processors.NewStateChangeBuilder(ledgerSeq, ledgerCloseTime, txID, nil)

	for opIdx, op := range tx.Envelope.Operations() {
		if op.Body.Type != xdr.OperationTypeInvokeHostFunction {
			continue
		}
		events, err := tx.GetContractEventsForOperation(uint32(opIdx))
		if err != nil {
			return fmt.Errorf("getting contract events for op %d: %w", opIdx, err)
		}
		opID := toid.New(int32(ledgerSeq), int32(tx.Index), int32(opIdx+1)).ToInt64()
		opBuilder := builder.Clone().WithOperationID(opID)

		for _, event := range events {
			if err := p.processEvent(event, opBuilder); err != nil {
				// Log and continue — a malformed event must not abort the whole ledger.
				log.Debugf("sep41: skipping malformed event in tx %s op %d: %v",
					tx.Result.TransactionHash.HexString(), opIdx, err)
				continue
			}
		}
	}
	return nil
}

func (p *processor) processEvent(event xdr.ContractEvent, opBuilder *processors.StateChangeBuilder) error {
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
	p.stagedContracts[contractStr] = struct{}{}

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
		p.applyBalanceDelta(decoded.From, contractStr, new(big.Int).Neg(decoded.Amount))
		p.applyBalanceDelta(decoded.To, contractStr, decoded.Amount)
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

	case EventMint:
		decoded, err := ParseMintEvent(event)
		if err != nil {
			return err
		}
		p.applyBalanceDelta(decoded.To, contractStr, decoded.Amount)
		mintBuilder := scBuilder.Clone().WithReason(types.StateChangeReasonMint).
			WithAccount(decoded.To).WithAmount(decoded.Amount.String())
		if decoded.ToMuxedID != nil {
			mintBuilder = mintBuilder.WithToMuxedID(*decoded.ToMuxedID)
		}
		p.stagedStateChanges = append(p.stagedStateChanges, mintBuilder.Build())

	case EventBurn:
		decoded, err := ParseBurnEvent(event)
		if err != nil {
			return err
		}
		p.applyBalanceDelta(decoded.From, contractStr, new(big.Int).Neg(decoded.Amount))
		p.stagedStateChanges = append(p.stagedStateChanges,
			scBuilder.Clone().WithReason(types.StateChangeReasonBurn).
				WithAccount(decoded.From).WithAmount(decoded.Amount.String()).Build(),
		)

	case EventClawback:
		decoded, err := ParseClawbackEvent(event)
		if err != nil {
			return err
		}
		p.applyBalanceDelta(decoded.From, contractStr, new(big.Int).Neg(decoded.Amount))
		// Reason=BURN reflects supply reduction — no dedicated CLAWBACK reason in the schema enum.
		p.stagedStateChanges = append(p.stagedStateChanges,
			scBuilder.Clone().WithReason(types.StateChangeReasonBurn).
				WithAccount(decoded.From).WithAmount(decoded.Amount.String()).Build(),
		)

	case EventApprove:
		decoded, err := ParseApproveEvent(event)
		if err != nil {
			return err
		}
		key := allowanceKey{Owner: decoded.From, Spender: decoded.Spender, ContractID: contractStr}
		p.stagedAllowances[key] = stagedAllowance{
			Amount:           new(big.Int).Set(decoded.Amount),
			ExpirationLedger: decoded.LiveUntilLedger,
		}
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

	default:
		// Non-SEP-41 event on a SEP-41 contract (e.g., custom event) — ignore.
	}

	return nil
}

func (p *processor) applyBalanceDelta(account, contractStr string, delta *big.Int) {
	key := balanceKey{Account: account, ContractID: contractStr}
	existing, ok := p.stagedBalanceDelta[key]
	if !ok {
		existing = new(big.Int)
		p.stagedBalanceDelta[key] = existing
	}
	existing.Add(existing, delta)
}

func (p *processor) resetStaged(ledgerSeq uint32) {
	p.ledgerNumber = ledgerSeq
	p.stagedStateChanges = nil
	p.stagedBalanceDelta = map[balanceKey]*big.Int{}
	p.stagedAllowances = map[allowanceKey]stagedAllowance{}
	p.stagedContracts = map[string]struct{}{}
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
	if len(p.stagedStateChanges) == 0 {
		return nil
	}
	if _, err := p.stateChanges.BatchCopy(ctx, dbTx, p.stagedStateChanges); err != nil {
		return fmt.Errorf("persisting %d SEP-41 state changes for ledger %d: %w",
			len(p.stagedStateChanges), p.ledgerNumber, err)
	}
	return nil
}

// PersistCurrentState applies staged balance deltas server-side (balance := existing + delta)
// and writes allowance values directly (approve semantics are a set, not an add). Runs inside
// the CAS-guarded transaction so each ledger's deltas are applied exactly once.
func (p *processor) PersistCurrentState(ctx context.Context, dbTx pgx.Tx) error {
	// Ensure a contract_tokens row exists for every contract we're about to reference.
	if err := p.ensureContractTokens(ctx, dbTx); err != nil {
		return fmt.Errorf("ensuring contract_tokens rows: %w", err)
	}

	// Build delta rows directly from the staged deltas — the data layer sums and cleans up zeros.
	deltas := make([]sep41data.Balance, 0, len(p.stagedBalanceDelta))
	for key, delta := range p.stagedBalanceDelta {
		deltas = append(deltas, sep41data.Balance{
			AccountAddress: key.Account,
			ContractID:     data.DeterministicContractID(key.ContractID),
			Balance:        delta.String(),
			LedgerNumber:   p.ledgerNumber,
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
			OwnerAddress:     key.Owner,
			SpenderAddress:   key.Spender,
			ContractID:       data.DeterministicContractID(key.ContractID),
			Amount:           staged.Amount.String(),
			ExpirationLedger: staged.ExpirationLedger,
			LedgerNumber:     p.ledgerNumber,
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

// LoadCurrentState is called inside the transaction of the first successful CAS advance of
// the current_state_cursor (handoff from migration to live ingestion). Because
// PersistCurrentState now applies deltas in SQL via existing + delta, the processor needs
// no preloaded in-memory state — correctness on restart is guaranteed by the CAS gate.
func (p *processor) LoadCurrentState(ctx context.Context, dbTx pgx.Tx) error {
	return nil
}

// ensureContractTokens inserts a contract_tokens row for every contract touched this ledger,
// if not already present. For newly inserted rows, metadata (name/symbol/decimals) is fetched
// via RPC inline; per-contract RPC failures fall back to default values (decimals=0, nil
// name/symbol) and are logged. No separate backfill path exists, so correctness depends on
// the RPC being reachable most of the time.
func (p *processor) ensureContractTokens(ctx context.Context, dbTx pgx.Tx) error {
	if len(p.stagedContracts) == 0 {
		return nil
	}
	ids := make([]string, 0, len(p.stagedContracts))
	for addr := range p.stagedContracts {
		ids = append(ids, addr)
	}
	existing, err := p.contractTokens.GetExisting(ctx, dbTx, ids)
	if err != nil {
		return fmt.Errorf("checking existing contract_tokens: %w", err)
	}
	existingSet := make(map[string]struct{}, len(existing))
	for _, id := range existing {
		existingSet[id] = struct{}{}
	}

	newIDs := make([]string, 0, len(ids))
	for _, addr := range ids {
		if _, ok := existingSet[addr]; !ok {
			newIDs = append(newIDs, addr)
		}
	}
	if len(newIDs) == 0 {
		return nil
	}

	// Fetch metadata for new contracts; missing entries keep default (zero) values.
	metadata := map[string]*data.Contract{}
	if p.metadataFetcher != nil {
		fetched, ferr := p.metadataFetcher.FetchMetadata(ctx, newIDs)
		if ferr != nil {
			// A fetcher-level error (context cancellation, pool failure) is worth logging
			// but must not abort the ledger — we still insert rows with defaults so the
			// state_changes history stays consistent with the balance movements.
			log.Ctx(ctx).Warnf("sep41 metadata fetch batch failed: %v", ferr)
		} else {
			metadata = fetched
		}
	}

	toInsert := make([]*data.Contract, 0, len(newIDs))
	for _, addr := range newIDs {
		c := &data.Contract{
			ID:         data.DeterministicContractID(addr),
			ContractID: addr,
			Type:       contractTokenType,
		}
		if meta, ok := metadata[addr]; ok {
			c.Name = meta.Name
			c.Symbol = meta.Symbol
			c.Decimals = meta.Decimals
		}
		toInsert = append(toInsert, c)
	}

	if err := p.contractTokens.BatchInsert(ctx, dbTx, toInsert); err != nil {
		return fmt.Errorf("inserting contract_tokens: %w", err)
	}
	return nil
}
