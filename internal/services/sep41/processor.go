package sep41

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/big"

	"github.com/alitto/pond/v2"
	"github.com/google/uuid"
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
	balanceFetcher    *balanceFetcher
	ownsMetadataPool  bool

	// Contracts classified as SEP-41. Populated from input.ProtocolContracts each ledger.
	sep41Contracts map[string]struct{}

	// Per-ledger staged state (rebuilt on every ProcessLedger call for determinism).
	ledgerNumber       uint32
	stagedStateChanges []types.StateChange
	// stagedTouched is the set of (account, contract) pairs whose balance
	// should be refreshed via RPC at PersistCurrentState time. We deliberately
	// do NOT track per-event amounts: SEP-41 only standardizes the interface,
	// so transfer/mint/burn amounts are not guaranteed to equal the actual
	// balance change (fee-on-transfer, rebasing, interest-bearing tokens
	// diverge). Authoritative balances come from `balance(addr)` only.
	stagedTouched    map[balanceKey]struct{}
	stagedAllowances map[allowanceKey]stagedAllowance
	stagedContracts  map[string]struct{} // C-addrs seen this ledger (for contract_tokens upsert)
}

// newProcessor constructs a SEP-41 processor from generic ProtocolDeps. The
// data layer paths are pulled from deps.Models; balance and metadata reads
// require deps.ContractMetadataService — when nil (offline migration paths
// without RPC), PersistHistory still works and the processor still stages
// touched pairs, but PersistCurrentState/LoadCurrentState skip the RPC
// refresh and leave existing rows untouched.
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
		p.balanceFetcher = newBalanceFetcher(deps.ContractMetadataService, pool)
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
		p.markTouched(decoded.From, contractStr)
		p.markTouched(decoded.To, contractStr)
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
		p.markTouched(decoded.To, contractStr)
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
		p.markTouched(decoded.From, contractStr)
		p.stagedStateChanges = append(p.stagedStateChanges,
			scBuilder.Clone().WithReason(types.StateChangeReasonBurn).
				WithAccount(decoded.From).WithAmount(decoded.Amount.String()).Build(),
		)

	case EventClawback:
		decoded, err := ParseClawbackEvent(event)
		if err != nil {
			return err
		}
		p.markTouched(decoded.From, contractStr)
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

// markTouched records that (account, contract) needs a `balance(addr)` refresh
// at PersistCurrentState time. The event amount is intentionally ignored —
// SEP-41 doesn't guarantee it equals the actual balance change.
func (p *processor) markTouched(account, contractStr string) {
	p.stagedTouched[balanceKey{Account: account, ContractID: contractStr}] = struct{}{}
}

func (p *processor) resetStaged(ledgerSeq uint32) {
	p.ledgerNumber = ledgerSeq
	p.stagedStateChanges = nil
	p.stagedTouched = map[balanceKey]struct{}{}
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

// PersistCurrentState refreshes per-pair balances by calling `balance(addr)`
// on each touched contract via Soroban simulation, then writes the
// authoritative absolute values. Allowances are written from the staged
// approve events (absolute, replacing prior grants). Runs inside the
// CAS-guarded transaction so each ledger's writes are applied exactly once.
//
// SEP-41 does not standardize the math relating event amounts to balance
// changes (fee-on-transfer, rebasing, interest-bearing tokens diverge), so
// we ignore event amounts and read the contract's view function directly.
// Per-pair RPC failures are logged and skipped — one bad token must not
// block ingest. The pair stays with its existing row value until the next
// event re-marks it.
func (p *processor) PersistCurrentState(ctx context.Context, dbTx pgx.Tx) error {
	// Ensure a contract_tokens row exists for every contract we're about to reference.
	if err := p.ensureContractTokens(ctx, dbTx); err != nil {
		return fmt.Errorf("ensuring contract_tokens rows: %w", err)
	}

	if err := p.refreshBalances(ctx, dbTx, p.touchedPairs()); err != nil {
		return fmt.Errorf("refreshing SEP-41 balances: %w", err)
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

// LoadCurrentState is called inside the transaction of the first successful
// CAS advance of the current_state_cursor (handoff from migration to live
// ingestion). It enumerates every (account, contract) pair that ever touched
// a SEP-41 token from the `state_changes` table — the canonical output of
// `protocol-migrate history` — and refreshes each row in `sep41_balances`
// from the contract's `balance(addr)` view function. This is what populates
// pre-migration holders, including quiet accounts that wouldn't otherwise
// surface a fresh event in live mode.
//
// Scaling note: this runs inside the handoff transaction, so a deployment
// with many holders can hold the transaction open for the duration of the
// RPC sweep. Concurrency is bounded by balanceFetchBatchSize. New pairs
// introduced after handoff are populated incrementally by
// PersistCurrentState as their owning contracts emit events.
func (p *processor) LoadCurrentState(ctx context.Context, dbTx pgx.Tx) error {
	if p.balances == nil || p.balanceFetcher == nil {
		// Without RPC the bootstrap can't refresh values; skip cleanly so
		// migration command paths (which don't pass a ContractMetadataService)
		// don't fail at handoff.
		return nil
	}

	pairs, err := p.balances.GetAllSEP41Pairs(ctx, dbTx)
	if err != nil {
		return fmt.Errorf("enumerating SEP-41 pairs for bootstrap: %w", err)
	}
	if len(pairs) == 0 {
		log.Ctx(ctx).Info("sep41 bootstrap found no holders in state_changes; nothing to refresh (likely a fresh deployment with no history migrated)")
		return nil
	}
	log.Ctx(ctx).Infof("sep41 bootstrap refreshing %d (account, contract) pairs from state_changes", len(pairs))

	keys := make([]balanceKey, 0, len(pairs))
	idByAddress := make(map[string]uuid.UUID, len(pairs))
	for _, pair := range pairs {
		keys = append(keys, balanceKey{Account: pair.AccountAddress, ContractID: pair.ContractAddress})
		idByAddress[pair.ContractAddress] = pair.ContractID
	}

	if err := p.refreshBalancesWithIDs(ctx, dbTx, keys, idByAddress); err != nil {
		return fmt.Errorf("bootstrapping SEP-41 balances: %w", err)
	}
	return nil
}

// touchedPairs returns the (account, contract) pairs marked dirty during
// ProcessLedger as a slice — handy for handing to balanceFetcher.
func (p *processor) touchedPairs() []balanceKey {
	if len(p.stagedTouched) == 0 {
		return nil
	}
	out := make([]balanceKey, 0, len(p.stagedTouched))
	for k := range p.stagedTouched {
		out = append(out, k)
	}
	return out
}

// refreshBalances looks up balance(addr) for each pair, derives the
// deterministic UUID for the contract, and upserts absolute values. Used by
// PersistCurrentState — the contract IDs come from data.DeterministicContractID
// because the sep41_balances row may not exist yet.
func (p *processor) refreshBalances(ctx context.Context, dbTx pgx.Tx, pairs []balanceKey) error {
	if len(pairs) == 0 {
		return nil
	}
	if p.balanceFetcher == nil {
		// No RPC configured — leave existing rows untouched. Touched pairs will
		// be retried the next time PersistCurrentState runs with RPC available.
		return nil
	}

	results, err := p.balanceFetcher.FetchBalances(ctx, pairs)
	if err != nil {
		return err
	}
	if len(results) == 0 {
		return nil
	}

	rows := make([]sep41data.Balance, 0, len(results))
	for k, bal := range results {
		rows = append(rows, sep41data.Balance{
			AccountAddress: k.Account,
			ContractID:     data.DeterministicContractID(k.ContractID),
			Balance:        bal.String(),
			LedgerNumber:   p.ledgerNumber,
		})
	}
	if err := p.balances.BatchUpsertAbsolute(ctx, dbTx, rows); err != nil {
		return fmt.Errorf("upserting SEP-41 balances: %w", err)
	}
	return nil
}

// refreshBalancesWithIDs is the bootstrap variant: contract UUIDs are taken
// from the supplied lookup (sourced from sep41_balances JOIN contract_tokens)
// rather than recomputed, matching what's actually persisted.
func (p *processor) refreshBalancesWithIDs(ctx context.Context, dbTx pgx.Tx, pairs []balanceKey, ids map[string]uuid.UUID) error {
	if len(pairs) == 0 || p.balanceFetcher == nil {
		return nil
	}

	results, err := p.balanceFetcher.FetchBalances(ctx, pairs)
	if err != nil {
		return err
	}
	if len(results) == 0 {
		return nil
	}

	rows := make([]sep41data.Balance, 0, len(results))
	for k, bal := range results {
		cid, ok := ids[k.ContractID]
		if !ok {
			cid = data.DeterministicContractID(k.ContractID)
		}
		rows = append(rows, sep41data.Balance{
			AccountAddress: k.Account,
			ContractID:     cid,
			Balance:        bal.String(),
			LedgerNumber:   p.ledgerNumber,
		})
	}
	if err := p.balances.BatchUpsertAbsolute(ctx, dbTx, rows); err != nil {
		return fmt.Errorf("upserting SEP-41 balances: %w", err)
	}
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
