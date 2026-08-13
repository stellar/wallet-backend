// AccountsProcessor extracts native XLM balance changes from account ledger entries.
// It processes ledger changes directly to capture ALL account balance modifications.
package processors

import (
	"context"
	"fmt"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
)

const (
	MinimumBaseReserveCount = 2
	BaseReserveStroops      = 5_000_000
)

// MinimumBalance returns the base reserve requirement in stroops for an account,
// matching stellar-core's getMinBalance: (2 + numSubEntries + numSponsoring - numSponsored) * baseReserve.
// It excludes liabilities; consumers needing spendable balance subtract selling liabilities themselves.
// https://developers.stellar.org/docs/build/guides/transactions/sponsored-reserves#effect-on-minimum-balance
func MinimumBalance(account xdr.AccountEntry) int64 {
	return int64(MinimumBaseReserveCount+account.NumSubEntries+account.NumSponsoring()-account.NumSponsored()) * BaseReserveStroops
}

// Balance-change phases within a single ledger, in Stellar's canonical close order: every
// transaction's fee is charged, then all operations apply, then post-apply (Soroban) fee refunds
// are credited. Refunds come from GetPostApplyFeeChanges, which surfaces the protocol-23+
// post-tx-set-apply refund that Core applies after every operation in the ledger (a global-final
// pass, not per-transaction) — so a refund is the chronologically-last change to its account, and
// phaseRefund dominating operations is correct, not an inversion. accountSortKey makes phase the
// dominant term, so fee < operation < refund holds regardless of transaction or operation index.
const (
	phaseFee       uint8 = 0
	phaseOperation uint8 = 1
	phaseRefund    uint8 = 2
)

// accountSortKey ranks a native-balance change within a single ledger by
// (phase, txIndex, opIndex). The buffer keeps the highest key per account, so this
// selects the chronologically-last change and writes its absolute balance. Phase sits
// in the high bits — above the tx (24 bits) and op (13 bits) fields — so fee < operation
// < refund holds for any tx/op count, and the key is a strict total order with no ties
// (≤1 fee and ≤1 refund per account per tx; operations are unique per (tx, op)). That
// makes the dedup independent of push/merge order.
//
// The ledger is intentionally omitted. Only the live path persists native balances, and it clears its
// buffer before every ledger (see IndexerBuffer.Clear), so every key that reaches the database shares
// one ledger. (The backfill buffer spans many ledgers and does deduplicate account changes in memory,
// but never persists them.) If balances are ever persisted from a buffer that spans multiple ledgers,
// prepend the ledger as the top term.
func accountSortKey(phase uint8, txIndex, opIndex uint32) int64 {
	return int64(phase)<<37 | int64(txIndex)<<13 | int64(opIndex)
}

// AccountsProcessor processes ledger changes to extract account balance modifications.
type AccountsProcessor struct {
	metricsService *metrics.IngestionMetrics
}

// NewAccountsProcessor creates a new accounts processor.
func NewAccountsProcessor(metricsService *metrics.IngestionMetrics) *AccountsProcessor {
	return &AccountsProcessor{
		metricsService: metricsService,
	}
}

// Name returns the processor name for logging and metrics.
func (p *AccountsProcessor) Name() string {
	return "accounts"
}

// ProcessOperation extracts account balance changes from an operation's ledger changes.
// Returns AccountChange structs with balance and liabilities data for database upsert.
func (p *AccountsProcessor) ProcessOperation(ctx context.Context, opWrapper *TransactionOperationWrapper) ([]types.AccountChange, error) {
	startTime := time.Now()
	defer func() {
		if p.metricsService != nil {
			duration := time.Since(startTime).Seconds()
			p.metricsService.StateChangeProcessingDuration.WithLabelValues("AccountsProcessor").Observe(duration)
		}
	}()

	changes, err := opWrapper.Changes()
	if err != nil {
		return nil, fmt.Errorf("getting operation changes: %w", err)
	}

	return p.buildAccountChanges(changes, opWrapper.Transaction.Ledger.LedgerSequence(), accountSortKey(phaseOperation, opWrapper.Transaction.Index, opWrapper.Index)), nil
}

// ProcessTransactionFees extracts native balance changes from a transaction's fee
// phases. The fee debit (GetFeeChanges) is charged before any operation applies; the
// Soroban fee refund (GetPostApplyFeeChanges) is credited after all operations.
// Neither appears in any operation's meta, so the per-operation path never sees an
// account whose balance moves only in these phases — e.g. a fee-bump fee source
// (issue #637).
//
// Balances are absolute (Core has already applied the fee/refund to the ledger
// entry), so the sort key only steers the buffer's dedup to the correct winner:
// fee changes get phaseFee and refund changes get phaseRefund, which sort below and
// above every operation respectively — reproducing the on-chain order
// fee < operation < refund (see accountSortKey).
func (p *AccountsProcessor) ProcessTransactionFees(ctx context.Context, tx ingest.LedgerTransaction) ([]types.AccountChange, error) {
	startTime := time.Now()
	defer func() {
		if p.metricsService != nil {
			duration := time.Since(startTime).Seconds()
			p.metricsService.StateChangeProcessingDuration.WithLabelValues("AccountsProcessor").Observe(duration)
		}
	}()

	ledgerSeq := tx.Ledger.LedgerSequence()

	feeChanges := p.buildAccountChanges(tx.GetFeeChanges(), ledgerSeq, accountSortKey(phaseFee, tx.Index, 0))
	refundChanges := p.buildAccountChanges(tx.GetPostApplyFeeChanges(), ledgerSeq, accountSortKey(phaseRefund, tx.Index, 0))
	return append(feeChanges, refundChanges...), nil
}

// buildAccountChanges converts the account-typed entries in a ledger change set into
// AccountChanges stamped with the given sort key. The per-operation and fee-phase paths share
// it; they differ only in that sort key, which is constant across all changes in one call.
func (p *AccountsProcessor) buildAccountChanges(changes []ingest.Change, ledgerSeq uint32, sortKey int64) []types.AccountChange {
	var accountChanges []types.AccountChange
	for _, change := range changes {
		if change.Type != xdr.LedgerEntryTypeAccount {
			continue
		}

		accChange, skip := p.buildAccountChange(change, ledgerSeq, sortKey)
		if skip {
			continue
		}

		accountChanges = append(accountChanges, accChange)
	}

	return accountChanges
}

// accountChangedExceptSigners reports whether anything about the account changed
// other than its signer list. It mirrors the byte-comparison semantics of the
// SDK's ingest.Change.AccountChangedExceptSigners — only the Signers slice is
// ignored, so side effects of signer changes (NumSubEntries, the V2
// signer-sponsoring IDs) still count as changes, a missing extension compares
// equal to a V1 extension with zero liabilities, and a differing
// LastModifiedLedgerSeq counts on its own — without the two full XDR marshals
// the byte comparison pays on every account change. The SDK function stays as
// the differential oracle in the tests.
func accountChangedExceptSigners(change ingest.Change) bool {
	if change.Pre == nil || change.Post == nil {
		return true
	}
	if change.Pre.LastModifiedLedgerSeq != change.Post.LastModifiedLedgerSeq {
		return true
	}
	pre := change.Pre.Data.MustAccount()
	post := change.Post.Data.MustAccount()
	if !pre.AccountId.Equals(post.AccountId) ||
		pre.Balance != post.Balance ||
		pre.SeqNum != post.SeqNum ||
		pre.NumSubEntries != post.NumSubEntries ||
		pre.Flags != post.Flags ||
		pre.HomeDomain != post.HomeDomain ||
		pre.Thresholds != post.Thresholds {
		return true
	}
	if !accountIDPtrEqual(pre.InflationDest, post.InflationDest) {
		return true
	}
	return !accountExtEqual(pre.Ext, post.Ext)
}

func accountIDPtrEqual(a, b *xdr.AccountId) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	return a == nil || a.Equals(*b)
}

// accountExtEqual compares account extensions with a V0 extension reading as a
// V1 extension carrying zero liabilities, exactly as the SDK normalizes before
// its byte comparison.
func accountExtEqual(a, b xdr.AccountEntryExt) bool {
	if accountLiabilities(a) != accountLiabilities(b) {
		return false
	}
	av2, bv2 := accountExtV2(a), accountExtV2(b)
	if (av2 == nil) != (bv2 == nil) {
		return false
	}
	if av2 == nil {
		return true
	}
	if av2.NumSponsored != bv2.NumSponsored ||
		av2.NumSponsoring != bv2.NumSponsoring ||
		!sponsoringIDsEqual(av2.SignerSponsoringIDs, bv2.SignerSponsoringIDs) {
		return false
	}
	av3, bv3 := av2.Ext.V3, bv2.Ext.V3
	if (av3 == nil) != (bv3 == nil) {
		return false
	}
	return av3 == nil || (av3.SeqLedger == bv3.SeqLedger && av3.SeqTime == bv3.SeqTime)
}

func accountLiabilities(ext xdr.AccountEntryExt) xdr.Liabilities {
	if ext.V1 == nil {
		return xdr.Liabilities{}
	}
	return ext.V1.Liabilities
}

func accountExtV2(ext xdr.AccountEntryExt) *xdr.AccountEntryExtensionV2 {
	if ext.V1 == nil {
		return nil
	}
	return ext.V1.Ext.V2
}

func sponsoringIDsEqual(a, b []xdr.SponsorshipDescriptor) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if (a[i] == nil) != (b[i] == nil) {
			return false
		}
		if a[i] != nil && !a[i].Equals(*b[i]) {
			return false
		}
	}
	return true
}

// buildAccountChange converts a ledger change to an AccountChange, stamped with the
// given ledger sequence and sort key. Returns (change, skip) where
// skip=true means the change should be ignored.
func (p *AccountsProcessor) buildAccountChange(change ingest.Change, ledgerSeq uint32, sortKey int64) (types.AccountChange, bool) {
	var accChange types.AccountChange

	// Skip if only signers changed (no balance/state change)
	if !accountChangedExceptSigners(change) {
		return accChange, true
	}

	// Determine operation type and get the relevant entry
	var entry *xdr.LedgerEntry
	switch {
	case change.Pre == nil && change.Post != nil:
		// Created
		accChange.Operation = types.AccountOpCreate
		entry = change.Post
	case change.Pre != nil && change.Post != nil:
		// Updated
		accChange.Operation = types.AccountOpUpdate
		entry = change.Post
	case change.Pre != nil && change.Post == nil:
		// Removed (account merge)
		accChange.Operation = types.AccountOpRemove
		entry = change.Pre
	default:
		return accChange, true // Invalid change, skip
	}

	account := entry.Data.MustAccount()
	liabilities := account.Liabilities()

	// Build the AccountChange
	accChange.AccountID = account.AccountId.Address()
	accChange.SortKey = sortKey
	accChange.LedgerNumber = ledgerSeq
	accChange.Balance = int64(account.Balance)
	accChange.BuyingLiabilities = int64(liabilities.Buying)
	accChange.SellingLiabilities = int64(liabilities.Selling)

	accChange.NumSubEntries = uint32(account.NumSubEntries)
	accChange.MinimumBalance = MinimumBalance(account)

	return accChange, false
}
