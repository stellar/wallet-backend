// AccountsProcessor extracts native XLM balance changes from account ledger entries.
// It processes ledger changes directly to capture ALL account balance modifications.
package processors

import (
	"context"
	"fmt"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/toid"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
)

const (
	MinimumBaseReserveCount = 2
	BaseReserveStroops      = 5_000_000
)

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

	changes, err := opWrapper.Transaction.GetOperationChanges(opWrapper.Index)
	if err != nil {
		return nil, fmt.Errorf("getting operation changes: %w", err)
	}

	var accountChanges []types.AccountChange

	for _, change := range changes {
		if change.Type != xdr.LedgerEntryTypeAccount {
			continue
		}

		accChange, skip, err := p.buildAccountChange(change, opWrapper.Transaction.Ledger.LedgerSequence(), opWrapper.ID())
		if err != nil {
			return nil, err
		}
		if skip {
			continue
		}

		accountChanges = append(accountChanges, accChange)
	}

	return accountChanges, nil
}

// ProcessTransactionFees extracts native balance changes from a transaction's fee
// phases. The fee debit (GetFeeChanges) is charged before any operation applies; the
// Soroban fee refund (GetPostApplyFeeChanges) is credited after all operations.
// Neither appears in any operation's meta, so the per-operation path never sees an
// account whose balance moves only in these phases — e.g. a fee-bump fee source
// (issue #637).
//
// Balances are absolute (Core has already applied the fee/refund to the ledger
// entry), so the synthetic OperationIDs only steer the buffer's max-OperationID
// dedup to the correct winner: fee changes get the ledger floor (toid op 0), which
// sorts below every operation TOID in the ledger; refund changes get the ledger
// ceiling (one below the next ledger's floor), which sorts above every operation
// TOID. This reproduces the on-chain order fee < operation < refund.
func (p *AccountsProcessor) ProcessTransactionFees(ctx context.Context, tx ingest.LedgerTransaction) ([]types.AccountChange, error) {
	startTime := time.Now()
	defer func() {
		if p.metricsService != nil {
			duration := time.Since(startTime).Seconds()
			p.metricsService.StateChangeProcessingDuration.WithLabelValues("AccountsProcessor").Observe(duration)
		}
	}()

	ledgerSeq := tx.Ledger.LedgerSequence()

	feeChanges, err := p.feeAccountChanges(tx.GetFeeChanges(), ledgerSeq, toid.New(int32(ledgerSeq), 0, 0).ToInt64())
	if err != nil {
		return nil, err
	}

	refundChanges, err := p.feeAccountChanges(tx.GetPostApplyFeeChanges(), ledgerSeq, toid.New(int32(ledgerSeq)+1, 0, 0).ToInt64()-1)
	if err != nil {
		return nil, err
	}

	return append(feeChanges, refundChanges...), nil
}

// feeAccountChanges converts the account-typed entries in a fee-phase change set into
// AccountChanges stamped with the given OperationID.
func (p *AccountsProcessor) feeAccountChanges(changes []ingest.Change, ledgerSeq uint32, operationID int64) ([]types.AccountChange, error) {
	var accountChanges []types.AccountChange
	for _, change := range changes {
		if change.Type != xdr.LedgerEntryTypeAccount {
			continue
		}

		accChange, skip, err := p.buildAccountChange(change, ledgerSeq, operationID)
		if err != nil {
			return nil, err
		}
		if skip {
			continue
		}

		accountChanges = append(accountChanges, accChange)
	}

	return accountChanges, nil
}

// buildAccountChange converts a ledger change to an AccountChange, stamped with the
// given ledger sequence and OperationID. Returns (change, skip, error) where
// skip=true means the change should be ignored.
func (p *AccountsProcessor) buildAccountChange(change ingest.Change, ledgerSeq uint32, operationID int64) (types.AccountChange, bool, error) {
	var accChange types.AccountChange

	// Skip if only signers changed (no balance/state change)
	changed, err := change.AccountChangedExceptSigners()
	if err != nil {
		return accChange, false, fmt.Errorf("checking account changes: %w", err)
	}
	if !changed {
		return accChange, true, nil
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
		return accChange, true, nil // Invalid change, skip
	}

	account := entry.Data.MustAccount()
	liabilities := account.Liabilities()

	// Build the AccountChange
	accChange.AccountID = account.AccountId.Address()
	accChange.OperationID = operationID
	accChange.LedgerNumber = ledgerSeq
	accChange.Balance = int64(account.Balance)
	accChange.BuyingLiabilities = int64(liabilities.Buying)
	accChange.SellingLiabilities = int64(liabilities.Selling)

	// Calculate the minimum balance for base reserves: https://developers.stellar.org/docs/build/guides/transactions/sponsored-reserves#effect-on-minimum-balance
	numSubEntries := account.NumSubEntries
	numSponsoring := account.NumSponsoring()
	numSponsored := account.NumSponsored()
	accChange.MinimumBalance = int64(MinimumBaseReserveCount+numSubEntries+numSponsoring-numSponsored)*BaseReserveStroops + accChange.SellingLiabilities

	return accChange, false, nil
}
