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
)

const (
	MinimumBaseReserveCount = 2
	BaseReserveStroops      = 5_000_000
)

// AccountsProcessor processes ledger changes to extract account balance modifications.
type AccountsProcessor struct {
	networkPassphrase string
	metricsService    MetricsServiceInterface
}

// NewAccountsProcessor creates a new accounts processor.
func NewAccountsProcessor(metricsService MetricsServiceInterface) *AccountsProcessor {
	return &AccountsProcessor{
		metricsService:    metricsService,
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
			p.metricsService.ObserveStateChangeProcessingDuration("AccountsProcessor", duration)
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

		accChange, skip, err := p.processAccountChange(change, opWrapper)
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

// processAccountChange converts a ledger change to an AccountChange.
// Returns (change, skip, error) where skip=true means the change should be ignored.
func (p *AccountsProcessor) processAccountChange(change ingest.Change, opWrapper *TransactionOperationWrapper) (types.AccountChange, bool, error) {
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
	accChange.OperationID = opWrapper.ID()
	accChange.LedgerNumber = opWrapper.Transaction.Ledger.LedgerSequence()
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
