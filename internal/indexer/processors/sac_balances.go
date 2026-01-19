// SACBalancesProcessor extracts SAC balance changes from contract data ledger entries.
// It follows the same pattern as TrustlinesProcessor, capturing ABSOLUTE balance values from change.Post.
package processors

import (
	"context"
	"fmt"

	"github.com/stellar/go-stellar-sdk/amount"
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/ingest/sac"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/wallet-backend/internal/indexer/types"
)

// SAC balance map field names as defined by the Stellar Asset Contract specification.
const (
	sacBalanceFieldAmount     = "amount"
	sacBalanceFieldAuthorized = "authorized"
	sacBalanceFieldClawback   = "clawback"
)

// SACBalancesProcessor processes ledger changes to extract SAC balance modifications.
// SAC balances are stored in contract data entries with a defined format:
// Key: ["Balance", holder_address]
// Value: {amount: i128, authorized: bool, clawback: bool}
type SACBalancesProcessor struct {
	networkPassphrase string
	metricsService    MetricsServiceInterface
}

// NewSACBalancesProcessor creates a new SAC balances processor.
func NewSACBalancesProcessor(networkPassphrase string, metricsService MetricsServiceInterface) *SACBalancesProcessor {
	return &SACBalancesProcessor{
		networkPassphrase: networkPassphrase,
		metricsService:    metricsService,
	}
}

// Name returns the processor name for logging and metrics.
func (p *SACBalancesProcessor) Name() string {
	return "sac_balances"
}

// ProcessOperation extracts SAC balance changes from an operation's ledger changes.
// Returns SACBalanceChange structs with absolute balance values for database upsert.
// Only processes changes for contract addresses (C...) since G-addresses use trustlines.
func (p *SACBalancesProcessor) ProcessOperation(ctx context.Context, opWrapper *TransactionOperationWrapper) ([]types.SACBalanceChange, error) {
	changes, err := opWrapper.Transaction.GetOperationChanges(opWrapper.Index)
	if err != nil {
		return nil, fmt.Errorf("getting operation changes: %w", err)
	}

	var sacBalanceChanges []types.SACBalanceChange

	for _, change := range changes {
		if change.Type != xdr.LedgerEntryTypeContractData {
			continue
		}

		sacChange, skip := p.processSACBalanceChange(change, opWrapper)
		if skip {
			continue
		}

		sacBalanceChanges = append(sacBalanceChanges, sacChange)
	}

	return sacBalanceChanges, nil
}

// processSACBalanceChange converts a ledger change to a SACBalanceChange.
// Returns (change, skip) where skip=true means the change should be ignored.
// Uses sac.ContractBalanceFromContractData from the SDK for validation:
// - Validates entry is a SAC balance entry (key = ["Balance", address], value = {amount, authorized, clawback})
// - Only returns true for contract address holders (C...), automatically skips G-addresses
// - Validates it's not the native asset contract
func (p *SACBalancesProcessor) processSACBalanceChange(change ingest.Change, opWrapper *TransactionOperationWrapper) (types.SACBalanceChange, bool) {
	var sacChange types.SACBalanceChange

	// Determine operation type and get the relevant entry
	var entry *xdr.LedgerEntry
	switch {
	case change.Pre == nil && change.Post != nil:
		// Created
		sacChange.Operation = types.SACBalanceOpAdd
		entry = change.Post
	case change.Pre != nil && change.Post != nil:
		// Updated
		sacChange.Operation = types.SACBalanceOpUpdate
		entry = change.Post
	case change.Pre != nil && change.Post == nil:
		// Removed
		sacChange.Operation = types.SACBalanceOpRemove
		entry = change.Pre
	default:
		return sacChange, true // Invalid change, skip
	}

	// Use SDK to validate and extract SAC balance data
	// ContractBalanceFromContractData validates:
	// - Entry is a SAC balance entry (key = ["Balance", address], value = {amount, authorized, clawback})
	// - Holder is a contract address (C...) - automatically skips G-addresses
	// - Contract is not the native asset contract
	holderBytes, _, ok := sac.ContractBalanceFromContractData(*entry, p.networkPassphrase)
	if !ok {
		return sacChange, true // Not a valid SAC balance entry or G-address holder
	}

	// Convert holder bytes to strkey
	holderAddress := strkey.MustEncode(strkey.VersionByteContract, holderBytes[:])

	// Extract contract ID
	contractData := entry.Data.MustContractData()
	contractIDBytes, ok := contractData.Contract.GetContractId()
	if !ok {
		return sacChange, true // No contract ID, skip
	}
	contractID := strkey.MustEncode(strkey.VersionByteContract, contractIDBytes[:])

	sacChange.AccountID = holderAddress
	sacChange.ContractID = contractID
	sacChange.OperationID = opWrapper.ID()
	sacChange.LedgerNumber = opWrapper.Transaction.Ledger.LedgerSequence()

	// For REMOVE operations, set defaults
	if sacChange.Operation == types.SACBalanceOpRemove {
		sacChange.Balance = "0"
		sacChange.IsAuthorized = false
		sacChange.IsClawbackEnabled = false
		return sacChange, false
	}

	// Extract balance value from the SAC balance map
	// SDK validates structure but doesn't return all fields, so we extract them here
	balance, authorized, clawback := p.extractSACBalanceFields(contractData.Val)
	sacChange.Balance = balance
	sacChange.IsAuthorized = authorized
	sacChange.IsClawbackEnabled = clawback

	return sacChange, false
}

// extractSACBalanceFields extracts balance, authorized, and clawback from a SAC balance map.
// Assumes ContractBalanceFromContractData already validated the structure.
// SAC balance format: {amount: i128, authorized: bool, clawback: bool}
func (p *SACBalancesProcessor) extractSACBalanceFields(val xdr.ScVal) (balance string, authorized bool, clawback bool) {
	balanceMap, ok := val.GetMap()
	if !ok || balanceMap == nil {
		return "0", false, false
	}

	for _, entry := range *balanceMap {
		keySymbol, ok := entry.Key.GetSym()
		if !ok {
			continue
		}

		switch string(keySymbol) {
		case sacBalanceFieldAmount:
			if i128, ok := entry.Val.GetI128(); ok {
				balance = amount.String128(i128)
			}
		case sacBalanceFieldAuthorized:
			if b, ok := entry.Val.GetB(); ok {
				authorized = b
			}
		case sacBalanceFieldClawback:
			if b, ok := entry.Val.GetB(); ok {
				clawback = b
			}
		}
	}

	// Ensure balance has a valid value (defaults to "0" if not found)
	if balance == "" {
		balance = "0"
	}

	return balance, authorized, clawback
}
