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

		sacChange, skip, err := p.processSACBalanceChange(change, opWrapper)
		if err != nil {
			return nil, err
		}
		if skip {
			continue
		}

		sacBalanceChanges = append(sacBalanceChanges, sacChange)
	}

	return sacBalanceChanges, nil
}

// processSACBalanceChange converts a ledger change to a SACBalanceChange.
// Returns (change, skip, error) where skip=true means the change should be ignored.
func (p *SACBalancesProcessor) processSACBalanceChange(change ingest.Change, opWrapper *TransactionOperationWrapper) (types.SACBalanceChange, bool, error) {
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
		return sacChange, true, nil // Invalid change, skip
	}

	contractData := entry.Data.MustContractData()

	// Extract contract ID
	contractIDBytes, ok := contractData.Contract.GetContractId()
	if !ok {
		return sacChange, true, nil // No contract ID, skip
	}
	contractID := strkey.MustEncode(strkey.VersionByteContract, contractIDBytes[:])

	// Check if this is a SAC contract (not SEP-41)
	if !p.isSACContract(*entry) {
		return sacChange, true, nil // Not a SAC contract, skip
	}

	// Check if this is a balance entry and extract holder address
	holderAddress, ok := p.extractHolderFromBalanceKey(contractData.Key)
	if !ok {
		return sacChange, true, nil // Not a balance entry, skip
	}

	// Only process contract addresses (C...) - G-addresses use trustlines
	if !isContractAddress(holderAddress) {
		return sacChange, true, nil // G-address, skip (stored in trustlines)
	}

	// For REMOVE operations, we don't have balance data
	if sacChange.Operation == types.SACBalanceOpRemove {
		sacChange.AccountID = holderAddress
		sacChange.ContractID = contractID
		sacChange.OperationID = opWrapper.ID()
		sacChange.LedgerNumber = opWrapper.Transaction.Ledger.LedgerSequence()
		sacChange.Balance = "0"
		sacChange.IsAuthorized = false
		sacChange.IsClawbackEnabled = false
		return sacChange, false, nil
	}

	// Extract balance value from the SAC balance map
	balance, authorized, clawback, err := p.extractSACBalanceValue(contractData.Val)
	if err != nil {
		return sacChange, true, nil // Invalid balance structure, skip
	}

	sacChange.AccountID = holderAddress
	sacChange.ContractID = contractID
	sacChange.OperationID = opWrapper.ID()
	sacChange.LedgerNumber = opWrapper.Transaction.Ledger.LedgerSequence()
	sacChange.Balance = balance
	sacChange.IsAuthorized = authorized
	sacChange.IsClawbackEnabled = clawback

	return sacChange, false, nil
}

// isSACContract checks if the ledger entry is for a Stellar Asset Contract.
func (p *SACBalancesProcessor) isSACContract(entry xdr.LedgerEntry) bool {
	_, isSAC := sac.AssetFromContractData(entry, p.networkPassphrase)
	return isSAC
}

// extractHolderFromBalanceKey extracts the holder address from a balance key.
// Balance keys have format: ["Balance", holder_address]
func (p *SACBalancesProcessor) extractHolderFromBalanceKey(key xdr.ScVal) (string, bool) {
	if key.Type != xdr.ScValTypeScvVec {
		return "", false
	}

	keyVec, ok := key.GetVec()
	if !ok || keyVec == nil || len(*keyVec) != 2 {
		return "", false
	}

	// First element must be "Balance" symbol
	sym, ok := (*keyVec)[0].GetSym()
	if !ok || string(sym) != "Balance" {
		return "", false
	}

	// Second element is the holder address
	addr, ok := (*keyVec)[1].GetAddress()
	if !ok {
		return "", false
	}

	addrStr, err := addr.String()
	if err != nil {
		return "", false
	}

	return addrStr, true
}

// extractSACBalanceValue extracts balance, authorized, and clawback from a SAC balance map.
// SAC balance format: {amount: i128, authorized: bool, clawback: bool}
func (p *SACBalancesProcessor) extractSACBalanceValue(val xdr.ScVal) (balance string, authorized bool, clawback bool, err error) {
	if val.Type != xdr.ScValTypeScvMap {
		return "", false, false, fmt.Errorf("expected ScMap, got %v", val.Type)
	}

	balanceMap, ok := val.GetMap()
	if !ok || balanceMap == nil {
		return "", false, false, fmt.Errorf("failed to get balance map")
	}

	if len(*balanceMap) != 3 {
		return "", false, false, fmt.Errorf("expected 3 entries (amount, authorized, clawback), got %d", len(*balanceMap))
	}

	var amountFound, authorizedFound, clawbackFound bool

	for _, entry := range *balanceMap {
		if entry.Key.Type != xdr.ScValTypeScvSymbol {
			continue
		}

		keySymbol, ok := entry.Key.GetSym()
		if !ok {
			continue
		}

		switch string(keySymbol) {
		case "amount":
			if entry.Val.Type != xdr.ScValTypeScvI128 {
				return "", false, false, fmt.Errorf("amount is not i128")
			}
			i128Parts := entry.Val.MustI128()
			balance = amount.String128(i128Parts)
			amountFound = true

		case "authorized":
			if entry.Val.Type != xdr.ScValTypeScvBool {
				return "", false, false, fmt.Errorf("authorized is not bool")
			}
			authorized = entry.Val.MustB()
			authorizedFound = true

		case "clawback":
			if entry.Val.Type != xdr.ScValTypeScvBool {
				return "", false, false, fmt.Errorf("clawback is not bool")
			}
			clawback = entry.Val.MustB()
			clawbackFound = true
		}
	}

	if !amountFound || !authorizedFound || !clawbackFound {
		return "", false, false, fmt.Errorf("missing required fields in balance map")
	}

	return balance, authorized, clawback, nil
}

// isContractAddress determines if the given address is a contract address (C...) or account address (G...)
func isContractAddress(address string) bool {
	return len(address) > 0 && address[0] == 'C'
}
