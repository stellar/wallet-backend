package contracts

import (
	"context"
	"fmt"
	"errors"
	operation_processor "github.com/stellar/go/processors/operation"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"

	"github.com/stellar/go/ingest"

	"github.com/stellar/wallet-backend/internal/indexer/processors"
	"github.com/stellar/wallet-backend/internal/indexer/types"
)

var (
	errNoContractDataChangeFound = errors.New("no contract data change found")
	errNoAuthorizedKeyFound     = errors.New("authorized key not found")
)


const (
	setAuthorizedFunctionName              = "set_authorized"
	AuthorizedFlagName                     = "authorized"
	AuthorizedToMaintainLiabilitesFlagName = "authorized_to_maintain_liabilites"
	txMetaVersionV3                        = 3
	txMetaVersionV4                        = 4
)

type SACEventsProcessor struct {
	networkPassphrase string
}

func NewSACEventsProcessor(networkPassphrase string) *SACEventsProcessor {
	return &SACEventsProcessor{
		networkPassphrase: networkPassphrase,
	}
}

func (p *SACEventsProcessor) Name() string {
	return "sac"
}

// ProcessOperation processes contract events and converts them into state changes.
func (p *SACEventsProcessor) ProcessOperation(_ context.Context, opWrapper *operation_processor.TransactionOperationWrapper) ([]types.StateChange, error) {
	if opWrapper.OperationType() != xdr.OperationTypeInvokeHostFunction {
		return nil, processors.ErrInvalidOpType
	}

	ledgerCloseTime := opWrapper.Transaction.Ledger.LedgerCloseTime()
	ledgerNumber := opWrapper.Transaction.Ledger.LedgerSequence()
	txHash := opWrapper.Transaction.Result.TransactionHash.HexString()
	txID := opWrapper.Transaction.ID()

	tx := opWrapper.Transaction
	contractEvents, err := tx.GetContractEventsForOperation(opWrapper.Index)
	if err != nil {
		return nil, fmt.Errorf("getting contract events for operation %d: %w", opWrapper.ID(), err)
	}

	// Get operation changes to access previous trustline flag state
	changes, err := tx.GetOperationChanges(opWrapper.Index)
	if err != nil {
		return nil, fmt.Errorf("getting operation changes for operation %d: %w", opWrapper.ID(), err)
	}

	stateChanges := make([]types.StateChange, 0)
	builder := processors.NewStateChangeBuilder(ledgerNumber, ledgerCloseTime, txHash, txID).WithOperationID(opWrapper.ID())
	for _, event := range contractEvents {
		// Validate basic contract contractEvent structure
		if event.Type != xdr.ContractEventTypeContract || event.ContractId == nil || event.Body.V != 0 {
			log.Debugf("processor: %s: skipping event with invalid contract structure: txHash=%s opID=%d eventType=%d contractId=%v bodyV=%d",
				p.Name(), txHash, opWrapper.ID(), event.Type, event.ContractId != nil, event.Body.V)
			continue
		}

		// Validate if number of topics matches the expected number of topics for an SAC set_authorized event
		topics := event.Body.V0.Topics
		if !p.validateExpectedTopicsForSAC(len(topics), tx.UnsafeMeta.V) {
			contractID := strkey.MustEncode(strkey.VersionByteContract, event.ContractId[:])
			log.Debugf("processor: %s: skipping event with invalid topic count for SAC: txHash=%s opID=%d contractId=%s topicCount=%d metaVersion=%d",
				p.Name(), txHash, opWrapper.ID(), contractID, len(topics), tx.UnsafeMeta.V)
			continue
		}

		fn, ok := topics[0].GetSym()
		if !ok {
			contractID := strkey.MustEncode(strkey.VersionByteContract, event.ContractId[:])
			log.Debugf("processor: %s: skipping event with non-symbol function name: txHash=%s opID=%d contractId=%s",
				p.Name(), txHash, opWrapper.ID(), contractID)
			continue
		}

		switch string(fn) {
		case setAuthorizedFunctionName:
			contractID := strkey.MustEncode(strkey.VersionByteContract, event.ContractId[:])
			isSAC, err := p.isSACContract(topics, contractID, tx.UnsafeMeta.V)
			if err != nil {
				log.Debugf("processor: %s: skipping event due to SAC contract validation failure: txHash=%s opID=%d contractId=%s error=%v",
					p.Name(), txHash, opWrapper.ID(), contractID, err)
				continue
			}
			if !isSAC {
				log.Debugf("processor: %s: skipping event due to non-SAC contract: txHash=%s opID=%d contractId=%s",
					p.Name(), txHash, opWrapper.ID(), contractID)
				continue
			}

			accountToAuthorize, err := p.extractAccount(topics, tx.UnsafeMeta.V)
			if err != nil {
				log.Debugf("processor: %s: skipping event due to account extraction failure: txHash=%s opID=%d contractId=%s error=%v",
					p.Name(), txHash, opWrapper.ID(), contractID, err)
				continue
			}

			value := event.Body.V0.Data
			isAuthorized, ok := value.GetB()
			if !ok {
				log.Debugf("processor: %s: skipping event with non-boolean authorization value: txHash=%s opID=%d contractId=%s",
					p.Name(), txHash, opWrapper.ID(), contractID)
				continue
			}

			/*
				Extract previous authorization state based on address type.

				Stellar Asset Contracts (SAC) handle authorization differently for classic accounts vs contract accounts:

				1. Classic Accounts (G...):
				   - Authorization is managed through trustline flags in the Stellar ledger
				   - Two relevant flags: AUTHORIZED and AUTHORIZED_TO_MAINTAIN_LIABILITIES
				   - When authorizing: set AUTHORIZED, clear AUTHORIZED_TO_MAINTAIN_LIABILITIES
				   - When deauthorizing: clear AUTHORIZED, set AUTHORIZED_TO_MAINTAIN_LIABILITIES
				   - This allows existing offers and liquidity pool shares to be maintained during deauth

				2. Contract Accounts (C...):
				   - Authorization is stored in the contract's BalanceValue struct
				   - BalanceValue contains: {amount: i128, authorized: bool, clawback: bool}
				   - Only the 'authorized' boolean flag is relevant for authorization
			*/
			var wasAuthorized, wasMaintainLiabilities bool
			if isContractAddress(accountToAuthorize) {
				// For contract addresses, check contract data changes for BalanceValue authorization
				wasAuthorized, err = p.extractContractAuthorizationChanges(changes, accountToAuthorize)
				if err != nil {
					log.Debugf("processor: %s: skipping event due to contract authorization extraction failure: txHash=%s opID=%d contractId=%s contractAddress=%s error=%v",
						p.Name(), txHash, opWrapper.ID(), contractID, accountToAuthorize, err)
					continue
				}
			} else {
				// For classic account addresses, check trustline flag changes
				wasAuthorized, wasMaintainLiabilities, err = p.extractTrustlineFlagChanges(changes, accountToAuthorize)
				if err != nil {
					log.Debugf("processor: %s: skipping event due to trustline flag extraction failure: txHash=%s opID=%d contractId=%s accountAddress=%s error=%v",
						p.Name(), txHash, opWrapper.ID(), contractID, accountToAuthorize, err)
					continue
				}
			}

			scBuilder := builder.WithCategory(types.StateChangeCategoryBalanceAuthorization).WithAccount(accountToAuthorize).WithToken(contractID)
			var flagChanges []types.StateChange

			/*
				Generate state changes based on actual flag transitions:
				- For classic accounts: handle trustline flag changes: AUTHORIZED and AUTHORIZED_TO_MAINTAIN_LIABILITIES
				- For contracts: only handle AUTHORIZED (contract accounts dont have trustlines)

				Only generate state changes for cases when the authorization state actually changed from its previous state.
			*/
			if isContractAddress(accountToAuthorize) {
				// Contract authorization: handle AUTHORIZED state
				if isAuthorized != wasAuthorized {
					if isAuthorized {
						flagChanges = append(flagChanges, scBuilder.Clone().
							WithReason(types.StateChangeReasonSet).
							Build())
					} else {
						flagChanges = append(flagChanges, scBuilder.Clone().
							WithReason(types.StateChangeReasonClear).
							Build())
					}
				}
			} else {
				// Account authorization: handle both flags as before
				if isAuthorized {
					// Authorizing: should set AUTHORIZED_FLAG if it wasn't already set
					if !wasAuthorized {
						flagChanges = append(flagChanges, scBuilder.Clone().
							WithReason(types.StateChangeReasonSet).
							WithFlags([]string{AuthorizedFlagName}).
							Build())
					}
					// Should clear AUTHORIZED_TO_MAINTAIN_LIABILITIES_FLAG if it was previously set
					if wasMaintainLiabilities {
						flagChanges = append(flagChanges, scBuilder.Clone().
							WithReason(types.StateChangeReasonClear).
							WithFlags([]string{AuthorizedToMaintainLiabilitesFlagName}).
							Build())
					}
				} else {
					// Deauthorizing: should clear AUTHORIZED_FLAG if it was previously set
					if wasAuthorized {
						flagChanges = append(flagChanges, scBuilder.Clone().
							WithReason(types.StateChangeReasonClear).
							WithFlags([]string{AuthorizedFlagName}).
							Build())
					}
					// Should set AUTHORIZED_TO_MAINTAIN_LIABILITIES_FLAG if it wasn't already set
					if !wasMaintainLiabilities {
						flagChanges = append(flagChanges, scBuilder.Clone().
							WithReason(types.StateChangeReasonSet).
							WithFlags([]string{AuthorizedToMaintainLiabilitesFlagName}).
							Build())
					}
				}
			}

			log.Debugf("processor: %s: generated %d state changes for address %s: txHash=%s opID=%d",
				p.Name(), len(flagChanges), accountToAuthorize, txHash, opWrapper.ID())
			stateChanges = append(stateChanges, flagChanges...)
		default:
			continue
		}
	}
	return stateChanges, nil
}

// validateExpectedTopicsForSAC validates the expected number of topics for a set_authorized event
func (p *SACEventsProcessor) validateExpectedTopicsForSAC(numTopics int, txMetaVersion int32) bool {
	/*
		For meta V3, a set_authorized event will have 4 topics: ["set_authorized", admin: Address, id: Address, sep0011_asset: String]
		For meta V4, a set_authorized event will have 3 topics: ["set_authorized", id: Address, sep0011_asset: String]
	*/
	switch txMetaVersion {
	case txMetaVersionV3:
		return numTopics == 4
	case txMetaVersionV4:
		return numTopics == 3
	default:
		return false
	}
}

// extractAccountAndAsset extracts the account and asset from the topics
func (p *SACEventsProcessor) extractAccount(topics []xdr.ScVal, txMetaVersion int32) (string, error) {
	var accountIdx int
	switch txMetaVersion {
	case txMetaVersionV3:
		accountIdx = 2
	case txMetaVersionV4:
		accountIdx = 1
	}
	account, err := extractAddressFromScVal(topics[accountIdx])
	if err != nil {
		return "", fmt.Errorf("invalid account: %w", err)
	}
	return account, nil
}

// isSACContract checks if a contract is an SAC contract
func (p *SACEventsProcessor) isSACContract(topics []xdr.ScVal, contractID string, txMetaVersion int32) (bool, error) {
	var assetIdx int
	switch txMetaVersion {
	case txMetaVersionV3:
		assetIdx = 3
	case txMetaVersionV4:
		assetIdx = 2
	}
	asset, err := extractAssetFromScVal(topics[assetIdx])
	if err != nil {
		return false, fmt.Errorf("invalid asset: %w", err)
	}
	assetContractID, err := asset.ContractID(p.networkPassphrase)
	if err != nil {
		return false, fmt.Errorf("invalid asset contract ID: %w", err)
	}
	return strkey.MustEncode(strkey.VersionByteContract, assetContractID[:]) == contractID, nil
}

// extractTrustlineFlagChanges extracts the previous trustline flags for the given account
// from the operation changes to determine what flags were set before the SAC operation
func (p *SACEventsProcessor) extractTrustlineFlagChanges(changes []ingest.Change, accountToAuthorize string) (wasAuthorized bool, wasMaintainLiabilities bool, err error) {
	for _, change := range changes {
		if change.Type != xdr.LedgerEntryTypeTrustline {
			continue
		}

		if change.Pre == nil {
			continue
		}

		prevTrustline := change.Pre.Data.MustTrustLine()
		trustlineAccount := prevTrustline.AccountId.Address()

		if trustlineAccount == accountToAuthorize {
			prevFlags := uint32(prevTrustline.Flags)
			wasAuthorized = (prevFlags & uint32(xdr.TrustLineFlagsAuthorizedFlag)) != 0
			wasMaintainLiabilities = (prevFlags & uint32(xdr.TrustLineFlagsAuthorizedToMaintainLiabilitiesFlag)) != 0
			return wasAuthorized, wasMaintainLiabilities, nil
		}
	}

	return false, false, fmt.Errorf("trustline change not found for account %s", accountToAuthorize)
}

// isContractAddress determines if the given address is a contract address (C...) or account address (G...)
func isContractAddress(address string) bool {
	// Contract addresses start with 'C' and account addresses start with 'G'
	return len(address) > 0 && address[0] == 'C'
}

// extractContractAuthorizationChanges extracts the previous authorization state for a contract address
// from the operation changes to determine what flags were set before the SAC operation
func (p *SACEventsProcessor) extractContractAuthorizationChanges(changes []ingest.Change, contractAddress string) (wasAuthorized bool, err error) {
	for _, change := range changes {
		if change.Type != xdr.LedgerEntryTypeContractData {
			continue
		}

		if change.Pre == nil {
			continue
		}

		prevContractData := change.Pre.Data.MustContractData()

		// Check if this is a balance entry by examining the key structure
		// Balance keys are vectors: ["Balance", contractAddress]
		if prevContractData.Key.Type == xdr.ScValTypeScvVec {
			keyVec, ok := prevContractData.Key.GetVec()
			if !ok || keyVec == nil || len(*keyVec) != 2 {
				continue
			}

			// Check if first element is "Balance" symbol
			balanceSymbol, ok := (*keyVec)[0].GetSym()
			if !ok || string(balanceSymbol) != "Balance" {
				continue
			}

			// Check if second element is our contract address
			keyAddr, err := extractAddressFromScVal((*keyVec)[1])
			if err != nil {
				continue // Skip if we can't extract the address
			}

			if keyAddr == contractAddress {
				// This is a balance entry for our contract address
				// Extract authorization flags from the balance data
				authorized, err := p.extractAuthorizedFromBalanceMap(prevContractData.Val)
				if err != nil {
					continue // Skip if we can't parse the balance value
				}
				return authorized, nil
			}
		}
	}

	return false, errNoContractDataChangeFound
}

// extractAuthorizedFromBalanceMap extracts the authorized flag from a SAC BalanceValue ScVal
// The balance value is a map with: {"amount": Int128, "authorized": Bool, "clawback": Bool}
// We only need the 'authorized' flag, but validate the map structure matches the BalanceValue definition
// from stellar-core: https://github.com/stellar/stellar-core/blob/master/src/rust/soroban/.../storage_types.rs#L24-L28
func (p *SACEventsProcessor) extractAuthorizedFromBalanceMap(balanceVal xdr.ScVal) (authorized bool, err error) {
	// Balance data must be stored as a map
	if balanceVal.Type != xdr.ScValTypeScvMap {
		return false, fmt.Errorf("expected ScMap for balance value, got %v", balanceVal.Type)
	}

	balanceMap, ok := balanceVal.GetMap()
	if !ok || balanceMap == nil {
		return false, fmt.Errorf("failed to extract map from balance value")
	}

	if len(*balanceMap) != 3 {
		return false, fmt.Errorf("invalid balance map structure: expected 3 entries (amount, authorized, clawback), got %d", len(*balanceMap))
	}

	// Find and extract the 'authorized' field using key-based lookup
	for _, entry := range *balanceMap {
		if entry.Key.Type != xdr.ScValTypeScvSymbol {
			continue
		}

		keySymbol, ok := entry.Key.GetSym()
		if !ok {
			continue
		}

		switch string(keySymbol) {
		case "authorized":
			if entry.Val.Type != xdr.ScValTypeScvBool {
				return false, fmt.Errorf("balance authorized value is not a boolean: %v", entry.Val.Type)
			}
			if authVal, ok := entry.Val.GetB(); ok {
				return authVal, nil
			}
			return false, fmt.Errorf("failed to extract boolean value from authorized field")
		default:
			continue
		}
	}

	return false, errNoAuthorizedKeyFound
}
