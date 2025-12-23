// Package resolvers provides utility functions for parsing Stellar ledger entries into GraphQL balance types.
// These functions support both single-account and multi-account balance queries.
package resolvers

import (
	"fmt"

	"github.com/stellar/go/amount"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/xdr"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	graphql1 "github.com/stellar/wallet-backend/internal/serve/graphql/generated"
)

// parseNativeBalance extracts native XLM balance from an account entry.
func parseNativeBalance(accountEntry xdr.AccountEntry, networkPassphrase string) (*graphql1.NativeBalance, error) {
	balanceStr := amount.String(accountEntry.Balance)

	// Get native asset contract ID
	nativeAsset := xdr.MustNewNativeAsset()
	contractID, err := nativeAsset.ContractID(networkPassphrase)
	if err != nil {
		return nil, fmt.Errorf("getting contract ID for native asset: %w", err)
	}
	tokenID := strkey.MustEncode(strkey.VersionByteContract, contractID[:])

	return &graphql1.NativeBalance{
		TokenID:   tokenID,
		Balance:   balanceStr,
		TokenType: graphql1.TokenTypeNative,
	}, nil
}

// parseTrustlineBalance extracts trustline balance from a trustline entry.
func parseTrustlineBalance(trustlineEntry xdr.TrustLineEntry, lastModifiedLedger uint32, networkPassphrase string) (*graphql1.TrustlineBalance, error) {
	balanceStr := amount.String(trustlineEntry.Balance)

	var assetType, assetCode, assetIssuer string
	asset := trustlineEntry.Asset.ToAsset()
	if err := asset.Extract(&assetType, &assetCode, &assetIssuer); err != nil {
		return nil, fmt.Errorf("extracting asset info: %w", err)
	}

	contractID, err := asset.ContractID(networkPassphrase)
	if err != nil {
		return nil, fmt.Errorf("getting contract ID for asset: %w", err)
	}
	tokenID := strkey.MustEncode(strkey.VersionByteContract, contractID[:])

	// Extract limit
	limitStr := amount.String(trustlineEntry.Limit)

	// Extract liabilities (V1 extension)
	var buyingLiabilities, sellingLiabilities string
	if trustlineEntry.Ext.V == 1 && trustlineEntry.Ext.V1 != nil {
		buyingLiabilities = amount.String(trustlineEntry.Ext.V1.Liabilities.Buying)
		sellingLiabilities = amount.String(trustlineEntry.Ext.V1.Liabilities.Selling)
	} else {
		buyingLiabilities = "0.0000000"
		sellingLiabilities = "0.0000000"
	}

	// Extract authorization flags
	flags := uint32(trustlineEntry.Flags)
	isAuthorized := (flags & uint32(xdr.TrustLineFlagsAuthorizedFlag)) != 0
	isAuthorizedToMaintainLiabilities := (flags & uint32(xdr.TrustLineFlagsAuthorizedToMaintainLiabilitiesFlag)) != 0

	return &graphql1.TrustlineBalance{
		TokenID:                           tokenID,
		Balance:                           balanceStr,
		TokenType:                         graphql1.TokenTypeClassic,
		Code:                              assetCode,
		Issuer:                            assetIssuer,
		Type:                              assetType,
		Limit:                             limitStr,
		BuyingLiabilities:                 buyingLiabilities,
		SellingLiabilities:                sellingLiabilities,
		LastModifiedLedger:                int32(lastModifiedLedger),
		IsAuthorized:                      isAuthorized,
		IsAuthorizedToMaintainLiabilities: isAuthorizedToMaintainLiabilities,
	}, nil
}

// parseContractIDFromContractData extracts the contract ID string from a contract data entry.
// Returns the contract ID string, a boolean indicating if extraction was successful, and any error.
func parseContractIDFromContractData(contractDataEntry *xdr.ContractDataEntry) (string, bool, error) {
	if contractDataEntry.Contract.ContractId == nil {
		return "", false, nil
	}

	contractID, ok := contractDataEntry.Contract.GetContractId()
	if !ok {
		return "", false, nil
	}

	contractIDStr, err := strkey.Encode(strkey.VersionByteContract, contractID[:])
	if err != nil {
		return "", false, fmt.Errorf("encoding contract ID: %w", err)
	}

	return contractIDStr, true, nil
}

// parseSACBalance extracts SAC (Stellar Asset Contract) balance from a contract data entry.
func parseSACBalance(contractDataEntry *xdr.ContractDataEntry, contractIDStr string, contract *data.Contract) (*graphql1.SACBalance, error) {
	if contractDataEntry.Val.Type != xdr.ScValTypeScvMap {
		return nil, fmt.Errorf("SAC balance expected to be map, got: %v", contractDataEntry.Val.Type)
	}

	balanceMap := contractDataEntry.Val.MustMap()
	if balanceMap == nil {
		return nil, fmt.Errorf("balance map is nil")
	}

	// Extract amount, authorized, and clawback from map
	var balanceStr string
	var isAuthorized, isClawbackEnabled bool
	var amountFound, authorizedFound, clawbackFound bool

	for _, entry := range *balanceMap {
		if entry.Key.Type == xdr.ScValTypeScvSymbol {
			keySymbol := string(entry.Key.MustSym())
			switch keySymbol {
			case "amount":
				if entry.Val.Type != xdr.ScValTypeScvI128 {
					return nil, fmt.Errorf("amount field is not i128, got: %v", entry.Val.Type)
				}
				i128Parts := entry.Val.MustI128()
				balanceStr = amount.String128(i128Parts)
				amountFound = true
			case "authorized":
				if entry.Val.Type != xdr.ScValTypeScvBool {
					return nil, fmt.Errorf("authorized field is not bool, got: %v", entry.Val.Type)
				}
				isAuthorized = entry.Val.MustB()
				authorizedFound = true
			case "clawback":
				if entry.Val.Type != xdr.ScValTypeScvBool {
					return nil, fmt.Errorf("clawback field is not bool, got: %v", entry.Val.Type)
				}
				isClawbackEnabled = entry.Val.MustB()
				clawbackFound = true
			}
		}
	}

	if !amountFound {
		return nil, fmt.Errorf("amount field not found in SAC balance map")
	}
	if !authorizedFound {
		return nil, fmt.Errorf("authorized field not found in SAC balance map")
	}
	if !clawbackFound {
		return nil, fmt.Errorf("clawback field not found in SAC balance map")
	}

	return &graphql1.SACBalance{
		TokenID:           contractIDStr,
		Balance:           balanceStr,
		TokenType:         graphql1.TokenTypeSac,
		Code:              *contract.Code,
		Issuer:            *contract.Issuer,
		Decimals:          int32(contract.Decimals),
		IsAuthorized:      isAuthorized,
		IsClawbackEnabled: isClawbackEnabled,
	}, nil
}

// parseSEP41Balance extracts SEP-41 token balance from a contract data entry.
func parseSEP41Balance(contractDataEntry *xdr.ContractDataEntry, contractIDStr string, contract *data.Contract) (*graphql1.SEP41Balance, error) {
	if contractDataEntry.Val.Type != xdr.ScValTypeScvI128 {
		return nil, fmt.Errorf("SEP-41 balance must be i128, got: %v", contractDataEntry.Val.Type)
	}

	i128Parts := contractDataEntry.Val.MustI128()
	balanceStr := amount.String128(i128Parts)

	return &graphql1.SEP41Balance{
		TokenID:   contractIDStr,
		Balance:   balanceStr,
		TokenType: graphql1.TokenTypeSep41,
		Name:      *contract.Name,
		Symbol:    *contract.Symbol,
		Decimals:  int32(contract.Decimals),
	}, nil
}

// parseContractBalance parses a contract data entry and returns the appropriate balance type.
// Returns nil if the contract type is unknown or the contract is not found.
func parseContractBalance(contractDataEntry *xdr.ContractDataEntry, contractIDStr string, contract *data.Contract) (graphql1.Balance, error) {
	switch contract.Type {
	case string(types.ContractTypeSAC):
		return parseSACBalance(contractDataEntry, contractIDStr, contract)
	case string(types.ContractTypeSEP41):
		return parseSEP41Balance(contractDataEntry, contractIDStr, contract)
	default:
		return nil, nil
	}
}

// parseAccountBalances parses ledger entries for a single account and returns balances.
// This is used by the multi-account balance resolver.
func parseAccountBalances(info *accountKeyInfo, ledgerEntriesByLedgerKeys map[string]*entities.LedgerEntryResult, networkPassphrase string) ([]graphql1.Balance, error) {
	var balances []graphql1.Balance
	for _, ledgerKey := range info.ledgerKeys {
		entry, exists := ledgerEntriesByLedgerKeys[ledgerKey]
		if !exists || entry == nil {
			continue
		}

		var ledgerEntryData xdr.LedgerEntryData
		if err := xdr.SafeUnmarshalBase64(entry.DataXDR, &ledgerEntryData); err != nil {
			return nil, fmt.Errorf("decoding ledger entry: %w", err)
		}

		//exhaustive:ignore
		switch ledgerEntryData.Type {
		case xdr.LedgerEntryTypeAccount:
			accountEntry := ledgerEntryData.MustAccount()
			nativeBalance, err := parseNativeBalance(accountEntry, networkPassphrase)
			if err != nil {
				return nil, err
			}
			balances = append(balances, nativeBalance)

		case xdr.LedgerEntryTypeTrustline:
			trustlineEntry := ledgerEntryData.MustTrustLine()
			trustlineBalance, err := parseTrustlineBalance(trustlineEntry, entry.LastModifiedLedger, networkPassphrase)
			if err != nil {
				return nil, err
			}
			balances = append(balances, trustlineBalance)

		case xdr.LedgerEntryTypeContractData:
			contractDataEntry := ledgerEntryData.MustContractData()

			contractIDStr, ok, err := parseContractIDFromContractData(&contractDataEntry)
			if err != nil {
				return nil, err
			}
			if !ok {
				continue
			}

			contract, exists := info.contractsByID[contractIDStr]
			if !exists {
				continue
			}

			balance, err := parseContractBalance(&contractDataEntry, contractIDStr, contract)
			if err != nil {
				return nil, err
			}
			if balance != nil {
				balances = append(balances, balance)
			}
		}
	}

	return balances, nil
}
