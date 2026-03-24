// Package resolvers provides utility functions for parsing Stellar ledger entries into GraphQL balance types.
// These functions support both single-account and multi-account balance queries.
package resolvers

import (
	"fmt"

	"github.com/stellar/go-stellar-sdk/amount"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/wallet-backend/internal/data"
	graphql1 "github.com/stellar/wallet-backend/internal/serve/graphql/generated"
)

// accountKeyInfo tracks ledger key information for a single account during multi-account balance fetch
type accountKeyInfo struct {
	address       string
	isContract    bool
	nativeBalance *data.NativeBalance     // Native XLM balance from DB
	trustlines    []data.TrustlineBalance // Full trustline balance data from DB
	sacBalances   []data.SACBalance       // SAC balances from DB (for contract addresses)
	collectionErr error                   // error during data collection phase
}

// buildNativeBalanceFromDB constructs a NativeBalance from database native balance data.
func buildNativeBalanceFromDB(nativeBalance *data.NativeBalance, networkPassphrase string) (*graphql1.NativeBalance, error) {
	// Get native asset contract ID
	nativeAsset := xdr.MustNewNativeAsset()
	contractID, err := nativeAsset.ContractID(networkPassphrase)
	if err != nil {
		return nil, fmt.Errorf("getting contract ID for native asset: %w", err)
	}
	tokenID := strkey.MustEncode(strkey.VersionByteContract, contractID[:])

	// Convert int64 balance to string format (stroops to decimal)
	balanceStr := amount.StringFromInt64(nativeBalance.Balance)
	minimumBalanceStr := amount.StringFromInt64(nativeBalance.MinimumBalance)
	buyingLiabilitiesStr := amount.StringFromInt64(nativeBalance.BuyingLiabilities)
	sellingLiabilitiesStr := amount.StringFromInt64(nativeBalance.SellingLiabilities)

	return &graphql1.NativeBalance{
		TokenID:            tokenID,
		Balance:            balanceStr,
		TokenType:          graphql1.TokenTypeNative,
		MinimumBalance:     minimumBalanceStr,
		BuyingLiabilities:  buyingLiabilitiesStr,
		SellingLiabilities: sellingLiabilitiesStr,
		LastModifiedLedger: nativeBalance.LedgerNumber,
	}, nil
}

// buildSACBalanceFromDB constructs a SACBalance from database SAC balance data.
// Uses embedded contract metadata from the JOIN with contract_tokens.
func buildSACBalanceFromDB(sacBalance data.SACBalance) *graphql1.SACBalance {
	return &graphql1.SACBalance{
		TokenID:           sacBalance.TokenID,
		Balance:           sacBalance.Balance,
		TokenType:         graphql1.TokenTypeSac,
		Code:              sacBalance.Code,
		Issuer:            sacBalance.Issuer,
		Decimals:          int32(sacBalance.Decimals),
		IsAuthorized:      sacBalance.IsAuthorized,
		IsClawbackEnabled: sacBalance.IsClawbackEnabled,
	}
}

// buildTrustlineBalanceFromDB constructs a TrustlineBalance from database trustline balance data.
func buildTrustlineBalanceFromDB(trustline data.TrustlineBalance, networkPassphrase string) (*graphql1.TrustlineBalance, error) {
	// Build xdr.Asset to compute contract ID
	asset, err := xdr.NewCreditAsset(trustline.Code, trustline.Issuer)
	if err != nil {
		return nil, fmt.Errorf("building asset from code/issuer: %w", err)
	}

	contractID, err := asset.ContractID(networkPassphrase)
	if err != nil {
		return nil, fmt.Errorf("getting contract ID for asset: %w", err)
	}
	tokenID := strkey.MustEncode(strkey.VersionByteContract, contractID[:])

	// Determine asset type string
	assetType := "credit_alphanum4"
	if len(trustline.Code) > 4 {
		assetType = "credit_alphanum12"
	}

	// Convert int64 balances to string format (stroops to decimal)
	balanceStr := amount.StringFromInt64(trustline.Balance)
	limitStr := amount.StringFromInt64(trustline.Limit)
	buyingLiabilities := amount.StringFromInt64(trustline.BuyingLiabilities)
	sellingLiabilities := amount.StringFromInt64(trustline.SellingLiabilities)

	// Extract authorization flags
	isAuthorized := (trustline.Flags & uint32(xdr.TrustLineFlagsAuthorizedFlag)) != 0
	isAuthorizedToMaintainLiabilities := (trustline.Flags & uint32(xdr.TrustLineFlagsAuthorizedToMaintainLiabilitiesFlag)) != 0

	return &graphql1.TrustlineBalance{
		TokenID:                           tokenID,
		Balance:                           balanceStr,
		TokenType:                         graphql1.TokenTypeClassic,
		Code:                              trustline.Code,
		Issuer:                            trustline.Issuer,
		Type:                              assetType,
		Limit:                             limitStr,
		BuyingLiabilities:                 buyingLiabilities,
		SellingLiabilities:                sellingLiabilities,
		LastModifiedLedger:                trustline.LedgerNumber,
		IsAuthorized:                      isAuthorized,
		IsAuthorizedToMaintainLiabilities: isAuthorizedToMaintainLiabilities,
	}, nil
}

// parseAccountBalances parses DB data for a single account and returns balances.
// Native XLM, trustlines, and SAC balances all come from DB.
// This is used by the multi-account balance resolver.
func parseAccountBalances(info *accountKeyInfo, networkPassphrase string) ([]graphql1.Balance, error) {
	var balances []graphql1.Balance

	// Add native balance from DB
	if info.nativeBalance != nil {
		nativeBalance, err := buildNativeBalanceFromDB(info.nativeBalance, networkPassphrase)
		if err != nil {
			return nil, fmt.Errorf("building native balance: %w", err)
		}
		balances = append(balances, nativeBalance)
	}

	// Add trustline balances from DB
	for _, trustline := range info.trustlines {
		trustlineBalance, err := buildTrustlineBalanceFromDB(trustline, networkPassphrase)
		if err != nil {
			return nil, fmt.Errorf("building trustline balance: %w", err)
		}
		balances = append(balances, trustlineBalance)
	}

	// Add SAC balances from DB (for C addresses)
	// Contract metadata is embedded in SACBalance from JOIN with contract_tokens
	for _, sacBalance := range info.sacBalances {
		balances = append(balances, buildSACBalanceFromDB(sacBalance))
	}

	return balances, nil
}
