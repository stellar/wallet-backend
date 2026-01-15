// Package resolvers provides utility functions for parsing Stellar ledger entries into GraphQL balance types.
// These functions support both single-account and multi-account balance queries.
package resolvers

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"

	"github.com/alitto/pond/v2"
	"github.com/stellar/go-stellar-sdk/amount"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/entities"
	graphql1 "github.com/stellar/wallet-backend/internal/serve/graphql/generated"
	"github.com/stellar/wallet-backend/internal/services"
)

// accountKeyInfo tracks ledger key information for a single account during multi-account balance fetch
type accountKeyInfo struct {
	address          string
	isContract       bool
	nativeBalance    *data.NativeBalance     // Native XLM balance from DB
	trustlines       []data.TrustlineBalance // Full trustline balance data from DB
	contractsByID    map[string]*data.Contract
	sep41ContractIDs []string
	ledgerKeys       []string // base64 XDR keys for this account (for SAC contracts only)
	collectionErr    error    // error during data collection phase
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

	return &graphql1.NativeBalance{
		TokenID:   tokenID,
		Balance:   balanceStr,
		TokenType: graphql1.TokenTypeNative,
	}, nil
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
		LastModifiedLedger:                int32(trustline.LedgerNumber),
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

// contractIDToHash converts a contract ID string to an xdr.ContractId.
func contractIDToHash(contractID string) (*xdr.ContractId, error) {
	idBytes := [32]byte{}
	rawBytes, err := hex.DecodeString(contractID)
	if err != nil {
		return nil, fmt.Errorf("invalid contract id (%s): %w", contractID, err)
	}
	if copy(idBytes[:], rawBytes[:]) != 32 {
		return nil, fmt.Errorf("couldn't copy 32 bytes to contract hash: %w", err)
	}

	hash := xdr.ContractId(idBytes)
	return &hash, nil
}

// addressToScVal converts a Stellar address string (G... account or C... contract) to an xdr.ScVal.
// This is used for passing address arguments to contract function calls.
func addressToScVal(address string) (xdr.ScVal, error) {
	scAddress := xdr.ScAddress{}

	switch address[0] {
	case 'C':
		scAddress.Type = xdr.ScAddressTypeScAddressTypeContract
		contractHash := strkey.MustDecode(strkey.VersionByteContract, address)
		contractID, err := contractIDToHash(hex.EncodeToString(contractHash))
		if err != nil {
			return xdr.ScVal{}, fmt.Errorf("address is not a valid contract: %w", err)
		}
		scAddress.ContractId = contractID

	case 'G':
		scAddress.Type = xdr.ScAddressTypeScAddressTypeAccount
		scAddress.AccountId = xdr.MustAddressPtr(address)
	case 'M':
		acct, err := strkey.DecodeMuxedAccount(address)
		if err != nil {
			return xdr.ScVal{}, fmt.Errorf("address is not a valid muxed account: %w", err)
		}
		scAddress.Type = xdr.ScAddressTypeScAddressTypeMuxedAccount
		scAddress.MuxedAccount = &xdr.MuxedEd25519Account{
			Id:      xdr.Uint64(acct.ID()),
			Ed25519: acct.Ed25519(),
		}
	case 'L':
		scAddress.Type = xdr.ScAddressTypeScAddressTypeLiquidityPool
		scAddress.LiquidityPoolId = &xdr.PoolId{}
		copy((*scAddress.LiquidityPoolId)[:], strkey.MustDecode(strkey.VersionByteLiquidityPool, address))
	case 'B':
		scAddress.Type = xdr.ScAddressTypeScAddressTypeClaimableBalance
		var someCb xdr.ClaimableBalanceId
		err := someCb.DecodeFromStrkey(address)
		if err != nil {
			return xdr.ScVal{}, fmt.Errorf("error in decoding claimable balance id from strkey: %w", err)
		}
		scAddress.ClaimableBalanceId = &someCb
	default:
		return xdr.ScVal{}, fmt.Errorf("unsupported address: %s", address)
	}

	return xdr.ScVal{
		Type:    xdr.ScValTypeScvAddress,
		Address: &scAddress,
	}, nil
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
func parseSEP41Balance(val xdr.ScVal, contractIDStr string, contract *data.Contract) (*graphql1.SEP41Balance, error) {
	if val.Type != xdr.ScValTypeScvI128 {
		return nil, fmt.Errorf("SEP-41 balance must be i128, got: %v", val.Type)
	}

	i128Parts := val.MustI128()
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

// getSep41Balances simulates an RPC call to the `balance(id)` function of each SEP-41 contract.
// The accountAddress parameter is the address of the account whose balance we're querying.
func getSep41Balances(ctx context.Context, accountAddress string, contractMetadataService services.ContractMetadataService, contractIDs []string, contractsByContractID map[string]*data.Contract, pool pond.Pool) ([]graphql1.Balance, error) {
	results := make([]graphql1.Balance, len(contractIDs))
	group := pool.NewGroupContext(ctx)
	var errs []error
	mu := sync.Mutex{}

	appendError := func(err error) {
		mu.Lock()
		errs = append(errs, err)
		mu.Unlock()
	}

	for i, contractID := range contractIDs {
		group.Submit(func() {
			// Convert the account address to an xdr.ScVal for passing to the balance function
			addressArg, err := addressToScVal(accountAddress)
			if err != nil {
				appendError(fmt.Errorf("converting account address to ScVal: %w", err))
				return
			}

			balanceResult, err := contractMetadataService.FetchSingleField(ctx, contractID, "balance", addressArg)
			if err != nil {
				appendError(fmt.Errorf("getting SEP41 balance for contract %s: %w", contractID, err))
				return
			}
			balance, err := parseSEP41Balance(balanceResult, contractID, contractsByContractID[contractID])
			if err != nil {
				appendError(fmt.Errorf("parsing SEP41 balance for contract %s: %w", contractID, err))
				return
			}
			results[i] = balance
		})
	}

	if err := group.Wait(); err != nil {
		return nil, fmt.Errorf("waiting for SEP41 balance fetch group: %w", err)
	}

	if len(errs) > 0 {
		return nil, fmt.Errorf("getting SEP41 balances: %w", errors.Join(errs...))
	}

	return results, nil
}

// parseAccountBalances parses ledger entries and DB data for a single account and returns balances.
// Native XLM and trustlines come from DB, while SAC contracts use RPC.
// This is used by the multi-account balance resolver.
func parseAccountBalances(ctx context.Context, info *accountKeyInfo, ledgerEntriesByLedgerKeys map[string]*entities.LedgerEntryResult, contractMetadataService services.ContractMetadataService, networkPassphrase string, pool pond.Pool) ([]graphql1.Balance, error) {
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

	// Parse RPC ledger entries (SAC contracts only)
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

			balance, err := parseSACBalance(&contractDataEntry, contractIDStr, contract)
			if err != nil {
				return nil, err
			}
			if balance != nil {
				balances = append(balances, balance)
			}
		}
	}

	if len(info.sep41ContractIDs) > 0 {
		sep41Balances, err := getSep41Balances(ctx, info.address, contractMetadataService, info.sep41ContractIDs, info.contractsByID, pool)
		if err != nil {
			return nil, fmt.Errorf("getting SEP41 balances: %w", err)
		}
		balances = append(balances, sep41Balances...)
	}

	return balances, nil
}
