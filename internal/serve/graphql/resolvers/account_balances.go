package resolvers

import (
	"context"

	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/vektah/gqlparser/v2/gqlerror"

	"github.com/stellar/wallet-backend/internal/data"
	graphql1 "github.com/stellar/wallet-backend/internal/serve/graphql/generated"
	"github.com/stellar/wallet-backend/internal/utils"
)

func (r *Resolver) getAccountBalances(ctx context.Context, address string) ([]graphql1.Balance, error) {
	internalErr := func() error {
		return &gqlerror.Error{
			Message: ErrMsgBalancesFetchFailed,
			Extensions: map[string]interface{}{
				"code": "INTERNAL_ERROR",
			},
		}
	}

	networkPassphrase := r.rpcService.NetworkPassphrase()
	var balances []graphql1.Balance

	if utils.IsContractAddress(address) {
		sacBalances, err := r.balanceReader.GetSACBalances(ctx, address)
		if err != nil {
			log.Ctx(ctx).Errorf("failed to get SAC balances for %s: %v", address, err)
			return nil, internalErr()
		}
		for _, sacBalance := range sacBalances {
			balances = append(balances, buildSACBalanceFromDB(sacBalance))
		}
	} else {
		nativeBalance, err := r.balanceReader.GetNativeBalance(ctx, address)
		if err != nil {
			log.Ctx(ctx).Errorf("failed to get native balance for %s: %v", address, err)
			return nil, internalErr()
		}
		if nativeBalance != nil {
			nativeBalanceResult, err := buildNativeBalanceFromDB(nativeBalance, networkPassphrase)
			if err != nil {
				return nil, internalErr()
			}
			balances = append(balances, nativeBalanceResult)
		}

		trustlines, err := r.balanceReader.GetTrustlineBalances(ctx, address)
		if err != nil {
			log.Ctx(ctx).Errorf("failed to get trustline balances for %s: %v", address, err)
			return nil, internalErr()
		}
		for _, trustline := range trustlines {
			trustlineBalance, err := buildTrustlineBalanceFromDB(trustline, networkPassphrase)
			if err != nil {
				return nil, internalErr()
			}
			balances = append(balances, trustlineBalance)
		}
	}

	contractTokens, err := r.accountContractTokensModel.GetByAccount(ctx, address)
	if err != nil {
		log.Ctx(ctx).Errorf("failed to get contract tokens for %s: %v", address, err)
		return nil, internalErr()
	}

	contractsByContractID := make(map[string]*data.Contract)
	sep41TokenIDs := make([]string, 0)
	for _, contract := range contractTokens {
		if contract.Type == "SEP41" {
			sep41TokenIDs = append(sep41TokenIDs, contract.ContractID)
			contractsByContractID[contract.ContractID] = contract
		}
	}

	if len(sep41TokenIDs) == 0 {
		return balances, nil
	}

	sep41Balances, err := getSep41Balances(ctx, address, r.contractMetadataService, sep41TokenIDs, contractsByContractID, r.pool)
	if err != nil {
		return nil, internalErr()
	}
	balances = append(balances, sep41Balances...)

	return balances, nil
}
