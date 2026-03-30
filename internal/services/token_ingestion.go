// Account token caching service - manages PostgreSQL storage of account token holdings
// including both classic Stellar trustlines and Stellar Asset Contract (SAC) balances.
package services

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/stellar/go-stellar-sdk/support/log"

	wbdata "github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer"
	"github.com/stellar/wallet-backend/internal/indexer/types"
)

// TokenIngestionService provides write access to account token storage during live ingestion.
type TokenIngestionService interface {
	// ProcessTokenChanges applies trustline and SAC balance changes to PostgreSQL.
	// This is called by the indexer for each ledger's state changes during live ingestion.
	ProcessTokenChanges(ctx context.Context, dbTx pgx.Tx, trustlineChangesByTrustlineKey map[indexer.TrustlineChangeKey]types.TrustlineChange, accountChangesByAccountID map[string]types.AccountChange, sacBalanceChangesByKey map[indexer.SACBalanceChangeKey]types.SACBalanceChange) error
}

// Verify interface compliance at compile time
var _ TokenIngestionService = (*tokenIngestionService)(nil)

// TokenIngestionServiceConfig holds configuration for creating a TokenIngestionService.
type TokenIngestionServiceConfig struct {
	TrustlineBalanceModel wbdata.TrustlineBalanceModelInterface
	NativeBalanceModel    wbdata.NativeBalanceModelInterface
	SACBalanceModel       wbdata.SACBalanceModelInterface
	NetworkPassphrase     string
}

// tokenIngestionService implements TokenIngestionService.
type tokenIngestionService struct {
	trustlineBalanceModel wbdata.TrustlineBalanceModelInterface
	nativeBalanceModel    wbdata.NativeBalanceModelInterface
	sacBalanceModel       wbdata.SACBalanceModelInterface
	networkPassphrase     string
}

// NewTokenIngestionService creates a TokenIngestionService for ingestion.
func NewTokenIngestionService(cfg TokenIngestionServiceConfig) *tokenIngestionService {
	return &tokenIngestionService{
		trustlineBalanceModel: cfg.TrustlineBalanceModel,
		nativeBalanceModel:    cfg.NativeBalanceModel,
		sacBalanceModel:       cfg.SACBalanceModel,
		networkPassphrase:     cfg.NetworkPassphrase,
	}
}

// ProcessTokenChanges processes token changes and stores them in PostgreSQL.
// This is called by the indexer for each ledger's state changes during live ingestion.
func (s *tokenIngestionService) ProcessTokenChanges(ctx context.Context, dbTx pgx.Tx, trustlineChangesByTrustlineKey map[indexer.TrustlineChangeKey]types.TrustlineChange, accountChangesByAccountID map[string]types.AccountChange, sacBalanceChangesByKey map[indexer.SACBalanceChangeKey]types.SACBalanceChange) error {
	if len(trustlineChangesByTrustlineKey) == 0 && len(accountChangesByAccountID) == 0 && len(sacBalanceChangesByKey) == 0 {
		return nil
	}

	if err := s.processTrustlineChanges(ctx, dbTx, trustlineChangesByTrustlineKey); err != nil {
		return err
	}
	if err := s.processNativeBalanceChanges(ctx, dbTx, accountChangesByAccountID); err != nil {
		return err
	}
	if err := s.processSACBalanceChanges(ctx, dbTx, sacBalanceChangesByKey); err != nil {
		return err
	}
	return nil
}

// processTrustlineChanges handles trustline balance upserts and deletes.
func (s *tokenIngestionService) processTrustlineChanges(ctx context.Context, dbTx pgx.Tx, changesByKey map[indexer.TrustlineChangeKey]types.TrustlineChange) error {
	if len(changesByKey) == 0 {
		return nil
	}

	var upserts []wbdata.TrustlineBalance
	var deletes []wbdata.TrustlineBalance
	for key, change := range changesByKey {
		fullData := wbdata.TrustlineBalance{
			AccountAddress:     change.AccountID,
			AssetID:            key.TrustlineID,
			Balance:            change.Balance,
			Limit:              change.Limit,
			BuyingLiabilities:  change.BuyingLiabilities,
			SellingLiabilities: change.SellingLiabilities,
			Flags:              change.Flags,
			LedgerNumber:       change.LedgerNumber,
		}
		if change.Operation == types.TrustlineOpRemove {
			deletes = append(deletes, fullData)
		} else {
			upserts = append(upserts, fullData)
		}
	}

	if len(upserts) > 0 || len(deletes) > 0 {
		if err := s.trustlineBalanceModel.BatchUpsert(ctx, dbTx, upserts, deletes); err != nil {
			return fmt.Errorf("upserting trustline balances: %w", err)
		}
	}
	log.Ctx(ctx).Infof("upserted %d trustlines, deleted %d trustlines", len(upserts), len(deletes))
	return nil
}

// processNativeBalanceChanges handles native XLM balance upserts and deletes.
func (s *tokenIngestionService) processNativeBalanceChanges(ctx context.Context, dbTx pgx.Tx, changesByAccountID map[string]types.AccountChange) error {
	if len(changesByAccountID) == 0 {
		return nil
	}

	var upserts []wbdata.NativeBalance
	var deletes []string
	for _, change := range changesByAccountID {
		if change.Operation == types.AccountOpRemove {
			deletes = append(deletes, change.AccountID)
		} else {
			upserts = append(upserts, wbdata.NativeBalance{
				AccountAddress:     change.AccountID,
				Balance:            change.Balance,
				MinimumBalance:     change.MinimumBalance,
				BuyingLiabilities:  change.BuyingLiabilities,
				SellingLiabilities: change.SellingLiabilities,
				LedgerNumber:       change.LedgerNumber,
			})
		}
	}

	if len(upserts) > 0 || len(deletes) > 0 {
		if err := s.nativeBalanceModel.BatchUpsert(ctx, dbTx, upserts, deletes); err != nil {
			return fmt.Errorf("upserting native balances: %w", err)
		}
		log.Ctx(ctx).Infof("upserted %d native balances, deleted %d native balances", len(upserts), len(deletes))
	}
	return nil
}

// processSACBalanceChanges handles SAC balance upserts and deletes for contract addresses.
func (s *tokenIngestionService) processSACBalanceChanges(ctx context.Context, dbTx pgx.Tx, changesByKey map[indexer.SACBalanceChangeKey]types.SACBalanceChange) error {
	if len(changesByKey) == 0 {
		return nil
	}

	var upserts []wbdata.SACBalance
	var deletes []wbdata.SACBalance
	for _, change := range changesByKey {
		contractID := wbdata.DeterministicContractID(change.ContractID)
		sacBal := wbdata.SACBalance{
			AccountAddress:    change.AccountID,
			ContractID:        contractID,
			Balance:           change.Balance,
			IsAuthorized:      change.IsAuthorized,
			IsClawbackEnabled: change.IsClawbackEnabled,
			LedgerNumber:      change.LedgerNumber,
		}
		if change.Operation == types.SACBalanceOpRemove {
			deletes = append(deletes, sacBal)
		} else {
			upserts = append(upserts, sacBal)
		}
	}

	if len(upserts) > 0 || len(deletes) > 0 {
		if err := s.sacBalanceModel.BatchUpsert(ctx, dbTx, upserts, deletes); err != nil {
			return fmt.Errorf("upserting SAC balances: %w", err)
		}
		log.Ctx(ctx).Infof("upserted %d SAC balances, deleted %d SAC balances", len(upserts), len(deletes))
	}
	return nil
}
