// Account token caching service - manages PostgreSQL storage of account token holdings
// including both classic Stellar trustlines and Stellar Asset Contract (SAC) balances.
package services

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stellar/go-stellar-sdk/support/log"

	wbdata "github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer"
	"github.com/stellar/wallet-backend/internal/indexer/types"
)

// TokenIngestionService provides write access to account token storage during live ingestion.
type TokenIngestionService interface {
	// ProcessTokenChanges applies trustline, contract, and SAC balance changes to PostgreSQL.
	// This is called by the indexer for each ledger's state changes during live ingestion.
	ProcessTokenChanges(ctx context.Context, dbTx pgx.Tx, trustlineChangesByTrustlineKey map[indexer.TrustlineChangeKey]types.TrustlineChange, contractChanges []types.ContractChange, accountChangesByAccountID map[string]types.AccountChange, sacBalanceChangesByKey map[indexer.SACBalanceChangeKey]types.SACBalanceChange) error
}

// Verify interface compliance at compile time
var _ TokenIngestionService = (*tokenIngestionService)(nil)

// TokenIngestionServiceConfig holds configuration for creating a TokenIngestionService.
type TokenIngestionServiceConfig struct {
	TrustlineBalanceModel      wbdata.TrustlineBalanceModelInterface
	NativeBalanceModel         wbdata.NativeBalanceModelInterface
	SACBalanceModel            wbdata.SACBalanceModelInterface
	AccountContractTokensModel wbdata.AccountContractTokensModelInterface
	NetworkPassphrase          string
}

// tokenIngestionService implements TokenIngestionService.
type tokenIngestionService struct {
	trustlineBalanceModel      wbdata.TrustlineBalanceModelInterface
	nativeBalanceModel         wbdata.NativeBalanceModelInterface
	sacBalanceModel            wbdata.SACBalanceModelInterface
	accountContractTokensModel wbdata.AccountContractTokensModelInterface
	networkPassphrase          string
}

// NewTokenIngestionService creates a TokenIngestionService for ingestion.
func NewTokenIngestionService(cfg TokenIngestionServiceConfig) *tokenIngestionService {
	return &tokenIngestionService{
		trustlineBalanceModel:      cfg.TrustlineBalanceModel,
		nativeBalanceModel:         cfg.NativeBalanceModel,
		sacBalanceModel:            cfg.SACBalanceModel,
		accountContractTokensModel: cfg.AccountContractTokensModel,
		networkPassphrase:          cfg.NetworkPassphrase,
	}
}

// ProcessTokenChanges processes token changes and stores them in PostgreSQL.
// This is called by the indexer for each ledger's state changes during live ingestion.
func (s *tokenIngestionService) ProcessTokenChanges(ctx context.Context, dbTx pgx.Tx, trustlineChangesByTrustlineKey map[indexer.TrustlineChangeKey]types.TrustlineChange, contractChanges []types.ContractChange, accountChangesByAccountID map[string]types.AccountChange, sacBalanceChangesByKey map[indexer.SACBalanceChangeKey]types.SACBalanceChange) error {
	if len(trustlineChangesByTrustlineKey) == 0 && len(contractChanges) == 0 && len(accountChangesByAccountID) == 0 && len(sacBalanceChangesByKey) == 0 {
		return nil
	}

	if err := s.processTrustlineChanges(ctx, dbTx, trustlineChangesByTrustlineKey); err != nil {
		return err
	}
	if err := s.processContractTokenChanges(ctx, dbTx, contractChanges); err != nil {
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
			AccountID:          types.AddressBytea(change.AccountID),
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

// processContractTokenChanges handles SEP-41 contract token inserts.
func (s *tokenIngestionService) processContractTokenChanges(ctx context.Context, dbTx pgx.Tx, changes []types.ContractChange) error {
	if len(changes) == 0 {
		return nil
	}

	contractTokensByAccount := make(map[string][]uuid.UUID)
	for _, change := range changes {
		if change.ContractID == "" {
			continue
		}
		if change.ContractType != types.ContractTypeSEP41 {
			continue
		}
		contractID := wbdata.DeterministicContractID(change.ContractID)
		contractTokensByAccount[change.AccountID] = append(contractTokensByAccount[change.AccountID], contractID)
	}

	if len(contractTokensByAccount) > 0 {
		if err := s.accountContractTokensModel.BatchInsert(ctx, dbTx, contractTokensByAccount); err != nil {
			return fmt.Errorf("batch inserting contract tokens: %w", err)
		}
		log.Ctx(ctx).Infof("inserted %d account-contract SEP41 relationships", len(contractTokensByAccount))
	}
	return nil
}

// processNativeBalanceChanges handles native XLM balance upserts and deletes.
func (s *tokenIngestionService) processNativeBalanceChanges(ctx context.Context, dbTx pgx.Tx, changesByAccountID map[string]types.AccountChange) error {
	if len(changesByAccountID) == 0 {
		return nil
	}

	var upserts []wbdata.NativeBalance
	var deletes []types.AddressBytea
	for _, change := range changesByAccountID {
		if change.Operation == types.AccountOpRemove {
			deletes = append(deletes, types.AddressBytea(change.AccountID))
		} else {
			upserts = append(upserts, wbdata.NativeBalance{
				AccountID:          types.AddressBytea(change.AccountID),
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
			AccountID:         types.AddressBytea(change.AccountID),
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
